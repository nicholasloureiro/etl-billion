"""ETL: generate synthetic vaccination rows -> insert directly into ClickHouse.

Designed for Railway deploy. Reuses the vectorized generator from
generate_synthetic_to_s3.py — each worker process builds a shard pyarrow Table
and inserts it via clickhouse_connect's Arrow path. No S3 intermediate.

Config (env vars; CLI flags override):
    CLICKHOUSE_HOST / CLICKHOUSE_PORT / CLICKHOUSE_USER /
    CLICKHOUSE_PASSWORD / CLICKHOUSE_DATABASE / CLICKHOUSE_SECURE
    TOTAL_ROWS    (default 1_000_000_000)
    MONTHS        (CSV of YYYYMM, default 202501..202512)
    SHARD_ROWS    (default 500_000 — sized for ~1 GB peak RSS per worker)
    WORKERS       (default 2)
    SEED          (default 42)
    SKIP_SCHEMA   (set non-empty to skip CREATE DATABASE/TABLE)
    INSERT_RETRIES (default 3)
"""
import argparse
import json
import logging
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from config import ClickHouseConfig, load_config
from database import ClickHouseConnection, ClickHouseRepository
from generate_synthetic_to_s3 import build_month_table

logger = logging.getLogger(__name__)

DEFAULT_TOTAL_ROWS = 1_000_000_000
DEFAULT_MONTHS = list(range(202501, 202513))
DEFAULT_SHARD_ROWS = 500_000
DEFAULT_WORKERS = 2
DEFAULT_INSERT_RETRIES = 3
DISTRIBUTIONS_PATH = os.path.join(os.path.dirname(__file__), "distributions.json")

CH_TABLE = "vacinas.events"

# Populated once per worker via _init_worker.
_DISTRIBUTIONS: dict | None = None
_CH_CLIENT: Any = None
_INSERT_RETRIES = DEFAULT_INSERT_RETRIES


def _init_worker(distributions_dict: dict, ch_cfg: dict, insert_retries: int) -> None:
    """Per-worker setup: cache distributions + open a fresh CH client."""
    global _DISTRIBUTIONS, _CH_CLIENT, _INSERT_RETRIES
    import clickhouse_connect

    _DISTRIBUTIONS = distributions_dict
    _INSERT_RETRIES = insert_retries
    _CH_CLIENT = clickhouse_connect.get_client(
        host=ch_cfg["host"],
        port=ch_cfg["port"],
        username=ch_cfg["user"],
        password=ch_cfg["password"],
        database=ch_cfg["database"],
        secure=ch_cfg["secure"],
    )


def _shard_seed(base_seed: int, year: int, month: int, shard_idx: int) -> int:
    return ((base_seed * 31 + year) * 13 + month) * 1000 + shard_idx


def _align_for_clickhouse(table: pa.Table) -> pa.Table:
    """Cast columns so the Arrow table matches the vacinas.events schema.

    Synthetic generator output differs in two places vs the CH DDL:
      - codigo_dose_vacina:    int8  -> String
      - numero_idade_paciente: int32 -> UInt16
    The all-NULL nullable-string columns are emitted as pa.null() type and
    must be re-typed to string for CH's Nullable(String) target.
    LowCardinality(String) target columns are auto-cast from plain string by CH.
    """
    idx = table.schema.get_field_index("codigo_dose_vacina")
    if idx >= 0 and table.schema.field(idx).type != pa.string():
        table = table.set_column(idx, "codigo_dose_vacina",
                                 pc.cast(table.column(idx), pa.string()))

    idx = table.schema.get_field_index("numero_idade_paciente")
    if idx >= 0 and table.schema.field(idx).type != pa.uint16():
        table = table.set_column(idx, "numero_idade_paciente",
                                 pc.cast(table.column(idx), pa.uint16()))

    null_typed = [f.name for f in table.schema if pa.types.is_null(f.type)]
    for name in null_typed:
        idx = table.schema.get_field_index(name)
        table = table.set_column(
            idx, name,
            pa.array([None] * table.num_rows, type=pa.string()),
        )

    return table


def _generate_and_insert_shard(
    year: int, month: int, shard_idx: int, n_rows: int, base_seed: int
) -> tuple[int, float, float]:
    """Build one shard and insert into ClickHouse. (rows, build_s, insert_s)."""
    assert _DISTRIBUTIONS is not None and _CH_CLIENT is not None

    t0 = time.time()
    rng = np.random.default_rng(_shard_seed(base_seed, year, month, shard_idx))
    table = build_month_table(year, month, n_rows, _DISTRIBUTIONS, rng)
    table = _align_for_clickhouse(table)
    build_s = time.time() - t0

    db, _, tbl = CH_TABLE.partition(".")
    last_err: Exception | None = None
    t1 = time.time()
    for attempt in range(1, _INSERT_RETRIES + 1):
        try:
            _CH_CLIENT.insert_arrow(tbl, table, database=db)
            return n_rows, build_s, time.time() - t1
        except Exception as e:
            last_err = e
            logger.warning(
                f"insert attempt {attempt}/{_INSERT_RETRIES} for "
                f"{year:04d}-{month:02d}#{shard_idx:03d} failed: {e}"
            )
            time.sleep(min(2 ** attempt, 30))
    raise RuntimeError(f"insert failed after {_INSERT_RETRIES} attempts: {last_err}")


def ensure_schema(cfg: ClickHouseConfig) -> None:
    """Create vacinas database + events table if missing."""
    conn = ClickHouseConnection(cfg)
    try:
        if not conn.health_check():
            raise RuntimeError(f"ClickHouse health check failed at {cfg.host}:{cfg.port}")
        client = conn.get_client()
        has_table = int(client.command(
            "SELECT count() FROM system.tables WHERE database='vacinas' AND name='events'"
        ))
        if has_table:
            logger.info("vacinas.events already exists")
            return
        logger.info("Creating vacinas.events schema...")
        ClickHouseRepository(conn).create_schema()
    finally:
        conn.close()
        ClickHouseConnection.reset_instance()


def _env_int(name: str, default: int) -> int:
    v = os.getenv(name)
    return int(v) if v else default


def _env_months() -> list[int] | None:
    v = os.getenv("MONTHS")
    if not v:
        return None
    return [int(m) for m in v.split(",") if m.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--total-rows", type=int,
                        default=_env_int("TOTAL_ROWS", DEFAULT_TOTAL_ROWS))
    parser.add_argument("--months", nargs="+", type=int,
                        default=_env_months() or DEFAULT_MONTHS)
    parser.add_argument("--shard-rows", type=int,
                        default=_env_int("SHARD_ROWS", DEFAULT_SHARD_ROWS))
    parser.add_argument("--workers", type=int,
                        default=_env_int("WORKERS", DEFAULT_WORKERS))
    parser.add_argument("--seed", type=int,
                        default=_env_int("SEED", 42))
    parser.add_argument("--insert-retries", type=int,
                        default=_env_int("INSERT_RETRIES", DEFAULT_INSERT_RETRIES))
    parser.add_argument("--distributions", default=DISTRIBUTIONS_PATH)
    parser.add_argument("--skip-schema", action="store_true",
                        default=bool(os.getenv("SKIP_SCHEMA")))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s",
    )

    cfg = load_config()
    logger.info(
        f"ClickHouse target: {cfg.clickhouse.host}:{cfg.clickhouse.port} "
        f"db={cfg.clickhouse.database} secure={cfg.clickhouse.secure}"
    )

    if not args.skip_schema:
        ensure_schema(cfg.clickhouse)

    with open(args.distributions) as f:
        distributions = json.load(f)

    rows_per_month = args.total_rows // len(args.months)
    tasks: list[tuple[int, int, int, int, int]] = []
    for month in args.months:
        year, mon = divmod(month, 100)
        n_shards = (rows_per_month + args.shard_rows - 1) // args.shard_rows
        for shard_idx in range(n_shards):
            start = shard_idx * args.shard_rows
            n_rows = min(args.shard_rows, rows_per_month - start)
            if n_rows <= 0:
                break
            tasks.append((year, mon, shard_idx, n_rows, args.seed))

    total_target = sum(t[3] for t in tasks)
    logger.info(
        f"Plan: {total_target:,} rows across {len(args.months)} months "
        f"({rows_per_month:,}/month), {len(tasks)} shards, "
        f"shard_rows={args.shard_rows:,}, workers={args.workers}"
    )

    ch_cfg_dict = {
        "host": cfg.clickhouse.host,
        "port": cfg.clickhouse.port,
        "user": cfg.clickhouse.user,
        "password": cfg.clickhouse.password,
        "database": cfg.clickhouse.database,
        "secure": cfg.clickhouse.secure,
    }

    total_rows = 0
    completed = 0
    t_start = time.time()

    with ProcessPoolExecutor(
        max_workers=args.workers,
        initializer=_init_worker,
        initargs=(distributions, ch_cfg_dict, args.insert_retries),
    ) as pool:
        futures = {pool.submit(_generate_and_insert_shard, *t): t for t in tasks}
        for fut in as_completed(futures):
            year, mon, shard_idx, *_ = futures[fut]
            rows, build_s, insert_s = fut.result()
            total_rows += rows
            completed += 1
            elapsed = time.time() - t_start
            rate = total_rows / max(elapsed, 1e-6)
            eta_s = (total_target - total_rows) / max(rate, 1e-6)
            logger.info(
                f"[{completed}/{len(tasks)}] {year:04d}-{mon:02d}#{shard_idx:03d}: "
                f"{rows:,} rows (build {build_s:.1f}s + insert {insert_s:.1f}s) | "
                f"total {total_rows:,} | {rate:,.0f} rows/s | ETA {eta_s/60:.1f} min"
            )

    wall = time.time() - t_start
    logger.info(
        f"Done. {total_rows:,} rows in {wall/60:.1f} min "
        f"({total_rows / max(wall, 1e-6):,.0f} rows/s overall)."
    )


if __name__ == "__main__":
    try:
        main()
    except Exception:
        logger.exception("ETL failed")
        sys.exit(1)
