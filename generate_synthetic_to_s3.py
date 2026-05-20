"""Vectorized synthetic vaccination data generator -> Parquet -> S3.

Reads weighted distributions from distributions.json (real Q1 2025 frequencies)
and produces statistically faithful records via NumPy bulk sampling. Writes
ZSTD-compressed Parquet directly to B2 with a sharded hive layout:

    s3://{bucket}/{prefix}/year_month=YYYY-MM/data-NN.parquet

Defaults to all 12 months of 2025, ~8.4M rows/month (~100M total). The existing
real Q1 2025 export at year_month=YYYY-MM/data.parquet coexists with the
synthetic shards — different filenames in the same partition, readers glob both.

Each month is split into shards of ~DEFAULT_SHARD_ROWS rows, and shards run in
parallel across DEFAULT_WORKERS processes.

Required env vars (same as export_to_s3.py):
    INGESTION_ENDPOINT_URL, INGESTION_BUCKET,
    INGESTION_AWS_ACCESS_KEY_ID, INGESTION_AWS_SECRET_ACCESS_KEY
"""
import argparse
import json
import logging
import os
import sys
import time
from calendar import monthrange
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date
from io import BytesIO
from typing import Sequence

import boto3
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

DEFAULT_MONTHS = list(range(202501, 202513))   # all of 2025
DEFAULT_ROWS_PER_MONTH = 8_400_000             # 12 * 8.4M ≈ 100.8M total
DEFAULT_SHARD_ROWS = 1_500_000
DEFAULT_WORKERS = min(8, os.cpu_count() or 4)
DISTRIBUTIONS_PATH = os.path.join(os.path.dirname(__file__), "distributions.json")

# Backblaze B2 us-east-005 region; harmless for other endpoints.
B2_REGION = "us-east-005"

# ----- Static distributions (lifted from synthetic_generator.py) -----------

SEX_VALUES = np.array(["F", "M", "I"])
SEX_PROBS = np.array([5168, 4831, 1], dtype=np.float64)
SEX_PROBS /= SEX_PROBS.sum()

NATUREZA_COD = np.array(["1", "2", "3", "4"])
NATUREZA_DESC = np.array([
    "ADMINISTRACAO PUBLICA",
    "ENTIDADES EMPRESARIAIS",
    "ENTIDADES SEM FINS LUCRATIVOS",
    "PESSOAS FISICAS",
])
NATUREZA_PROBS = np.array([9466, 431, 102, 1], dtype=np.float64)
NATUREZA_PROBS /= NATUREZA_PROBS.sum()

ESTRATEGIA_COD = np.array(["1", "2", "8", "", "3", "4", "5", "6", "7", "9"])
ESTRATEGIA_DESC = np.array([
    "Rotina", "Especial", "Serviço Privado", "", "Pós-exposição",
    "Campanha indiscriminada", "Intensificação", "Campanha seletiva",
    "Monitoramento rápido de cobertura vacinal", "Pesquisa",
])
ESTRATEGIA_PROBS = np.array([8948, 378, 377, 113, 83, 32, 20, 13, 11, 10], dtype=np.float64)
ESTRATEGIA_PROBS /= ESTRATEGIA_PROBS.sum()

VIA_COD = np.array(["0", "10916", "10915", "10918", "10917"])
VIA_DESC = np.array([
    "Sem registro no sistema de informação de origem",
    "Subcutânea", "Intramuscular", "Oral", "Intradérmica",
])
VIA_PROBS = np.array([60, 15, 15, 8, 2], dtype=np.float64)
VIA_PROBS /= VIA_PROBS.sum()

LOCAL_COD = np.array(["0", "14", "15", "2", "1", "7", "8", "17"])
LOCAL_DESC = np.array([
    "Sem registro no sistema de informação de origem",
    "Face Externa Superior do Braço Direito",
    "Face Externa Superior do Braço Esquerdo",
    "Deltóide Esquerdo", "Deltóide Direito",
    "Vasto Lateral da Coxa Direita",
    "Vasto Lateral da Coxa Esquerda", "Glúteo",
])
LOCAL_PROBS = np.array([50, 12, 12, 8, 8, 4, 4, 2], dtype=np.float64)
LOCAL_PROBS /= LOCAL_PROBS.sum()

SISTEMA_COD = np.array(["18602", "29376", "39474", "18822", "36307", "38976"])
SISTEMA_DESC = np.array([
    "ESUS APS - NACIONAL (OFFLINE)",
    "SIGA Saúde",
    "Prontuário Eletrônico do Cidadão / e-SUS APS",
    "SIMUS",
    "Prontuário Eletrônico do Cidadão / e-SUS APS",
    "Snak Sistemas",
])

# ----- Helpers -------------------------------------------------------------


def _norm(weights: Sequence[int]) -> np.ndarray:
    arr = np.asarray(weights, dtype=np.float64)
    return arr / arr.sum()


def _sample(probs: np.ndarray, n: int, rng: np.random.Generator) -> np.ndarray:
    return rng.choice(len(probs), size=n, p=probs)


def _generate_dates(year: int, month: int, n: int, rng: np.random.Generator) -> np.ndarray:
    """Vectorized weekday-biased dates as numpy datetime64[D]."""
    dim = monthrange(year, month)[1]
    weekday_w = np.array(
        [0.06 if date(year, month, d + 1).weekday() >= 5 else 0.18 for d in range(dim)]
    )
    p = weekday_w / weekday_w.sum()
    offsets = rng.choice(dim, size=n, p=p)
    base = np.datetime64(f"{year:04d}-{month:02d}-01", "D")
    return base + offsets.astype("timedelta64[D]")


def _hex_ids(n: int, char_len: int, rng: np.random.Generator) -> np.ndarray:
    """n random hex strings of length char_len. Vectorized via bulk hex encoding."""
    if char_len % 2:
        raise ValueError("char_len must be even")
    raw = rng.bytes(n * (char_len // 2))
    flat = np.frombuffer(raw.hex().encode("ascii"), dtype=f"S{char_len}")
    return flat.astype(f"U{char_len}")


def _doc_ids(n: int, rng: np.random.Generator) -> np.ndarray:
    """UUID-shaped 'XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX-i0b0'. Vectorized."""
    raw = rng.bytes(n * 16)  # 16 bytes -> 32 hex chars per id
    hex_chars = np.frombuffer(raw.hex().encode("ascii"), dtype="S1").reshape(n, 32)
    out = np.empty((n, 41), dtype="S1")
    out[:, 0:8] = hex_chars[:, 0:8]
    out[:, 8] = b"-"
    out[:, 9:13] = hex_chars[:, 8:12]
    out[:, 13] = b"-"
    out[:, 14:18] = hex_chars[:, 12:16]
    out[:, 18] = b"-"
    out[:, 19:23] = hex_chars[:, 16:20]
    out[:, 23] = b"-"
    out[:, 24:36] = hex_chars[:, 20:32]
    out[:, 36:41] = np.frombuffer(b"-i0b0", dtype="S1")
    return np.frombuffer(np.ascontiguousarray(out).tobytes(), dtype="S41").astype("U41")


def _lotes(n: int, rng: np.random.Generator) -> np.ndarray:
    """Lote codes like '23ABCD456NA'. Vectorized."""
    prefixes = np.array(["23", "24", "25"])
    suffixes = np.array(["NA", "NB", "NC", ""])
    pre = prefixes[rng.integers(0, 3, size=n)]
    suf = suffixes[rng.integers(0, 4, size=n)]
    letter_bytes = rng.integers(65, 91, size=n * 4, dtype=np.uint8).tobytes()
    letters = np.frombuffer(letter_bytes, dtype="S4").astype("U4")
    nums = np.char.zfill(rng.integers(1, 1000, size=n).astype(str), 3)
    return np.char.add(np.char.add(np.char.add(pre, letters), nums), suf)


def _zfill_int(arr: np.ndarray, width: int) -> np.ndarray:
    return np.char.zfill(arr.astype(str), width)


# ----- Table builder --------------------------------------------------------


def build_month_table(
    year: int,
    month: int,
    n: int,
    distributions: dict,
    rng: np.random.Generator,
) -> pa.Table:
    t0 = time.time()
    logger.info(f"Generating {n:,} rows for {year:04d}-{month:02d}")

    # Vacina (sigla, descricao, codigo, count)
    vac = distributions["vacina"]
    vac_sigla = np.array([d[0] for d in vac])
    vac_desc = np.array([d[1] for d in vac])
    vac_cod = np.array([d[2] for d in vac])
    vac_idx = _sample(_norm([d[3] for d in vac]), n, rng)

    # Dose (codigo_str, descricao, count)
    dose = distributions["dose"]
    dose_cod_str = [d[0] for d in dose]
    dose_desc_arr = np.array([d[1] for d in dose])
    dose_idx = _sample(_norm([d[2] for d in dose]), n, rng)
    dose_cod_int = np.array(
        [int(c) if c.isdigit() and 0 <= int(c) <= 127 else None for c in dose_cod_str],
        dtype=object,
    )

    # Municipio (codigo, nome, uf, count) — drives both estab city and UF
    mun = distributions["municipio"]
    mun_cod = np.array([d[0] for d in mun])
    mun_nome = np.array([d[1] for d in mun])
    mun_uf = np.array([d[2] for d in mun])
    mun_idx = _sample(_norm([d[3] for d in mun]), n, rng)

    # UF (sigla, nome, count) — independent draw for *_paciente UF
    uf = distributions["uf"]
    uf_sigla = np.array([d[0] for d in uf])
    uf_nome = np.array([d[1] for d in uf])
    uf_idx = _sample(_norm([d[2] for d in uf]), n, rng)
    uf_sigla_to_nome = {}
    for sigla, nome, _ in uf:
        uf_sigla_to_nome.setdefault(sigla, nome)

    # Tipo estabelecimento
    tipo = distributions["tipo_estabelecimento"]
    tipo_cod = np.array([d[0] for d in tipo])
    tipo_desc = np.array([d[1] for d in tipo])
    tipo_idx = _sample(_norm([d[2] for d in tipo]), n, rng)

    # Fabricante (70% present, 30% null — matches synthetic_generator)
    fab = distributions["fabricante"]
    fab_cod = np.array([d[0] for d in fab])
    fab_desc = np.array([d[1] for d in fab])
    fab_idx = _sample(_norm([d[2] for d in fab]), n, rng)
    fab_present = rng.random(n) < 0.7
    fab_cod_col = np.where(fab_present, fab_cod[fab_idx], None)
    fab_desc_col = np.where(fab_present, fab_desc[fab_idx], "")

    # Raca
    raca = distributions["raca"]
    raca_cod = np.array([d[0] for d in raca])
    raca_nome = np.array([d[1] for d in raca])
    raca_idx = _sample(_norm([d[2] for d in raca]), n, rng)

    # Idade (uint16)
    idade = distributions["idade"]
    idade_vals = np.array([d[0] for d in idade], dtype=np.int32)
    idade_idx = _sample(_norm([d[1] for d in idade]), n, rng)

    # Hardcoded distributions
    sex_idx = rng.choice(len(SEX_VALUES), size=n, p=SEX_PROBS)
    nat_idx = rng.choice(len(NATUREZA_COD), size=n, p=NATUREZA_PROBS)
    est_idx = rng.choice(len(ESTRATEGIA_COD), size=n, p=ESTRATEGIA_PROBS)
    via_idx = rng.choice(len(VIA_COD), size=n, p=VIA_PROBS)
    loc_idx = rng.choice(len(LOCAL_COD), size=n, p=LOCAL_PROBS)
    sis_idx = rng.integers(0, len(SISTEMA_COD), size=n)

    # Dates
    dates_vacina = _generate_dates(year, month, n, rng)
    entry_offsets = rng.integers(1, 61, size=n).astype("timedelta64[D]")
    dates_entrada = dates_vacina + entry_offsets

    # IDs — patient pool of 60% so some patients get multiple doses
    n_patients = max(1, int(n * 0.6))
    pool = _hex_ids(n_patients, char_len=64, rng=rng)
    patient_ids = rng.choice(pool, size=n)
    document_ids = _doc_ids(n, rng)
    lotes = _lotes(n, rng)
    cnes = _zfill_int(rng.integers(1_000_000, 10_000_000, size=n), 7)
    cep = _zfill_int(rng.integers(10_000, 100_000, size=n), 5)

    # Resolved per-row arrays
    mun_nome_col = mun_nome[mun_idx]
    mun_uf_col = mun_uf[mun_idx]
    estab_uf_nome = np.array(
        [uf_sigla_to_nome.get(s, "") for s in mun_uf_col], dtype=object
    )
    razao = np.char.add("ESTABELECIMENTO DE SAUDE ", mun_nome_col)
    fantasia = np.char.add("UBS ", mun_nome_col)

    null_str = np.full(n, None, dtype=object)
    brasil = np.full(n, "BRASIL")
    pais_cod = np.full(n, "10")
    final_str = np.full(n, "final")
    nac_b = np.full(n, "B")

    cols = {
        "descricao_natureza_estabelecimento": NATUREZA_DESC[nat_idx],
        "codigo_via_administracao": VIA_COD[via_idx],
        "nome_pais_paciente": brasil,
        "codigo_origem_registro": null_str,
        "codigo_pais_paciente": pais_cod,
        "nome_raca_cor_paciente": raca_nome[raca_idx],
        "sigla_vacina": vac_sigla[vac_idx],
        "codigo_vacina_fabricante": fab_cod_col,
        "data_vacina": pa.array(dates_vacina, type=pa.date32()),
        "codigo_condicao_maternal": null_str,
        "nome_razao_social_estabelecimento": razao,
        "sigla_uf_estabelecimento": mun_uf_col,
        "nome_municipio_estabelecimento": mun_nome_col,
        "codigo_sistema_origem": SISTEMA_COD[sis_idx],
        "status_documento": final_str,
        "descricao_tipo_estabelecimento": tipo_desc[tipo_idx],
        "codigo_documento": document_ids,
        "codigo_municipio_estabelecimento": mun_cod[mun_idx],
        "descricao_estrategia_vacinacao": ESTRATEGIA_DESC[est_idx],
        "data_deletado_rnds": pa.nulls(n, type=pa.date32()),
        "nome_uf_paciente": uf_nome[uf_idx],
        "numero_cep_paciente": cep,
        "codigo_etnia_indigena_paciente": null_str,
        "codigo_vacina_categoria_atendimento": null_str,
        "descricao_local_aplicacao": LOCAL_DESC[loc_idx],
        "numero_idade_paciente": pa.array(idade_vals[idade_idx], type=pa.int32()),
        "codigo_lote_vacina": lotes,
        "codigo_cnes_estabelecimento": cnes,
        "descricao_vacina_fabricante": fab_desc_col,
        "codigo_tipo_estabelecimento": tipo_cod[tipo_idx],
        "codigo_natureza_estabelecimento": NATUREZA_COD[nat_idx],
        "codigo_raca_cor_paciente": raca_cod[raca_idx],
        "codigo_vacina_grupo_atendimento": null_str,
        "codigo_paciente": patient_ids,
        "descricao_sistema_origem": SISTEMA_DESC[sis_idx],
        "codigo_municipio_paciente": mun_cod[mun_idx],
        "nome_municipio_paciente": mun_nome_col,
        "nome_fantasia_estalecimento": fantasia,
        "descricao_condicao_maternal": null_str,
        "descricao_vacina_grupo_atendimento": null_str,
        "codigo_local_aplicacao": LOCAL_COD[loc_idx],
        "sigla_uf_paciente": uf_sigla[uf_idx],
        "nome_uf_estabelecimento": estab_uf_nome,
        "codigo_vacina": vac_cod[vac_idx],
        "descricao_via_administracao": VIA_DESC[via_idx],
        "codigo_estrategia_vacinacao": ESTRATEGIA_COD[est_idx],
        "descricao_vacina": vac_desc[vac_idx],
        "descricao_origem_registro": null_str,
        "data_entrada_rnds": pa.array(dates_entrada, type=pa.date32()),
        "descricao_vacina_categoria_atendimento": null_str,
        "nome_etnia_indigena_paciente": null_str,
        "tipo_sexo_paciente": SEX_VALUES[sex_idx],
        "descricao_nacionalidade_paciente": nac_b,
        "codigo_troca_documento": null_str,
        "codigo_dose_vacina": pa.array(dose_cod_int[dose_idx], type=pa.int8()),
        "descricao_dose_vacina": dose_desc_arr[dose_idx],
    }
    table = pa.table(cols)
    logger.info(f"  built table in {time.time() - t0:.1f}s ({len(table.schema)} cols)")
    return table


# ----- S3 upload ------------------------------------------------------------


def make_s3_client(endpoint: str, access_key: str, secret_key: str):
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=B2_REGION,
        config=Config(signature_version="s3v4"),
    )


def write_and_upload(table: pa.Table, s3_client, bucket: str, key: str) -> int:
    t0 = time.time()
    buf = BytesIO()
    # Smaller row groups give DuckDB finer-grained min/max stats, which
    # let it prune most of the file when a sorted column is filtered.
    # 50K rows × ~70 columns is still well within DuckDB's recommended
    # 100K-1M-row band once you account for multi-row-group reads.
    pq.write_table(
        table,
        buf,
        compression="zstd",
        use_dictionary=True,
        row_group_size=50_000,
    )
    size = buf.tell()
    buf.seek(0)
    s3_client.upload_fileobj(buf, bucket, key)
    logger.info(
        f"  uploaded s3://{bucket}/{key} ({size / 1e6:.1f} MB) in {time.time() - t0:.1f}s"
    )
    return size


# ----- Worker process state -------------------------------------------------

# Populated once per worker via _init_worker; avoids re-pickling distributions
# and re-creating the S3 client on every shard task.
_DISTRIBUTIONS: dict | None = None
_S3_CLIENT = None


def _init_worker(distributions_dict: dict, endpoint: str, access_key: str, secret_key: str) -> None:
    global _DISTRIBUTIONS, _S3_CLIENT
    _DISTRIBUTIONS = distributions_dict
    _S3_CLIENT = make_s3_client(endpoint, access_key, secret_key)


def _shard_seed(base_seed: int, year: int, month: int, shard_idx: int) -> int:
    return ((base_seed * 31 + year) * 13 + month) * 1000 + shard_idx


def _generate_and_upload_shard(
    year: int,
    month: int,
    shard_idx: int,
    n_rows: int,
    base_seed: int,
    bucket: str,
    key: str,
) -> tuple[int, int, float]:
    """Build one shard and upload to S3. Returns (rows, bytes, seconds)."""
    assert _DISTRIBUTIONS is not None and _S3_CLIENT is not None
    t0 = time.time()
    rng = np.random.default_rng(_shard_seed(base_seed, year, month, shard_idx))
    table = build_month_table(year, month, n_rows, _DISTRIBUTIONS, rng)
    # Sort to match the ClickHouse `events` ORDER BY
    # (sigla_uf_paciente, sigla_vacina, data_vacina, numero_idade_paciente)
    # so the CH bulk INSERT skips a full resort of every block. The
    # synthetic generator emits patient state and establishment state
    # independently — they are NOT correlated — so sorting by the
    # establishment column buys nothing for the dashboard's
    # patient-state filters.
    table = table.sort_by([
        ("sigla_uf_paciente", "ascending"),
        ("sigla_vacina", "ascending"),
        ("data_vacina", "ascending"),
    ])
    size = write_and_upload(table, _S3_CLIENT, bucket, key)
    return n_rows, size, time.time() - t0


# ----- CLI ------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months", nargs="+", type=int, default=DEFAULT_MONTHS,
        help="YYYYMM month codes (default: all 12 months of 2025)",
    )
    parser.add_argument(
        "--rows", type=int, default=DEFAULT_ROWS_PER_MONTH,
        help=f"Rows per month (default: {DEFAULT_ROWS_PER_MONTH:,})",
    )
    parser.add_argument(
        "--shard-rows", type=int, default=DEFAULT_SHARD_ROWS,
        help=f"Rows per parquet shard (default: {DEFAULT_SHARD_ROWS:,})",
    )
    parser.add_argument(
        "--workers", type=int, default=DEFAULT_WORKERS,
        help=f"Parallel worker processes (default: {DEFAULT_WORKERS})",
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--distributions", default=DISTRIBUTIONS_PATH, help="Path to distributions.json"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s",
    )

    endpoint = os.getenv("INGESTION_ENDPOINT_URL")
    bucket = os.getenv("INGESTION_BUCKET")
    access_key = os.getenv("INGESTION_AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("INGESTION_AWS_SECRET_ACCESS_KEY")
    prefix = os.getenv("INGESTION_PREFIX", "vacinas")

    missing = [
        n for n, v in [
            ("INGESTION_ENDPOINT_URL", endpoint),
            ("INGESTION_BUCKET", bucket),
            ("INGESTION_AWS_ACCESS_KEY_ID", access_key),
            ("INGESTION_AWS_SECRET_ACCESS_KEY", secret_key),
        ] if not v
    ]
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    assert endpoint and bucket and access_key and secret_key

    with open(args.distributions) as f:
        distributions = json.load(f)
    logger.info(f"Loaded distributions from {args.distributions}")

    # Build the shard work list: one task per (month, shard_idx).
    tasks: list[tuple[int, int, int, int, int, str, str]] = []
    for month in args.months:
        year, mon = divmod(month, 100)
        n_shards = (args.rows + args.shard_rows - 1) // args.shard_rows
        for shard_idx in range(n_shards):
            start = shard_idx * args.shard_rows
            n_rows = min(args.shard_rows, args.rows - start)
            if n_rows <= 0:
                break
            key = f"{prefix}/year_month={year:04d}-{mon:02d}/data-{shard_idx:02d}.parquet"
            tasks.append((year, mon, shard_idx, n_rows, args.seed, bucket, key))

    total_target = sum(t[3] for t in tasks)
    logger.info(
        f"Plan: {len(tasks)} shards across {len(args.months)} months, "
        f"{total_target:,} rows target, {args.workers} workers"
    )

    total_rows = 0
    total_bytes = 0
    completed = 0
    t_start = time.time()
    with ProcessPoolExecutor(
        max_workers=args.workers,
        initializer=_init_worker,
        initargs=(distributions, endpoint, access_key, secret_key),
    ) as pool:
        futures = {pool.submit(_generate_and_upload_shard, *task): task for task in tasks}
        for fut in as_completed(futures):
            year, mon, shard_idx, *_ = futures[fut]
            rows, size, elapsed = fut.result()
            total_rows += rows
            total_bytes += size
            completed += 1
            logger.info(
                f"[{completed}/{len(tasks)}] {year:04d}-{mon:02d}#{shard_idx:02d}: "
                f"{rows:,} rows, {size / 1e6:.1f} MB in {elapsed:.1f}s"
            )

    wall = time.time() - t_start
    logger.info(
        f"Done. {total_rows:,} rows, {total_bytes / 1e6:.1f} MB total, "
        f"{wall:.1f}s wall ({total_rows / max(wall, 1e-6):,.0f} rows/s)."
    )


if __name__ == "__main__":
    main()
