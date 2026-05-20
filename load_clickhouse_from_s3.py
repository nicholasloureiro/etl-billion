"""Bulk-load the vacinas.events table from B2 parquet via ClickHouse's
s3() table function.

Run after `app/sql/schema_clickhouse.sql` has provisioned the database.
Iterates month-by-month (sequentially — the parallelism is inside the
INSERT itself via max_insert_threads) so the per-partition merge can
keep up, and OPTIMIZEs each partition FINAL before moving on. This
keeps the active-parts count low and avoids the "too many parts"
death spiral that bites batch loads at billion-row scale.

Required env (same convention as the rest of `vacinas_data/`):

    CLICKHOUSE_HOST          # e.g. abc123.us-east-1.aws.clickhouse.cloud
    CLICKHOUSE_PORT          # 8443 for CH Cloud / native TLS
    CLICKHOUSE_USER
    CLICKHOUSE_PASSWORD
    CLICKHOUSE_DATABASE      # default: vacinas
    CLICKHOUSE_SECURE        # default: true

    INGESTION_ENDPOINT_URL   # https://s3.us-east-005.backblazeb2.com
    INGESTION_BUCKET
    INGESTION_AWS_ACCESS_KEY_ID
    INGESTION_AWS_SECRET_ACCESS_KEY
    INGESTION_PREFIX         # default: vacinas

CLI:
    python3 load_clickhouse_from_s3.py                  # all months found
    python3 load_clickhouse_from_s3.py --months 202501 202502
    python3 load_clickhouse_from_s3.py --skip-mv-backfill
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
import time

import boto3
import clickhouse_connect
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def make_ch_client():
    return clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        port=int(os.environ.get("CLICKHOUSE_PORT", "8443")),
        username=os.environ.get("CLICKHOUSE_USER", "default"),
        password=os.environ["CLICKHOUSE_PASSWORD"],
        database=os.environ.get("CLICKHOUSE_DATABASE", "vacinas"),
        secure=os.environ.get("CLICKHOUSE_SECURE", "true").lower() == "true",
        # Bulk loads can take 1–2 h per partition at the high end.
        send_receive_timeout=7200,
    )


def discover_months(bucket: str, prefix: str, endpoint: str,
                    access_key: str, secret_key: str) -> list[str]:
    """List year_month=YYYY-MM/ partitions present in S3."""
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-005",
        config=Config(signature_version="s3v4"),
    )
    paginator = s3.get_paginator("list_objects_v2")
    months: set[str] = set()
    pfx = prefix.rstrip("/") + "/"
    for page in paginator.paginate(Bucket=bucket, Prefix=pfx, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            key = cp["Prefix"][len(pfx):].rstrip("/")
            if key.startswith("year_month="):
                months.add(key[len("year_month="):])
    return sorted(months)


# Columns in the same order as schema_clickhouse.sql. Keep in sync if
# the DDL grows.
EVENTS_COLUMNS = [
    "data_vacina",
    "sigla_uf_paciente",
    "nome_uf_paciente",
    "nome_municipio_paciente",
    "codigo_municipio_paciente",
    "codigo_paciente",
    "tipo_sexo_paciente",
    "numero_idade_paciente",
    "nome_raca_cor_paciente",
    "codigo_raca_cor_paciente",
    "codigo_pais_paciente",
    "nome_pais_paciente",
    "descricao_nacionalidade_paciente",
    "numero_cep_paciente",
    "codigo_etnia_indigena_paciente",
    "nome_etnia_indigena_paciente",
    "sigla_vacina",
    "descricao_vacina",
    "codigo_vacina",
    "codigo_dose_vacina",
    "descricao_dose_vacina",
    "codigo_vacina_fabricante",
    "descricao_vacina_fabricante",
    "codigo_lote_vacina",
    "codigo_via_administracao",
    "descricao_via_administracao",
    "codigo_local_aplicacao",
    "descricao_local_aplicacao",
    "codigo_estrategia_vacinacao",
    "descricao_estrategia_vacinacao",
    "codigo_vacina_categoria_atendimento",
    "descricao_vacina_categoria_atendimento",
    "codigo_vacina_grupo_atendimento",
    "descricao_vacina_grupo_atendimento",
    "codigo_condicao_maternal",
    "descricao_condicao_maternal",
    "sigla_uf_estabelecimento",
    "nome_uf_estabelecimento",
    "nome_municipio_estabelecimento",
    "codigo_municipio_estabelecimento",
    "codigo_cnes_estabelecimento",
    "nome_razao_social_estabelecimento",
    "nome_fantasia_estalecimento",
    "codigo_tipo_estabelecimento",
    "descricao_tipo_estabelecimento",
    "codigo_natureza_estabelecimento",
    "descricao_natureza_estabelecimento",
    "codigo_documento",
    "status_documento",
    "codigo_sistema_origem",
    "descricao_sistema_origem",
    "codigo_origem_registro",
    "descricao_origem_registro",
    "codigo_troca_documento",
    "data_entrada_rnds",
    "data_deletado_rnds",
]


def build_insert_sql(bucket_url: str, ym: str, b2_key_id: str, b2_app_key: str) -> str:
    """Build the INSERT INTO ... SELECT ... FROM s3() statement for one month."""
    # Most columns flow through unchanged; only those whose CH type
    # differs from the parquet type need explicit casts. Wrap dose and
    # age in toUInt8 (parquet has them as Int8/Int16 / nullable).
    select_exprs = []
    for col in EVENTS_COLUMNS:
        if col == "numero_idade_paciente":
            select_exprs.append(f"toUInt8(coalesce({col}, 0)) AS {col}")
        elif col == "codigo_dose_vacina":
            select_exprs.append(f"toUInt8(coalesce({col}, 0)) AS {col}")
        elif col == "data_vacina":
            select_exprs.append(f"toDate({col}) AS {col}")
        else:
            select_exprs.append(col)
    select_list = ",\n    ".join(select_exprs)

    return f"""
    INSERT INTO vacinas.events ({", ".join(EVENTS_COLUMNS)})
    SELECT
        {select_list}
    FROM s3(
        '{bucket_url}/year_month={ym}/*.parquet',
        '{b2_key_id}',
        '{b2_app_key}',
        'Parquet'
    )
    SETTINGS
        max_insert_threads = 16,
        max_insert_block_size = 5242880,
        min_insert_block_size_rows = 5242880,
        min_insert_block_size_bytes = 1073741824,
        parts_to_throw_insert = 3000,
        max_partitions_per_insert_block = 1,
        input_format_parquet_allow_missing_columns = 1,
        input_format_null_as_default = 1
    """


MV_BACKFILL_SQL = {
    "mv_state_vaccine_date_data": """
        INSERT INTO vacinas.mv_state_vaccine_date_data
        SELECT
            sigla_uf_paciente,
            sigla_vacina,
            data_vacina,
            count() AS total_doses,
            uniqState(codigo_paciente) AS unique_patients,
            countIf(tipo_sexo_paciente = 'F') AS female_count,
            countIf(tipo_sexo_paciente = 'M') AS male_count,
            sum(numero_idade_paciente) AS sum_age,
            count(numero_idade_paciente) AS count_age,
            countIf(codigo_dose_vacina = 1) AS first_doses,
            countIf(codigo_dose_vacina = 2) AS second_doses,
            countIf(codigo_dose_vacina >= 3) AS boosters,
            countIf(numero_idade_paciente < 18) AS children,
            countIf(numero_idade_paciente BETWEEN 18 AND 29) AS young_adults,
            countIf(numero_idade_paciente BETWEEN 30 AND 49) AS adults,
            countIf(numero_idade_paciente BETWEEN 50 AND 64) AS middle_aged,
            countIf(numero_idade_paciente >= 65) AS seniors
        FROM vacinas.events
        GROUP BY sigla_uf_paciente, sigla_vacina, data_vacina
    """,
    "mv_state_demo_data": """
        INSERT INTO vacinas.mv_state_demo_data
        SELECT
            sigla_uf_paciente,
            numero_idade_paciente,
            tipo_sexo_paciente,
            nome_raca_cor_paciente,
            data_vacina,
            count() AS total_doses,
            uniqState(codigo_paciente) AS unique_patients,
            countIf(codigo_dose_vacina = 1) AS first_doses,
            countIf(codigo_dose_vacina = 2) AS second_doses,
            countIf(codigo_dose_vacina >= 3) AS boosters,
            countIf(codigo_dose_vacina >= 2) AS completed_series
        FROM vacinas.events
        GROUP BY
            sigla_uf_paciente,
            numero_idade_paciente,
            tipo_sexo_paciente,
            nome_raca_cor_paciente,
            data_vacina
    """,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months", nargs="+", default=None,
        help="YYYY-MM partitions to load. Default: discover from S3.",
    )
    parser.add_argument(
        "--skip-optimize", action="store_true",
        help=(
            "Skip the single final OPTIMIZE TABLE FINAL after the load. "
            "Background merges still consolidate parts eventually, but "
            "queries see a higher part count until they do."
        ),
    )
    parser.add_argument(
        "--keep-mvs-attached", action="store_true",
        help=(
            "Leave the materialized views attached during the bulk load. "
            "MV triggers fire on every INSERT block which roughly doubles "
            "ingest wall time at billion-row scale; default behaviour is "
            "to DETACH the MVs before the loop, ATTACH after, then "
            "backfill mv_*_data with a single GROUP BY pass."
        ),
    )
    parser.add_argument(
        "--parallel", type=int, default=1,
        help=(
            "How many month partitions to INSERT in parallel. Default 1 "
            "(sequential). Bumping above 2 needs a beefy CH server — "
            "parallel inserts compound part-merge pressure."
        ),
    )
    args = parser.parse_args()

    required_env = [
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PASSWORD",
        "INGESTION_ENDPOINT_URL",
        "INGESTION_BUCKET",
        "INGESTION_AWS_ACCESS_KEY_ID",
        "INGESTION_AWS_SECRET_ACCESS_KEY",
    ]
    missing = [k for k in required_env if not os.environ.get(k)]
    if missing:
        logger.error("Missing env vars: %s", ", ".join(missing))
        sys.exit(1)

    endpoint = os.environ["INGESTION_ENDPOINT_URL"]
    bucket = os.environ["INGESTION_BUCKET"]
    prefix = os.environ.get("INGESTION_PREFIX", "vacinas")
    b2_key_id = os.environ["INGESTION_AWS_ACCESS_KEY_ID"]
    b2_app_key = os.environ["INGESTION_AWS_SECRET_ACCESS_KEY"]

    # CH's s3() function uses the bucket URL directly.
    if endpoint.endswith("/"):
        endpoint = endpoint[:-1]
    bucket_url = f"{endpoint}/{bucket}/{prefix}"

    if args.months:
        months = args.months
    else:
        logger.info("Discovering year_month partitions in s3://%s/%s ...", bucket, prefix)
        months = discover_months(
            bucket, prefix, endpoint, b2_key_id, b2_app_key,
        )
        if not months:
            logger.error("No year_month=YYYY-MM/ prefixes found under s3://%s/%s/",
                         bucket, prefix)
            sys.exit(1)
        logger.info("Found %d month partitions: %s", len(months), ", ".join(months))

    client = make_ch_client()

    # Sanity: the events table must already exist (run schema DDL first).
    exists = client.query(
        "SELECT count() FROM system.tables "
        "WHERE database = currentDatabase() AND name = 'events'"
    ).result_rows[0][0]
    if not exists:
        logger.error("vacinas.events not found. Run app/sql/schema_clickhouse.sql first.")
        sys.exit(1)

    # MV bypass: detach the views so the triggers don't fire on every
    # INSERT block during the bulk phase. They're reattached and the
    # mv_*_data tables backfilled in one shot at the end.
    mv_names = ("mv_state_vaccine_date", "mv_state_demo")
    detached = False
    if not args.keep_mvs_attached:
        for mv in mv_names:
            try:
                client.command(f"DETACH TABLE vacinas.{mv}")
            except Exception as e:
                logger.warning("Could not DETACH %s (already detached?): %s", mv, e)
        detached = True
        logger.info("Detached MVs for the bulk load.")

    grand_total = time.time()

    def _load_one(ym: str) -> tuple[str, float]:
        t0 = time.time()
        sql = build_insert_sql(bucket_url, ym, b2_key_id, b2_app_key)
        logger.info("[%s] INSERT INTO events ... FROM s3()", ym)
        client.command(sql)
        return ym, time.time() - t0

    if args.parallel > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed
        logger.info("Loading %d months with %d-way parallelism", len(months), args.parallel)
        with ThreadPoolExecutor(max_workers=args.parallel) as pool:
            for fut in as_completed(pool.submit(_load_one, ym) for ym in months):
                ym, dt = fut.result()
                logger.info("[%s] insert done in %.1fs", ym, dt)
    else:
        for ym in months:
            ym, dt = _load_one(ym)
            logger.info("[%s] insert done in %.1fs", ym, dt)

    logger.info("All partitions loaded in %.1fs", time.time() - grand_total)

    if not args.skip_optimize:
        # One whole-table OPTIMIZE FINAL at the end beats per-partition
        # OPTIMIZE during the loop at billion-row scale — the bulk phase
        # produces fewer, bigger parts and one merge pass collapses
        # them to one part per partition.
        t0 = time.time()
        logger.info("OPTIMIZE TABLE vacinas.events FINAL ...")
        client.command("OPTIMIZE TABLE vacinas.events FINAL")
        logger.info("OPTIMIZE finished in %.1fs", time.time() - t0)

    if detached:
        # Reattach and backfill the MV target tables from the
        # now-loaded events table — the triggers never fired so a
        # single GROUP BY pass is the only path to consistency.
        for mv in mv_names:
            client.command(f"ATTACH TABLE vacinas.{mv}")
        logger.info("Reattached MVs; backfilling target tables.")
        for mv_name, mv_sql in MV_BACKFILL_SQL.items():
            t0 = time.time()
            logger.info("TRUNCATE + backfill %s ...", mv_name)
            client.command(f"TRUNCATE TABLE vacinas.{mv_name}")
            client.command(mv_sql)
            logger.info("%s backfilled in %.1fs", mv_name, time.time() - t0)
    else:
        logger.info(
            "MVs stayed attached; mv_*_data populated live via trigger, "
            "no manual backfill needed."
        )


if __name__ == "__main__":
    main()
