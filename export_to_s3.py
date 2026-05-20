"""Export vacinas.events from ClickHouse to S3 as monthly Parquet files.

Writes three hive-partitioned files for the static Q1 2025 dataset:
    {prefix}/year_month=2025-01/data.parquet
    {prefix}/year_month=2025-02/data.parquet
    {prefix}/year_month=2025-03/data.parquet

Uses ClickHouse's INSERT INTO FUNCTION s3(...) so data never touches local
disk. Run once after ingestion is complete; reruns overwrite the same keys.
"""
import argparse
import logging
import os
import sys

from config import load_config
from database import ClickHouseConnection

logger = logging.getLogger(__name__)

DEFAULT_MONTHS = [202501, 202502, 202503]


def build_s3_url(endpoint: str, bucket: str, key: str) -> str:
    endpoint = endpoint.rstrip("/")
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    return f"{endpoint}/{bucket}/{key.lstrip('/')}"


def export_month(client, month: int, s3_url: str, access_key: str, secret_key: str) -> int:
    select_sql = """
    SELECT
        descricao_natureza_estabelecimento,
        codigo_via_administracao,
        nome_pais_paciente,
        codigo_origem_registro,
        codigo_pais_paciente,
        nome_raca_cor_paciente,
        sigla_vacina,
        codigo_vacina_fabricante,
        data_vacina,
        codigo_condicao_maternal,
        nome_razao_social_estabelecimento,
        sigla_uf_estabelecimento,
        nome_municipio_estabelecimento,
        codigo_sistema_origem,
        status_documento,
        descricao_tipo_estabelecimento,
        codigo_documento,
        codigo_municipio_estabelecimento,
        descricao_estrategia_vacinacao,
        data_deletado_rnds,
        nome_uf_paciente,
        numero_cep_paciente,
        codigo_etnia_indigena_paciente,
        codigo_vacina_categoria_atendimento,
        descricao_local_aplicacao,
        numero_idade_paciente,
        codigo_lote_vacina,
        codigo_cnes_estabelecimento,
        descricao_vacina_fabricante,
        codigo_tipo_estabelecimento,
        codigo_natureza_estabelecimento,
        codigo_raca_cor_paciente,
        codigo_vacina_grupo_atendimento,
        codigo_paciente,
        descricao_sistema_origem,
        codigo_municipio_paciente,
        nome_municipio_paciente,
        nome_fantasia_estalecimento,
        descricao_condicao_maternal,
        descricao_vacina_grupo_atendimento,
        codigo_local_aplicacao,
        sigla_uf_paciente,
        nome_uf_estabelecimento,
        codigo_vacina,
        descricao_via_administracao,
        codigo_estrategia_vacinacao,
        descricao_vacina,
        descricao_origem_registro,
        data_entrada_rnds,
        descricao_vacina_categoria_atendimento,
        nome_etnia_indigena_paciente,
        tipo_sexo_paciente,
        descricao_nacionalidade_paciente,
        codigo_troca_documento,
        toInt8OrNull(codigo_dose_vacina) AS codigo_dose_vacina,
        descricao_dose_vacina
    FROM vacinas.events
    WHERE toYYYYMM(data_vacina) = %(month)s
    ORDER BY sigla_uf_estabelecimento, sigla_vacina, data_vacina
    """

    insert_sql = (
        "INSERT INTO FUNCTION "
        "s3(%(url)s, %(access_key)s, %(secret_key)s, 'Parquet') "
        + select_sql
        + " SETTINGS "
        "s3_truncate_on_insert = 1, "
        "s3_create_new_file_on_insert = 0, "
        "output_format_parquet_compression_method = 'zstd'"
    )

    logger.info(f"Exporting month {month} -> {s3_url}")
    client.command(
        insert_sql,
        parameters={
            "url": s3_url,
            "access_key": access_key,
            "secret_key": secret_key,
            "month": month,
        },
    )

    count = int(
        client.command(
            "SELECT count(*) FROM vacinas.events WHERE toYYYYMM(data_vacina) = %(month)s",
            parameters={"month": month},
        )
    )
    logger.info(f"Month {month}: {count:,} rows")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--months",
        nargs="+",
        type=int,
        default=DEFAULT_MONTHS,
        help="YYYYMM month codes to export (default: 202501 202502 202503)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    endpoint = os.getenv("INGESTION_ENDPOINT_URL")
    bucket = os.getenv("INGESTION_BUCKET")
    access_key = os.getenv("INGESTION_AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("INGESTION_AWS_SECRET_ACCESS_KEY")
    prefix = os.getenv("INGESTION_PREFIX", "vacinas")

    missing = [
        name
        for name, val in [
            ("INGESTION_ENDPOINT_URL", endpoint),
            ("INGESTION_BUCKET", bucket),
            ("INGESTION_AWS_ACCESS_KEY_ID", access_key),
            ("INGESTION_AWS_SECRET_ACCESS_KEY", secret_key),
        ]
        if not val
    ]
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    assert endpoint and bucket and access_key and secret_key

    config = load_config()
    connection = ClickHouseConnection(config.clickhouse)
    if not connection.health_check():
        logger.error("ClickHouse health check failed")
        sys.exit(1)
    client = connection.get_client()

    total = 0
    try:
        for month in args.months:
            year, mon = divmod(month, 100)
            key = f"{prefix}/year_month={year:04d}-{mon:02d}/data.parquet"
            url = build_s3_url(endpoint, bucket, key)
            total += export_month(client, month, url, access_key, secret_key)
    finally:
        connection.close()
        ClickHouseConnection.reset_instance()

    logger.info(f"Done. {total:,} rows across {len(args.months)} file(s).")


if __name__ == "__main__":
    main()
