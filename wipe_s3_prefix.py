"""Delete every object under INGESTION_PREFIX in the S3-compatible bucket.

Use this before re-running generate_synthetic_to_s3.py when you want a
clean slate (e.g. so freshly sorted shards are not mixed with the old
unsorted Q1 dump).

Required env vars (same as the rest of the ingest pipeline):
    INGESTION_ENDPOINT_URL, INGESTION_BUCKET,
    INGESTION_AWS_ACCESS_KEY_ID, INGESTION_AWS_SECRET_ACCESS_KEY,
    INGESTION_PREFIX (default: vacinas)

Lists everything first, prints a summary, and requires the user to
type the exact prefix to confirm before any DeleteObjects call fires.
"""
from __future__ import annotations

import logging
import os
import sys

import boto3
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("wipe_s3_prefix")

B2_REGION = "us-east-005"


def make_s3_client(endpoint: str, access_key: str, secret_key: str):
    if not endpoint.startswith(("http://", "https://")):
        endpoint = f"https://{endpoint}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=B2_REGION,
        config=Config(signature_version="s3v4", max_pool_connections=50),
    )


def main() -> None:
    endpoint = os.getenv("INGESTION_ENDPOINT_URL")
    bucket = os.getenv("INGESTION_BUCKET")
    access_key = os.getenv("INGESTION_AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("INGESTION_AWS_SECRET_ACCESS_KEY")
    prefix = os.getenv("INGESTION_PREFIX", "vacinas").rstrip("/") + "/"

    missing = [
        name for name, val in [
            ("INGESTION_ENDPOINT_URL", endpoint),
            ("INGESTION_BUCKET", bucket),
            ("INGESTION_AWS_ACCESS_KEY_ID", access_key),
            ("INGESTION_AWS_SECRET_ACCESS_KEY", secret_key),
        ] if not val
    ]
    if missing:
        logger.error(f"Missing env vars: {', '.join(missing)}")
        sys.exit(1)
    assert endpoint and bucket and access_key and secret_key

    s3 = make_s3_client(endpoint, access_key, secret_key)

    logger.info(f"Listing s3://{bucket}/{prefix} ...")
    paginator = s3.get_paginator("list_objects_v2")
    keys: list[str] = []
    total_bytes = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            total_bytes += obj["Size"]

    if not keys:
        logger.info("Nothing to delete; prefix is already empty.")
        return

    logger.info(
        f"Found {len(keys):,} objects ({total_bytes / 1e9:.2f} GB) under "
        f"s3://{bucket}/{prefix}"
    )
    sample_lines = keys[:5]
    if len(keys) > 5:
        sample_lines.append("...")
    logger.info("Sample:\n  " + "\n  ".join(sample_lines))

    expected = prefix.rstrip("/")
    typed = input(
        f"\nType the exact prefix '{expected}' to confirm permanent deletion "
        f"(anything else aborts): "
    ).strip()
    if typed != expected:
        logger.info("Aborted; nothing deleted.")
        return

    deleted = 0
    while keys:
        batch, keys = keys[:1000], keys[1000:]
        resp = s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
        )
        errors = resp.get("Errors", [])
        if errors:
            logger.error(f"DeleteObjects returned {len(errors)} errors: {errors[:3]}")
        deleted += len(batch) - len(errors)
        logger.info(f"  deleted {deleted:,} so far")

    logger.info(f"Done. Removed {deleted:,} objects from s3://{bucket}/{prefix}")


if __name__ == "__main__":
    main()
