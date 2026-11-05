"""Idempotently load Debezium Parquet objects from MinIO into PostgreSQL raw."""

from __future__ import annotations

import argparse
import io
import json
import logging
import os
from datetime import datetime, timezone
from typing import Any

import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import Json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
load_dotenv(os.path.join(ROOT, ".env"))
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")

TABLES = {"customers", "accounts", "transactions"}
FIELDS = {
    "customers": {"id": "customer_id", "first_name": "first_name", "last_name": "last_name", "email": "email", "created_at": "created_at"},
    "accounts": {"id": "account_id", "customer_id": "customer_id", "account_type": "account_type", "balance": "balance", "currency": "currency", "created_at": "created_at"},
    "transactions": {"id": "transaction_id", "account_id": "account_id", "txn_type": "txn_type", "amount": "amount", "related_account_id": "related_account_id", "status": "status", "created_at": "created_at"},
}


def clients():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", os.getenv("MINIO_ROOT_USER")),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", os.getenv("MINIO_ROOT_PASSWORD")),
        region_name="us-east-1",
    )
    conn = psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "postgres"),
        port=os.getenv("WAREHOUSE_PORT", "5432"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
        connect_timeout=10,
    )
    return s3, conn


def normalized(value: Any):
    if pd.isna(value):
        return None
    if hasattr(value, "to_pydatetime"):
        return value.to_pydatetime()
    if hasattr(value, "item"):
        return value.item()
    return value


def read_records(s3, bucket: str, key: str) -> list[dict[str, Any]]:
    response = s3.get_object(Bucket=bucket, Key=key)
    frame = pd.read_parquet(io.BytesIO(response["Body"].read()), engine="pyarrow")
    return [
        {str(k): normalized(v) for k, v in row.items()}
        for row in frame.to_dict(orient="records")
    ]


def insert_event(cur, table: str, record: dict[str, Any]) -> int:
    metadata = {
        "_operation", "_source_table", "_kafka_topic", "_kafka_partition",
        "_kafka_offset", "_event_timestamp",
    }
    topic = record.get("_kafka_topic")
    partition = record.get("_kafka_partition")
    offset = record.get("_kafka_offset")
    op = record.get("_operation") or "r"
    if not topic or partition is None or offset is None:
        raise ValueError(f"Parquet row for {table} is missing Kafka identity metadata")
    # Hashing the source coordinates yields a stable key independent of object name.
    event_id = f"{topic}:{int(partition)}:{int(offset)}"
    source = {k: v for k, v in record.items() if k not in metadata}
    columns = FIELDS[table]
    names = list(columns.values())
    vals = [source.get(k) for k in columns]
    event_ts = record.get("_event_timestamp")
    if event_ts is not None:
        event_ts = datetime.fromtimestamp(float(event_ts) / 1000, tz=timezone.utc)
    raw_table = f"raw.{table}"
    db_fields = ", ".join(names)
    placeholders = ", ".join(["%s"] * len(names))
    cur.execute(
        f"INSERT INTO {raw_table} (event_id, {db_fields}, operation_type, kafka_topic, kafka_partition, kafka_offset, event_timestamp, source_table, payload) "
        f"VALUES (%s, {placeholders}, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
        (event_id, *vals, op, topic, int(partition), int(offset), event_ts, table, Json(source, dumps=lambda value: json.dumps(value, default=str))),
    )
    return cur.rowcount


def process_object(s3, conn, bucket: str, key: str, table: str) -> tuple[bool, int]:
    head = s3.head_object(Bucket=bucket, Key=key)
    etag = head.get("ETag", "").strip('"')
    cur = conn.cursor()
    try:
        cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{bucket}/{key}",))
        cur.execute(
            "SELECT etag, processing_status FROM raw.ingested_files WHERE bucket=%s AND object_key=%s FOR UPDATE",
            (bucket, key),
        )
        old = cur.fetchone()
        if old and old[0] == etag and old[1] == "processed":
            conn.commit()
            logging.info("Already processed s3://%s/%s", bucket, key)
            return False, 0
        cur.execute(
            "INSERT INTO raw.ingested_files (bucket, object_key, etag, table_name, processing_status, error_message, row_count, ingestion_timestamp) "
            "VALUES (%s,%s,%s,%s,'processing',NULL,NULL,NOW()) "
            "ON CONFLICT (bucket, object_key) DO UPDATE SET etag=EXCLUDED.etag, table_name=EXCLUDED.table_name, processing_status='processing', error_message=NULL, row_count=NULL, ingestion_timestamp=NOW()",
            (bucket, key, etag, table),
        )
        conn.commit()

        records = read_records(s3, bucket, key)
        inserted = 0
        with conn:
            with conn.cursor() as write_cur:
                for record in records:
                    inserted += insert_event(write_cur, table, record)
                write_cur.execute(
                    "UPDATE raw.ingested_files SET processing_status='processed', row_count=%s, error_message=NULL WHERE bucket=%s AND object_key=%s",
                    (len(records), bucket, key),
                )
        logging.info("Loaded %d rows (%d new CDC events) from s3://%s/%s", len(records), inserted, bucket, key)
        return True, inserted
    except Exception as exc:
        conn.rollback()
        with conn:
            with conn.cursor() as fail_cur:
                fail_cur.execute(
                    "INSERT INTO raw.ingested_files (bucket, object_key, etag, table_name, processing_status, error_message) "
                    "VALUES (%s,%s,%s,%s,'failed',%s) ON CONFLICT (bucket, object_key) DO UPDATE SET etag=EXCLUDED.etag, processing_status='failed', error_message=EXCLUDED.error_message, ingestion_timestamp=NOW()",
                    (bucket, key, etag, table, str(exc)[:4000]),
                )
        logging.exception("Failed to process s3://%s/%s", bucket, key)
        raise
    finally:
        cur.close()


def run(table: str | None = None) -> int:
    bucket = os.getenv("MINIO_BUCKET", "raw")
    s3, conn = clients()
    processed = 0
    try:
        pager = s3.get_paginator("list_objects_v2")
        tables = [table] if table else sorted(TABLES)
        for name in tables:
            for page in pager.paginate(Bucket=bucket, Prefix=f"{name}/"):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if not key.lower().endswith(".parquet"):
                        continue
                    did_process, _ = process_object(s3, conn, bucket, key, name)
                    processed += int(did_process)
        logging.info("Completed MinIO ingestion; %d objects processed", processed)
        return processed
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--table", choices=sorted(TABLES))
    args = parser.parse_args()
    run(args.table)
