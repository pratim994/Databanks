"""Local end-to-end banking pipeline: MinIO CDC -> PostgreSQL -> dbt."""

from datetime import timedelta
import os
import subprocess
import sys
from urllib.request import urlopen

import boto3
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pendulum import datetime

ROOT = "/opt/airflow"
DBT_DIR = f"{ROOT}/banking_dbt"
PROFILE_DIR = "/home/airflow/.dbt"
INGESTOR = f"{ROOT}/consumer/minio_to_postgres.py"


def check_services():
    """Check network dependencies at task runtime, not during DAG parsing."""
    with psycopg2.connect(
        host=os.environ["POSTGRES_HOST"],
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )
    s3.list_buckets()
    connect_url = os.environ.get("DEBEZIUM_CONNECT_URL", "http://connect:8083")
    with urlopen(f"{connect_url}/connectors", timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(f"Debezium Connect health returned HTTP {response.status}")


def check_minio_files():
    s3 = boto3.client(
        "s3",
        endpoint_url=os.environ["MINIO_ENDPOINT"],
        aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
        aws_secret_access_key=os.environ["MINIO_SECRET_KEY"],
        region_name="us-east-1",
    )
    bucket = os.environ["MINIO_BUCKET"]
    s3.head_bucket(Bucket=bucket)
    for table in ("customers", "accounts", "transactions"):
        result = s3.list_objects_v2(Bucket=bucket, Prefix=f"{table}/", MaxKeys=1)
        if not result.get("Contents"):
            raise RuntimeError(f"No {table} Parquet files found in MinIO bucket {bucket!r}")


def ingest(table):
    subprocess.run([sys.executable, INGESTOR, "--table", table], check=True, timeout=1800)


def validate_raw_data():
    checks = {
        "customers": "SELECT count(*) FROM raw.customers WHERE operation_type NOT IN ('c','r','u','d')",
        "accounts": "SELECT count(*) FROM raw.accounts WHERE operation_type NOT IN ('c','r','u','d')",
        "transactions": "SELECT count(*) FROM raw.transactions WHERE amount IS NOT NULL AND amount <= 0",
        "failed files": "SELECT count(*) FROM raw.ingested_files WHERE processing_status = 'failed'",
        "duplicate CDC identifiers": "SELECT (SELECT count(*) FROM raw.customers) - count(DISTINCT event_id) FROM raw.customers",
    }
    with psycopg2.connect(
        host=os.environ["POSTGRES_HOST"], port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ["POSTGRES_DB"], user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"], connect_timeout=10,
    ) as conn:
        with conn.cursor() as cur:
            for label, query in checks.items():
                cur.execute(query)
                count = cur.fetchone()[0]
                if count:
                    raise ValueError(f"Raw data quality check {label!r} failed for {count} rows")


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=35),
}

with DAG(
    dag_id="banking_data_pipeline",
    description="Load CDC Parquet from MinIO into PostgreSQL and transform with dbt.",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["banking", "postgres", "dbt"],
    doc_md="""### Banking data pipeline
Checks PostgreSQL, MinIO, and Debezium; loads each table idempotently; validates
raw data; then runs dbt staging models, SCD2 snapshots, marts, and tests.
""",
) as dag:
    services = PythonOperator(task_id="check_services", python_callable=check_services)
    files = PythonOperator(task_id="check_minio_files", python_callable=check_minio_files)
    customer_load = PythonOperator(task_id="ingest_customers", python_callable=ingest, op_args=["customers"])
    account_load = PythonOperator(task_id="ingest_accounts", python_callable=ingest, op_args=["accounts"])
    transaction_load = PythonOperator(task_id="ingest_transactions", python_callable=ingest, op_args=["transactions"])
    raw_validation = PythonOperator(task_id="validate_raw_data", python_callable=validate_raw_data)
    staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select path:models/staging --profiles-dir {PROFILE_DIR}",
    )
    snapshots = BashOperator(
        task_id="run_dbt_snapshots",
        bash_command=f"cd {DBT_DIR} && dbt snapshot --profiles-dir {PROFILE_DIR}",
    )
    marts = BashOperator(
        task_id="run_dbt_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select path:models/marts --profiles-dir {PROFILE_DIR}",
    )
    quality = BashOperator(
        task_id="run_data_quality_tests",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILE_DIR}",
    )

    services >> files >> [customer_load, account_load, transaction_load]
    [customer_load, account_load, transaction_load] >> raw_validation >> staging >> snapshots >> marts >> quality
