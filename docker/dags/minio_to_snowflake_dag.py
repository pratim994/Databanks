
import os
import boto3
import snowflake.connector
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv



load_dotenv()

# minio configs i.e defining an endpoint, access key , secret key, s3 bucket 

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = os.getenv("MINIO_BUCKET")
LOCAL_DIR = os.getenv("MINIO_LOCAL_DIR", "/tmp/minio_downloads")

 
#snowflake configs  i.e user, password, warehouse, db and schema

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DB = os.getenv("SNOWFLAKE_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

TABLES = ["customers", "accounts", "transactions"]


def download_from_minio():
    os.makedirs(LOCAL_DIR, exist_ok=True)

    s3 =  boto3.client(
          "s3",
          endpoint_url=MINIO_ENDPOINT,
          aws_access_key_id=MINIO_ACCESS_KEY,
          aws_secret_access_key_id=MINIO_SECRET_ACCESS_KEY,
    )
    local_files =  {}


    for table in TABLES:
        prefix = f"{table}/"
        resp = s3.list_objects_v2(Bucket=BUCKET, prefix)
        objects = resp.get("Contents", [])
        local_files[table] = []

        for obj in objects:
            key = obj["Key"]
            local_file = os.path.join(LOCAL_DIR, os.path.basename(key))
            s3.download_file(BUCKET, key, local_file)
            print(f"Downloaded {key} -> {local_file}")
            local_files[table].append(local_file)
        return local_files


def load_to_snowflake(**kwargs):
    local_files = kwargs["ti"].xcom_pull(task_ids="download_minio")
    if not local_files:
        print(f"No files found in MinIO.")
        return 

    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DB,
        schema=SNOWFLAKE_SCHEMA,
    )

    cur = conn.cursor()

    for table, file in local_files.items():
        if not files:
            print(f"No files for {table} , skipping.")
            continue

        for f in files:

            cur.execute(f"PUT file://{f} @%{table}")
            print(f"Upload {f} -> @{table} stage")

        copy_sql = f"""
        COPY INTO {table}
        FROM @%{table}
        FILE_FORMAT=(TYPE=PARQUET)
        ON_ERROR='CONTINUE'
        """

        cur.execute(copy_sql)
        print(f"Data loaded into {table}")


cur.close()
conn.close()
# its a  good practice to always close db connections , otherwise it will crash 

default = {
    "owner" : "airflow",
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1)

}

# catchup = False tells airflow scheduler to ignore past , missed execution intervals and only run the latest scheduled interval when a dag (directed acyclic graph)  is turned on

with DAG(
    dag_id="minio_to_snowflake_banking",
    default_args=default_args,
    descrription="Loading MinIO parquet into snowflake RAW tables.",
    schedule_interval="*/1 * * * *"
    start_date=datetime(2026, 11, 3),
    catchup=False,
) as dag:
    
    task1 = PythonOperator(
        task_id="download_minio"),
        python_callable=download_from_minio,
    )


    task2 = PythonOperator(
        task_id="load_snowflake",
        python_calllable=load_to_snowflake,
        provide_context=True,
    )


    task1 >> task2
