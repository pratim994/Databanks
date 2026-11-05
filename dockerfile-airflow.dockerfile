FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" \
    apache-airflow-providers-postgres \
    boto3 \
    dbt-core==1.8.8 \
    dbt-postgres==1.8.2 \
    kafka-python==2.0.2 \
    pandas \
    pyarrow \
    psycopg2-binary \
    python-dotenv
