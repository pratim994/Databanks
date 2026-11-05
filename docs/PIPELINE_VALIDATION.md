# Pipeline validation record

Validation is recorded honestly and only after the associated command is executed. Compose configuration was parsed successfully during implementation. The existing Compose stack was observed running at initial inspection, but this alone does not validate CDC through dbt.

## Commands

Start/update the stack without removing persistent data:

```bash
docker compose up -d --build
docker compose ps
```

Check CDC and services:

```bash
curl -fsS http://localhost:8083/connectors/postgres-connector/status
docker compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list
```

Start a host consumer and generate a batch in separate terminals:

```bash
python consumer/kafka_to_minio.py
python data-generator/faker_generator.py --once
```

Then enable/trigger `banking_data_pipeline` in Airflow at `http://localhost:8080`. Run dbt directly inside the Airflow image when debugging:

```bash
docker compose exec -T airflow-scheduler bash -lc 'cd /opt/airflow/banking_dbt && dbt debug --profiles-dir /home/airflow/.dbt && dbt parse --profiles-dir /home/airflow/.dbt && dbt compile --profiles-dir /home/airflow/.dbt && dbt run --profiles-dir /home/airflow/.dbt && dbt snapshot --profiles-dir /home/airflow/.dbt && dbt test --profiles-dir /home/airflow/.dbt'
```

The loader can be safely rerun to prove manifest idempotency:

```bash
docker compose exec -T airflow-scheduler python /opt/airflow/consumer/minio_to_postgres.py
docker compose exec -T airflow-scheduler python /opt/airflow/consumer/minio_to_postgres.py
```

Inspect counts, manifest state, and CDC history using `psql` inside the banking PostgreSQL service. To demonstrate SCD2, update a customer or account source attribute, wait for Debezium and the consumer to write the update, then trigger the DAG twice (staging before snapshot). Confirm multiple rows and one `dbt_valid_to IS NULL` row for the changed natural key in the respective `analytics.*_snapshot` table.

## Execution record

- `docker compose config --quiet` — passed after Compose edits.
- Repository-wide syntax, dbt, DAG import, CDC, MinIO ingestion, warehouse tests, and SCD2 history checks — pending execution after image rebuild and runtime validation.

No end-to-end success is claimed yet. The runtime stack must be rebuilt and the checks above executed before acceptance.
