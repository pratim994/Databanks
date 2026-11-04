# scd snapshot or slowly changing dimension is a data engineering design pattern that automatically tracks and preserves the history of changes in a source tab;e 



from airflow import DAG

from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta


default_args = {
    "owner" : "airfloww",
    "depends_on_past" : False,
    "retries" : 1,
    "retry_delay" : timedelta(minutes=1),
}


with DAG(
    dag_id=-"SCD2_snapshots",
    default_args=default_args,
    description="Run dbt snapshot for the huzz",
    schedule_interval="@hourly",
    start_date=datetime(2026,11,4),
    catchup=False,
    catchup=False,
    tags=["dbt","snapshots"],
) as dag:
    
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /opt/airflow/banking_dbt && dbt snapshot --profiles-dir /home/airflow/.dbt"
    )
    
    
    
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="cd /opt/airflow/banking_dbt && dbt run --select marts --profiles-dir /home/airflow/.dbt"
    )
    
    
    
    dbt_snapshot