from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    'dag_dbt_example',
    start_date=datetime(2024, 10, 29),
    schedule_interval='@daily',
    tags=['FORMACION']
) as dag:
    
    ejecutar_dbt = BashOperator(
        task_id='run_dbt',
        bash_command='cd /opt/airflow/dbt_scripts && dbt compile',
        env={'DBT_PROFILES_DIR': '/opt/airflow/dbt_scripts'}
    )

    ejecutar_dbt
