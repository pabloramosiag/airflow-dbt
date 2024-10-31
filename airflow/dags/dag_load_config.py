import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime

def load_config(**kwargs):
    config_path = Variable.get(
        "config_path", "/opt/airflow/data/configs/"
    )
    with open(config_path, 'r') as f:
        config = json.load(f)
    kwargs['ti'].xcom_push(key='config', value=config)

def print_config(**kwargs):
    config = kwargs['ti'].xcom_pull(key='config', task_ids='load_config')
    print("Configuración cargada:", config)

with DAG(
    dag_id='dag_load_config',
    start_date=datetime(2024, 10, 28),
    schedule_interval=None,
    catchup=False,
    tags=['FORMACION']
):
    load_config_task = PythonOperator(
        task_id='load_config',
        python_callable=load_config,
        provide_context=True
    )

    print_config_task = PythonOperator(
        task_id='print_config',
        python_callable=print_config,
        provide_context=True
    )

    load_config_task >> print_config_task
