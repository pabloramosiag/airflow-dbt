from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators.bash import BashOperator
import time

ALL_TASKS = ["dbt_seed_bronze",
             "dbt_run_bronze", 
             "dbt_test_bronze", 
             "dbt_run_silver",
             "dbt_test_silver",
             "dbt_run_gold",
             "dbt_test_gold"]

def run_dbt_commands(ALL_TASKS):
    tasks = []
    for command in ALL_TASKS:
        task = DummyOperator(
            task_id=command,
            retries=6,
        )
        if tasks:
            tasks[-1] >> task
        tasks.append(task)
    return 

with DAG(
    dag_id="my_dag",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['FORMACION']
):
    run_dbt_commands(ALL_TASKS)