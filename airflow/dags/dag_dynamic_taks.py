from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
import time

ALL_TASKS = ["task1", "task2", "task3"]

with DAG(
    dag_id="my_dag",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
):
    tasks = []
    for task in ALL_TASKS:
        task = DummyOperator(task_id=task)
        if tasks:
            tasks[-1] >> task
        tasks.append(task)