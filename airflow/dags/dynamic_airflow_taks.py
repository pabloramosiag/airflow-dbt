
from __future__ import annotations
from datetime import datetime

from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.operators.python_operator import PythonOperator

def add_function(x: int, y: int):
    return x + y

with DAG(dag_id="dag_dynamic_task_mapping", 
         schedule=None, 
         start_date=datetime(2022, 3, 4),
         tags=['FORMACION']
    ) as dag:

    added_values = PythonOperator.partial(
        task_id="add",
        python_callable=add_function,
        op_kwargs={"y": 10},
        map_index_template="Input x={{ task.op_args[0] }}",
    ).expand(op_args=[[1], [2], [3]])