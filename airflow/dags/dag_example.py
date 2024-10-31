from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from operators.hello_operator import HelloOperator
import time

# Default arguments for the DAG
default_args = {
    "owner": "Pablo Ramos",
    "start_date": datetime(2024, 7, 7),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG object
dag = DAG(
    "Example-dag",
    default_args=default_args,
    description="DAG for example",
    schedule_interval="@daily",
    catchup=False,
    tags=['FORMACION']
)

def dormir(*args, **kwars):
    time.sleep(5)
    print("Se durmio")

# Define the start of the DAG
start = DummyOperator(task_id="start", dag=dag)

# Define the start of the DAG
mid = PythonOperator(task_id="mid", python_callable=dormir, dag=dag)

# create_pet_table = SQLExecuteQueryOperator(
#     task_id="create_pet_table",
#     conn_id="postgres_conn",
#     sql="""
#     CREATE TABLE IF NOT EXISTS pet (
#     pet_id SERIAL PRIMARY KEY,
#     name VARCHAR NOT NULL,
#     pet_type VARCHAR NOT NULL,
#     birth_date DATE NOT NULL,
#     OWNER VARCHAR NOT NULL);
#     """,
# )

bash = BashOperator(
    task_id="bash",
    bash_command="echo hola",
    dag=dag
)

hello = HelloOperator(
    task_id="hello",
    name="Pablo",
    dag=dag
)

# Define the end of the DAG
end = DummyOperator(task_id="end", dag=dag)

# Set up task dependencies
start >> mid >> bash >> hello >> end
