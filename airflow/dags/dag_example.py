from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator

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

# Define the start of the DAG
start = DummyOperator(task_id="start", dag=dag)
# Define the end of the DAG
end = DummyOperator(task_id="end", dag=dag)

# Set up task dependencies
start >> end
