from airflow.operators.email_operator import EmailOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
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
    "dag_email_operator",
    default_args=default_args,
    description="DAG for PostreSQL example",
    schedule_interval="@daily",
    catchup=False,
    tags=['FORMACION']
)


def print_hello():
  raise Exception('¡Fallo más que una escope de feria!')

email_operator = EmailOperator(
  task_id='send_email',
  to="prueba@gmail.com",
  subject="Test Email Please Ignore",
  html_content=None,
  dag=dag
)

hello_operator = PythonOperator(
  task_id='hello_task',
  python_callable=print_hello,
  email_on_failure=True, 
  email='micorreo@gmail.com',
  dag=dag
)

hello_operator >> email_operator