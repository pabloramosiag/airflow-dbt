from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
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
    "postgreSQL-dag",
    default_args=default_args,
    description="DAG for PostreSQL example",
    schedule_interval="@daily",
    catchup=False,
    tags=['FORMACION']
)

# Define the start of the DAG
start = DummyOperator(task_id="start", dag=dag)

def check_encoding():
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("SHOW client_encoding;")
    print(cursor.fetchone())

check_encoding_task = PythonOperator(
    task_id='check_encoding',
    python_callable=check_encoding,
    dag=dag
)


create_sales_table = PostgresOperator(
    task_id="create_sales_table",
    postgres_conn_id="postgres_conn",
    sql="""
        CREATE TABLE IF NOT EXISTS sales (
            sale_id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            order_type VARCHAR NOT NULL,
            order_date DATE NOT NULL,
            order_value VARCHAR NOT NULL);
    """,
    dag=dag
)

create_pet_table = SQLExecuteQueryOperator(
    task_id="create_pet_table",
    conn_id="postgres_conn",
    sql="""
        CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
    """,
)

# Define the end of the DAG
end = DummyOperator(task_id="end", dag=dag)

# Set up task dependencies
start >> check_encoding_task >> create_sales_table >> create_pet_table >> end
