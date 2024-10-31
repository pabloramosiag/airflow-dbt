from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from pendulum import datetime
import os
import shutil
import pandas as pd

def get_latest_sales_file(dir_path):
    files = [f for f in os.listdir(dir_path) if os.path.isfile(os.path.join(dir_path, f))]
    if not files:
        raise FileNotFoundError(f"No files found in directory {dir_path}")
    latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(dir_path, x)))
    return os.path.join(dir_path, latest_file)

def ingest_sales_file(file_path, postgres_conn_id):
    df = pd.read_csv(file_path)
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    conn = hook.get_conn()
    cursor = conn.cursor()
    for _, row in df.iterrows():
        cursor.execute(
            """
            INSERT INTO sales (sale_id, name, order_type, order_date, order_value)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (sale_id) DO NOTHING
            """,
            (row['sale_id'], row['name'], row['order_type'], row['order_date'], row['order_value'])
        )
    conn.commit()


def move_file(file_path, target_dir):
    if not os.path.exists(target_dir):
        os.makedirs(target_dir)
    shutil.move(file_path, os.path.join(target_dir, os.path.basename(file_path)))

with DAG(
    dag_id='ingest_sales_files',
    schedule='@daily',
    catchup=False,
    start_date=datetime(2024, 10, 27),
    max_active_runs=1,
    tags=['FORMACION']
) as dag:

    start = EmptyOperator(task_id='start')

    create_sales_table = PostgresOperator(
        task_id="create_sales_table",
        postgres_conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS sales (
                sale_id VARCHAR PRIMARY KEY,
                name VARCHAR NOT NULL,
                order_type VARCHAR NOT NULL,
                order_date DATE NOT NULL,
                order_value VARCHAR NOT NULL
            );
        """
    )
    
    waiting_for_sale_file = FileSensor(
        task_id='waiting_for_sale_file',
        filepath='sales/raw', 
        fs_conn_id="my_file_system",
        poke_interval=30,
        mode='poke'
    )

    get_sales_file = PythonOperator(
        task_id='get_sales_file',
        python_callable=get_latest_sales_file,
        op_kwargs={'dir_path': '/opt/airflow/data/sales/raw'},
    )

    ingest_sales = PythonOperator(
        task_id='ingest_sales',
        python_callable=ingest_sales_file,
        op_kwargs={
            'file_path': '{{ ti.xcom_pull(task_ids="get_sales_file") }}',  # Dynamic file path
            'postgres_conn_id': 'postgres_conn'
        }
    )

    move_sales_file = PythonOperator(
        task_id='move_sales_file',
        python_callable=move_file,
        op_kwargs={
            'file_path': '{{ ti.xcom_pull(task_ids="get_sales_file") }}',  # Dynamic file path
            'target_dir': '/opt/airflow/data/sales/ingested'
        }
    )

    end = EmptyOperator(task_id='end')

    start >> create_sales_table 
    create_sales_table >> waiting_for_sale_file 
    waiting_for_sale_file >> get_sales_file 
    get_sales_file >> ingest_sales 
    ingest_sales >> move_sales_file
    move_sales_file >> end
