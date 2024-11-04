from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.filesystem import FileSensor
from oauth2client.service_account import ServiceAccountCredentials
from pathlib import Path
import gspread
import psycopg2
import pandas as pd
import os

# Configurar credenciales y alcance
CONFIG_PATH = Path('/home/vcereijo/airflow/config/credentials.json')
#CSV_PATH = Path('data/gFormResponse.csv')
CSV_PATH = Path('/home/vcereijo/airflow/data/')
CSV_FILE = CSV_PATH.joinpath('gFormResponse.csv')

DB_CONNECTION_STRING = Variable.get(
    "DB_CONNECTION_STRING", "dbname=dbGForms user=postgresql password=postgresql1234 host=localhost"
)
SHEET_KEY = Variable.get(
    "SHEET_KEY", '173MU-NU0xw7KVSngGZxG3EulZrarQDR2jBVFTlJpPoU'
)
SCHEDULE = None  # Actualiza esto si necesitas un schedule en lugar de None

# scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
# creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG_PATH, scope)
# client = gspread.authorize(creds)
#connection_string = "postgresql+psycopg2://postgresql:postgresql1234@localhost:5432/postgres"

# Configurar el DAG
default_args = {
    'owner': 'airflow',
    'start_date': '2023-01-01',
    'retries': 1,
}

dag = DAG(dag_id="google_form_dag", default_args=default_args, schedule_interval=None)

def get_gspread_client():
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(CONFIG_PATH, scope)
    return gspread.authorize(creds)

# Función para extraer datos
def download_csv_from_api_google():
    if not os.path.exists(os.path.dirname(CSV_PATH)):
        os.makedirs(os.path.dirname(CSV_PATH))
    client = get_gspread_client()
    sheet = client.open_by_key(SHEET_KEY).sheet1
    data = sheet.get_all_records()
    df = pd.DataFrame(data)
    df.to_csv(str(CSV_FILE), index=False)

download_csv_from_api_google = PythonOperator(
    task_id='download_data',
    python_callable=download_csv_from_api_google,
    dag=dag,
)

# file_sensor = FileSensor(
#         task_id='waiting_for_file_async',
#         filepath= str(CSV_FILE),
#         deferrable=True,
#         dag=dag
#     )

# empty_task = EmptyOperator(
#     task_id='empty_task',
#     dag=dag,
# )

def load_csv_to_postgres():
    conn = psycopg2.connect(DB_CONNECTION_STRING)

    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SET datestyle = 'European, DMY';")
            cursor.execute("""
                CREATE TABLE raw_google_form_data_tmp ("Marca temporal" timestamp, "Nombre" varchar(100), "Apellidos" varchar(100));
            """)

            with open(str(CSV_FILE), 'r') as f:
                cursor.copy_expert("COPY raw_google_form_data_tmp FROM STDIN WITH CSV HEADER DELIMITER ','", f)

            cursor.execute("""
                INSERT INTO raw_google_form_data
                SELECT *
                FROM raw_google_form_data_tmp  -- Temporarily copied data
                ON CONFLICT ("Marca temporal")
                DO NOTHING;  -- Ignorar los registros duplicados
            """)
            cursor.execute("""
                DROP TABLE IF EXISTS raw_google_form_data_tmp;
            """)
    conn.close()

load_csv_task = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=load_csv_to_postgres,
    provide_context=True,
)

dbt_task = BashOperator(
    task_id='run_dbt',
    bash_command='cd /mnt/c/DevVcereijo/AllWorkspaces/dbtWorkspace/dbt_forms && dbt run --models gForms.*',
    dag=dag,
)

download_csv_from_api_google >> load_csv_task >> dbt_task
