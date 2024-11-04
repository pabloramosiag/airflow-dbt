from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import json


# Create your DAG instance
dag = DAG(
    'run-custom-dbt-command',
    description='DAG to run a custom dbt command through Airflow',
    default_args = {
        'owner': 'Pablo Ramos',
        'depends_on_past': False,
        'retries': 0,
    },
    params={
        'dbt_command': "dbt seed/run/test -s MY_MODEL",
        'project_profile': "MY_PROFILE",
        "full_refresh": False
    },
    catchup=False,  # Do not run any missed DAG runs on catchup
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['FORMACION']
)


def dag_setup(**context):
    dbt_command = context["dag_run"].conf["dbt_command"]
    project_profile = context["dag_run"].conf["project_profile"]
    full_refresh = context["dag_run"].conf["full_refresh"]

    params = {
        'dbt_command': dbt_command,
        'project_profile': project_profile,
        'full_refresh': full_refresh
    }
    print('--- The parameters for this DAG execution are:')
    print(json.dumps(params, indent=4))
    return params

dag_setup = PythonOperator(
    task_id='dag_setup',
    python_callable=dag_setup,
    execution_timeout=timedelta(seconds=60),
    provide_context=True,
    dag=dag
)

run_dbt_command = BashOperator(
        task_id='run_dbt_command',
        bash_command='cd /opt/airflow/dbt_scripts && {{ ti.xcom_pull(task_ids="dag_setup")["dbt_command"] }} -t {{ ti.xcom_pull(task_ids="dag_setup")["project_profile"] }}',
        env={'DBT_PROFILES_DIR': '/opt/airflow/dbt_scripts'}
    )

dag_setup >> run_dbt_command
