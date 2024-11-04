from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dbt_scripts.generate_source_ddl import generate_source_ddl
import os

dag = DAG(
    "create_tables_from_source",
    description="DAG to create tables from a dbt source",
    default_args={
        "owner": "Pablo Ramos",
        "depends_on_past": False,
        "retries": 0,
    },
    params={
        "source_name": "",
        "dbt_profile": "",
        "tables": "",
        "replace": False,
    },
    catchup=False,  # Do not run any missed DAG runs on catchup
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["FORMACION"],
)


def create_ddl_for_source(**context):
    # Fetching parameters from DAG run configuration
    params = context["dag_run"].conf

    task_instance = context["task_instance"]
    manifest_path = f"/opt/airflow/dbt_scripts/target/manifest.json"
    print("Current path:", Path.cwd())
    print("Manifest path:", manifest_path)

    # Generate DDL
    ddl = generate_source_ddl(
        source_name=params["source_name"],
        manifest_path=manifest_path,
        tables=params["tables"],
        replace=params["replace"],
    )

    # Push the DDL and the profile to XCom for the next task to retrieve
    task_instance.xcom_push(key="generated_ddl", value=ddl)

    return ddl  # This return is optional as XCom push is explicit

# compile_dbt_task = BashOperator(
#         task_id='compile_dbt',
#         bash_command='cd /opt/airflow/dbt_scripts && dbt compile',
#         env={'DBT_PROFILES_DIR': '/opt/airflow/dbt_scripts'},
#         dag=dag,
#     )

create_ddl_task = PythonOperator(
    task_id="create_ddl",
    python_callable=create_ddl_for_source,
    provide_context=True,
    dag=dag,
)

create_tables_from_ddl = PostgresOperator(
    task_id="create_tables_from_ddl",
    postgres_conn_id="postgres_conn",
    sql='{{ ti.xcom_pull(key="generated_ddl", task_ids="create_ddl") }}',
    dag=dag
)

create_ddl_task >> create_tables_from_ddl
