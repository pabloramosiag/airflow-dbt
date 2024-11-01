from datetime import datetime
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

DAG = DAG(
  dag_id='example_xcom_dag',
  start_date=datetime.now(),
  schedule_interval='@once',
  tags=['FORMACION']
)

def push_function(**kwargs):
    ls = ['a', 'b', 'c']
    a = ['g']
    ti = kwargs['ti']
    ti.xcom_push(key="example1", value='d')
    ti.xcom_push(key="example1", value='e')
    return ls, a

push_task1 = PythonOperator(
    task_id='push_task1', 
    python_callable=push_function,
    provide_context=True,
    dag=DAG)

push_task2 = PythonOperator(
    task_id='push_task2', 
    python_callable=push_function,
    provide_context=True,
    dag=DAG)

# ti.xcom_push(key="return_value", value=ls)

def pull_function(**kwargs):
    ti = kwargs['ti']
    print(ti.xcom_pull(key="return_value", task_ids='push_task1'))
    ls = ti.xcom_pull(task_ids='push_task2')
    print(ls)
    print(ti.xcom_pull(key="example1", task_ids='push_task1'))

pull_task = PythonOperator(
    task_id='pull_task', 
    python_callable=pull_function,
    provide_context=True,
    dag=DAG)

push_task1 >> push_task2 >> pull_task