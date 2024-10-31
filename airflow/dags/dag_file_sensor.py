from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.empty import EmptyOperator
from pendulum import datetime, now

# Create Simple DAG
with DAG( dag_id= 'dag_file_sensor', 
          schedule= '@daily',
          catchup= False,
          start_date= datetime(2024,10,27),
          max_active_runs= 1,
          tags=['FORMACION']
        ) :
    # Start
    start= EmptyOperator(task_id= 'start')
    # Add Sensor
    waiting_for_file = FileSensor(
        task_id='waiting_for_file',
        filepath='',
        fs_conn_id="my_file_system",
        poke_interval=30
    )      
    # End
    end= EmptyOperator(task_id= 'end')
    # Set Dependencies Flow
    start >> waiting_for_file >> end