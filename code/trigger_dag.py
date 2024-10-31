import requests
from requests.auth import HTTPBasicAuth

# Configuración de la API de Airflow
AIRFLOW_URL = "http://localhost:8080/api/v1"
DAG_ID = "Example-dag"  # Reemplaza con el ID de tu DAG
USERNAME = "admin"  # Cambia si tienes otro usuario
PASSWORD = "admin"  # Cambia si tienes otra contraseña

def check_airflow_api():
    url = f"{AIRFLOW_URL}/dags"
    try:
        response = requests.get(
            url,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        print("Conexión exitosa a la API de Airflow")
        print("DAGs disponibles:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con Airflow: {e}")

#check_airflow_api()

def trigger_dag(dag_id):
    # Endpoint de la API para ejecutar el DAG
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    data = {"conf": {}}

    try:
        response = requests.post(
            url,
            json=data,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
        
        if response.status_code == 200:
            print("DAG ejecutado exitosamente:", response.json())
        else:
            print(f"Error al ejecutar el DAG. Código de estado: {response.status_code}")
            print("Detalles:", response.json())
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con Airflow: {e}")

# Ejecutar el DAG
trigger_dag(DAG_ID)
