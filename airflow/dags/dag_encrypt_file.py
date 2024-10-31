from airflow import DAG
from datetime import datetime
from operators.decrypt_operator import DecryptFileOperator
from operators.encrypt_operator import EncryptFileOperator
from cryptography.fernet import Fernet
import os 

# Ruta donde se almacenará la clave de encriptación
encryption_key_path = '/opt/airflow/data/encryption_key.key'

# Generar y almacenar la clave si no existe
if not os.path.exists(encryption_key_path):
    encryption_key = Fernet.generate_key()
    with open(encryption_key_path, 'wb') as key_file:
        key_file.write(encryption_key)
else:
    # Leer la clave existente
    with open(encryption_key_path, 'rb') as key_file:
        encryption_key = key_file.read()

# Definir el DAG
with DAG('encrypt_decrypt_file', 
         start_date=datetime(2024, 10, 28), 
         schedule_interval='@once', 
         catchup=False, 
         tags=['FORMACION'],
         access_control = {
             'SALES': {'can_read'}
         }) as dag:
    
    # # Operador para encriptar el archivo
    encrypt_task = EncryptFileOperator(
        task_id='encrypt_file',
        file_path='/opt/airflow/data/file.txt',
        key=encryption_key
    )

    # Operador para desencriptar el archivo
    decrypt_task = DecryptFileOperator(
        task_id='decrypt_file',
        encrypted_file_path='/opt/airflow/data/file.txt.enc',
        key=encryption_key
    )

    encrypt_task >> decrypt_task
