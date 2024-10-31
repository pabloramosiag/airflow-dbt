from airflow.models import BaseOperator
from cryptography.fernet import Fernet
import os

class EncryptFileOperator(BaseOperator):
    def __init__(self, file_path, key, **kwargs):
        super().__init__(**kwargs)
        self.file_path = file_path
        self.key = key

    def execute(self, context):
        # Cargar la clave de encriptación
        cipher_suite = Fernet(self.key)

        # Leer el contenido del archivo
        with open(self.file_path, 'rb') as file:
            file_data = file.read()

        # Encriptar el contenido
        encrypted_data = cipher_suite.encrypt(file_data)

        # Guardar el archivo encriptado
        encrypted_file_path = f"{self.file_path}.enc"
        with open(encrypted_file_path, 'wb') as encrypted_file:
            encrypted_file.write(encrypted_data)

        self.log.info(f"Archivo encriptado y guardado en {encrypted_file_path}")
        return encrypted_file_path
