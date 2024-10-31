from airflow.models import BaseOperator
from cryptography.fernet import Fernet
import os

class DecryptFileOperator(BaseOperator):
    def __init__(self, encrypted_file_path, key, **kwargs):
        super().__init__(**kwargs)
        self.encrypted_file_path = encrypted_file_path
        self.key = key

    def execute(self, context):
        # Carga la clave de encriptación
        cipher_suite = Fernet(self.key)

        # Lee el archivo encriptado
        with open(self.encrypted_file_path, 'rb') as encrypted_file:
            encrypted_data = encrypted_file.read()

        # Desencripta el contenido
        decrypted_data = cipher_suite.decrypt(encrypted_data)

        # Guarda el archivo desencriptado
        original_file_path = self.encrypted_file_path.replace('.enc', '')
        with open(original_file_path, 'wb') as decrypted_file:
            decrypted_file.write(decrypted_data)

        self.log.info(f"Archivo desencriptado y guardado en {original_file_path}")
        return original_file_path

