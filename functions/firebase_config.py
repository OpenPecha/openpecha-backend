import firebase_admin
from firebase_admin import credentials, storage
from google.cloud import logging as cloud_logging

try:
    firebase_admin.get_app()  # Check if Firebase is already initialized
except ValueError:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {"storageBucket": "pecha-backend.firebasestorage.app"})


storage_bucket = storage.bucket()

logging_client = cloud_logging.Client()
logging_client.setup_logging()
