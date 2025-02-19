import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud import logging  # pylint:disable=no-name-in-module

try:
    firebase_admin.get_app()  # Check if Firebase is already initialized
except ValueError:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {"storageBucket": "pecha-backend.firebasestorage.app"})

# Firestore client
db = firestore.client()
storage_bucket = storage.bucket()

logging_client = logging.Client()
logging_client.setup_logging()
