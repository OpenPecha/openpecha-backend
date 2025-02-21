import logging
import sys

import firebase_admin
from firebase_admin import credentials, firestore, storage
from google.cloud import logging as cloud_logging

try:
    firebase_admin.get_app()  # Check if Firebase is already initialized
except ValueError:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred, {"storageBucket": "pecha-backend.firebasestorage.app"})

# Firestore client
db = firestore.client()
storage_bucket = storage.bucket()

logging_client = cloud_logging.Client()
cloud_handler = logging_client.get_default_handler()

# Create a console (terminal) handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG)  # Adjust log level as needed

# Create a logger and attach both handlers
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)  # Ensure all logs are captured

# Ensure logs go to both Cloud Logging and the terminal
logger.addHandler(cloud_handler)
logger.addHandler(console_handler)
