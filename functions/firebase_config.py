import os
import firebase_admin
from firebase_admin import credentials, storage
from google.cloud import logging as cloud_logging

try:
    firebase_admin.get_app()  # Check if Firebase is already initialized
except ValueError:
    cred = credentials.ApplicationDefault()
    # Get project ID from environment
    project_id = os.environ.get('GCP_PROJECT') or os.environ.get('GCLOUD_PROJECT')
    
    # Map project to storage bucket
    if project_id == 'pecha-backend-test-3a4d0':
        storage_bucket_name = "pecha-backend-test-3a4d0.appspot.com"
    else:
        storage_bucket_name = "pecha-backend.firebasestorage.app"
    
    firebase_admin.initialize_app(cred, {"storageBucket": storage_bucket_name})


storage_bucket = storage.bucket()

logging_client = cloud_logging.Client()
logging_client.setup_logging()
