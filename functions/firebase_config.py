import firebase_admin
from firebase_admin import credentials, firestore

try:
    firebase_admin.get_app()  # Check if Firebase is already initialized
except ValueError:
    cred = credentials.ApplicationDefault()
    firebase_admin.initialize_app(cred)

# Firestore client
db = firestore.client()
# storage_client = storage.Client()
# STORAGE_BUCKET = "pecha-backend.firebasestorage.app"
