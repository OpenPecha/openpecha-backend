from google.api_core.exceptions import NotFound
from google.cloud import firestore, storage

# Set up Firestore clients
prod_db = firestore.Client(project="pecha-backend")
dev_db = firestore.Client(project="pecha-backend-dev")

# Set up Storage clients
prod_storage = storage.Client(project="pecha-backend")
dev_storage = storage.Client(project="pecha-backend-dev")

# Replace with actual bucket names
prod_bucket_name = "your-prod-bucket-name"
dev_bucket_name = "your-dev-bucket-name"


def copy_firestore_data():
    collections = prod_db.collections()
    for coll in collections:
        for doc in coll.stream():
            data = doc.to_dict()
            dev_db.collection(coll.id).document(doc.id).set(data)
            print(f"Copied document {doc.id} from {coll.id}")


def copy_storage_data():
    try:
        prod_bucket = prod_storage.bucket("pecha-backend.firebasestorage.app")
        dev_bucket = dev_storage.bucket("pecha-backend-dev.firebasestorage.app")

        blobs = list(prod_bucket.list_blobs())
        for blob in blobs:
            dest_blob = dev_bucket.blob(blob.name)
            dest_blob.rewrite(blob)
            print(f"Copied file: {blob.name}")
    except NotFound as e:
        print(f"Bucket not found: {e}")


if __name__ == "__main__":
    copy_firestore_data()
    copy_storage_data()
