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


def migrate_metadata():
    metadata_collection = dev_db.collection("metadata")

    for doc in metadata_collection.stream():
        data = doc.to_dict()

        # Transform the old schema fields to new schema
        if "commentary_of" in data and data["commentary_of"]:
            data["parent"] = data["commentary_of"]
            data["type"] = "commentary"
            # Remove old field
            del data["commentary_of"]
        elif "translation_of" in data and data["translation_of"]:
            data["parent"] = data["translation_of"]
            data["type"] = "translation"
            # Remove old field
            del data["translation_of"]
        elif "version_of" in data and data["version_of"]:
            data["parent"] = data["version_of"]
            data["type"] = "version"
            # Remove old field
            del data["version_of"]
        else:
            # None of the old fields are set, or they are set to null/empty
            data["parent"] = None
            data["type"] = "root"

        # Clean up any remaining old fields that might be null/empty
        for old_field in ["commentary_of", "version_of", "translation_of"]:
            if old_field in data:
                del data[old_field]

        print(f"Transformed metadata document {doc.id}: parent={data.get('parent')}, type={data.get('type')}")

        # Write to dev database
        dev_db.collection("metadata").document(doc.id).set(data)
        print(f"Copied metadata document {doc.id}")


def copy_firestore_data():
    # Target the metadata collection directly
    metadata_collection = prod_db.collection("metadata")

    for doc in metadata_collection.stream():
        data = doc.to_dict()

        # Transform the old schema fields to new schema
        if "commentary_of" in data and data["commentary_of"]:
            data["parent"] = data["commentary_of"]
            data["type"] = "commentary"
            # Remove old field
            del data["commentary_of"]
        elif "translation_of" in data and data["translation_of"]:
            data["parent"] = data["translation_of"]
            data["type"] = "translation"
            # Remove old field
            del data["translation_of"]
        elif "version_of" in data and data["version_of"]:
            data["parent"] = data["version_of"]
            data["type"] = "version"
            # Remove old field
            del data["version_of"]
        else:
            # None of the old fields are set, or they are set to null/empty
            data["parent"] = None
            data["type"] = "root"

        # Clean up any remaining old fields that might be null/empty
        for old_field in ["commentary_of", "version_of", "translation_of"]:
            if old_field in data:
                del data[old_field]

        print(f"Transformed metadata document {doc.id}: parent={data.get('parent')}, type={data.get('type')}")

        # Write to dev database
        dev_db.collection("metadata").document(doc.id).set(data)
        print(f"Copied metadata document {doc.id}")

    # Copy other collections without transformation
    collections = prod_db.collections()
    for coll in collections:
        if coll.id == "metadata":
            continue  # Skip metadata - already processed above

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
    migrate_metadata()
    # copy_firestore_data()
    # copy_storage_data()
