import logging

from firebase_config import storage_bucket

logger = logging.getLogger(__name__)


def store_serialized(pecha_id, json):
    storage_path = f"serialized_data/{pecha_id}.json"
    blob = storage_bucket.blob(storage_path)
    blob.upload_from_string(json, content_type="application/json")
    blob.make_public()

    logger.log("Uploaded to storage: %s", blob.public_url)
    return blob.public_url
