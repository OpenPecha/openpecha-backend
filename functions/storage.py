import logging
import shutil
import tempfile
from pathlib import Path

from firebase_admin import storage
from openpecha.pecha import Pecha

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self):
        self.bucket = storage.bucket()

    def store_pechaorg_json(self, pecha_id: str, json_data: str) -> str:
        return self._upload_file(Storage._pechaorg_json_path(pecha_id=pecha_id), json_data=json_data)

    def store_pecha_opf(self, pecha: Pecha) -> str:
        zip_path = f"{pecha.id}.zip"
        shutil.make_archive(pecha.id, "zip", pecha.pecha_path)

        return self._upload_file(storage_path=Storage._pecha_opf_path(pecha_id=pecha.id), file_path=zip_path)

    def retrieve_pechaorg_json(self, pecha_id: str) -> str:
        json_bytes = self._get_file(Storage._pechaorg_json_path(pecha_id))
        return json_bytes.decode("utf-8")

    def retrieve_pecha_opf(self, pecha_id: str) -> Path:
        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / f"{pecha_id}.zip"
            zip_path.write_bytes(self._get_file(Storage._pecha_opf_path(pecha_id)))

            return zip_path

    def rollback_pechaorg_json(self, pecha_id: str):
        self._rollback(f"pechaorg/{pecha_id}.json")

    def rollback_pecha_opf(self, pecha_id: str):
        self._rollback(f"opf/{pecha_id}.zip")

    @staticmethod
    def _pecha_opf_path(pecha_id: str) -> str:
        return f"opf/{pecha_id}.zip"

    @staticmethod
    def _pechaorg_json_path(pecha_id: str) -> str:
        return f"pechaorg/{pecha_id}.json"

    def _rollback(self, storage_path):
        blob = self.bucket.blob(storage_path)
        blob.delete()
        logger.info("Rolled back: %s", blob.name)

    def _upload_file(self, storage_path: str, json_data=None, file_path=None) -> str:
        blob = self.bucket.blob(storage_path)

        if json_data:
            blob.upload_from_string(json_data, content_type="application/json")
        elif file_path:
            blob.upload_from_filename(file_path)
        else:
            raise ValueError("Either content or file_path must be provided")

        blob.make_public()
        logger.info("Uploaded to storage: %s", blob.public_url)
        return blob.public_url

    def _get_file(self, storage_path: str) -> bytes:
        blob = self.bucket.blob(storage_path)

        if not blob.exists():
            raise FileNotFoundError(f"File not found in storage: {storage_path}")

        file_data = blob.download_as_bytes()
        logger.info("Retrieved from storage: %s", storage_path)
        return file_data
