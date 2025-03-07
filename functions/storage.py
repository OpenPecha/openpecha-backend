import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import IO, Any

from firebase_admin import storage
from openpecha.pecha import Pecha

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self):
        self.bucket = storage.bucket()

    def store_pechaorg_json(self, pecha_id: str, json_dict: dict[str, Any]) -> str:
        json_str = json.dumps(json_dict)

        blob = self.bucket.blob(Storage._pechaorg_json_path(pecha_id))
        blob.upload_from_string(json_str, content_type="application/json")
        blob.make_public()
        logger.info("Uploaded to storage: %s", blob.public_url)

        return blob.public_url

    def store_pecha_opf(self, pecha: Pecha) -> str:
        zip_path = f"{pecha.id}.zip"
        shutil.make_archive(pecha.id, "zip", pecha.pecha_path)

        blob = self.bucket.blob(Storage._pecha_opf_path(pecha_id=pecha.id))
        blob.upload_from_filename(zip_path)
        blob.make_public()
        logger.info("Uploaded to storage: %s", blob.public_url)

        return blob.public_url

    def store_pecha_doc(self, pecha_id: str, doc: IO[bytes]) -> str:
        blob = self.bucket.blob(Storage._pecha_doc_path(pecha_id))
        blob.upload_from_file(doc)
        blob.make_public()
        logger.info("Uploaded to storage: %s", blob.public_url)

        return blob.public_url

    def retrieve_pechaorg_json(self, pecha_id: str) -> dict[str, Any]:
        json_bytes = self._get_file(Storage._pechaorg_json_path(pecha_id))
        json_str = json_bytes.decode("utf-8")
        return json.loads(json_str)

    def retrieve_pecha_opf(self, pecha_id: str) -> Path:
        temp_dir = tempfile.gettempdir()
        zip_path = Path(temp_dir) / f"{pecha_id}.zip"
        zip_path.write_bytes(self._get_file(Storage._pecha_opf_path(pecha_id)))

        return zip_path

    def delete_pechaorg_json(self, pecha_id: str):
        self._delete(Storage._pechaorg_json_path(pecha_id))

    def delete_pecha_opf(self, pecha_id: str):
        self._delete(Storage._pecha_opf_path(pecha_id))

    def delete_pecha_doc(self, pecha_id: str):
        self._delete(Storage._pecha_doc_path(pecha_id))

    def pechaorg_json_exists(self, pecha_id: str) -> bool:
        return self._file_exists(Storage._pechaorg_json_path(pecha_id))

    def pecha_opf_exists(self, pecha_id: str) -> bool:
        return self._file_exists(Storage._pecha_opf_path(pecha_id))

    @staticmethod
    def _pecha_opf_path(pecha_id: str) -> str:
        return f"opf/{pecha_id}.zip"

    @staticmethod
    def _pechaorg_json_path(pecha_id: str) -> str:
        return f"pechaorg/{pecha_id}.json"

    @staticmethod
    def _pecha_doc_path(pecha_id: str) -> str:
        return f"doc/{pecha_id}.docx"

    def _delete(self, storage_path):
        blob = self.bucket.blob(storage_path)
        blob.delete()
        logger.info("Rolled back: %s", blob.name)

    def _get_file(self, storage_path: str) -> bytes:
        blob = self.bucket.blob(storage_path)

        if not blob.exists():
            raise FileNotFoundError(f"File not found in storage: {storage_path}")

        file_data = blob.download_as_bytes()
        logger.info("Retrieved from storage: %s, size: %s", storage_path, len(file_data))
        return file_data

    def _file_exists(self, storage_path: str) -> bool:
        return self.bucket.blob(storage_path).exists()
