import json
import logging
import shutil
import tempfile
from pathlib import Path
from typing import IO, Any

from firebase_admin import storage
from google.cloud.storage.blob import Blob
from openpecha.pecha import Pecha

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self):
        self.bucket = storage.bucket()

    def store_pecha_json(self, pecha_id: str, base_language: str, json_dict: dict[str, Any]) -> str:
        json_str = json.dumps(json_dict, ensure_ascii=False)

        blob = self._blob(Storage._pecha_json_path(pecha_id, base_language))
        blob.upload_from_string(json_str, content_type="application/json")
        logger.info("Uploaded to storage: %s", blob.public_url)
        blob.make_public()

        return blob.public_url

    def store_pecha_opf(self, pecha: Pecha) -> str:
        path = Path(tempfile.gettempdir()) / pecha.id
        zip_path = shutil.make_archive(str(path), "zip", pecha.pecha_path)

        blob = self._blob(Storage._pecha_opf_path(pecha_id=pecha.id))
        blob.upload_from_filename(zip_path)
        logger.info("Uploaded to storage: %s", blob.public_url)
        blob.make_public()

        return blob.public_url

    def store_pecha_doc(self, pecha_id: str, doc: IO[bytes]) -> str:
        blob = self._blob(Storage._pecha_doc_path(pecha_id))
        blob.upload_from_file(doc)
        logger.info("Uploaded to storage: %s", blob.public_url)
        blob.make_public()

        return blob.public_url

    def store_bdrc_data(self, pecha_id: str, bdrc_data: IO[bytes]) -> str:
        blob = self._blob(Storage._pecha_bdrc_path(pecha_id))
        blob.upload_from_file(bdrc_data)
        logger.info("Uploaded to storage: %s", blob.public_url)
        blob.make_public()

        return blob.public_url

    def retrieve_pecha_opf(self, pecha_id: str) -> Path:
        temp_dir = tempfile.gettempdir()
        zip_path = Path(temp_dir) / f"{pecha_id}.zip"
        zip_path.write_bytes(self._get_file(Storage._pecha_opf_path(pecha_id)))

        return zip_path

    def delete_pecha_json(self, pecha_id: str):
        self._delete(f"json/{pecha_id}")

    def delete_pecha_opf(self, pecha_id: str):
        self._delete(Storage._pecha_opf_path(pecha_id))

    def delete_pecha_doc(self, pecha_id: str):
        self._delete(Storage._pecha_doc_path(pecha_id))

    def pecha_opf_exists(self, pecha_id: str) -> bool:
        return self._file_exists(Storage._pecha_opf_path(pecha_id))

    @staticmethod
    def _pecha_opf_path(pecha_id: str) -> str:
        return f"opf/{pecha_id}.zip"

    @staticmethod
    def _pecha_json_path(pecha_id: str, base_language: str | None = None) -> str:
        return f"json/{pecha_id}/{base_language}.json"

    @staticmethod
    def _pecha_doc_path(pecha_id: str) -> str:
        return f"doc/{pecha_id}.docx"

    @staticmethod
    def _pecha_bdrc_path(pecha_id: str) -> str:
        return f"bdrc/{pecha_id}_bdrc_ocr.zip"

    def _blob(self, path: str) -> Blob:
        blob = self.bucket.blob(path)
        blob.cache_control = "no-cache, no-store, must-revalidate"

        return blob

    def _delete(self, storage_path):
        blob = self.bucket.blob(storage_path)
        blob.delete()
        logger.info("Rolled back: %s", blob.name)

    def _get_file(self, storage_path: str) -> bytes:
        blob = self.bucket.blob(storage_path)
        blob.reload()

        if not blob.exists():
            raise FileNotFoundError(f"File not found in storage: {storage_path}")

        file_data = blob.download_as_bytes()
        logger.info("Retrieved from storage: %s, size: %s", storage_path, len(file_data))
        return file_data

    def _file_exists(self, storage_path: str) -> bool:
        return self.bucket.blob(storage_path).exists()
