import logging
import shutil
import tempfile
from pathlib import Path

from firebase_admin import storage
from google.cloud.storage.blob import Blob
from openpecha.pecha import Pecha

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self) -> None:
        self.bucket = storage.bucket()

    def store_pecha(self, pecha: Pecha) -> str:
        path = Path(tempfile.gettempdir()) / pecha.id
        zip_path = shutil.make_archive(str(path), "zip", pecha.pecha_path)

        blob = self._blob(Storage._pecha_opf_path(pecha_id=pecha.id))
        blob.upload_from_filename(zip_path)
        logger.info("Uploaded to storage: %s", blob.public_url)
        blob.make_public()

        return blob.public_url

    def retrieve_pecha(self, pecha_id: str) -> Path:
        temp_dir = tempfile.gettempdir()
        zip_path = Path(temp_dir) / f"{pecha_id}.zip"
        zip_path.write_bytes(self._get_file(Storage._pecha_opf_path(pecha_id)))

        return zip_path

    def delete_pecha(self, pecha_id: str) -> None:
        self._delete(Storage._pecha_opf_path(pecha_id))

    def rollback_pecha(self, pecha_id: str) -> None:
        self._rollback(Storage._pecha_opf_path(pecha_id))

    def pecha_exists(self, pecha_id: str) -> bool:
        return self._file_exists(Storage._pecha_opf_path(pecha_id))

    @staticmethod
    def _pecha_opf_path(pecha_id: str) -> str:
        return f"opf/{pecha_id}.zip"

    def _blob(self, path: str) -> Blob:
        blob = self.bucket.blob(path)
        blob.cache_control = "no-store"

        return blob

    def _delete(self, storage_path: str) -> None:
        blob = self.bucket.blob(storage_path)
        blob.delete()
        logger.info("Rolled back: %s", blob.name)

    def _rollback(self, storage_path: str) -> None:
        self.bucket.reload()

        versions = [
            blob for blob in self.bucket.list_blobs(prefix=storage_path, versions=True) if blob.name == storage_path
        ]

        if not versions:
            raise FileNotFoundError(f"File not found in storage: {storage_path}")

        if len(versions) < 2:
            logger.warning("No previous version available to rollback for: %s", storage_path)
            return

        versions.sort(key=lambda b: int(b.generation), reverse=True)
        current_version = versions[0]
        previous_version = versions[1]

        restored_blob = self.bucket.copy_blob(previous_version, self.bucket, storage_path)

        logger.info(
            "Rolled back %s from generation %s to previous generation %s (new generation %s)",
            storage_path,
            current_version.generation,
            previous_version.generation,
            restored_blob.generation,
        )

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


class MockStorage:
    def __init__(self) -> None:
        self.bucket = storage.bucket()

    def store_text(self, expression_id: str, manifestation_id: str, base_text: str) -> str:
        # Write base_text to temp file for streaming upload
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"{expression_id}_{manifestation_id}.txt"
        
        try:
            temp_file.write_text(base_text, encoding="utf-8")
            
            blob = self._blob(MockStorage._base_text_path(expression_id, manifestation_id))
            blob.upload_from_filename(str(temp_file))
            logger.info("Uploaded base text to storage: %s", blob.public_url)
            blob.make_public()
            
            return blob.public_url
        finally:
            # Clean up temp file
            if temp_file.exists():
                temp_file.unlink()

    def delete_base_text(self, expression_id: str, manifestation_id: str) -> None:
        self._delete(MockStorage._base_text_path(expression_id, manifestation_id))

    def rollback_base_text(self, expression_id: str, manifestation_id: str) -> None:
        self._rollback(MockStorage._base_text_path(expression_id, manifestation_id))

    def base_text_exists(self, expression_id: str, manifestation_id: str) -> bool:
        return self._file_exists(MockStorage._base_text_path(expression_id, manifestation_id))

    @staticmethod
    def _base_text_path(expression_id: str, manifestation_id: str) -> str:
        return f"base/{expression_id}/{manifestation_id}.txt"

    def _blob(self, path: str) -> Blob:
        blob = self.bucket.blob(path)
        blob.cache_control = "no-store"

        return blob

    def _delete(self, storage_path: str) -> None:
        blob = self.bucket.blob(storage_path)
        blob.delete()
        logger.info("Rolled back: %s", blob.name)

    def _rollback(self, storage_path: str) -> None:
        self.bucket.reload()

        versions = [
            blob for blob in self.bucket.list_blobs(prefix=storage_path, versions=True) if blob.name == storage_path
        ]

        if not versions:
            raise FileNotFoundError(f"File not found in storage: {storage_path}")

        if len(versions) < 2:
            logger.warning("No previous version available to rollback for: %s", storage_path)
            return

        versions.sort(key=lambda b: int(b.generation), reverse=True)
        current_version = versions[0]
        previous_version = versions[1]

        restored_blob = self.bucket.copy_blob(previous_version, self.bucket, storage_path)

        logger.info(
            "Rolled back %s from generation %s to previous generation %s (new generation %s)",
            storage_path,
            current_version.generation,
            previous_version.generation,
            restored_blob.generation,
        )

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