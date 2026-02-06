import logging
import tempfile
from pathlib import Path

from exceptions import DataNotFoundError
from firebase_admin import storage
from google.cloud.storage.blob import Blob

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self) -> None:
        self.bucket = storage.bucket()

    def store_base_text(self, expression_id: str, manifestation_id: str, base_text: str) -> str:
        # Write base_text to temp file for streaming upload
        temp_dir = Path(tempfile.gettempdir())
        temp_file = temp_dir / f"{expression_id}_{manifestation_id}.txt"

        try:
            temp_file.write_text(base_text, encoding="utf-8")

            blob = self._blob(Storage._base_text_path(expression_id, manifestation_id))
            blob.upload_from_filename(str(temp_file))
            logger.info("Uploaded base text to storage: %s", blob.public_url)
            blob.make_public()

            return blob.public_url
        finally:
            # Clean up temp file
            if temp_file.exists():
                temp_file.unlink()

    def delete_base_text(self, expression_id: str, manifestation_id: str) -> None:
        self._delete(Storage._base_text_path(expression_id, manifestation_id))

    def rollback_base_text(self, expression_id: str, manifestation_id: str) -> None:
        self._rollback(Storage._base_text_path(expression_id, manifestation_id))

    def base_text_exists(self, expression_id: str, manifestation_id: str) -> bool:
        return self._file_exists(Storage._base_text_path(expression_id, manifestation_id))

    @staticmethod
    def _base_text_path(expression_id: str, manifestation_id: str) -> str:
        return f"base_texts/{expression_id}/{manifestation_id}.txt"

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
            raise DataNotFoundError(f"File not found in storage: {storage_path}")

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
            raise DataNotFoundError(f"File not found in storage: {storage_path}")
        logger.info("Retrieving file from storage")
        file_data: bytes = blob.download_as_bytes()
        logger.info("Retrieved from storage: %s, size: %s", storage_path, len(file_data))
        return file_data

    def _file_exists(self, storage_path: str) -> bool:
        return self.bucket.blob(storage_path).exists()

    def retrieve_base_text(self, expression_id: str, manifestation_id: str) -> str:
        """Fetch base text content from Firebase Storage.

        Expects the file stored at base_texts/{expression_id}/{manifestation_id}.txt
        (consistent with existing storage utilities).
        """
        data = self._get_file(Storage._base_text_path(expression_id, manifestation_id))
        return data.decode("utf-8")

    def update_base_text_range(
        self,
        expression_id: str,
        manifestation_id: str,
        start: int,
        end: int,
        new_content: str,
    ) -> str:
        current_text = self.retrieve_base_text(expression_id, manifestation_id)
        updated_text = current_text[:start] + new_content + current_text[end:]
        return self.store_base_text(expression_id, manifestation_id, updated_text)

    def apply_insert(self, expression_id: str, manifestation_id: str, position: int, text: str) -> str:
        """Insert text at the specified position."""
        current_text = self.retrieve_base_text(expression_id, manifestation_id)
        updated_text = current_text[:position] + text + current_text[position:]
        return self.store_base_text(expression_id, manifestation_id, updated_text)

    def apply_delete(self, expression_id: str, manifestation_id: str, start: int, end: int) -> str:
        """Delete text in the specified range [start, end)."""
        current_text = self.retrieve_base_text(expression_id, manifestation_id)
        updated_text = current_text[:start] + current_text[end:]
        return self.store_base_text(expression_id, manifestation_id, updated_text)

    def apply_replace(self, expression_id: str, manifestation_id: str, start: int, end: int, text: str) -> str:
        """Replace text in the specified range [start, end) with new text."""
        current_text = self.retrieve_base_text(expression_id, manifestation_id)
        updated_text = current_text[:start] + text + current_text[end:]
        return self.store_base_text(expression_id, manifestation_id, updated_text)
