import logging
import tempfile
from pathlib import Path
from typing import Any

from firebase_config import db
from openpecha.pecha import Pecha, get_pecha_alignment_data
from openpecha.pecha.parsers.google_doc import DocxParser
from storage import Storage
from werkzeug.datastructures import FileStorage

logger = logging.getLogger(__name__)


def get_metadata_chain(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    chain = [metadata]
    while (next_id := next(filter(metadata.get, ("commentary_of", "version_of", "translation_of")), None)) and (
        metadata := db.collection("metadata").document(next_id).get().to_dict()
    ):
        chain.append(metadata)

    return chain


def create_tmp() -> Path:
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        return Path(temp.name)


def parse(docx_file: FileStorage, metadata: dict[str, Any], pecha_id: str | None = None) -> Pecha:
    if not docx_file.filename:
        raise ValueError("docx_file has no filename")

    path = create_tmp()
    docx_file.save(path)

    return DocxParser().parse(
        docx_file=path,
        metadatas=get_metadata_chain(metadata=metadata),
        output_path=Path(tempfile.gettempdir()),
        pecha_id=pecha_id,
    )


def process_pecha(text: FileStorage, metadata: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Handles Pecha processing: parsing, alignment, serialization, storage, and database transactions.

    Returns:
        - `(None, pecha.id)` if successful.
        - `("Error message", None)` if an error occurs.
    """
    try:
        pecha = parse(text, metadata)

        logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

        alignment = get_pecha_alignment_data(pecha)
    except Exception as e:
        return f"Could not process metadata {str(e)}", None

    storage = Storage()

    try:
        storage.store_pecha_opf(pecha)
    except Exception as e:
        logger.error("Error saving Pecha to storage: %s", e)
        return f"Failed to save to storage {str(e)}", None

    try:
        with db.transaction() as transaction:
            doc_ref_metadata = db.collection("metadata").document(pecha.id)
            doc_ref_alignment = db.collection("alignment").document(pecha.id)

            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to DB: %s", pecha.id)

            if alignment:
                transaction.set(doc_ref_alignment, alignment)

            logger.info("Alignment saved to DB: %s", pecha.id)

    except Exception as e:
        logger.error("Error saving to DB: %s", e)
        try:
            storage.rollback_pecha_opf(pecha_id=pecha.id)
            storage.rollback_pechaorg_json(pecha_id=pecha.id)
        except Exception as rollback_error:
            logger.error("Rollback failed: %s", rollback_error)

        return f"Failed to save to DB {str(e)}", None

    return None, pecha.id


def publish_pecha(pecha_id: str):
    storage = Storage()
    serialized_json = ""
    try:
        storage.store_pechaorg_json(pecha_id=pecha_id, json_data=serialized_json)
    except Exception as e:
        logger.error("Error saving PechaOrg JSON to storage: %s", e)
        return f"Failed to save to storage {str(e)}", None
