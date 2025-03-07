import json
import logging
import tempfile
import zipfile
from pathlib import Path
from typing import Any

from firebase_config import db
from openpecha.pecha import Pecha
from openpecha.pecha.parsers.docx import DocxParser
from openpecha.pecha.serializers.pecha_db import Serializer
from openpecha.pecha.serializers.pecha_db.updated_opf_serializer import update_serialize_json
from storage import Storage
from werkzeug.datastructures import FileStorage

logger = logging.getLogger(__name__)


# def db_get_alignment(pecha_id: str) -> dict[str, Any]:
#     return db.collection("alignment").document(pecha_id).get().to_dict()


def db_get_metadata(pecha_id: str) -> dict[str, Any]:
    return db.collection("metadata").document(pecha_id).get().to_dict()


def get_metadata_chain(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    chain = [metadata]
    while (next_id := next(filter(metadata.get, ("commentary_of", "version_of", "translation_of")), None)) and (
        metadata := db_get_metadata(next_id)
    ):
        chain.append(metadata)

    return chain


def get_id_metadata_chain(pecha_id: str, metadata: dict[str, Any]) -> list[tuple[str, dict[str, Any]]]:
    chain = [(pecha_id, metadata)]

    logger.info("Starting metadata chain traversal from Pecha ID: %s", pecha_id)

    next_id = metadata.get("commentary_of") or metadata.get("version_of") or metadata.get("translation_of")

    while next_id:
        metadata = db_get_metadata(next_id)

        if not metadata:
            raise ValueError(f"Metadata for ID {next_id} not found.")

        logger.info("Traversing to Pecha ID: %s", next_id)
        chain.append((next_id, metadata))

        next_id = metadata.get("commentary_of") or metadata.get("version_of") or metadata.get("translation_of")

    logger.info("Metadata Chain: %s", [(pecha_id, metadata) for pecha_id, metadata in chain])

    return chain


def retrieve_pecha(pecha_id) -> Pecha:
    zip_path = Storage().retrieve_pecha_opf(pecha_id)

    temp_dir = tempfile.gettempdir()
    extract_path = Path(temp_dir) / pecha_id
    zipfile.ZipFile(zip_path).extractall(extract_path)
    return Pecha.from_path(extract_path)


def get_pecha_chain(pecha_ids: list[str]) -> list[Pecha]:
    return [retrieve_pecha(pecha_id=pecha_id) for pecha_id in pecha_ids]


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
        pecha_id=pecha_id,
    )


def serialize(pecha: Pecha) -> dict[str, Any]:
    # alignment = db_get_alignment(pecha_id=pecha.id)
    metadata = db_get_metadata(pecha_id=pecha.id)

    id_metadata_chain = get_id_metadata_chain(pecha_id=pecha.id, metadata=metadata)
    metadata_chain = [md for _, md in id_metadata_chain]

    storage = Storage()
    if storage.pechaorg_json_exists(pecha_id=pecha.id):
        pecha_json = storage.retrieve_pechaorg_json(pecha_id=pecha.id)

        logger.info("Serialized Pecha %s already exist, updating the json", pecha.id)
        return update_serialize_json(pecha=pecha, metadatas=metadata_chain, json=pecha_json)

    id_chain = [id for id, _ in id_metadata_chain]
    logger.info("Pecha IDs: %s", ", ".join(id_chain))
    pecha_chain = get_pecha_chain(pecha_ids=id_chain)

    logger.info("Serialized Pecha %s doesn't exist, starting serialize", pecha.id)
    logger.info("Pechas: %s", [pecha.id for pecha in pecha_chain])

    return Serializer().serialize(pechas=pecha_chain, metadatas=metadata_chain)


def process_pecha(
    text: FileStorage, metadata: dict[str, Any], pecha_id: str | None = None
) -> tuple[str | None, str | None]:
    """
    Handles Pecha processing: parsing, alignment, serialization, storage, and database transactions.

    Returns:
        - `(None, pecha.id)` if successful.
        - `("Error message", None)` if an error occurs.
    """
    try:
        pecha = parse(docx_file=text, metadata=metadata, pecha_id=pecha_id)

        logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)
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
            # doc_ref_alignment = db.collection("alignment").document(pecha.id)

            logger.info("Saving metadata to DB: %s", json.dumps(metadata))
            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to DB: %s", pecha.id)

            # if alignment:
            #     logger.info("Saving alignment to DB: %s", json.dumps(alignment))
            #     transaction.set(doc_ref_alignment, alignment)

            # logger.info("Alignment saved to DB: %s", pecha.id)

    except Exception as e:
        logger.error("Error saving to DB: %s", e)
        try:
            storage.rollback_pecha_opf(pecha_id=pecha.id)
        except Exception as rollback_error:
            logger.error("Rollback failed: %s", rollback_error)

        return f"Failed to save to DB {str(e)} metadata: {metadata}", None

    return None, pecha.id
