import json
import logging
import tempfile
import zipfile
from enum import Enum, auto
from pathlib import Path
from typing import Any

from exceptions import DataNotFound
from firebase_config import db
from google.cloud.firestore_v1.base_query import FieldFilter, Or
from metadata_model import MetadataModel
from openpecha.pecha import Pecha
from openpecha.pecha.parsers.docx import DocxParser
from openpecha.pecha.parsers.ocr import BdrcParser
from openpecha.pecha.serializers.pecha_db import Serializer
from openpecha.pecha.serializers.pecha_db.updated_opf_serializer import update_serialize_json
from storage import Storage
from werkzeug.datastructures import FileStorage

logger = logging.getLogger(__name__)


class TraversalMode(Enum):
    UPWARD = auto()
    FULL_TREE = auto()


class Relationship(Enum):
    COMMENTARY = "commentary_of"
    VERSION = "version_of"
    TRANSLATION = "translation_of"


def validate_relationship(metadata: MetadataModel, parent: MetadataModel, relationship: Relationship) -> bool:
    if parent is None:
        return True

    if metadata is None:
        return False

    match relationship:
        case Relationship.COMMENTARY:
            return parent.language == metadata.language
        case Relationship.VERSION:
            return parent.language == metadata.language
        case Relationship.TRANSLATION:
            return parent.language != metadata.language


def db_get_metadata(pecha_id: str) -> dict[str, Any]:
    doc = db.collection("metadata").document(pecha_id).get()
    if not doc.exists:
        raise DataNotFound(f"Metadata not found for ID: {pecha_id}")
    return doc.to_dict()


def get_metadata_chain(
    pecha_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    traversal_mode: TraversalMode = TraversalMode.UPWARD,
    relationships: list[Relationship] | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    logger.info("Getting metadata chain for: id: %s, metadata: %s", pecha_id, metadata)

    if metadata is None and pecha_id is None:
        raise ValueError("Either metadata or pecha_id must be provided")

    if relationships is None:
        relationships = list(Relationship)

    if pecha_id is None:
        logger.info("Pecha ID not provided, using metadata")
    else:
        logger.info("Pecha ID provided: %s getting metadata from DB", pecha_id)
        metadata = db_get_metadata(pecha_id)

    ref_fields = [r.value for r in relationships]
    chain = [(pecha_id or "", metadata)]

    logger.info("Following forward references")
    current = metadata
    while next_id := next((current.get(field) for field in ref_fields if current.get(field)), None):
        if next_metadata := db_get_metadata(next_id):
            chain.append((next_id, next_metadata))
            current = next_metadata

    if traversal_mode is TraversalMode.FULL_TREE:
        root_id = chain[-1][0]
        logger.info("Starting collection of all related pechas from root: %s", root_id)
        to_process = [root_id]  # Queue of IDs to process
        processed = {root_id}  # Track processed IDs to avoid cycles

        while to_process:
            current_id = to_process.pop(0)  # Get next ID to process

            # Find all metadata that reference current_id
            docs = (
                db.collection("metadata")
                .where(filter=Or([FieldFilter(f, "==", current_id) for f in ref_fields]))
                .stream()
            )

            for doc in docs:
                if doc.id not in processed:
                    metadata = doc.to_dict()
                    if metadata is not None:
                        chain.insert(0, (doc.id, metadata))  # Add to start to maintain order
                    processed.add(doc.id)
                    to_process.append(doc.id)  # Add to queue for processing

    logger.info("Metadata chain: %s", chain)
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
        raise ValueError("Docx file has no filename")

    logger.info("Parsing docx file: %s", docx_file.filename)

    path = create_tmp()
    docx_file.save(path)

    metadatas = [md for _, md in get_metadata_chain(pecha_id=pecha_id, metadata=metadata)]

    return DocxParser().parse(
        docx_file=path,
        metadatas=metadatas,
        pecha_id=pecha_id,
    )


def parse_bdrc(data: FileStorage, metadata: dict[str, Any], pecha_id: str | None = None) -> Pecha:
    if not data.filename:
        raise ValueError("Data has no filename")

    logger.info("Parsing data file: %s", data.filename)

    path = create_tmp()
    data.save(path)

    return BdrcParser().parse(
        input=path,
        metadata=metadata,
        pecha_id=pecha_id,
    )


def get_category_chain(category_id: str) -> list[dict[str, Any]]:
    categories = []
    current_id = category_id

    while current_id:
        category_doc = db.collection("category").document(current_id).get()
        if not category_doc.exists:
            raise ValueError(f"Category with ID {current_id} not found")

        category_data = category_doc.to_dict()
        categories.insert(0, category_data)
        current_id = category_data.get("parent")

    return categories


def serialize(pecha: Pecha, reserialize: bool) -> dict[str, Any]:
    metadata_chain = get_metadata_chain(pecha_id=pecha.id)
    metadatas = [md for _, md in metadata_chain]

    if metadatas is None:
        raise ValueError("No metadata found for Pecha")

    storage = Storage()
    if storage.pechaorg_json_exists(pecha_id=pecha.id) and not reserialize:
        pecha_json = storage.retrieve_pechaorg_json(pecha_id=pecha.id)

        logger.info("Serialized Pecha %s already exist, updating the json", pecha.id)
        return update_serialize_json(pecha=pecha, metadatas=metadatas, json=pecha_json)

    logger.info("Serialized Pecha %s doesn't exist, starting serialize", pecha.id)

    category_id = metadatas[0].get("category")
    if category_id is None:
        raise ValueError("No category found in metadata")

    # Build the category chain from the given category to the root
    category_chain = get_category_chain(category_id)
    logger.info("Category chain retrieved with %d categories", len(category_chain))

    id_chain = [id for id, _ in metadata_chain]
    logger.info("Pecha IDs: %s", ", ".join(id_chain))

    pecha_chain = get_pecha_chain(pecha_ids=id_chain)
    logger.info("Pechas: %s", [pecha.id for pecha in pecha_chain])

    return Serializer().serialize(pechas=pecha_chain, metadatas=metadatas, pecha_category=category_chain)


def process_pecha(text: FileStorage, metadata: dict[str, Any], pecha_id: str | None = None) -> str:
    """
    Handles Pecha processing: parsing, alignment, serialization, storage, and database transactions.

    Returns:
        - `pecha.id` if successful.

    Raises:
        - Exception if an error occurs during processing.
    """
    pecha = parse(docx_file=text, pecha_id=pecha_id, metadata=metadata)
    logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

    storage = Storage()

    stream = text.stream
    stream.seek(0)
    storage.store_pecha_doc(pecha_id=pecha.id, doc=stream)
    storage.store_pecha_opf(pecha)

    try:
        with db.transaction() as transaction:
            doc_ref_metadata = db.collection("metadata").document(pecha.id)
            # doc_ref_alignment = db.collection("alignment").document(pecha.id)

            logger.info("Saving metadata to DB: %s", json.dumps(metadata, ensure_ascii=False))
            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to DB: %s", pecha.id)
    except Exception as e:
        logger.error("Error saving to DB: %s", e)
        try:
            storage.delete_pecha_opf(pecha_id=pecha.id)
        except Exception as rollback_error:
            logger.error("Rollback failed: %s", rollback_error)

        raise

    return pecha.id


def process_bdrc_pecha(data: FileStorage, metadata: dict[str, Any], pecha_id: str | None = None) -> str:
    pecha = parse_bdrc(data=data, metadata=metadata, pecha_id=pecha_id)
    logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

    storage = Storage()

    stream = data.stream
    stream.seek(0)
    storage.store_bdrc_data(pecha_id=pecha.id, bdrc_data=stream)
    storage.store_pecha_opf(pecha)

    try:
        with db.transaction() as transaction:
            doc_ref_metadata = db.collection("metadata").document(pecha.id)
            # doc_ref_alignment = db.collection("alignment").document(pecha.id)

            logger.info("Saving metadata to DB: %s", json.dumps(metadata, ensure_ascii=False))
            transaction.set(doc_ref_metadata, metadata)
            logger.info("Metadata saved to DB: %s", pecha.id)
    except Exception as e:
        logger.error("Error saving to DB: %s", e)
        try:
            storage.delete_pecha_opf(pecha_id=pecha.id)
        except Exception as rollback_error:
            logger.error("Rollback failed: %s", rollback_error)

        raise

    return pecha.id
