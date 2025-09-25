import logging
import tempfile
import zipfile
from enum import Enum, auto
from pathlib import Path

from database import Database
from metadata_model import MetadataModel, TextType
from openpecha.pecha import Pecha
from openpecha.pecha.annotations import AnnotationModel
from storage import Storage

logger = logging.getLogger(__name__)


class TraversalMode(Enum):
    UPWARD = auto()
    FULL_TREE = auto()


def validate_relationship(metadata: MetadataModel, parent: MetadataModel, relationship: TextType) -> bool:
    if parent is None:
        return True

    if metadata is None:
        return False

    match relationship:
        case TextType.COMMENTARY:
            return parent.language == metadata.language
        case TextType.VERSION:
            return parent.language == metadata.language
        case TextType.TRANSLATION:
            return parent.language != metadata.language


def get_metadata_tree(
    pecha_id: str | None = None,
    metadata: MetadataModel | None = None,
    traversal_mode: TraversalMode = TraversalMode.UPWARD,
    relationships: list[TextType] | None = None,
) -> list[tuple[str, MetadataModel]]:
    logger.info("Getting metadata chain for: id: %s, metadata: %s", pecha_id, metadata)

    if relationships is None:
        relationships = list(TextType)

    database = Database()

    if pecha_id is None:
        logger.info("Pecha ID not provided, using metadata")
    else:
        logger.info("Pecha ID provided: %s getting metadata from DB", pecha_id)
        metadata = database.get_metadata(pecha_id)

    if metadata is None:
        raise ValueError("Either metadata or pecha_id must be provided")

    chain = [(pecha_id or "", metadata)]

    logger.info("Following forward references")
    current = metadata
    while parent_id := current.parent:
        parent_metadata = database.get_metadata(parent_id)
        chain.append((parent_id, parent_metadata))
        current = parent_metadata

    if traversal_mode is TraversalMode.FULL_TREE:
        root_id = chain[-1][0]
        logger.info("Starting collection of all related pechas from root: %s", root_id)
        # Initialize processed with all IDs already in the chain to avoid duplicates
        processed = {id for id, _ in chain if id}
        to_process = [root_id]  # Queue of IDs to process

        while to_process:
            current_id = to_process.pop(0)  # Get next ID to process

            children = database.get_children_metadata(current_id, relationships)

            for child_id, child_metadata in children.items():
                if child_id not in processed:
                    chain.insert(0, (child_id, child_metadata))  # Add to start to maintain order
                    processed.add(child_id)
                    to_process.append(child_id)  # Add to queue for processing

    logger.info("Metadata chain: %s", chain)
    return chain


def retrieve_pecha(pecha_id: str) -> Pecha:
    zip_path = Storage().retrieve_pecha_opf(pecha_id)

    temp_dir = tempfile.gettempdir()
    extract_path = Path(temp_dir) / "pecha_extracts"
    extract_path.mkdir(exist_ok=True)

    pecha_path = extract_path / pecha_id
    pecha_path.mkdir(exist_ok=True)

    with zipfile.ZipFile(zip_path) as zip_file:
        logger.info("Extracting ZIP to: %s", pecha_path)
        zip_file.extractall(pecha_path)

    if not (pecha_path / "base").exists():
        logger.warning("Base directory not found in zip of pecha %s, checking one level down", pecha_id)
        subdirs = [d for d in pecha_path.iterdir() if d.is_dir()]
        if subdirs and (subdirs[0] / "base").exists():
            pecha_path = subdirs[0]

    logger.info("Using pecha path: %s", pecha_path)
    return Pecha.from_path(pecha_path)


def get_pecha_chain(pecha_ids: list[str]) -> dict[str, Pecha]:
    return {pecha_id: retrieve_pecha(pecha_id=pecha_id) for pecha_id in pecha_ids}


def get_annotation_chain(pecha_ids: list[str]) -> dict[str, list[AnnotationModel]]:
    annotations = dict[str, list[AnnotationModel]]()
    for pecha_id in pecha_ids:
        annotations_dict = Database().get_annotation_by_field("pecha_id", pecha_id)
        annotations[pecha_id] = list(annotations_dict.values())
    return annotations


def create_tmp() -> Path:
    with tempfile.NamedTemporaryFile(delete=False) as temp:
        return Path(temp.name)
