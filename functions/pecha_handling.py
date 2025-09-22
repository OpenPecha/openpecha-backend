import logging
import tempfile
import zipfile
from enum import Enum, auto
from pathlib import Path
from typing import Any

from category_model import CategoryModel
from database import Database
from metadata_model import MetadataModel, TextType
from openpecha.pecha import Pecha
from openpecha.pecha.annotations import AnnotationModel, PechaAlignment
from openpecha.pecha.layer import AnnotationType
from openpecha.pecha.parsers.docx import DocxParser

# from openpecha.pecha.serializers import SerializerLogicHandler
from storage import Storage
from werkzeug.datastructures import FileStorage

# from openpecha.pecha.parsers.ocr import BdrcParser


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
    logger.info("Looking for pecha at: %s", pecha_path)
    logger.info("Pecha path exists: %s", pecha_path.exists())

    if pecha_path.exists():
        contents = list(pecha_path.iterdir())
        logger.info("Pecha directory contents: %s", contents)
        base_exists = (pecha_path / "base").exists()
        layers_exists = (pecha_path / "layers").exists()
        logger.info("Base dir exists: %s, Layers dir exists: %s", base_exists, layers_exists)

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


def parse(
    docx_file: FileStorage, annotation_type: AnnotationType, metadata: MetadataModel, pecha_id: str | None = None
) -> Pecha:
    if not docx_file.filename:
        raise ValueError("Docx file has no filename")

    logger.info("Parsing docx file: %s", docx_file.filename)

    path = create_tmp()
    docx_file.save(path)

    metadatas = [md for _, md in get_metadata_tree(pecha_id=pecha_id, metadata=metadata)]

    return DocxParser().parse(
        docx_file=path,
        metadatas=metadatas,
        pecha_id=pecha_id,
        annotation_type=annotation_type,
    )


# def parse_bdrc(data: FileStorage, metadata: MetadataModel, pecha_id: str | None = None) -> Pecha:
#     if not data.filename:
#         raise ValueError("Data has no filename")

#     logger.info("Parsing data file: %s", data.filename)

#     path = create_tmp()
#     data.save(path)

#     return BdrcParser().parse(
#         input=path,
#         metadata=metadata.model_dump(),
#         pecha_id=pecha_id,
#     )


def get_category_chain(category_id: str) -> list[CategoryModel]:
    categories: list[CategoryModel] = []
    current_id: str | None = category_id

    while current_id:
        category = Database().get_category(category_id=current_id)
        categories.insert(0, category)
        current_id = category.parent

    return categories


def serialize(pecha: Pecha, annotation: AnnotationModel, base_language: str) -> dict[str, Any]:
    metadatas = get_metadata_tree(pecha_id=pecha.id, traversal_mode=TraversalMode.FULL_TREE)
    logger.info("Metadata chain retrieved with metadatas: %s", metadatas)
    if metadatas is None:
        raise ValueError("No metadata found for Pecha")

    logger.info("Starting to serialize pecha %s", pecha.id)

    category_id = metadatas[0][1].category
    if category_id is None:
        raise ValueError("No category found in metadata")

    category_chain = get_category_chain(category_id)
    logger.info("Category chain retrieved with %d categories", len(category_chain))
    logger.info("Category Chain: %s", category_chain)

    ids = [id for id, _ in metadatas]
    logger.info("Pecha IDs: %s", ", ".join(ids))

    pechas = get_pecha_chain(pecha_ids=ids)
    logger.info("Pechas: %s", list(pechas.keys()))

    annotations = get_annotation_chain(pecha_ids=ids)
    logger.info(
        "Annotations: %s", [f"{pecha_id} {ann.title}" for pecha_id, anns in annotations.items() for ann in anns]
    )

    # return SerializerLogicHandler().serialize(
    #     pechatree=pechas,
    #     metadatatree=metadatas,
    #     annotations=annotations,
    #     pecha_category=[CategoryModel.model_dump(c) for c in category_chain],
    #     annotation_path=annotation.path,
    #     base_language=base_language,
    # )

    return []


def process_pecha(
    text: FileStorage, metadata: MetadataModel, aligned_to: AnnotationModel | None = None, pecha_id: str | None = None
) -> str:
    """
    Handles Pecha processing: parsing, alignment, serialization, storage, and database transactions.
    """
    logger.info("Processing pecha with aligned to: %s", aligned_to)
    annotation_type = AnnotationType.ALIGNMENT if aligned_to else AnnotationType.SEGMENTATION
    pecha, annotation_path = parse(
        docx_file=text, annotation_type=annotation_type, pecha_id=pecha_id, metadata=metadata
    )
    logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)
    logger.info("Annotation path: %s", annotation_path)

    annotation_type_name = "alignment" if annotation_type == AnnotationType.ALIGNMENT else "segmentation"
    annotation = AnnotationModel(
        pecha_id=pecha.id,
        document_id=text.filename,
        title=f"Default display - {annotation_type_name}",
        path=annotation_path,
        type=annotation_type,
        aligned_to=PechaAlignment(pecha_id=aligned_to.pecha_id, alignment_id=aligned_to.path) if aligned_to else None,
    )

    storage = Storage()

    stream = text.stream
    stream.seek(0)
    storage.store_pecha_doc(pecha_id=pecha.id, doc=stream)
    storage.store_pecha_opf(pecha)

    database = Database()
    database.set_metadata(pecha_id=pecha.id, metadata=metadata)
    annotation_id = database.add_annotation(annotation=annotation)

    logger.info("Annotation added successfully: %s", annotation_id)

    return pecha.id


# def process_bdrc_pecha(data: FileStorage, metadata: MetadataModel, pecha_id: str | None = None) -> str:
#     pecha = parse_bdrc(data=data, metadata=metadata, pecha_id=pecha_id)
#     logger.info("Pecha created: %s %s", pecha.id, pecha.pecha_path)

#     storage = Storage()

#     stream = data.stream
#     stream.seek(0)
#     storage.store_bdrc_data(pecha_id=pecha.id, bdrc_data=stream)
#     storage.store_pecha_opf(pecha)

#     Database().set_metadata(pecha_id=pecha.id, metadata=metadata)

#     return pecha.id
