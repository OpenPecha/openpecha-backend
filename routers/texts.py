import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Header, Path, Query, status

from background_tasks import trigger_search_segmenter
from database import Database
from dependencies import get_api_key, get_db, get_storage
from identifier import generate_id
from models import (
    ExpressionInput,
    ExpressionOutput,
    ExpressionPatch,
    IdResponse,
    ManifestationOutput,
    ManifestationType,
)
from request_models import EditionRequestModel, TextsQueryParams
from storage_s3 import Storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/texts", tags=["Texts"])


@router.get(
    "",
    summary="List all texts",
    description="Retrieve a paginated list of texts (expressions) with optional filters.",
)
async def get_all_texts(
    params: Annotated[TextsQueryParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str | None, Header(alias="X-Application")] = None,
) -> list[ExpressionOutput]:
    """List all texts with optional filtering."""
    return await db.expression.get_all(
        offset=params.offset,
        limit=params.limit,
        filters=params,
        application=x_application,
    )


@router.get(
    "/{expression_id}",
    summary="Get a text",
    description="Retrieve a single text (expression) by its ID.",
    responses={404: {"description": "Text not found"}},
)
async def get_text(
    expression_id: Annotated[str, Path(description="The ID of the text to retrieve")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str | None, Header(alias="X-Application")] = None,
) -> ExpressionOutput:
    """Fetch a text (expression) by its ID."""
    return await db.expression.get(expression_id=expression_id, application=x_application)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Create a text",
    description="Create a new text (expression) with the provided data.",
)
async def create_text(
    data: ExpressionInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    """Create a new text (expression)."""
    expression_id = await db.expression.create(data)
    logger.info("Successfully created expression with ID: %s", expression_id)
    return IdResponse(id=expression_id)


@router.get(
    "/{expression_id}/editions",
    summary="List editions for a text",
    description="Retrieve all editions (manifestations) for a given text.",
)
async def get_editions(
    expression_id: Annotated[str, Path(description="The ID of the text")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    edition_type: ManifestationType | None = None,
) -> list[ManifestationOutput]:
    """List all editions for a text."""
    return await db.manifestation.get_all(
        expression_id=expression_id,
        manifestation_type=edition_type,
    )


@router.post(
    "/{expression_id}/editions",
    status_code=status.HTTP_201_CREATED,
    summary="Create an edition",
    description="Create a new edition (manifestation) for a text.",
)
async def create_edition(
    expression_id: Annotated[str, Path(description="The ID of the text")],
    data: EditionRequestModel,
    background_tasks: BackgroundTasks,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
) -> IdResponse:
    """Create a new edition for a text."""
    manifestation_id = generate_id()

    await storage.store_base_text(
        expression_id=expression_id,
        manifestation_id=manifestation_id,
        base_text=data.content,
    )

    await db.manifestation.create(
        manifestation=data.metadata,
        manifestation_id=manifestation_id,
        expression_id=expression_id,
        pagination=data.pagination,
        segmentation=data.segmentation,
    )

    background_tasks.add_task(trigger_search_segmenter, manifestation_id)

    return IdResponse(id=manifestation_id)


@router.patch(
    "/{expression_id}",
    summary="Update a text",
    description="Partially update a text (expression) with the provided data.",
)
async def update_text(
    expression_id: Annotated[str, Path(description="The ID of the text to update")],
    data: ExpressionPatch,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str | None, Header(alias="X-Application")] = None,
) -> ExpressionOutput:
    """Update a text (expression)."""
    logger.info("Updating text %s with: %s", expression_id, data.model_dump_json())
    return await db.expression.update(expression_id, data, application=x_application)


@router.post(
    "/{expression_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Add tag to text",
    description="Add a tag to a text.",
)
async def tag_text(
    expression_id: Annotated[str, Path(description="The ID of the text")],
    tag_id: Annotated[str, Path(description="The ID of the tag to add")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Add a tag to a text."""
    work_id = await db.expression.get_work_id(expression_id)
    await db.tag.tag_work(work_id, tag_id)


@router.delete(
    "/{expression_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove tag from text",
    description="Remove a tag from a text.",
)
async def untag_text(
    expression_id: Annotated[str, Path(description="The ID of the text")],
    tag_id: Annotated[str, Path(description="The ID of the tag to remove")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Remove a tag from a text."""
    work_id = await db.expression.get_work_id(expression_id)
    await db.tag.untag_work(work_id, tag_id)
