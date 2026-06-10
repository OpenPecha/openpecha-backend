import logging
from typing import TYPE_CHECKING, Annotated

from fastapi import APIRouter, BackgroundTasks, Depends, Path, Query, status

from content_search import ContentSearchService
from dependencies import OptionalAppHeader, get_api_key, get_content_search, get_db, get_storage
from identifier import generate_id
from models.edition import EditionOutput
from models.requests import EditionRequestModel, EditionsQueryParams, TextsQueryParams
from models.responses import IdResponse, PaginatedResponse
from models.text import TextInput, TextOutput, TextPatch

if TYPE_CHECKING:
    from database import Database
    from storage import Storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/texts", tags=["Texts"])


@router.get(
    "",
    summary="List all texts",
    description="Retrieve a paginated list of texts (texts) with optional filters.",
)
async def get_all_texts(
    params: Annotated[TextsQueryParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: OptionalAppHeader = None,
) -> PaginatedResponse[TextOutput]:
    """List all texts with optional filtering."""
    texts = await db.text.get_all(
        offset=params.offset,
        limit=params.limit + 1,
        filters=params,  # TextsQueryParams inherits from TextFilter, so this works
        application=x_application,
    )
    return PaginatedResponse.from_items(texts, offset=params.offset, limit=params.limit)


@router.get(
    "/{text_id}",
    summary="Get a text",
    description="Retrieve a single text (text) by its ID.",
    responses={404: {"description": "Text not found"}},
)
async def get_text(
    text_id: Annotated[str, Path(description="The ID of the text to retrieve")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: OptionalAppHeader = None,
) -> TextOutput:
    """Fetch a text (text) by its ID."""
    return await db.text.get(text_id=text_id, application=x_application)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    summary="Create a text",
    description="Create a new text (text) with the provided data.",
)
async def create_text(
    data: TextInput,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> IdResponse:
    """Create a new text (text)."""
    text_id = await db.text.create(data)
    logger.info("Successfully created text with ID: %s", text_id)
    return IdResponse(id=text_id)


@router.get(
    "/{text_id}/editions",
    summary="List editions for a text",
    description="Retrieve all editions (editions) for a given text.",
)
async def get_editions(
    text_id: Annotated[str, Path(description="The ID of the text")],
    params: Annotated[EditionsQueryParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> list[EditionOutput]:
    """List all editions for a text."""
    return await db.edition.get_all(
        text_id=text_id,
        edition_type=params.edition_type,
    )


@router.post(
    "/{text_id}/editions",
    status_code=status.HTTP_201_CREATED,
    summary="Create an edition",
    description="Create a new edition (edition) for a text.",
)
async def create_edition(
    text_id: Annotated[str, Path(description="The ID of the text")],
    data: EditionRequestModel,
    background_tasks: BackgroundTasks,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
    content_search: Annotated[ContentSearchService, Depends(get_content_search)],
) -> IdResponse:
    """Create a new edition for a text."""
    edition_id = generate_id()

    await storage.store_base_text(
        text_id=text_id,
        edition_id=edition_id,
        base_text=data.content,
    )

    await db.edition.create(
        edition=data.metadata,
        edition_id=edition_id,
        text_id=text_id,
        pagination=data.pagination,
        segmentation=data.segmentation,
    )

    background_tasks.add_task(content_search.index_edition, edition_id, db, storage)

    return IdResponse(id=edition_id)


@router.patch(
    "/{text_id}",
    summary="Update a text",
    description="Partially update a text with the provided data.",
)
async def update_text(
    text_id: Annotated[str, Path(description="The ID of the text to update")],
    data: TextPatch,
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: OptionalAppHeader = None,
) -> TextOutput:
    """Update a text."""
    logger.info("Updating text %s with: %s", text_id, data.model_dump_json())
    return await db.text.update(text_id, data, application=x_application)


@router.delete(
    "/{text_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete text",
    description="Delete a text if it has no editions or incoming text relationships.",
)
async def delete_text(
    text_id: Annotated[str, Path(description="The ID of the text to delete")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Delete a text."""
    await db.text.delete(text_id)


@router.post(
    "/{text_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Add tag to text",
    description="Add a tag to a text.",
)
async def tag_text(
    text_id: Annotated[str, Path(description="The ID of the text")],
    tag_id: Annotated[str, Path(description="The ID of the tag to add")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Add a tag to a text."""
    work_id = await db.text.get_work_id(text_id)
    await db.tag.tag_work(work_id, tag_id)


@router.delete(
    "/{text_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove tag from text",
    description="Remove a tag from a text.",
)
async def untag_text(
    text_id: Annotated[str, Path(description="The ID of the text")],
    tag_id: Annotated[str, Path(description="The ID of the tag to remove")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Remove a tag from a text."""
    work_id = await db.text.get_work_id(text_id)
    await db.tag.untag_work(work_id, tag_id)
