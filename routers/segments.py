import logging
from typing import TYPE_CHECKING, Annotated

import httpx
from fastapi import APIRouter, Depends, Path, Query, status

from config import settings
from dependencies import OptionalAppHeader, get_api_key, get_db, get_storage
from exceptions import DataNotFoundError, InvalidRequestError
from models.annotation import SegmentOutput
from models.requests import PaginationParams, SegmentsQueryParams
from models.responses import PaginatedResponse
from models.search import SearchFilter, SearchResponse, SearchResult

if TYPE_CHECKING:
    from database import Database
    from storage import Storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/segments", tags=["Segments"])


@router.get(
    "/{segment_id}/related",
    summary="Get related segments",
    description="Retrieve segments related to a given segment.",
)
async def get_related(
    segment_id: Annotated[str, Path(description="The ID of the segment")],
    params: Annotated[PaginationParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: OptionalAppHeader = None,
) -> PaginatedResponse[SegmentOutput]:
    """Get related segments."""
    try:
        segment = await db.segment.get(segment_id)
    except DataNotFoundError:
        return PaginatedResponse.from_items([], offset=params.offset, limit=params.limit)
    segments = await db.segment.get_related(
        edition_id=segment.edition_id,
        spans=[(segment.span.start, segment.span.end)],
        application=x_application,
        offset=params.offset,
        limit=params.limit + 1,
    )
    return PaginatedResponse.from_items(segments, offset=params.offset, limit=params.limit)


@router.get(
    "/{segment_id}/content",
    summary="Get segment content",
    description="Retrieve the text content of a segment.",
)
async def get_segment_content(
    segment_id: Annotated[str, Path(description="The ID of the segment")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    storage: Annotated[Storage, Depends(get_storage)],
    x_application: OptionalAppHeader = None,
) -> str:
    """Get segment content."""
    segment = await db.segment.get(segment_id, application=x_application)
    base_text = await storage.retrieve_base_text(
        text_id=segment.text_id,
        edition_id=segment.edition_id,
    )
    return base_text[segment.span.start : segment.span.end]


@router.post(
    "/{segment_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Add tag to segment",
    description="Add a tag to a segment.",
)
async def tag_segment(
    segment_id: Annotated[str, Path(description="The ID of the segment")],
    tag_id: Annotated[str, Path(description="The ID of the tag")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Add a tag to a segment."""
    await db.tag.tag_segment(segment_id, tag_id)


@router.delete(
    "/{segment_id}/tags/{tag_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Remove tag from segment",
    description="Remove a tag from a segment.",
)
async def untag_segment(
    segment_id: Annotated[str, Path(description="The ID of the segment")],
    tag_id: Annotated[str, Path(description="The ID of the tag")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> None:
    """Remove a tag from a segment."""
    await db.tag.untag_segment(segment_id, tag_id)


@router.get(
    "/search",
    summary="Search segments",
    description="Search segments using the external search API.",
)
async def search_segments(
    query: Annotated[str, Query(description="Search query")],
    params: Annotated[SegmentsQueryParams, Query()],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
) -> SearchResponse:
    """Search segments."""
    filter_obj = SearchFilter(title=params.title) if params.title else None

    try:
        logger.info("Forwarding search request to %s/search", settings.search_api_url)

        request_params = {
            "query": query,
            "search_type": params.search_type,
            "limit": params.limit,
            "return_text": params.return_text,
        }
        if filter_obj and filter_obj.title:
            request_params["title"] = filter_obj.title

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(f"{settings.search_api_url}/search", params=request_params)
            response.raise_for_status()
            search_response_data = response.json()

    except httpx.RequestError:
        logger.exception("Error calling search API")
        raise InvalidRequestError("Failed to call search API") from None

    enriched_results = []

    for result_item in search_response_data.get("results", []):
        segment_id = result_item.get("id")
        if not segment_id:
            enriched_results.append(
                SearchResult(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )
            continue

        try:
            segment = await db.segment.get(segment_id)
            segmentation_ids = await db.segment.find_by_span(
                edition_id=segment.edition_id,
                start=segment.span.start,
                end=segment.span.end,
            )
            enriched_results.append(
                SearchResult(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=segmentation_ids,
                )
            )
        except DataNotFoundError:
            logger.warning("Segment %s not found, skipping segmentation mapping", segment_id)
            enriched_results.append(
                SearchResult(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )
        except Exception:
            logger.exception("Error processing segment %s", segment_id)
            enriched_results.append(
                SearchResult(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )

    return SearchResponse(
        query=search_response_data.get("query", query),
        results=enriched_results,
        count=len(enriched_results),
    )
