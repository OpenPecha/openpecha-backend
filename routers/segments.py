import logging
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, Header, Path, Query, status

from database import Database
from dependencies import get_api_key, get_db, get_storage
from exceptions import DataNotFoundError, InvalidRequestError
from models import SearchFilterModel, SearchResponseModel, SearchResultModel, SegmentOutput
from storage_s3 import Storage

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v2/segments", tags=["Segments"])

SEARCH_API_URL = "https://openpecha-search.onrender.com"


@router.get(
    "/{segment_id}/related",
    summary="Get related segments",
    description="Retrieve segments related to a given segment.",
)
async def get_related(
    segment_id: Annotated[str, Path(description="The ID of the segment")],
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    x_application: Annotated[str | None, Header(alias="X-Application")] = None,
) -> list[SegmentOutput]:
    """Get related segments."""
    return await db.segment.get_related(segment_id, application=x_application)


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
    x_application: Annotated[str | None, Header(alias="X-Application")] = None,
) -> str:
    """Get segment content."""
    segment = await db.segment.get(segment_id, application=x_application)
    base_text = await storage.retrieve_base_text(
        expression_id=segment.text_id,
        manifestation_id=segment.manifestation_id,
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
    _api_key: Annotated[str, Depends(get_api_key)],
    db: Annotated[Database, Depends(get_db)],
    search_type: str = "semantic",
    limit: int = 10,
    *,
    return_text: bool = True,
    title: str | None = None,
) -> SearchResponseModel:
    """Search segments."""
    filter_obj = SearchFilterModel(title=title) if title else None

    try:
        logger.info("Forwarding search request to %s/search", SEARCH_API_URL)

        params = {
            "query": query,
            "search_type": search_type,
            "limit": limit,
            "return_text": return_text,
        }
        if filter_obj and filter_obj.title:
            params["title"] = filter_obj.title

        async with httpx.AsyncClient(timeout=60) as client:
            response = await client.get(f"{SEARCH_API_URL}/search", params=params)
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
                SearchResultModel(
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
                manifestation_id=segment.manifestation_id,
                start=segment.span.start,
                end=segment.span.end,
            )
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=segmentation_ids,
                )
            )
        except DataNotFoundError:
            logger.warning("Segment %s not found, skipping segmentation mapping", segment_id)
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )
        except Exception:
            logger.exception("Error processing segment %s", segment_id)
            enriched_results.append(
                SearchResultModel(
                    id=result_item.get("id", ""),
                    distance=result_item.get("distance", 0.0),
                    entity=result_item.get("entity", {}),
                    segmentation_ids=[],
                )
            )

    return SearchResponseModel(
        query=search_response_data.get("query", query),
        search_type=search_response_data.get("search_type", search_type),
        results=enriched_results,
        count=len(enriched_results),
    )
