"""Background tasks for the OpenPecha API."""

import logging

import httpx

logger = logging.getLogger(__name__)

SEARCH_SEGMENTER_URL = "https://sqs-search-segmenter-api.onrender.com"


def trigger_search_segmenter(edition_id: str) -> None:
    """Triggers the search segmenter API (fire-and-forget background task)."""
    url = f"{SEARCH_SEGMENTER_URL}/jobs/create"
    payload = {"edition_id": edition_id}
    try:
        with httpx.Client(timeout=10) as client:
            response = client.post(url, json=payload)
            logger.info(
                "Search segmenter API called for edition %s. Status: %s",
                edition_id,
                response.status_code,
            )
    except Exception:
        logger.exception("Failed to trigger search segmenter for %s", edition_id)


def trigger_delete_search_segments(segment_ids: list[str]) -> None:
    """Triggers the delete search segments API (fire-and-forget background task)."""
    url = f"{SEARCH_SEGMENTER_URL}/jobs/delete"
    payload = {"segment_ids": segment_ids}
    try:
        with httpx.Client(timeout=10) as client:
            response = client.post(url, json=payload)
            logger.info(
                "Delete search segments API called for segment_ids %s. Status: %s",
                segment_ids,
                response.status_code,
            )
    except Exception:
        logger.exception("Failed to trigger delete search segments for %s", segment_ids)
