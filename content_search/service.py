from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from content_search.opensearch_client import ContentSearchOpenSearchClient
from models.annotation import SegmentWithContextOutput
from models.content_search import (
    ContentSearchResponse,
    ContentSearchResult,
    ContentSearchSegment,
    ContentSearchSpan,
)

if TYPE_CHECKING:
    from database import Database
    from storage import Storage

logger = logging.getLogger(__name__)

DEFAULT_CONTEXT_CHARS = 120
EXACT_MAX_GRAM = 64
SEGMENT_PAGE_SIZE = 1000


class ContentSearchService:
    def __init__(
        self,
        *,
        endpoint: str,
        index_name: str,
        region: str,
        auth_mode: str = "basic",
        username: str = "",
        password: str = "",
        context_chars: int = DEFAULT_CONTEXT_CHARS,
    ) -> None:
        self._client = ContentSearchOpenSearchClient(
            endpoint=endpoint,
            index_name=index_name,
            region=region,
            auth_mode=auth_mode,
            username=username,
            password=password,
        )
        self.context_chars = context_chars

    async def connect(self) -> None:
        await self._client.connect()
        await self.setup_index()

    async def close(self) -> None:
        await self._client.close()

    async def delete_index(self) -> None:
        await self._client.delete_index()

    async def refresh_index(self) -> None:
        await self._client.refresh_index()

    async def setup_index(self) -> None:
        if await self._client.index_exists():
            return
        await self._client.create_index(_index_body())

    async def index_edition(self, edition_id: str, db: Database, storage: Storage, *, refresh: bool = True) -> None:
        try:
            edition = await db.edition.get(edition_id)
            text = await db.text.get(edition.text_id)
            content = await storage.retrieve_base_text(text_id=edition.text_id, edition_id=edition_id)
            segments = await _get_display_segments_for_edition(db, edition_id=edition_id, text_id=edition.text_id)
            documents = _build_chunk_documents(
                text_id=edition.text_id,
                edition_id=edition_id,
                edition_type=edition.type.value,
                language=text.language,
                title=text.title.root,
                source=edition.source,
                content=content,
                segments=segments,
                context_chars=self.context_chars,
            )

            await self.delete_edition(edition_id, refresh=refresh)
            await self._client.bulk_index(documents, refresh=refresh)
            logger.info("Indexed %d content search chunks for edition %s", len(documents), edition_id)
        except Exception:
            logger.exception("Failed to index content search chunks for edition %s", edition_id)
            raise

    async def delete_edition(self, edition_id: str, *, refresh: bool = True) -> None:
        await self._client.delete_edition(edition_id, refresh=refresh)

    async def search(
        self,
        *,
        query: str,
        search_type: str,
        limit: int,
        text_id: str | None = None,
        edition_id: str | None = None,
    ) -> ContentSearchResponse:
        response = await self._client.search(
            _search_body(
                query=query,
                search_type=search_type,
                limit=limit,
                text_id=text_id,
                edition_id=edition_id,
            ),
        )
        results = _parse_results(response, query=query, search_type=search_type, limit=limit)
        return ContentSearchResponse(query=query, results=results, count=len(results))


async def _get_display_segments_for_edition(
    db: Database,
    *,
    edition_id: str,
    text_id: str,
) -> list[SegmentWithContextOutput]:
    display_segmentations = await db.annotation.segmentation.get_all(edition_id)
    segments: list[SegmentWithContextOutput] = []
    for segmentation in display_segmentations:
        offset = 0
        while True:
            page = await db.annotation.segmentation.get_segments(
                segmentation.id,
                offset=offset,
                limit=SEGMENT_PAGE_SIZE,
            )
            segments.extend(
                SegmentWithContextOutput(
                    id=segment.id,
                    segmentation_id=segmentation.id,
                    edition_id=edition_id,
                    text_id=text_id,
                    lines=segment.lines,
                )
                for segment in page
            )
            if len(page) < SEGMENT_PAGE_SIZE:
                break
            offset += SEGMENT_PAGE_SIZE
    return segments


def _index_body() -> dict:
    return {
        "settings": {
            "index": {
                "max_ngram_diff": EXACT_MAX_GRAM - 2,
            },
            "analysis": {
                "filter": {
                    "content_exact_ngram_filter": {
                        "type": "ngram",
                        "min_gram": 2,
                        "max_gram": EXACT_MAX_GRAM,
                        "preserve_original": True,
                    },
                },
                "analyzer": {
                    "content_search_default": {
                        "type": "custom",
                        "tokenizer": "icu_tokenizer",
                        "filter": ["lowercase"],
                    },
                    "content_exact_ngram": {
                        "type": "custom",
                        "tokenizer": "keyword",
                        "filter": ["content_exact_ngram_filter"],
                    },
                },
            },
        },
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "document_type": {"type": "keyword"},
                "text_id": {"type": "keyword"},
                "edition_id": {"type": "keyword"},
                "primary_segment_id": {"type": "keyword"},
                "edition_type": {"type": "keyword"},
                "language": {"type": "keyword"},
                "title": {"type": "object", "enabled": False},
                "source": {"type": "keyword"},
                "segment_ids": {"type": "keyword"},
                "segments": {
                    "type": "nested",
                    "properties": {
                        "id": {"type": "keyword"},
                        "span_start": {"type": "integer"},
                        "span_end": {"type": "integer"},
                    },
                },
                "context_span_start": {"type": "integer"},
                "context_span_end": {"type": "integer"},
                "content": {
                    "type": "text",
                    "analyzer": "content_search_default",
                    "fields": {
                        "exact": {
                            "type": "text",
                            "analyzer": "content_exact_ngram",
                            "search_analyzer": "keyword",
                        },
                    },
                },
            },
        },
    }


def _build_chunk_documents(
    *,
    text_id: str,
    edition_id: str,
    edition_type: str,
    language: str,
    title: dict[str, str],
    source: str | None,
    content: str,
    segments: list[SegmentWithContextOutput],
    context_chars: int,
) -> list[dict]:
    if not segments:
        return [
            _document(
                document_id=f"{edition_id}:full",
                text_id=text_id,
                edition_id=edition_id,
                primary_segment_id=None,
                edition_type=edition_type,
                language=language,
                title=title,
                source=source,
                content=content,
                context_start=0,
                context_end=len(content),
                segments=[],
            )
        ]

    documents = []
    for segment in segments:
        anchor = segment.span
        context_start = max(0, anchor.start - context_chars)
        context_end = min(len(content), anchor.end + context_chars)
        covered_segments = _segments_overlapping(segments, context_start, context_end)
        documents.append(
            _document(
                document_id=f"{edition_id}:{segment.id}",
                text_id=text_id,
                edition_id=edition_id,
                primary_segment_id=segment.id,
                edition_type=edition_type,
                language=language,
                title=title,
                source=source,
                content=content[context_start:context_end],
                context_start=context_start,
                context_end=context_end,
                segments=covered_segments,
            )
        )
    return documents


def _document(
    *,
    document_id: str,
    text_id: str,
    edition_id: str,
    primary_segment_id: str | None,
    edition_type: str,
    language: str,
    title: dict[str, str],
    source: str | None,
    content: str,
    context_start: int,
    context_end: int,
    segments: list[SegmentWithContextOutput],
) -> dict:
    segment_docs = [
        {
            "id": segment.id,
            "span_start": segment.span.start,
            "span_end": segment.span.end,
        }
        for segment in segments
    ]
    return {
        "id": document_id,
        "document_type": "chunk",
        "text_id": text_id,
        "edition_id": edition_id,
        "primary_segment_id": primary_segment_id,
        "edition_type": edition_type,
        "language": language,
        "title": title,
        "source": source,
        "segments": segment_docs,
        "segment_ids": [segment["id"] for segment in segment_docs],
        "context_span_start": context_start,
        "context_span_end": context_end,
        "content": content,
    }


def _segments_overlapping(
    segments: list[SegmentWithContextOutput],
    start: int,
    end: int,
) -> list[SegmentWithContextOutput]:
    return [segment for segment in segments if segment.span.start < end and segment.span.end > start]


def _search_body(
    *,
    query: str,
    search_type: str,
    limit: int,
    text_id: str | None,
    edition_id: str | None,
) -> dict:
    filters = []
    if text_id:
        filters.append({"term": {"text_id": text_id}})
    if edition_id:
        filters.append({"term": {"edition_id": edition_id}})

    must = _exact_candidate_clauses(query) if search_type == "exact" else [{"match": {"content": query}}]
    return {
        "size": max(limit * 10, 50) if search_type == "exact" else limit,
        "query": {"bool": {"must": must, "filter": filters}},
        "highlight": {
            "fields": {
                "content": {
                    "number_of_fragments": 1,
                    "fragment_size": 180,
                }
            }
        },
    }


def _exact_candidate_clauses(query: str) -> list[dict]:
    return [{"match": {"content.exact": {"query": part, "operator": "and"}}} for part in _exact_candidate_parts(query)]


def _exact_candidate_parts(query: str) -> list[str]:
    if len(query) <= EXACT_MAX_GRAM:
        return [query]

    middle_start = max(0, (len(query) - EXACT_MAX_GRAM) // 2)
    parts = [
        query[:EXACT_MAX_GRAM],
        query[middle_start : middle_start + EXACT_MAX_GRAM],
        query[-EXACT_MAX_GRAM:],
    ]
    return list(dict.fromkeys(parts))


def _parse_results(response: dict, *, query: str, search_type: str, limit: int) -> list[ContentSearchResult]:
    results: list[ContentSearchResult] = []
    seen: set[tuple[str, int | None, int | None, int, int]] = set()
    for hit in response.get("hits", {}).get("hits", []):
        source = hit.get("_source", {})
        if search_type == "exact":
            for result in _exact_results_from_hit(hit, source, query):
                key = (
                    result.edition_id,
                    result.match_span.start if result.match_span else None,
                    result.match_span.end if result.match_span else None,
                    0,
                    0,
                )
                if key in seen:
                    continue
                seen.add(key)
                results.append(result)
                if len(results) >= limit:
                    return results
        else:
            result = _similar_result_from_hit(hit, source)
            key = (result.edition_id, None, None, result.context_span.start, result.context_span.end)
            if key in seen:
                continue
            seen.add(key)
            results.append(result)
            if len(results) >= limit:
                return results
    return results


def _exact_results_from_hit(hit: dict, source: dict, query: str) -> list[ContentSearchResult]:
    content = source.get("content", "")
    context_start = source.get("context_span_start", 0)
    results = []
    search_from = 0
    while True:
        local_start = content.find(query, search_from)
        if local_start == -1:
            break
        local_end = local_start + len(query)
        match_span = ContentSearchSpan(start=context_start + local_start, end=context_start + local_end)
        results.append(
            _result(
                hit=hit,
                source=source,
                match_span=match_span,
                matched_text=query,
                segments=_segments_from_source(source, match_span),
            )
        )
        search_from = local_start + 1
    return results


def _similar_result_from_hit(hit: dict, source: dict) -> ContentSearchResult:
    return _result(hit=hit, source=source, match_span=None, matched_text=None, segments=_segments_from_source(source))


def _result(
    *,
    hit: dict,
    source: dict,
    match_span: ContentSearchSpan | None,
    matched_text: str | None,
    segments: list[ContentSearchSegment],
) -> ContentSearchResult:
    return ContentSearchResult(
        text_id=source["text_id"],
        edition_id=source["edition_id"],
        segments=segments,
        segment_ids=[segment.id for segment in segments],
        context_span=ContentSearchSpan(start=source["context_span_start"], end=source["context_span_end"]),
        match_span=match_span,
        score=float(hit.get("_score") or 0.0),
        snippet=_snippet(hit),
        matched_text=matched_text,
    )


def _segments_from_source(
    source: dict,
    match_span: ContentSearchSpan | None = None,
) -> list[ContentSearchSegment]:
    raw_segments = source.get("segments", [])
    if match_span is not None:
        raw_segments = [
            segment
            for segment in raw_segments
            if segment["span_start"] < match_span.end and segment["span_end"] > match_span.start
        ]
    return [
        ContentSearchSegment(
            id=segment["id"],
            span=ContentSearchSpan(start=segment["span_start"], end=segment["span_end"]),
        )
        for segment in raw_segments
    ]


def _snippet(hit: dict) -> str | None:
    highlights = hit.get("highlight", {}).get("content", [])
    if highlights:
        return highlights[0]
    return None
