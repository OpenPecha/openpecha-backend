# pylint: disable=redefined-outer-name
"""
Integration tests for segment-related endpoints.

Tests endpoints:
- GET /v2/segments/{segment_id}
- GET /v2/segments/{segment_id}/related
- GET /v2/segments/{segment_id}/content

Requires Docker for Neo4j testcontainer.
"""

import logging

import pytest
from identifier import generate_id
from models.base import LocalizedString
from models.contribution import ContributionInput
from models.edition import EditionInput, EditionType
from models.enums import ContributorRole
from models.person import PersonInput
from models.text import TextInput

logger = logging.getLogger(__name__)

APPLICATION_HEADER = {"X-Application": "test_application"}


class SegmentTestBase:
    """Shared helpers for segment integration tests."""

    async def _create_person(self, db) -> str:
        return await db.person.create(
            PersonInput(
                name=LocalizedString({"en": "Test Author", "bo": "སློབ་དཔོན།"}),
                bdrc="P" + generate_id()[:8],
            )
        )

    async def _create_text(self, db, person_id, title=None, language="bo", **kwargs) -> str:
        if title is None:
            title = LocalizedString({"en": "Test text", "bo": "བརྟག་དཔྱད།"})
        text_data = TextInput(
            category_id="category",
            title=title,
            language=language,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
            **kwargs,
        )
        return await db.text.create(text_data)

    async def _create_edition(self, client, text_id, content="Sample text content", edition_type=EditionType.DIPLOMATIC):
        edition_data = {
            "content": content,
            "metadata": {
                "type": edition_type.value,
                "bdrc": f"W{generate_id()[:8]}",
                "source": "Test Source",
            },
        }
        if edition_type == EditionType.DIPLOMATIC:
            edition_data["pagination"] = {
                "volumes": [
                    {"pages": [{"reference": "1a", "lines": [{"start": 0, "end": len(content)}]}]}
                ]
            }
        elif edition_type == EditionType.CRITICAL:
            edition_data["segmentation"] = {
                "segments": [{"lines": [{"start": 0, "end": len(content)}]}]
            }
        response = await client.post(f"/v2/texts/{text_id}/editions", json=edition_data)
        assert response.status_code == 201, f"Failed to create edition: {response.json()}"
        return response.json()["id"]

    @staticmethod
    def _segment_payload(segment):
        """Build a segment payload from a (start, end) tuple or a dict with verse metadata.

        Dict form: {"lines": [(start, end), ...] or [(start, end)], "type": ..., "verse_index": ...}
        or {"start": s, "end": e, "type": ..., "verse_index": ...} shorthand for a single line.
        """
        if isinstance(segment, dict):
            if "lines" in segment:
                lines = [{"start": ln[0], "end": ln[1]} for ln in segment["lines"]]
            else:
                lines = [{"start": segment["start"], "end": segment["end"]}]
            payload = {"lines": lines}
            for key in ("type", "verse_index"):
                if key in segment:
                    payload[key] = segment[key]
            return payload
        return {"lines": [{"start": segment[0], "end": segment[1]}]}

    async def _post_segmentation(self, client, edition_id, segments):
        data = {"segments": [self._segment_payload(s) for s in segments]}
        resp = await client.post(f"/v2/editions/{edition_id}/segmentations", json=data)
        assert resp.status_code == 201, f"Failed to create segmentation: {resp.json()}"
        return resp.json()["id"]

    async def _post_segmentation_raw(self, client, edition_id, segments):
        """POST a segmentation without asserting success; return the raw response."""
        data = {"segments": [self._segment_payload(s) for s in segments]}
        return await client.post(f"/v2/editions/{edition_id}/segmentations", json=data)

    async def _post_alignment(self, client, source_edition_id, target_edition_id, source_segments, target_segments, alignment_map):
        """Create an alignment between two editions.

        Args:
            source_segments: list of (start, end) tuples for the source (aligned) segments
            target_segments: list of (start, end) tuples for the target segments
            alignment_map: list of (source_idx, [target_indices]) pairs
        """
        aligned_segments = []
        for src_idx, target_indices in alignment_map:
            aligned_segments.append({
                "lines": [{"start": source_segments[src_idx][0], "end": source_segments[src_idx][1]}],
                "target_indices": target_indices,
            })

        data = {
            "target_edition_id": target_edition_id,
            "target_segments": [{"lines": [{"start": t[0], "end": t[1]}]} for t in target_segments],
            "aligned_segments": aligned_segments,
        }
        resp = await client.post(f"/v2/editions/{source_edition_id}/alignments", json=data)
        assert resp.status_code == 201, f"Failed to create alignment: {resp.json()}"
        return resp.json()["id"]

    async def _get_segment_ids_from_segmentation(self, client, edition_id, segmentation_index=0):
        resp = await client.get(f"/v2/editions/{edition_id}/segmentations")
        assert resp.status_code == 200
        segmentations = resp.json()
        if not segmentations:
            return []
        selected_segmentation_id = segmentations[segmentation_index]["id"]
        segments_resp = await client.get(f"/v2/segmentations/{selected_segmentation_id}/segments")
        assert segments_resp.status_code == 200
        return [seg["id"] for seg in segments_resp.json()["items"]]

    async def _create_translation_text(self, db, person_id, original_text_id, language="en"):
        return await self._create_text(
            db, person_id,
            title=LocalizedString({language: "Translation"}),
            language=language,
            translation_of=original_text_id,
        )

    async def _create_commentary_text(self, db, person_id, original_text_id):
        return await self._create_text(
            db, person_id,
            title=LocalizedString({"bo": "འགྲེལ་པ།", "en": "Commentary"}),
            commentary_of=original_text_id,
        )

    def _collect_all_segments(self, data):
        """Flatten all segments from the response data."""
        data = self._related_items(data)
        if not data or "segmentations" not in data[0]:
            return data
        segments = []
        for group in data:
            for sgn in group["segmentations"]:
                segments.extend(sgn["segments"])
        return segments

    def _collect_edition_ids(self, data):
        """Collect all edition IDs from the response data."""
        return {g["edition_id"] for g in self._related_items(data)}

    def _related_items(self, data):
        """Return flat related-segment items from either old or paginated payloads."""
        if isinstance(data, dict):
            return data["items"]
        return data


# ---------------------------------------------------------------------------
# Segment annotation labels
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestSegmentAnnotationLabels(SegmentTestBase):
    """Tests segmentation labels on display and alignment annotations."""

    # ---- segmentation label correctness ----

    async def test_display_segmentation_has_correct_label(self, client, test_database):
        """Display segmentation created via POST should have :Segmentation:Display labels."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "0123456789")
        sgn_id = await self._post_segmentation(client, edition_id, [(0, 5), (5, 10)])

        async with test_database.get_session() as session:
            result = await session.run("""
                MATCH (sgn:Segmentation:Display {id: $sgn_id})
                RETURN sgn.id AS id
            """, sgn_id=sgn_id)
            record = await result.single()
            assert record is not None, "Display segmentation should have :Display label"

    async def test_alignment_segmentations_have_correct_labels(self, client, test_database):
        """Alignment segmentations should have :Aligned and :Target labels."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(test_database, person_id, title=LocalizedString({"en": "Src", "bo": "འབྱུང།"}))
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")

        tgt_text_id = await self._create_text(test_database, person_id, title=LocalizedString({"en": "Tgt", "bo": "དམིགས།"}))
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")

        alignment_id = await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        async with test_database.get_session() as session:
            # Aligned segmentation (source side)
            result = await session.run("""
                MATCH (sgn:Segmentation:Aligned {id: $sgn_id})
                RETURN sgn.id AS id
            """, sgn_id=alignment_id)
            record = await result.single()
            assert record is not None, "Aligned segmentation should have :Aligned label"

            # Target segmentation (via aligned segments)
            result = await session.run("""
                MATCH (sgn:Segmentation:Aligned {id: $sgn_id})<-[:SEGMENT_OF]-(:Segment)-[:ALIGNED_TO]->(:Segment)
                      -[:SEGMENT_OF]->(target_sgn:Segmentation:Target)
                RETURN target_sgn.id AS id
            """, sgn_id=alignment_id)
            record = await result.single()
            assert record is not None, "Target segmentation should have :Target label"

    async def test_get_segmentations_only_returns_display(self, client, test_database):
        """GET /editions/{id}/segmentations should only return display segmentations."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(test_database, person_id, title=LocalizedString({"en": "Src", "bo": "འབྱུང།"}))
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        sgn_id = await self._post_segmentation(client, src_edition_id, [(0, 5), (5, 10)])

        tgt_text_id = await self._create_text(test_database, person_id, title=LocalizedString({"en": "Tgt", "bo": "དམིགས།"}))
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        resp = await client.get(f"/v2/editions/{src_edition_id}/segmentations")
        assert resp.status_code == 200
        data = resp.json()
        assert {segmentation["id"] for segmentation in data} == {sgn_id}, (
            "Should only return the display segmentation, not the alignment ones"
        )


# ---------------------------------------------------------------------------
# GET /v2/segments/{segment_id}/related
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestDirectSegmentRelated(SegmentTestBase):
    """Tests for GET /v2/segments/{segment_id}/related."""

    async def test_nonexistent_segment_returns_empty(self, client, test_database):
        """Non-existent segment_id -> empty list."""
        resp = await client.get("/v2/segments/nonexistent_id/related")
        assert resp.status_code == 200
        assert self._related_items(resp.json()) == []

    async def test_segment_with_no_alignment(self, client, test_database):
        """Segment exists but has no ALIGNED_TO relationship -> empty list."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "0123456789")
        await self._post_segmentation(client, edition_id, [(0, 5), (5, 10)])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)
        assert len(seg_ids) >= 1

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/related")
        assert resp.status_code == 200
        assert self._related_items(resp.json()) == []

    async def test_segment_with_alignment_returns_related(self, client, test_database):
        """Segment has alignment -> returns related display segments from target edition."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 5), (5, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, tgt_edition_id, [(0, 5), (5, 10)])

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 5), (5, 10)],
            target_segments=[(0, 5), (5, 10)],
            alignment_map=[(0, [0]), (1, [1])],
        )

        seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        assert len(seg_ids) >= 1

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/related")
        assert resp.status_code == 200
        data = self._related_items(resp.json())
        assert len(data) >= 1
        assert data[0]["edition_id"] == tgt_edition_id

    async def test_segment_related_filters_by_text_edition_language(self, client, test_database):
        """Direct segment related endpoint uses the same related-result filters."""
        person_id = await self._create_person(test_database)

        root_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Direct Filter Root"})
        )
        root_edition_id = await self._create_edition(client, root_text_id, "0123456789")
        await self._post_segmentation(client, root_edition_id, [(0, 10)])

        trans_text_id = await self._create_translation_text(test_database, person_id, root_text_id, language="en")
        trans_edition_id = await self._create_edition(client, trans_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, trans_edition_id, [(0, 10)])

        comm_text_id = await self._create_commentary_text(test_database, person_id, root_text_id)
        comm_edition_id = await self._create_edition(client, comm_text_id, "KLMNOPQRST")
        await self._post_segmentation(client, comm_edition_id, [(0, 10)])

        await self._post_alignment(
            client, root_edition_id, trans_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )
        await self._post_alignment(
            client, root_edition_id, comm_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        seg_ids = await self._get_segment_ids_from_segmentation(client, root_edition_id)
        assert len(seg_ids) >= 1

        text_resp = await client.get(f"/v2/segments/{seg_ids[0]}/related?text_id={trans_text_id}")
        assert text_resp.status_code == 200
        assert {item["text_id"] for item in self._related_items(text_resp.json())} == {trans_text_id}

        edition_resp = await client.get(f"/v2/segments/{seg_ids[0]}/related?edition_id={comm_edition_id}")
        assert edition_resp.status_code == 200
        assert {item["edition_id"] for item in self._related_items(edition_resp.json())} == {comm_edition_id}

        language_resp = await client.get(f"/v2/segments/{seg_ids[0]}/related?language=en")
        assert language_resp.status_code == 200
        assert {item["text_id"] for item in self._related_items(language_resp.json())} == {trans_text_id}

        empty_resp = await client.get(
            f"/v2/segments/{seg_ids[0]}/related?text_id={trans_text_id}&edition_id={comm_edition_id}"
        )
        assert empty_resp.status_code == 200
        assert self._related_items(empty_resp.json()) == []

    async def test_segment_related_has_more(self, client, test_database):
        """Direct related segment pagination should report when another page exists."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, tgt_edition_id, [(0, 5), (5, 10)])

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 5), (5, 10)],
            alignment_map=[(0, [0, 1])],
        )

        seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        assert len(seg_ids) >= 1

        first_page = await client.get(f"/v2/segments/{seg_ids[0]}/related?limit=1")
        assert first_page.status_code == 200
        first_body = first_page.json()
        assert len(first_body["items"]) == 1
        assert first_body["has_more"] is True
        assert first_body["offset"] == 0
        assert first_body["limit"] == 1

        second_page = await client.get(f"/v2/segments/{seg_ids[0]}/related?limit=1&offset=1")
        assert second_page.status_code == 200
        second_body = second_page.json()
        assert len(second_body["items"]) == 1
        assert second_body["has_more"] is False
        assert second_body["offset"] == 1
        assert second_body["limit"] == 1

    async def test_segment_related_with_application_header(self, client, test_database):
        """X-Application header should filter tags on returned segments."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, tgt_edition_id, [(0, 10)])

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        assert len(seg_ids) >= 1

        resp = await client.get(
            f"/v2/segments/{seg_ids[0]}/related",
            headers=APPLICATION_HEADER,
        )
        assert resp.status_code == 200

    async def test_segment_related_excludes_same_edition(self, client, test_database):
        """Related segments should not include segments from the same edition."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, tgt_edition_id, [(0, 10)])

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        assert len(seg_ids) >= 1

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/related")
        assert resp.status_code == 200
        data = self._related_items(resp.json())
        for segment in data:
            assert segment["edition_id"] != src_edition_id, \
                "Related segments should exclude the queried segment's own edition"


# ---------------------------------------------------------------------------
# GET /v2/segments/{segment_id}
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestGetSegment(SegmentTestBase):
    """Tests for GET /v2/segments/{segment_id}."""

    async def test_nonexistent_segment_returns_404(self, client, test_database):
        """Non-existent segment_id -> 404."""
        resp = await client.get("/v2/segments/nonexistent_id")
        assert resp.status_code == 404

    async def test_get_segment_with_context(self, client, test_database):
        """Get a segment with segmentation, edition, text, line, and tag context."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "Hello World!", EditionType.DIPLOMATIC)
        segmentation_id = await self._post_segmentation(client, edition_id, [(0, 5), (5, 12)])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)
        assert len(seg_ids) == 2

        resp = await client.get(f"/v2/segments/{seg_ids[0]}")
        assert resp.status_code == 200
        assert resp.json() == {
            "id": seg_ids[0],
            "segmentation_id": segmentation_id,
            "edition_id": edition_id,
            "text_id": text_id,
            "type": "paragraph",
            "lines": [{"start": 0, "end": 5}],
        }


# ---------------------------------------------------------------------------
# GET /v2/segments/{segment_id}/content
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestSegmentContent(SegmentTestBase):
    """Tests for GET /v2/segments/{segment_id}/content."""

    async def test_nonexistent_segment_returns_404(self, client, test_database):
        """Non-existent segment_id -> 404."""
        resp = await client.get("/v2/segments/nonexistent_id/content")
        assert resp.status_code == 404

    async def test_get_content_basic(self, client, test_database):
        """Get content of a segment from a diplomatic edition."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "Hello World!", EditionType.DIPLOMATIC)
        await self._post_segmentation(client, edition_id, [(0, 5), (5, 12)])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)
        assert len(seg_ids) == 2

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/content")
        assert resp.status_code == 200
        assert resp.json() == "Hello"

        resp2 = await client.get(f"/v2/segments/{seg_ids[1]}/content")
        assert resp2.status_code == 200
        assert resp2.json() == " World!"

    async def test_get_content_tibetan(self, client, test_database):
        """Segment content with Tibetan text."""
        tibetan = "བོད་སྐད་ཀྱི་ཡིག་ཆ།"
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, tibetan)
        mid = len(tibetan) // 2
        await self._post_segmentation(client, edition_id, [(0, mid), (mid, len(tibetan))])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/content")
        assert resp.status_code == 200
        assert resp.json() == tibetan[:mid]

        resp2 = await client.get(f"/v2/segments/{seg_ids[1]}/content")
        assert resp2.status_code == 200
        assert resp2.json() == tibetan[mid:]

    async def test_get_content_full_span(self, client, test_database):
        """Single segment covering the entire text."""
        content = "Full content here"
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, content)
        await self._post_segmentation(client, edition_id, [(0, len(content))])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)
        resp = await client.get(f"/v2/segments/{seg_ids[0]}/content")
        assert resp.status_code == 200
        assert resp.json() == content

    async def test_get_content_single_char_segment(self, client, test_database):
        """Segment spanning a single character."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "ABCDE")
        await self._post_segmentation(client, edition_id, [(0, 1), (1, 5)])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/content")
        assert resp.status_code == 200
        assert resp.json() == "A"

    async def test_content_after_edition_update(self, client, test_database):
        """Content should reflect the latest edition text after an insert."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "Hello World")
        await self._post_segmentation(client, edition_id, [(0, 5), (5, 11)])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)

        patch_resp = await client.patch(
            f"/v2/editions/{edition_id}/content",
            json={"type": "insert", "position": 5, "text": " Beautiful"},
        )
        assert patch_resp.status_code == 204

        resp = await client.get(f"/v2/segments/{seg_ids[0]}/content")
        assert resp.status_code == 200
        assert resp.json() == "Hello Beautiful"

    async def test_content_from_aligned_target_segment(self, client, test_database):
        """Get content of a segment that belongs to the target side of an alignment."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")

        alignment_id = await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 5), (5, 10)],
            alignment_map=[(0, [0, 1])],
        )

        alignments_resp = await client.get(f"/v2/alignments/{alignment_id}/segments")
        alignment_data = alignments_resp.json()["items"]
        assert len(alignment_data) >= 1
        target_seg_ids = [s["id"] for s in alignment_data[0]["target_segments"]]

        resp = await client.get(f"/v2/segments/{target_seg_ids[0]}/content")
        assert resp.status_code == 200
        assert resp.json() == "ABCDE"

        resp2 = await client.get(f"/v2/segments/{target_seg_ids[1]}/content")
        assert resp2.status_code == 200
        assert resp2.json() == "FGHIJ"


# ---------------------------------------------------------------------------
# Tags on related segments
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestSegmentsRelatedWithTags(SegmentTestBase):
    """Tests that tags on related display segments are properly returned/filtered."""

    async def test_related_segments_include_tag_ids(self, client, test_database):
        """Tags on display segments should appear in the response."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, tgt_edition_id, [(0, 10)])

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        tag_resp = await client.post("/v2/tags/", json={"title": {"en": "Test Seg Tag"}}, headers=APPLICATION_HEADER)
        assert tag_resp.status_code == 201
        tag_id = tag_resp.json()["id"]

        # Tag the display segment on the target edition
        tgt_seg_ids = await self._get_segment_ids_from_segmentation(client, tgt_edition_id)
        assert len(tgt_seg_ids) >= 1
        tag_seg_resp = await client.post(f"/v2/segments/{tgt_seg_ids[0]}/tags/{tag_id}")
        assert tag_seg_resp.status_code == 204

        # Query from source
        src_seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        assert len(src_seg_ids) >= 1
        resp = await client.get(
            f"/v2/segments/{src_seg_ids[0]}/related",
            headers=APPLICATION_HEADER,
        )
        assert resp.status_code == 200
        data = resp.json()
        all_segs = self._collect_all_segments(data)

        found_tag = False
        for seg in all_segs:
            if seg["id"] == tgt_seg_ids[0] and seg.get("tag_ids") and tag_id in seg["tag_ids"]:
                found_tag = True
        assert found_tag, "Tag should appear on the related display segment"

    async def test_related_segments_tag_filtering_by_application(self, client, test_database):
        """Tags from a different application should not appear on related segments."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Src"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས།", "en": "Tgt"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(client, tgt_edition_id, [(0, 10)])

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 10)],
            target_segments=[(0, 10)],
            alignment_map=[(0, [0])],
        )

        tag_resp = await client.post("/v2/tags/", json={"title": {"en": "App A Tag"}}, headers=APPLICATION_HEADER)
        tag_id = tag_resp.json()["id"]

        tgt_seg_ids = await self._get_segment_ids_from_segmentation(client, tgt_edition_id)
        await client.post(f"/v2/segments/{tgt_seg_ids[0]}/tags/{tag_id}")

        async with test_database.get_session() as session:
            await session.run("MERGE (app:Application {id: 'other_app', name: 'Other App'})")

        # Query with a different application header
        src_seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        resp = await client.get(
            f"/v2/segments/{src_seg_ids[0]}/related",
            headers={"X-Application": "other_app"},
        )
        assert resp.status_code == 200
        data = resp.json()
        all_segs = self._collect_all_segments(data)
        for seg in all_segs:
            if seg["id"] == tgt_seg_ids[0]:
                assert seg.get("tag_ids") is None or tag_id not in seg.get("tag_ids", []), \
                    "Tag from test_application should not appear when querying with other_app"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestSegmentsRelatedEdgeCases(SegmentTestBase):
    """Edge cases for segment related queries."""

    async def test_find_by_span_returns_correct_segment_ids(self, client, test_database):
        """Directly test find_by_span to ensure correct overlap logic."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "0123456789ABCDEF")
        await self._post_segmentation(client, edition_id, [(0, 4), (4, 8), (8, 12), (12, 16)])

        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)
        assert len(seg_ids) == 4

        found = await test_database.segment.find_by_span(edition_id, start=2, end=6)
        assert len(found) == 2, "Span [2,6) should overlap segments [0,4) and [4,8)"
        assert seg_ids[0] in found
        assert seg_ids[1] in found

        found2 = await test_database.segment.find_by_span(edition_id, start=4, end=8)
        assert len(found2) == 1, "Span [4,8) should only overlap segment [4,8)"
        assert seg_ids[1] in found2

        found3 = await test_database.segment.find_by_span(edition_id, start=0, end=16)
        assert len(found3) == 4, "Span [0,16) should overlap all segments"

        found4 = await test_database.segment.find_by_span(edition_id, start=16, end=20)
        assert len(found4) == 0, "Span [16,20) should overlap no segments"

    async def test_segment_get_with_broken_graph(self, client, test_database):
        """Segment node exists but missing path to Edition/Text -> DataNotFoundError."""
        from exceptions import DataNotFoundError

        async with test_database.get_session() as session:
            await session.run("""
                CREATE (seg:Segment {id: 'orphan_segment'})
                CREATE (span:Span {start: 0, end: 10})-[:SPAN_OF]->(seg)
            """)

        with pytest.raises(DataNotFoundError):
            await test_database.segment.get("orphan_segment")

    async def test_segment_get_with_zero_length_spans_excluded(self, client, test_database):
        """Spans where start == end should be excluded by the WHERE clause."""
        from exceptions import DataNotFoundError

        seg_id = f"seg_zero_span_{generate_id()[:6]}"
        async with test_database.get_session() as session:
            await session.run("""
                CREATE (text:Text {id: $text_id})
                CREATE (ed:Edition {id: $ed_id})-[:EDITION_OF]->(text)
                CREATE (sgn:Segmentation {id: $sgn_id})-[:SEGMENTATION_OF]->(ed)
                CREATE (seg:Segment {id: $seg_id})-[:SEGMENT_OF]->(sgn)
                CREATE (span:Span {start: 5, end: 5})-[:SPAN_OF]->(seg)
            """, text_id=f"t_{seg_id}", ed_id=f"e_{seg_id}", sgn_id=f"sgn_{seg_id}", seg_id=seg_id)

        with pytest.raises(DataNotFoundError):
            await test_database.segment.get(seg_id)


# ---------------------------------------------------------------------------
# find_by_span boundary tests (directly testing the database layer)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestFindBySpanBoundaries(SegmentTestBase):
    """Detailed boundary tests for SegmentDatabase.find_by_span."""

    async def _setup_edition_with_segments(self, client, test_database, segments):
        """Helper: create edition + segmentation, return (edition_id, seg_ids)."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        total_len = max(s[1] for s in segments)
        content = "X" * total_len
        edition_id = await self._create_edition(client, text_id, content)
        await self._post_segmentation(client, edition_id, segments)
        seg_ids = await self._get_segment_ids_from_segmentation(client, edition_id)
        return edition_id, seg_ids

    async def test_exact_segment_match(self, client, test_database):
        """Span exactly matches one segment."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(0, 10), (10, 20)]
        )
        found = await test_database.segment.find_by_span(edition_id, 0, 10)
        assert found == [seg_ids[0]]

    async def test_span_inside_segment(self, client, test_database):
        """Span is strictly inside a segment."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(0, 20)]
        )
        found = await test_database.segment.find_by_span(edition_id, 5, 15)
        assert found == [seg_ids[0]]

    async def test_span_covers_multiple_segments(self, client, test_database):
        """Span covers 3 segments completely."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(0, 5), (5, 10), (10, 15)]
        )
        found = await test_database.segment.find_by_span(edition_id, 0, 15)
        assert set(found) == set(seg_ids)

    async def test_span_overlaps_only_by_one_char(self, client, test_database):
        """Span overlaps a segment by exactly 1 character."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(0, 10), (10, 20)]
        )
        found = await test_database.segment.find_by_span(edition_id, 9, 11)
        assert set(found) == {seg_ids[0], seg_ids[1]}

    async def test_span_before_all_segments(self, client, test_database):
        """Span is completely before all segments."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(5, 10), (10, 15)]
        )
        found = await test_database.segment.find_by_span(edition_id, 0, 5)
        assert found == []

    async def test_span_after_all_segments(self, client, test_database):
        """Span is completely after all segments."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(0, 5), (5, 10)]
        )
        found = await test_database.segment.find_by_span(edition_id, 10, 20)
        assert found == []

    async def test_gap_between_segments(self, client, test_database):
        """Segments have a gap; span falls entirely in the gap."""
        edition_id, seg_ids = await self._setup_edition_with_segments(
            client, test_database, [(0, 5), (10, 15)]
        )
        found = await test_database.segment.find_by_span(edition_id, 5, 10)
        assert found == []

    async def test_wrong_edition_returns_empty(self, client, test_database):
        """find_by_span with an edition_id that has no segmentations -> empty."""
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, "Hello")
        found = await test_database.segment.find_by_span(edition_id, 0, 5)
        assert found == []


# ---------------------------------------------------------------------------
# Verse segments (type + verse_index on Display segmentations)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio(loop_scope="session")
class TestVerseSegments(SegmentTestBase):
    """Tests for the `verse` segment subtype and its `verse_index`."""

    async def _edition(self, client, test_database, content="0123456789"):
        person_id = await self._create_person(test_database)
        text_id = await self._create_text(test_database, person_id)
        edition_id = await self._create_edition(client, text_id, content)
        return edition_id

    async def test_round_trip_lists_verse_fields(self, client, test_database):
        """A verse segment exposes type/verse_index; a paragraph segment is typed but has no index."""
        edition_id = await self._edition(client, test_database)
        segmentation_id = await self._post_segmentation(
            client,
            edition_id,
            [
                {"start": 0, "end": 5, "type": "verse", "verse_index": [1, 1]},
                {"start": 5, "end": 10},
            ],
        )

        resp = await client.get(f"/v2/segmentations/{segmentation_id}/segments")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 2

        verse, plain = items[0], items[1]
        assert verse["type"] == "verse"
        assert verse["verse_index"] == [1, 1]
        assert plain["type"] == "paragraph"
        assert "verse_index" not in plain

    async def test_explicit_null_type_rejected(self, client, test_database):
        """An explicit `type: null` is rejected; omit `type` to default to paragraph."""
        edition_id = await self._edition(client, test_database)
        resp = await self._post_segmentation_raw(
            client, edition_id, [{"start": 0, "end": 5, "type": None}]
        )
        assert resp.status_code == 422

    async def test_get_single_segment_verse_fields(self, client, test_database):
        """GET /v2/segments/{id} returns the verse index for a verse and omits it for a paragraph."""
        edition_id = await self._edition(client, test_database)
        segmentation_id = await self._post_segmentation(
            client,
            edition_id,
            [
                {"start": 0, "end": 5, "type": "verse", "verse_index": [2, 10]},
                {"start": 5, "end": 10},
            ],
        )
        items = (await client.get(f"/v2/segmentations/{segmentation_id}/segments")).json()["items"]
        verse_id, plain_id = items[0]["id"], items[1]["id"]

        verse = (await client.get(f"/v2/segments/{verse_id}")).json()
        assert verse["type"] == "verse"
        assert verse["verse_index"] == [2, 10]

        plain = (await client.get(f"/v2/segments/{plain_id}")).json()
        assert plain["type"] == "paragraph"
        assert "verse_index" not in plain

    async def test_verse_without_index_rejected(self, client, test_database):
        """type=verse with no verse_index -> 422."""
        edition_id = await self._edition(client, test_database)
        resp = await self._post_segmentation_raw(
            client, edition_id, [{"start": 0, "end": 5, "type": "verse"}]
        )
        assert resp.status_code == 422

    async def test_index_without_type_rejected(self, client, test_database):
        """verse_index without type=verse -> 422 (defaults to paragraph, which forbids an index)."""
        edition_id = await self._edition(client, test_database)
        resp = await self._post_segmentation_raw(
            client, edition_id, [{"start": 0, "end": 5, "verse_index": [1, 1]}]
        )
        assert resp.status_code == 422

    @pytest.mark.parametrize("bad_index", [[0, 1], [1, 0], [-1, 1], [1], [1, 2, 3], "1.1", ["a", "b"]])
    async def test_bad_index_format_rejected(self, client, test_database, bad_index):
        """Malformed verse_index values (not an int pair, or parts < 1) -> 422."""
        edition_id = await self._edition(client, test_database)
        resp = await self._post_segmentation_raw(
            client, edition_id, [{"start": 0, "end": 5, "type": "verse", "verse_index": bad_index}]
        )
        assert resp.status_code == 422, f"expected 422 for verse_index={bad_index!r}"

    async def test_invalid_type_value_rejected(self, client, test_database):
        """An unknown segment type value -> 422."""
        edition_id = await self._edition(client, test_database)
        resp = await self._post_segmentation_raw(
            client, edition_id, [{"start": 0, "end": 5, "type": "prose"}]
        )
        assert resp.status_code == 422

    async def test_duplicate_verse_index_rejected(self, client, test_database):
        """Two verse segments sharing a verse_index within one segmentation -> 422."""
        edition_id = await self._edition(client, test_database)
        resp = await self._post_segmentation_raw(
            client,
            edition_id,
            [
                {"start": 0, "end": 5, "type": "verse", "verse_index": [1, 1]},
                {"start": 5, "end": 10, "type": "verse", "verse_index": [1, 1]},
            ],
        )
        assert resp.status_code == 422

    async def test_related_endpoint_surfaces_verse(self, client, test_database):
        """A verse Display segment reached via the related endpoint carries the verse fields."""
        person_id = await self._create_person(test_database)
        src_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "རྩ་བ།", "en": "Source"})
        )
        src_edition_id = await self._create_edition(client, src_text_id, "0123456789")
        await self._post_segmentation(client, src_edition_id, [(0, 5), (5, 10)])

        tgt_text_id = await self._create_text(
            test_database, person_id, title=LocalizedString({"bo": "དམིགས་བསལ།", "en": "Target"})
        )
        tgt_edition_id = await self._create_edition(client, tgt_text_id, "ABCDEFGHIJ")
        await self._post_segmentation(
            client,
            tgt_edition_id,
            [{"start": 0, "end": 5, "type": "verse", "verse_index": [1, 1]}, (5, 10)],
        )

        await self._post_alignment(
            client, src_edition_id, tgt_edition_id,
            source_segments=[(0, 5), (5, 10)],
            target_segments=[(0, 5), (5, 10)],
            alignment_map=[(0, [0]), (1, [1])],
        )

        src_seg_ids = await self._get_segment_ids_from_segmentation(client, src_edition_id)
        assert len(src_seg_ids) >= 1
        resp = await client.get(f"/v2/segments/{src_seg_ids[0]}/related")
        assert resp.status_code == 200
        items = self._collect_all_segments(resp.json())
        verse_items = [i for i in items if i.get("verse_index") == [1, 1]]
        assert verse_items, f"expected a verse segment in related results: {items}"
        assert verse_items[0]["type"] == "verse"

    async def test_multiple_distinct_verses_persist(self, client, test_database):
        """Several verses with distinct indices (incl. [1, 10] vs [1, 1]) persist; a paragraph stays a paragraph."""
        edition_id = await self._edition(client, test_database)
        segmentation_id = await self._post_segmentation(
            client,
            edition_id,
            [
                {"start": 0, "end": 2, "type": "verse", "verse_index": [1, 1]},
                {"start": 2, "end": 4, "type": "verse", "verse_index": [1, 10]},
                {"start": 4, "end": 6, "type": "verse", "verse_index": [2, 1]},
                {"start": 6, "end": 8},
            ],
        )

        items = (await client.get(f"/v2/segmentations/{segmentation_id}/segments")).json()["items"]
        assert len(items) == 4

        # [1, 10] is a distinct index from [1, 1] (verse indices are integer pairs, not floats).
        verse_indices = [i.get("verse_index") for i in items if i.get("type") == "verse"]
        assert sorted(verse_indices) == [[1, 1], [1, 10], [2, 1]]

        plain = [i for i in items if i.get("type") == "paragraph"]
        assert len(plain) == 1
        assert "verse_index" not in plain[0]
