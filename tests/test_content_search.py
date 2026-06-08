# ruff: noqa: ANN001, ANN201, E501, S101
import pytest

from config import settings
from identifier import generate_id
from main import create_app
from models.base import LocalizedString
from models.contribution import ContributionInput
from models.enums import ContributorRole, EditionType
from models.person import PersonInput
from models.text import TextInput


@pytest.mark.asyncio(loop_scope="session")
async def test_non_testing_startup_requires_opensearch_endpoint(monkeypatch) -> None:
    monkeypatch.setattr(settings, "opensearch_endpoint", "")
    app = create_app(testing=False)

    with pytest.raises(RuntimeError, match="OPENSEARCH_ENDPOINT is required"):
        async with app.router.lifespan_context(app):
            pass


async def _create_person(db) -> str:
    return await db.person.create(PersonInput(name=LocalizedString({"en": "Search Author"}), bdrc="P" + generate_id()[:8]))


async def _create_text(db, person_id: str, title: str = "Search Text", language: str = "bo") -> str:
    return await db.text.create(
        TextInput(
            category_id="category",
            title=LocalizedString({language: title}),
            language=language,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
    )


async def _create_critical_edition(client, text_id: str, content: str, segments: list[tuple[int, int]]) -> str:
    response = await client.post(
        f"/v2/texts/{text_id}/editions",
        json={
            "content": content,
            "metadata": {"type": EditionType.CRITICAL.value, "source": "Search Source"},
            "segmentation": {"segments": [{"lines": [{"start": start, "end": end}]} for start, end in segments]},
        },
    )
    assert response.status_code == 201, response.json()
    return response.json()["id"]


@pytest.mark.asyncio(loop_scope="session")
class TestContentSearch:
    async def test_exact_search_returns_match_span_and_all_overlapping_segments(
        self,
        client,
        test_database,
    ):
        person_id = await _create_person(test_database)
        text_id = await _create_text(test_database, person_id)
        edition_id = await _create_critical_edition(
            client,
            text_id,
            content="abcdefghi",
            segments=[(0, 3), (3, 6), (6, 9)],
        )

        response = await client.get("/v2/content-search", params={"query": "cdef", "search_type": "exact"})

        assert response.status_code == 200, response.json()
        data = response.json()
        assert data["count"] == 1
        result = data["results"][0]
        assert result["text_id"] == text_id
        assert result["edition_id"] == edition_id
        assert result["match_span"] == {"start": 2, "end": 6}
        assert result["matched_text"] == "cdef"
        assert {segment["id"] for segment in result["segments"]} == set(result["segment_ids"])
        assert len(result["segments"]) == 2

    async def test_tibetan_exact_search_returns_unicode_match_span(self, client, test_database):
        person_id = await _create_person(test_database)
        text_id = await _create_text(test_database, person_id, title="བོད་ཡིག་ཚོལ་བ།")
        first_segment = "བཀྲ་ཤིས་"
        second_segment = "བདེ་ལེགས།"
        content = first_segment + second_segment
        query = "ཤིས་བདེ"
        expected_start = content.index(query)
        expected_end = expected_start + len(query)
        edition_id = await _create_critical_edition(
            client,
            text_id,
            content=content,
            segments=[(0, len(first_segment)), (len(first_segment), len(content))],
        )

        response = await client.get("/v2/content-search", params={"query": query, "search_type": "exact"})

        assert response.status_code == 200, response.json()
        data = response.json()
        assert data["count"] == 1
        result = data["results"][0]
        assert result["text_id"] == text_id
        assert result["edition_id"] == edition_id
        assert result["match_span"] == {"start": expected_start, "end": expected_end}
        assert result["matched_text"] == query
        assert len(result["segments"]) == 2

    async def test_tibetan_similar_search_uses_icu_analyzer(self, client, test_database):
        person_id = await _create_person(test_database)
        text_id = await _create_text(test_database, person_id, title="བོད་ཡིག་འདྲ་མཚུངས།")
        content = "འདི་ནི་བོད་ཡིག་གི་བདེ་ལེགས་ཞེས་པའི་ཚིག་ཡིན།"
        await _create_critical_edition(
            client,
            text_id,
            content=content,
            segments=[(0, len(content))],
        )

        response = await client.get("/v2/content-search", params={"query": "བདེ་ལེགས", "search_type": "similar"})

        assert response.status_code == 200, response.json()
        data = response.json()
        assert data["count"] == 1
        result = data["results"][0]
        assert result["text_id"] == text_id
        assert result["match_span"] is None
        assert result["context_span"] == {"start": 0, "end": len(content)}
        assert result["segments"]

    async def test_similar_search_returns_context_without_match_span(self, client, test_database):
        person_id = await _create_person(test_database)
        text_id = await _create_text(test_database, person_id)
        content = "བཀྲ་ཤིས་ search བདེ་ལེགས།"
        await _create_critical_edition(
            client,
            text_id,
            content=content,
            segments=[(0, len(content))],
        )

        response = await client.get("/v2/content-search", params={"query": "SEARCH", "search_type": "similar"})

        assert response.status_code == 200, response.json()
        result = response.json()["results"][0]
        assert result["match_span"] is None
        assert result["context_span"]["start"] == 0
        assert result["segments"]

    async def test_search_filters_by_text_and_edition(self, client, test_database):
        person_id = await _create_person(test_database)
        first_text_id = await _create_text(test_database, person_id, "First")
        second_text_id = await _create_text(test_database, person_id, "Second")
        first_edition_id = await _create_critical_edition(client, first_text_id, "shared phrase one", [(0, 17)])
        await _create_critical_edition(client, second_text_id, "shared phrase two", [(0, 17)])

        text_response = await client.get(
            "/v2/content-search",
            params={"query": "shared", "search_type": "exact", "text_id": first_text_id},
        )
        edition_response = await client.get(
            "/v2/content-search",
            params={"query": "shared", "search_type": "exact", "edition_id": first_edition_id},
        )

        assert text_response.status_code == 200, text_response.json()
        assert {result["text_id"] for result in text_response.json()["results"]} == {first_text_id}
        assert edition_response.status_code == 200, edition_response.json()
        assert {result["edition_id"] for result in edition_response.json()["results"]} == {first_edition_id}

    async def test_patch_reindexes_content(self, client, test_database):
        person_id = await _create_person(test_database)
        text_id = await _create_text(test_database, person_id)
        edition_id = await _create_critical_edition(client, text_id, "hello world", [(0, 11)])

        response_before = await client.get("/v2/content-search", params={"query": "planet", "search_type": "exact"})
        assert response_before.json()["count"] == 0

        patch_response = await client.patch(
            f"/v2/editions/{edition_id}/content",
            json={"type": "replace", "start": 6, "end": 11, "text": "planet"},
        )
        assert patch_response.status_code == 204

        response_after = await client.get("/v2/content-search", params={"query": "planet", "search_type": "exact"})
        assert response_after.status_code == 200
        assert response_after.json()["count"] == 1

    async def test_delete_removes_indexed_documents(self, client, test_database):
        person_id = await _create_person(test_database)
        text_id = await _create_text(test_database, person_id)
        edition_id = await _create_critical_edition(client, text_id, "delete me", [(0, 9)])

        assert (await client.get("/v2/content-search", params={"query": "delete", "search_type": "exact"})).json()[
            "count"
        ] == 1

        delete_response = await client.delete(f"/v2/editions/{edition_id}")
        assert delete_response.status_code == 204

        assert (await client.get("/v2/content-search", params={"query": "delete", "search_type": "exact"})).json()[
            "count"
        ] == 0
