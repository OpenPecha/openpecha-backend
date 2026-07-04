# ruff: noqa: ANN001, ANN201, S101
import pytest

from models.base import LocalizedString
from models.contribution import PersonContributionInput
from models.enums import ContributorRole
from models.person import PersonInput
from models.text import TextInput


async def _create_person(
    test_database,
    *,
    name: dict[str, str],
    alt_names: list[dict[str, str]] | None = None,
) -> str:
    return await test_database.person.create(
        PersonInput(
            name=LocalizedString(name),
            alt_names=[LocalizedString(alt) for alt in alt_names] if alt_names else None,
        )
    )


async def _create_text(
    test_database,
    *,
    person_id: str,
    title: dict[str, str],
    language: str = "sa",
    alt_titles: list[dict[str, str]] | None = None,
) -> str:
    return await test_database.text.create(
        TextInput(
            category_id="category",
            title=LocalizedString(title),
            alt_titles=[LocalizedString(alt) for alt in alt_titles] if alt_titles else None,
            language=language,
            contributions=[PersonContributionInput(type="person", id=person_id, role=ContributorRole.AUTHOR)],
        )
    )


@pytest.mark.asyncio(loop_scope="session")
class TestCatalogSearch:
    async def test_person_sanskrit_lenient_search_returns_canonical_record(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(test_database, name={"sa-x-iast": "Śāntideva"})
        await catalog_search.index_person(person_id, test_database)

        response = await catalog_client.get("/v2/persons", params={"name": "Shantideva"})

        assert response.status_code == 200, response.json()
        assert response.json()["items"][0]["id"] == person_id
        assert response.json()["items"][0]["name"]["sa-x-iast"] == "Śāntideva"

    async def test_person_diacritic_free_search_returns_canonical_record(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(test_database, name={"sa-x-iast": "Śāntideva"})
        await catalog_search.index_person(person_id, test_database)

        response = await catalog_client.get("/v2/persons", params={"name": "Santideva"})

        assert response.status_code == 200, response.json()
        assert response.json()["items"][0]["id"] == person_id
        assert response.json()["items"][0]["name"]["sa-x-iast"] == "Śāntideva"

    async def test_person_alternative_name_lenient_search_returns_canonical_record(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(
            test_database,
            name={"en": "Peace Deity"},
            alt_names=[{"sa-x-iast": "Śāntideva"}],
        )
        await catalog_search.index_person(person_id, test_database)

        response = await catalog_client.get("/v2/persons", params={"name": "Shantideva"})

        assert response.status_code == 200, response.json()
        assert response.json()["items"][0]["id"] == person_id
        assert response.json()["items"][0]["alt_names"][0]["sa-x-iast"] == "Śāntideva"

    async def test_text_title_sanskrit_lenient_search_returns_canonical_record(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(test_database, name={"en": "Search Author"})
        text_id = await _create_text(test_database, person_id=person_id, title={"sa-x-iast": "Nāgārjuna"})
        await catalog_search.index_text(text_id, test_database)

        response = await catalog_client.get("/v2/texts", params={"title": "Nagarjuna"})

        assert response.status_code == 200, response.json()
        assert response.json()["items"][0]["id"] == text_id
        assert response.json()["items"][0]["title"]["sa-x-iast"] == "Nāgārjuna"

    async def test_text_alternative_title_lenient_search_returns_canonical_record(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(test_database, name={"en": "Search Author"})
        text_id = await _create_text(
            test_database,
            person_id=person_id,
            title={"en": "Fundamental Verses"},
            language="en",
            alt_titles=[{"sa-x-iast": "Nāgārjuna"}],
        )
        await catalog_search.index_text(text_id, test_database)

        response = await catalog_client.get("/v2/texts", params={"title": "Nagarjuna"})

        assert response.status_code == 200, response.json()
        assert response.json()["items"][0]["id"] == text_id
        assert response.json()["items"][0]["alt_titles"][0]["sa-x-iast"] == "Nāgārjuna"

    async def test_person_tibetan_phonetic_search_returns_tibetan_record(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(test_database, name={"bo": "ཞི་བ་ལྷ་"})
        await catalog_search.index_person(person_id, test_database)

        response = await catalog_client.get("/v2/persons", params={"name": "zhi ba lha"})

        assert response.status_code == 200, response.json()
        assert response.json()["items"][0]["id"] == person_id

    async def test_text_title_search_filters_before_pagination(
        self,
        catalog_client,
        catalog_search,
        test_database,
    ):
        person_id = await _create_person(test_database, name={"en": "Search Author"})
        sanskrit_text_id = await _create_text(
            test_database,
            person_id=person_id,
            title={"sa-x-iast": "Bodhicaryāvatāra"},
            language="sa",
        )
        english_text_id = await _create_text(
            test_database,
            person_id=person_id,
            title={"en": "Bodhicaryavatara"},
            language="en",
        )
        await catalog_search.index_text(sanskrit_text_id, test_database)
        await catalog_search.index_text(english_text_id, test_database)

        response = await catalog_client.get(
            "/v2/texts",
            params={"title": "Bodhicaryavatara", "language": "sa", "limit": 1},
        )

        assert response.status_code == 200, response.json()
        body = response.json()
        assert [item["id"] for item in body["items"]] == [sanskrit_text_id]
        assert body["has_more"] is False
