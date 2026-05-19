# pylint: disable=redefined-outer-name
import pytest

from identifier import generate_id
from models.base import LocalizedString
from models.contribution import ContributionInput
from models.edition import EditionInput, EditionType
from models.enums import ContributorRole
from models.person import PersonInput
from models.text import TextInput


@pytest.fixture
def test_person_data() -> PersonInput:
    return PersonInput(name=LocalizedString({"en": "Outline Test Author"}), bdrc=f"P{generate_id()[:8]}")


async def _create_test_person(db, person_data: PersonInput) -> str:
    return await db.person.create(person_data)


async def _create_test_text(db, person_id: str) -> str:
    text_data = TextInput(
        category_id="category",
        title=LocalizedString({"en": "Outline Test Text", "bo": "ས་བཅད་ཚོད་ལྟ།"}),
        language="bo",
        contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
    )
    return await db.text.create(text_data)


async def _create_test_edition(db, text_id: str) -> str:
    edition_id = generate_id()
    edition_data = EditionInput(type=EditionType.DIPLOMATIC, bdrc=f"W{edition_id[:8]}", source="Outline Test Source")
    await db.edition.create(edition_data, edition_id, text_id)
    return edition_id


async def _create_test_graph(test_database, person_data: PersonInput) -> tuple[str, str]:
    person_id = await _create_test_person(test_database, person_data)
    text_id = await _create_test_text(test_database, person_id)
    edition_id = await _create_test_edition(test_database, text_id)
    return text_id, edition_id


def _outline_payload(name: str = "Main sa bcad") -> dict:
    return {
        "metadata": {"name": name},
        "sections": [
            {
                "title": {"en": "Chapter 1", "bo": "ལེའུ་དང་པོ།"},
                "summary": {"en": "Opening topic"},
                "span": {"start": 0, "end": 10},
                "subsections": [
                    {
                        "title": {"en": "Section 1.2"},
                        "span": {"start": 5, "end": 10},
                    },
                    {
                        "title": {"en": "Section 1.1"},
                        "summary": {"en": "Introductory topic"},
                        "span": {"start": 0, "end": 5},
                        "subsections": [
                            {
                                "title": {"en": "Subsection 1.1.1"},
                                "span": {"start": 0, "end": 2},
                            }
                        ],
                    },
                ],
            }
        ],
    }


async def _scalar(test_database, query: str, **params):
    async with test_database.get_session() as session:
        result = await session.run(query, **params)
        record = await result.single()
        return None if record is None else record[0]


@pytest.mark.asyncio(loop_scope="session")
class TestOutlines:
    async def test_outline_round_trip_with_nested_sections(self, client, test_database, test_person_data):
        text_id, edition_id = await _create_test_graph(test_database, test_person_data)

        post_response = await client.post(f"/v2/editions/{edition_id}/outlines", json=_outline_payload())
        assert post_response.status_code == 201, post_response.json()
        outline_id = post_response.json()["id"]

        get_response = await client.get(f"/v2/outlines/{outline_id}")
        assert get_response.status_code == 200
        outline = get_response.json()
        assert outline["id"] == outline_id
        assert outline["edition_id"] == edition_id
        assert outline["text_id"] == text_id
        assert outline["metadata"]["name"] == "Main sa bcad"
        assert outline["sections"][0]["title"]["en"] == "Chapter 1"
        assert outline["sections"][0]["title"]["bo"] == "ལེའུ་དང་པོ།"
        assert outline["sections"][0]["summary"]["en"] == "Opening topic"
        assert outline["sections"][0]["subsections"][0]["summary"]["en"] == "Introductory topic"
        assert outline["sections"][0]["subsections"][0]["subsections"][0]["title"]["en"] == "Subsection 1.1.1"
        assert outline["sections"][0]["subsections"][1]["title"]["en"] == "Section 1.2"

        list_response = await client.get(f"/v2/editions/{edition_id}/outlines")
        assert list_response.status_code == 200
        assert [item["id"] for item in list_response.json()] == [outline_id]

        delete_response = await client.delete(f"/v2/outlines/{outline_id}")
        assert delete_response.status_code == 204
        assert (await client.get(f"/v2/outlines/{outline_id}")).status_code == 404
        assert (await client.delete(f"/v2/outlines/{outline_id}")).status_code == 204

    async def test_multiple_outlines_can_attach_to_one_edition(self, client, test_database, test_person_data):
        _, edition_id = await _create_test_graph(test_database, test_person_data)

        first = await client.post(f"/v2/editions/{edition_id}/outlines", json=_outline_payload("First outline"))
        second = await client.post(f"/v2/editions/{edition_id}/outlines", json=_outline_payload("Second outline"))

        assert first.status_code == 201, first.json()
        assert second.status_code == 201, second.json()
        response = await client.get(f"/v2/editions/{edition_id}/outlines")
        assert response.status_code == 200
        names = {outline["metadata"]["name"] for outline in response.json()}
        assert names == {"First outline", "Second outline"}

    async def test_add_outline_edition_not_found(self, client):
        response = await client.post("/v2/editions/nonexistent_id/outlines", json=_outline_payload())

        assert response.status_code == 404
        assert "error" in response.json()

    async def test_add_outline_rejects_subsection_outside_parent_span(self, client, test_database, test_person_data):
        _, edition_id = await _create_test_graph(test_database, test_person_data)
        outline = _outline_payload()
        outline["sections"][0]["subsections"][0]["span"] = {"start": 9, "end": 12}

        response = await client.post(f"/v2/editions/{edition_id}/outlines", json=outline)

        assert response.status_code == 422
        assert "contained" in str(response.json()).lower()

    async def test_delete_edition_cascades_outlines(self, client, test_database, test_person_data):
        _, edition_id = await _create_test_graph(test_database, test_person_data)
        post_response = await client.post(f"/v2/editions/{edition_id}/outlines", json=_outline_payload())
        assert post_response.status_code == 201, post_response.json()
        outline_id = post_response.json()["id"]

        await test_database.edition.delete(edition_id)

        assert (await client.get(f"/v2/outlines/{outline_id}")).status_code == 404
        assert await _scalar(test_database, "RETURN EXISTS { (:Outline {id: $outline_id}) }", outline_id=outline_id) is False
        assert (
            await _scalar(
                test_database,
                "MATCH (:OutlineSection)-[:SECTION_OF]->(:Outline {id: $outline_id}) RETURN count(*)",
                outline_id=outline_id,
            )
            == 0
        )

    async def test_patch_content_adjusts_outline_section_spans(self, client, test_database):
        person_response = await client.post("/v2/persons", json={"name": {"en": "Outline Patch Author"}})
        assert person_response.status_code == 201, person_response.json()
        person_id = person_response.json()["id"]
        text_response = await client.post(
            "/v2/texts",
            json={
                "title": {"en": "Outline Patch Text"},
                "language": "en",
                "category_id": "category",
                "contributions": [{"person_id": person_id, "role": "author"}],
            },
        )
        assert text_response.status_code == 201, text_response.json()
        text_id = text_response.json()["id"]
        edition_response = await client.post(
            f"/v2/texts/{text_id}/editions",
            json={
                "content": "0123456789",
                "metadata": {
                    "type": "diplomatic",
                    "bdrc": f"W{generate_id()[:8]}",
                    "source": "Outline Patch Source",
                },
                "pagination": {
                    "volumes": [{"pages": [{"reference": "1a", "lines": [{"start": 0, "end": 10}]}]}],
                },
            },
        )
        assert edition_response.status_code == 201, edition_response.json()
        edition_id = edition_response.json()["id"]
        outline_data = {
            "sections": [
                {
                    "title": {"en": "Middle section"},
                    "span": {"start": 2, "end": 5},
                }
            ]
        }
        outline_response = await client.post(f"/v2/editions/{edition_id}/outlines", json=outline_data)
        assert outline_response.status_code == 201, outline_response.json()
        outline_id = outline_response.json()["id"]

        patch_response = await client.patch(
            f"/v2/editions/{edition_id}/content",
            json={"type": "insert", "position": 3, "text": "XX"},
        )
        assert patch_response.status_code == 204

        outline = (await client.get(f"/v2/outlines/{outline_id}")).json()
        assert outline["sections"][0]["span"] == {"start": 2, "end": 7}
