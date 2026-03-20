# pylint: disable=redefined-outer-name
"""
Integration tests for v2/tags endpoints and tag-related functionality.

Tests endpoints:
- GET /v2/tags/ (get all tags)
- POST /v2/tags/ (create tag)
- DELETE /v2/tags/<tag_id> (delete tag)
- POST /v2/texts/<expression_id>/tags/<tag_id> (tag a text)
- DELETE /v2/texts/<expression_id>/tags/<tag_id> (untag a text)
- POST /v2/segments/<segment_id>/tags/<tag_id> (tag a segment)
- DELETE /v2/segments/<segment_id>/tags/<tag_id> (untag a segment)
- Inline tagging via POST /v2/texts (create expression with tag_ids)
- Tag filtering via GET /v2/texts?tag_id=...
"""

import json
import logging

import pytest
from models import TagInput

logger = logging.getLogger(__name__)

APPLICATION_HEADER = {"X-Application": "test_application"}


@pytest.fixture
def test_tag_data():
    """Sample tag data for testing"""
    return {
        "title": {"en": "Philosophy", "bo": "གྲུབ་མཐའ"},
    }


@pytest.fixture
def test_tag_data_minimal():
    """Minimal tag data for testing"""
    return {"title": {"en": "Meditation"}}


@pytest.fixture
def test_tag_data_with_description():
    """Tag data with description"""
    return {
        "title": {"en": "Ethics", "bo": "ཚུལ་ཁྲིམས"},
        "description": {"en": "Texts related to ethical conduct", "bo": "ཚུལ་ཁྲིམས་ཀྱི་གཞུང་།"},
    }


def _create_tag(client, tag_data):
    """Helper to create a tag and return its ID."""
    response = client.post(
        "/v2/tags/",
        data=json.dumps(tag_data),
        content_type="application/json",
        headers=APPLICATION_HEADER,
    )
    assert response.status_code == 201
    return json.loads(response.data)["id"]


def _create_expression(client, tag_ids=None):
    """Helper to create an expression and return its ID."""
    expression_data = {
        "title": {"bo": "ཚོད་ལྟའི་གཞུང་།"},
        "language": "bo",
        "category_id": "category",
        "license": "public",
        "contributions": [{"person_id": "test_person", "role": "author"}],
    }
    if tag_ids is not None:
        expression_data["tag_ids"] = tag_ids
    response = client.post(
        "/v2/texts/",
        data=json.dumps(expression_data),
        content_type="application/json",
    )
    assert response.status_code == 201
    return json.loads(response.data)["id"]


def _seed_person(test_database):
    """Seed a test person for expression creation."""
    with test_database.get_session() as session:
        session.run("""
            MERGE (p:Person {id: 'test_person'})
            MERGE (n:Nomen {id: 'test_person_nomen'})
            MERGE (p)-[:HAS_NAME]->(n)
            MERGE (lt:LocalizedText {text: 'Test Author'})
            WITH n, lt
            MATCH (lang:Language {code: 'en'})
            MERGE (n)-[:HAS_LOCALIZATION]->(lt)-[:HAS_LANGUAGE]->(lang)
        """).consume()


class TestGetAllTags:
    """Tests for GET /v2/tags/ endpoint"""

    def test_get_all_tags_empty(self, client, test_database):
        """Test getting tags when none exist returns empty list"""
        response = client.get("/v2/tags/", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_all_tags_returns_created_tags(self, client, test_database):
        """Test getting tags returns previously created tags"""
        tag_id = _create_tag(client, {"title": {"en": "Test Tag"}})

        response = client.get("/v2/tags/", headers=APPLICATION_HEADER)

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1
        tag_ids = [t["id"] for t in data]
        assert tag_id in tag_ids

    def test_get_all_tags_missing_application_header(self, client, test_database):
        """Test getting tags without X-Application header fails"""
        response = client.get("/v2/tags/")

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_get_all_tags_invalid_application(self, client, test_database):
        """Test getting tags with invalid application returns 404"""
        response = client.get("/v2/tags/", headers={"X-Application": "nonexistent_app"})

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data


class TestCreateTag:
    """Tests for POST /v2/tags/ endpoint"""

    def test_create_tag_success(self, client, test_database, test_tag_data):
        """Test successfully creating a tag"""
        response = client.post(
            "/v2/tags/",
            data=json.dumps(test_tag_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data
        assert data["id"] is not None

    def test_create_tag_minimal(self, client, test_database, test_tag_data_minimal):
        """Test creating tag with minimal data"""
        response = client.post(
            "/v2/tags/",
            data=json.dumps(test_tag_data_minimal),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data

    def test_create_tag_with_description(self, client, test_database, test_tag_data_with_description):
        """Test creating tag with description"""
        tag_id = _create_tag(client, test_tag_data_with_description)

        response = client.get("/v2/tags/", headers=APPLICATION_HEADER)
        tags = json.loads(response.data)
        created_tag = next((t for t in tags if t["id"] == tag_id), None)

        assert created_tag is not None
        assert created_tag["description"] is not None
        assert created_tag["description"]["en"] == "Texts related to ethical conduct"
        assert created_tag["description"]["bo"] == "ཚུལ་ཁྲིམས་ཀྱི་གཞུང་།"

    def test_create_tag_duplicate_rejected(self, client, test_database):
        """Test creating tag with duplicate title in same application fails"""
        tag_data = {"title": {"en": "Unique Tag Title For Dup Test"}}

        response1 = client.post(
            "/v2/tags/",
            data=json.dumps(tag_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )
        assert response1.status_code == 201

        response2 = client.post(
            "/v2/tags/",
            data=json.dumps(tag_data),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )
        assert response2.status_code == 422
        data = json.loads(response2.data)
        assert "error" in data
        assert "already exists" in data["error"].lower()

    def test_create_tag_missing_title(self, client, test_database):
        """Test creating tag without title fails"""
        response = client.post(
            "/v2/tags/",
            data=json.dumps({}),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data

    def test_create_tag_empty_title(self, client, test_database):
        """Test creating tag with empty title fails"""
        response = client.post(
            "/v2/tags/",
            data=json.dumps({"title": {}}),
            content_type="application/json",
            headers=APPLICATION_HEADER,
        )

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data

    def test_create_tag_missing_application_header(self, client, test_database):
        """Test creating tag without X-Application header fails"""
        tag_data = {"title": {"en": "No App Header"}}

        response = client.post(
            "/v2/tags/",
            data=json.dumps(tag_data),
            content_type="application/json",
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert "error" in data


class TestDeleteTag:
    """Tests for DELETE /v2/tags/<tag_id> endpoint"""

    def test_delete_tag_success(self, client, test_database):
        """Test successfully deleting a tag"""
        tag_id = _create_tag(client, {"title": {"en": "Tag To Delete"}})

        response = client.delete(f"/v2/tags/{tag_id}", headers=APPLICATION_HEADER)

        assert response.status_code == 200

        get_response = client.get("/v2/tags/", headers=APPLICATION_HEADER)
        tags = json.loads(get_response.data)
        tag_ids = [t["id"] for t in tags]
        assert tag_id not in tag_ids

    def test_delete_tag_nonexistent(self, client, test_database):
        """Test deleting non-existent tag returns 404"""
        response = client.delete("/v2/tags/nonexistent_tag_id", headers=APPLICATION_HEADER)

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "error" in data


class TestTagWork:
    """Tests for tagging/untagging works via text endpoints"""

    def test_tag_work(self, client, test_database):
        """Test adding a tag to a work via expression endpoint"""
        _seed_person(test_database)
        tag_id = _create_tag(client, {"title": {"en": "Work Tag"}})
        expression_id = _create_expression(client)

        response = client.post(f"/v2/texts/{expression_id}/tags/{tag_id}")

        assert response.status_code == 200

        get_response = client.get(f"/v2/texts/{expression_id}")
        expression = json.loads(get_response.data)
        assert tag_id in expression["tag_ids"]

    def test_untag_work(self, client, test_database):
        """Test removing a tag from a work via expression endpoint"""
        _seed_person(test_database)
        tag_id = _create_tag(client, {"title": {"en": "Work Untag"}})
        expression_id = _create_expression(client)

        client.post(f"/v2/texts/{expression_id}/tags/{tag_id}")

        response = client.delete(f"/v2/texts/{expression_id}/tags/{tag_id}")

        assert response.status_code == 200

        get_response = client.get(f"/v2/texts/{expression_id}")
        expression = json.loads(get_response.data)
        assert tag_id not in expression["tag_ids"]

    def test_tag_work_multiple_tags(self, client, test_database):
        """Test adding multiple tags to a work"""
        _seed_person(test_database)
        tag_id_1 = _create_tag(client, {"title": {"en": "Multi Tag 1"}})
        tag_id_2 = _create_tag(client, {"title": {"en": "Multi Tag 2"}})
        expression_id = _create_expression(client)

        client.post(f"/v2/texts/{expression_id}/tags/{tag_id_1}")
        client.post(f"/v2/texts/{expression_id}/tags/{tag_id_2}")

        get_response = client.get(f"/v2/texts/{expression_id}")
        expression = json.loads(get_response.data)
        assert tag_id_1 in expression["tag_ids"]
        assert tag_id_2 in expression["tag_ids"]

    def test_tag_nonexistent_expression(self, client, test_database):
        """Test tagging a non-existent expression returns 404"""
        tag_id = _create_tag(client, {"title": {"en": "Orphan Tag"}})

        response = client.post(f"/v2/texts/nonexistent_expr/tags/{tag_id}")

        assert response.status_code == 404


class TestInlineTagging:
    """Tests for inline tag_ids on expression create and update"""

    def test_create_expression_with_tag_ids(self, client, test_database):
        """Test creating an expression with inline tag_ids"""
        _seed_person(test_database)
        tag_id = _create_tag(client, {"title": {"en": "Inline Create Tag"}})

        expression_id = _create_expression(client, tag_ids=[tag_id])

        get_response = client.get(f"/v2/texts/{expression_id}")
        expression = json.loads(get_response.data)
        assert tag_id in expression["tag_ids"]

    def test_create_expression_without_tag_ids(self, client, test_database):
        """Test creating an expression without tag_ids yields empty list"""
        _seed_person(test_database)

        expression_id = _create_expression(client)

        get_response = client.get(f"/v2/texts/{expression_id}")
        expression = json.loads(get_response.data)
        assert expression["tag_ids"] == []

    def test_update_expression_tag_ids(self, client, test_database):
        """Test updating expression tag_ids via PATCH"""
        _seed_person(test_database)
        tag_id_1 = _create_tag(client, {"title": {"en": "Patch Tag 1"}})
        tag_id_2 = _create_tag(client, {"title": {"en": "Patch Tag 2"}})

        expression_id = _create_expression(client, tag_ids=[tag_id_1])

        patch_data = {"tag_ids": [tag_id_2]}
        response = client.patch(
            f"/v2/texts/{expression_id}",
            data=json.dumps(patch_data),
            content_type="application/json",
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert tag_id_2 in data["tag_ids"]
        assert tag_id_1 not in data["tag_ids"]


class TestTagSegment:
    """Tests for tagging/untagging segments"""

    def test_tag_segment(self, client, test_database):
        """Test adding a tag to a segment"""
        tag_id = _create_tag(client, {"title": {"en": "Segment Tag"}})

        with test_database.get_session() as session:
            session.run("""
                CREATE (sgn:Segmentation {id: 'test_sgn_tag'})
                CREATE (seg:Segment {id: 'test_seg_tag'})
                CREATE (seg)-[:SEGMENT_OF]->(sgn)
            """).consume()

        test_database.tag.tag_segment("test_seg_tag", tag_id)

        with test_database.get_session() as session:
            result = session.run("""
                MATCH (s:Segment {id: 'test_seg_tag'})-[:HAS_TAG]->(t:Tag {id: $tag_id})
                RETURN t.id AS tag_id
            """, tag_id=tag_id).single()
            assert result is not None
            assert result["tag_id"] == tag_id

    def test_untag_segment(self, client, test_database):
        """Test removing a tag from a segment"""
        tag_id = _create_tag(client, {"title": {"en": "Segment Untag"}})

        with test_database.get_session() as session:
            session.run("""
                CREATE (sgn:Segmentation {id: 'test_sgn_untag'})
                CREATE (seg:Segment {id: 'test_seg_untag'})
                CREATE (seg)-[:SEGMENT_OF]->(sgn)
            """).consume()

        test_database.tag.tag_segment("test_seg_untag", tag_id)
        test_database.tag.untag_segment("test_seg_untag", tag_id)

        with test_database.get_session() as session:
            result = session.run("""
                MATCH (s:Segment {id: 'test_seg_untag'})-[:HAS_TAG]->(t:Tag {id: $tag_id})
                RETURN t.id AS tag_id
            """, tag_id=tag_id).single()
            assert result is None

    def test_tag_segment_via_api(self, client, test_database):
        """Test tagging a segment via API endpoint"""
        tag_id = _create_tag(client, {"title": {"en": "API Segment Tag"}})

        with test_database.get_session() as session:
            session.run("""
                CREATE (sgn:Segmentation {id: 'test_sgn_api'})
                CREATE (seg:Segment {id: 'test_seg_api'})
                CREATE (seg)-[:SEGMENT_OF]->(sgn)
            """).consume()

        response = client.post(f"/v2/segments/test_seg_api/tags/{tag_id}")
        assert response.status_code == 200

        with test_database.get_session() as session:
            result = session.run("""
                MATCH (s:Segment {id: 'test_seg_api'})-[:HAS_TAG]->(t:Tag {id: $tag_id})
                RETURN t.id AS tag_id
            """, tag_id=tag_id).single()
            assert result is not None
            assert result["tag_id"] == tag_id

    def test_untag_segment_via_api(self, client, test_database):
        """Test untagging a segment via API endpoint"""
        tag_id = _create_tag(client, {"title": {"en": "API Segment Untag"}})

        with test_database.get_session() as session:
            session.run("""
                CREATE (sgn:Segmentation {id: 'test_sgn_api_untag'})
                CREATE (seg:Segment {id: 'test_seg_api_untag'})
                CREATE (seg)-[:SEGMENT_OF]->(sgn)
            """).consume()

        client.post(f"/v2/segments/test_seg_api_untag/tags/{tag_id}")

        response = client.delete(f"/v2/segments/test_seg_api_untag/tags/{tag_id}")
        assert response.status_code == 200

        with test_database.get_session() as session:
            result = session.run("""
                MATCH (s:Segment {id: 'test_seg_api_untag'})-[:HAS_TAG]->(t:Tag {id: $tag_id})
                RETURN t.id AS tag_id
            """, tag_id=tag_id).single()
            assert result is None


class TestTagFiltering:
    """Tests for filtering texts by tag_id"""

    def test_filter_texts_by_tag_id(self, client, test_database):
        """Test filtering texts by tag_id returns only tagged texts"""
        _seed_person(test_database)
        tag_id = _create_tag(client, {"title": {"en": "Filter Tag"}})

        expression_id_tagged = _create_expression(client, tag_ids=[tag_id])
        expression_id_untagged = _create_expression(client)

        response = client.get(f"/v2/texts/?tag_id={tag_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        result_ids = [expr["id"] for expr in data]
        assert expression_id_tagged in result_ids
        assert expression_id_untagged not in result_ids

    def test_filter_texts_by_nonexistent_tag_returns_empty(self, client, test_database):
        """Test filtering by non-existent tag_id returns empty list"""
        response = client.get("/v2/texts/?tag_id=nonexistent_tag")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data == []


class TestTagLocalization:
    """Tests for tag title/description localization"""

    def test_tag_multi_language_title(self, client, test_database):
        """Test that tag titles are properly localized"""
        tag_data = {
            "title": {
                "en": "English Tag Title",
                "bo": "བོད་ཡིག་ཁ་བྱང་།",
                "zh": "中文标签",
            }
        }

        tag_id = _create_tag(client, tag_data)

        response = client.get("/v2/tags/", headers=APPLICATION_HEADER)
        tags = json.loads(response.data)
        created_tag = next((t for t in tags if t["id"] == tag_id), None)

        assert created_tag is not None
        assert created_tag["title"]["en"] == "English Tag Title"
        assert created_tag["title"]["bo"] == "བོད་ཡིག་ཁ་བྱང་།"
        assert created_tag["title"]["zh"] == "中文标签"

    def test_tag_without_description(self, client, test_database):
        """Test that tag without description returns null"""
        tag_id = _create_tag(client, {"title": {"en": "No Desc Tag"}})

        response = client.get("/v2/tags/", headers=APPLICATION_HEADER)
        tags = json.loads(response.data)
        created_tag = next((t for t in tags if t["id"] == tag_id), None)

        assert created_tag is not None
        assert created_tag.get("description") is None


class TestTagSharing:
    """Tests for tag sharing between works"""

    def test_same_tag_on_multiple_works(self, client, test_database):
        """Test that two works can share the same tag node"""
        _seed_person(test_database)
        tag_id = _create_tag(client, {"title": {"en": "Shared Tag"}})

        expression_id_1 = _create_expression(client, tag_ids=[tag_id])
        expression_id_2 = _create_expression(client, tag_ids=[tag_id])

        get_1 = client.get(f"/v2/texts/{expression_id_1}")
        get_2 = client.get(f"/v2/texts/{expression_id_2}")

        expr_1 = json.loads(get_1.data)
        expr_2 = json.loads(get_2.data)

        assert tag_id in expr_1["tag_ids"]
        assert tag_id in expr_2["tag_ids"]


def _seed_app_b(test_database):
    """Seed a second application and its own category for isolation tests."""
    with test_database.get_session() as session:
        session.run("""
            MERGE (app:Application {id: 'app_b', name: 'Application B'})
            WITH app
            MERGE (cat:Category {id: 'category_b'})-[:BELONGS_TO]->(app)
            MERGE (nomen:Nomen {id: 'category_b_nomen'})
            MERGE (cat)-[:HAS_TITLE]->(nomen)
            MERGE (lt:LocalizedText {text: 'App B Category'})
            WITH nomen, lt
            MATCH (lang:Language {code: 'en'})
            MERGE (nomen)-[:HAS_LOCALIZATION]->(lt)-[:HAS_LANGUAGE]->(lang)
        """).consume()


APP_B_HEADER = {"X-Application": "app_b"}


class TestTagApplicationIsolation:
    """Tests that tags are isolated per application."""

    def test_listing_tags_only_returns_own_application(self, client, test_database):
        """Tags created for App A should not appear when listing tags for App B."""
        _seed_app_b(test_database)

        tag_id_a = _create_tag(client, {"title": {"en": "App A Only Tag"}})

        response_b = client.get("/v2/tags/", headers=APP_B_HEADER)
        assert response_b.status_code == 200
        tags_b = json.loads(response_b.data)
        tag_ids_b = [t["id"] for t in tags_b]
        assert tag_id_a not in tag_ids_b

    def test_listing_tags_returns_own_tags(self, client, test_database):
        """Tags created for App B should appear when listing tags for App B, not App A."""
        _seed_app_b(test_database)

        tag_b_data = {"title": {"en": "App B Exclusive Tag"}}
        response_create = client.post(
            "/v2/tags/",
            data=json.dumps(tag_b_data),
            content_type="application/json",
            headers=APP_B_HEADER,
        )
        assert response_create.status_code == 201
        tag_id_b = json.loads(response_create.data)["id"]

        response_b = client.get("/v2/tags/", headers=APP_B_HEADER)
        tags_b = json.loads(response_b.data)
        tag_ids_b = [t["id"] for t in tags_b]
        assert tag_id_b in tag_ids_b

        response_a = client.get("/v2/tags/", headers=APPLICATION_HEADER)
        tags_a = json.loads(response_a.data)
        tag_ids_a = [t["id"] for t in tags_a]
        assert tag_id_b not in tag_ids_a

    def test_same_tag_title_allowed_across_applications(self, client, test_database):
        """Two applications can each have a tag with the same title."""
        _seed_app_b(test_database)

        tag_data = {"title": {"en": "Cross App Same Title"}}
        tag_id_a = _create_tag(client, tag_data)

        response_b = client.post(
            "/v2/tags/",
            data=json.dumps(tag_data),
            content_type="application/json",
            headers=APP_B_HEADER,
        )
        assert response_b.status_code == 201
        tag_id_b = json.loads(response_b.data)["id"]

        assert tag_id_a != tag_id_b

    def test_expression_tag_ids_filtered_by_application(self, client, test_database):
        """When getting an expression, tag_ids should only include tags belonging to the caller's application."""
        _seed_app_b(test_database)
        _seed_person(test_database)

        tag_id_a = _create_tag(client, {"title": {"en": "App A Expr Tag"}})

        tag_b_data = {"title": {"en": "App B Expr Tag"}}
        response_b_create = client.post(
            "/v2/tags/",
            data=json.dumps(tag_b_data),
            content_type="application/json",
            headers=APP_B_HEADER,
        )
        tag_id_b = json.loads(response_b_create.data)["id"]

        expression_id = _create_expression(client, tag_ids=[tag_id_a])

        work_id = test_database.expression.get_work_id(expression_id)
        test_database.tag.tag_work(work_id, tag_id_b)

        response_a = client.get(
            f"/v2/texts/{expression_id}",
            headers=APPLICATION_HEADER,
        )
        expr_a = json.loads(response_a.data)
        assert tag_id_a in expr_a["tag_ids"]
        assert tag_id_b not in expr_a["tag_ids"]

        response_b = client.get(
            f"/v2/texts/{expression_id}",
            headers=APP_B_HEADER,
        )
        expr_b = json.loads(response_b.data)
        assert tag_id_b in expr_b["tag_ids"]
        assert tag_id_a not in expr_b["tag_ids"]

    def test_delete_tag_does_not_affect_other_application(self, client, test_database):
        """Deleting a tag in App A should not affect App B's tags."""
        _seed_app_b(test_database)

        tag_id_a = _create_tag(client, {"title": {"en": "Delete Isolation Tag"}})

        tag_b_data = {"title": {"en": "App B Surviving Tag"}}
        response_b_create = client.post(
            "/v2/tags/",
            data=json.dumps(tag_b_data),
            content_type="application/json",
            headers=APP_B_HEADER,
        )
        tag_id_b = json.loads(response_b_create.data)["id"]

        client.delete(f"/v2/tags/{tag_id_a}", headers=APPLICATION_HEADER)

        response_b = client.get("/v2/tags/", headers=APP_B_HEADER)
        tags_b = json.loads(response_b.data)
        tag_ids_b = [t["id"] for t in tags_b]
        assert tag_id_b in tag_ids_b
