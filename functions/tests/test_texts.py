# pylint: disable=redefined-outer-name
"""Integration tests for v2/texts endpoints using real Neo4j test instance.

Tests endpoints:
- GET /v2/texts/ (get all texts with filtering and pagination)
- GET /v2/texts/{text_id} (get single text)
- POST /v2/texts/ (create text)
- GET /v2/texts/{text_id}/instances/ (get instances of a text)

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance
"""
import json
import os

import pytest
from dotenv import load_dotenv
from main import create_app
from models import ExpressionModelInput, PersonModelInput
from neo4j_database import Neo4JDatabase

# Load .env file if it exists
load_dotenv()


@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables"""
    test_uri = os.environ.get("NEO4J_TEST_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD environment variables."
        )

    yield {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance"""
    # Set environment variables so API endpoints can connect to test database
    os.environ["NEO4J_URI"] = neo4j_connection["uri"]
    os.environ["NEO4J_PASSWORD"] = neo4j_connection["auth"][1]

    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Create full-text index for fuzzy search (if not exists)
        session.run("""
            CREATE FULLTEXT INDEX localized_text_fulltext IF NOT EXISTS
            FOR (lt:LocalizedText) ON EACH [lt.text]
        """)
        # Create test copyright and license nodes
        session.run("MERGE (c:Copyright {status: 'In copyright'})")
        session.run("MERGE (c:Copyright {status: 'Public domain'})")
        session.run("MERGE (l:License {name: 'CC BY'})")
        session.run("MERGE (l:License {name: 'CC0'})")

        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
        session.run("MERGE (l:Language {code: 'tib', name: 'Spoken Tibetan'})")
        session.run("MERGE (l:Language {code: 'en', name: 'English'})")
        session.run("MERGE (l:Language {code: 'sa', name: 'Sanskrit'})")
        session.run("MERGE (l:Language {code: 'zh', name: 'Chinese'})")
        session.run("MERGE (l:Language {code: 'cmg', name: 'Classical Mongolian'})")


        # Create test text types (TextType enum values)
        session.run("MERGE (t:TextType {name: 'root'})")
        session.run("MERGE (t:TextType {name: 'commentary'})")
        session.run("MERGE (t:TextType {name: 'translation'})")

        # Create test role types (only allowed values per constraints)
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'reviser'})")

        # Create test license type
        session.run("MERGE (l:License {name: 'CC0'})")

    yield db

    # Cleanup after test
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")


@pytest.fixture
def client():
    """Create Flask test client"""
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


@pytest.fixture
def test_person_data():
    """Sample person data for testing"""
    return {
        "name": {"en": "Test Author", "bo": "སློབ་དཔོན།"},
        "alt_names": [{"en": "Alternative Name", "bo": "མིང་གཞན།"}],
        "bdrc": "P123456",
        "wiki": "Q123456",
    }

@pytest.fixture
def test_expression_data():
    """Sample expression data for testing"""
    return {
        "type": "root",
        "title": {"en": "Test Expression", "bo": "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"},
        "alt_titles": [{"en": "Alternative Title", "bo": "མཚན་བྱང་གཞན།"}],
        "language": "en",
        "contributions": [],  # Will be populated with actual person IDs
        "date": "2024-01-01",
        "bdrc": "W123456",
        "wiki": "Q789012",
    }


class TestGetAllTextsV2:
    """Tests for GET /v2/texts/ endpoint (get all texts)"""

    def test_get_all_metadata_empty_database(self, client, test_database):
        """Test getting all texts from empty database"""
        response = client.get("/v2/texts/")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == 0

    def test_get_all_metadata_default_pagination(self, client, test_database, test_person_data, test_expression_data):
        """Test default pagination (limit=20, offset=0)"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create test expression
        expression_ids = []
        for i in range(25):
            test_expression_data["bdrc"] = f"W123456{i+1}"
            test_expression_data["wiki"] = f"Q789012{i+1}"
            test_expression_data["date"] = f"2024-01-01{i+1}"
            test_expression_data["category_id"] = category_id
            test_expression_data["title"] = {"en": f"Test Expression {i+1}", "bo": f"བརྟག་དཔྱད་ཚིག་སྒྲུབ་{i+1}།"}
            test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
            expression = ExpressionModelInput.model_validate(test_expression_data)
            
            expression_id = test_database.create_expression(expression)

            expression_ids.append(expression_id)

        response = client.get("/v2/texts?limit=25&offset=0")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)
        assert len(data) == len(expression_ids)
        
        response_expression_ids = [expression["id"] for expression in data]

        assert set(response_expression_ids) == set(expression_ids)


    def test_get_all_metadata_custom_pagination(self, client, test_database, test_person_data):
        """Test custom pagination parameters"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create multiple expressions
        expression_ids = []
        for i in range(5):
            expr_data = {
                "type": "root",
                "title": {"en": f"Expression {i+1}", "bo": f"ཚིག་སྒྲུབ་{i+1}།"},
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(expr_data)
            expr_id = test_database.create_expression(expression)
            expression_ids.append(expr_id)

        # Test limit=2, offset=1
        response = client.get("/v2/texts?limit=2&offset=1")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 2

        response_expression_ids = [expression["id"] for expression in data]
        assert set(response_expression_ids) == set(expression_ids[1:3])

    def test_get_all_metadata_filter_by_type(self, client, test_database, test_person_data):
        """Test filtering by expression type"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id,
        }
        translation_expression = ExpressionModelInput.model_validate(translation_data)
        translation_id = test_database.create_expression(translation_expression)

        commentary_data = {
            "type": "commentary",
            "title": {"en": "Commentary Expression", "bo": "འགྲེལ་པ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        commentary_expression = ExpressionModelInput.model_validate(commentary_data)
        commentary_id = test_database.create_expression(commentary_expression)

        translation_response = client.get("/v2/texts?type=translation")

        assert translation_response.status_code == 200
        data = json.loads(translation_response.data)
        assert len(data) == 1
        assert data[0]["id"] == translation_id
        assert data[0]["type"] == "translation"

        commentary_response = client.get("/v2/texts?type=commentary")
        assert commentary_response.status_code == 200
        data = json.loads(commentary_response.data)
        assert len(data) == 1
        assert data[0]["id"] == commentary_id
        assert data[0]["type"] == "commentary"

    def test_get_all_metadata_filter_by_language(self, client, test_database, test_person_data):
        """Test filtering by language"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)


        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create English expression
        en_data = {
            "type": "root",
            "title": {"en": "English Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        en_expression = ExpressionModelInput.model_validate(en_data)
        en_id = test_database.create_expression(en_expression)

        # Create Tibetan expression
        bo_data = {
            "type": "root",
            "title": {"bo": "བོད་ཡིག་ཚིག་སྒྲུབ།"},
            "category_id": category_id,
            "language": "bo",
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        bo_expression = ExpressionModelInput.model_validate(bo_data)
        bo_id = test_database.create_expression(bo_expression)

        # Filter by English
        response = client.get("/v2/texts?language=en")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == en_id
        assert data[0]["language"] == "en"

        # Filter by Tibetan
        response = client.get("/v2/texts?language=bo")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["id"] == bo_id
        assert data[0]["language"] == "bo"

    def test_get_all_metadata_filter_by_title(self, client, test_database, test_person_data):
        """Test filtering by title"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        titles = [
            {
                "en": "Human being",
                "bo": "དཔེ་གཞི།"
            },
            {
                "en": "Buddha",
                "bo": "བོད་ཡིག།"
            },
            {
                "en": "Buddha dharma",
                "bo": "བོད་ཡིག། དཔེ་གཞི།"
            }
        ]

        expression_ids = []
        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionModelInput.model_validate(root_data)
            expression_id = test_database.create_expression(root_expression)
            expression_ids.append(expression_id)

        en_title_search_response = client.get("/v2/texts?title=Buddha")
        assert en_title_search_response.status_code == 200
        data = json.loads(en_title_search_response.data)
        assert len(data) == 2
        assert "Buddha" in data[0]["title"]["en"]
        assert "Buddha" in data[1]["title"]["en"]

        bo_title_search_response = client.get("/v2/texts?title=དཔེ་གཞི།")
        assert bo_title_search_response.status_code == 200
        data = json.loads(bo_title_search_response.data)
        assert len(data) == 2
        assert "དཔེ་གཞི།" in data[0]["title"]["bo"]
        assert "དཔེ་གཞི།" in data[1]["title"]["bo"]

        bo_title_search_response = client.get("/v2/texts?title=བོད")
        assert bo_title_search_response.status_code == 200
        data = json.loads(bo_title_search_response.data)
        assert len(data) == 2
        assert "བོད" in data[0]["title"]["bo"]
        assert "བོད" in data[1]["title"]["bo"]

    def test_get_all_metadata_filter_by_title_with_no_title_present_in_db(self, client, test_database, test_person_data):
        """Test filtering by title with empty title"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        titles = [
            {
                "en": "Human being",
                "bo": "དཔེ་གཞི།"
            },
            {
                "en": "Buddha",
                "bo": "བོད་ཡིག།"
            },
            {
                "en": "Buddha dharma",
                "bo": "བོད་ཡིག། དཔེ་གཞི།"
            }
        ]

        expression_ids = []
        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            root_expression = ExpressionModelInput.model_validate(root_data)
            expression_id = test_database.create_expression(root_expression)
            expression_ids.append(expression_id)

        response = client.get("/v2/texts?title=invalid_title")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0



    def test_get_all_metadata_filter_by_author(self, client, test_database):
        """Test filtering by author name"""
        author_primary = PersonModelInput.model_validate(
            {
                "name": {"en": "Tsongkhapa"},
                "alt_names": [{"en": "Lama Tsongkhapa"}],
                "bdrc": "P111111",
                "wiki": "Q111111",
            }
        )
        author_primary_id = test_database.create_person(author_primary)

        author_alt = PersonModelInput.model_validate(
            {
                "name": {"en": "Milarepa"},
                "alt_names": [{"en": "Mila"}],
                "bdrc": "P222222",
                "wiki": "Q222222",
            }
        )
        author_alt_id = test_database.create_person(author_alt)

        translator = PersonModelInput.model_validate(
            {
                "name": {"en": "Tsongkhapa Translator"},
                "alt_names": [{"en": "Translator Name"}],
                "bdrc": "P333333",
                "wiki": "Q333333",
            }
        )
        translator_id = test_database.create_person(translator)

        category_id = test_database.create_category(
            application="test_application",
            title={"en": "Test Category"},
        )

        primary_author_data = {
            "type": "root",
            "title": {"en": "Primary Author Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": author_primary_id, "role": "author"}],
        }
        primary_author_expression = ExpressionModelInput.model_validate(primary_author_data)
        primary_author_id = test_database.create_expression(primary_author_expression)

        alt_author_data = {
            "type": "root",
            "title": {"en": "Alt Author Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": author_alt_id, "role": "author"}],
        }
        alt_author_expression = ExpressionModelInput.model_validate(alt_author_data)
        alt_author_id = test_database.create_expression(alt_author_expression)

        translator_data = {
            "type": "root",
            "title": {"en": "Translator Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": translator_id, "role": "translator"}],
        }
        translator_expression = ExpressionModelInput.model_validate(translator_data)
        test_database.create_expression(translator_expression)

        response = client.get("/v2/texts?author=Tsongkhapa")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {primary_author_id}

        response = client.get("/v2/texts?author=Lama")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {primary_author_id}

        response = client.get("/v2/texts?author=Mila")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {alt_author_id}

        response = client.get("/v2/texts?author=NoMatch")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0

    def test_get_all_metadata_filter_by_author_alt_name_substring(self, client, test_database):
        """Test filtering by substring of author's alternate name including Tibetan, Sanskrit, and Mongolian"""
        # Create person with multiple distinctive alternate names in various scripts
        author_with_alt_names = PersonModelInput.model_validate(
            {
                "name": {"en": "Padmasambhava"},
                "alt_names": [
                    {"en": "Guru Rinpoche"},
                    {"bo": "པདྨ་འབྱུང་གནས།"},  # Tibetan: Padma Jungne
                    {"en": "Lotus Born Master"},
                    {"sa": "पद्मसम्भव"},  # Sanskrit: Padmasambhava
                    {"cmg": "Бадамжунай"},  # Mongolian: Badamjunai
                ],
                "bdrc": "P444444",
                "wiki": "Q444444",
            }
        )
        author_id = test_database.create_person(author_with_alt_names)

        category_id = test_database.create_category(
            application="test_application",
            title={"en": "Test Category"},
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Padmasambhava Text"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": author_id, "role": "author"}],
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Test searching by substring of first alternate name "Guru Rinpoche"
        response = client.get("/v2/texts?author=Guru")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=Rinpoche")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test searching by substring of another alternate name "Lotus Born Master"
        response = client.get("/v2/texts?author=Lotus")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=Born")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test searching by substring of primary name "Padmasambhava"
        response = client.get("/v2/texts?author=Padma")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test searching by Tibetan alternate name (full and substring)
        response = client.get("/v2/texts?author=པདྨ་འབྱུང་གནས།")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=པདྨ")  # Tibetan substring "Padma"
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=འབྱུང་གནས")  # Tibetan substring "Jungne"
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test searching by Sanskrit alternate name (full and substring)
        response = client.get("/v2/texts?author=पद्मसम्भव")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=पद्म")  # Sanskrit substring "Padma"
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=सम्भव")  # Sanskrit substring "sambhava"
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test searching by Mongolian alternate name (full and substring)
        response = client.get("/v2/texts?author=Бадамжунай")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=Бадам")  # Mongolian substring "Badam"
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        response = client.get("/v2/texts?author=жунай")  # Mongolian substring "junai"
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test case insensitive search (if supported)
        response = client.get("/v2/texts?author=guru")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert {item["id"] for item in data} == {expression_id}

        # Test no match
        response = client.get("/v2/texts?author=NonExistent")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0

    def test_get_all_metadata_multiple_filters(self, client, test_database, test_person_data):
        """Test combining multiple filters"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression in Tibetan
        for i in range(2):
            translation_data = {
                "type": "translation",
                "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"} if i % 2 == 0 else {"zh": "Translation Expression"},
                "language": "bo" if i % 2 == 0 else "zh",
                "category_id": category_id,
                "target": root_id,
                "contributions": [{"person_id": person_id, "role": "translator"}],
            }
            translation_expression = ExpressionModelInput.model_validate(translation_data)
            test_database.create_expression(translation_expression)

        # Filter by type=root AND language=en
        response = client.get("/v2/texts?type=translation&language=zh")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["type"] == "translation"
        assert data[0]["language"] == "zh"

    def test_get_all_metadata_invalid_limit(self, client, test_database):
        """Test invalid limit parameters"""

        # Test limit too low
        response = client.get("/v2/texts?limit=0")
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "Limit must be between 1 and 100" in data["error"]

        # Test non-integer limit (Flask converts to None, then defaults to 20)
        response = client.get("/v2/texts?limit=abc")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)  # Should return empty list with default pagination

        # Test limit too high
        response = client.get("/v2/texts?limit=101")
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "Limit must be between 1 and 100" in data["error"]

    def test_get_all_metadata_invalid_offset(self, client, test_database):
        """Test invalid offset parameters"""

        # Test negative offset
        response = client.get("/v2/texts?offset=-1")
        assert response.status_code == 400
        data = json.loads(response.data)
        assert "Offset must be non-negative" in data["error"]

        # Test non-integer offset (Flask converts to None, then defaults to 0)
        response = client.get("/v2/texts?offset=abc")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert isinstance(data, list)  # Should return empty list with default pagination

    def test_get_all_metadata_edge_pagination(self, client, test_database, test_person_data):
        """Test edge cases for pagination"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create one expression
        expr_data = {
            "type": "root",
            "title": {"en": "Single Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        expression = ExpressionModelInput.model_validate(expr_data)
        test_database.create_expression(expression)

        # Test limit=1 (minimum)
        response = client.get("/v2/texts?limit=1")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

        # Test limit=100 (maximum)
        response = client.get("/v2/texts?limit=100")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

        # Test large offset (beyond available data)
        response = client.get("/v2/texts?offset=1000")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 0

    def test_get_all_metadata_fuzzy_title_search(self, client, test_database, test_person_data):
        """Test fuzzy search by title with typos - fuzzy matching is automatic for title/author"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        titles = [
            {"en": "Shakespeare Sonnets"},
            {"en": "Buddhist Philosophy"},
            {"en": "Meditation Guide"},
        ]

        expression_ids = []
        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(root_data)
            expression_ids.append(test_database.create_expression(expression))

        # Test fuzzy search with typo - "Shakspeare" should match "Shakespeare"
        response = client.get("/v2/texts?title=Shakspeare")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert "Shakespeare" in data[0]["title"]["en"]

        # Test fuzzy search with typo - "Buddist" should match "Buddhist"
        response = client.get("/v2/texts?title=Buddist")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert "Buddhist" in data[0]["title"]["en"]

        # Test exact match still works
        response = client.get("/v2/texts?title=Shakespeare")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert "Shakespeare" in data[0]["title"]["en"]

    def test_get_all_metadata_fuzzy_author_search(self, client, test_database):
        """Test fuzzy search by author name with typos - fuzzy matching is automatic"""
        # Create authors with different names
        author1 = PersonModelInput.model_validate({
            "name": {"en": "Tsongkhapa"},
            "alt_names": [{"en": "Lama Tsongkhapa"}],
            "bdrc": "P111111",
            "wiki": "Q111111",
        })
        author1_id = test_database.create_person(author1)

        author2 = PersonModelInput.model_validate({
            "name": {"en": "Nagarjuna"},
            "alt_names": [{"en": "Arya Nagarjuna"}],
            "bdrc": "P222222",
            "wiki": "Q222222",
        })
        author2_id = test_database.create_person(author2)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        # Create expressions by different authors
        expr1_data = {
            "type": "root",
            "title": {"en": "Lamrim Chenmo"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": author1_id, "role": "author"}],
        }
        test_database.create_expression(ExpressionModelInput.model_validate(expr1_data))

        expr2_data = {
            "type": "root",
            "title": {"en": "Mulamadhyamakakarika"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": author2_id, "role": "author"}],
        }
        test_database.create_expression(ExpressionModelInput.model_validate(expr2_data))

        # Test fuzzy search with typo - "Tsongkapa" (missing 'h') should match "Tsongkhapa"
        response = client.get("/v2/texts?author=Tsongkapa")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["title"]["en"] == "Lamrim Chenmo"

        # Test fuzzy search with typo - "Nagarjuna" with slight typo
        response = client.get("/v2/texts?author=Nagrajuna")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["title"]["en"] == "Mulamadhyamakakarika"

    def test_get_all_metadata_fuzzy_combined_filters(self, client, test_database, test_person_data):
        """Test fuzzy search combined with other filters"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        # Create expressions in different languages
        expr_en_data = {
            "type": "root",
            "title": {"en": "Buddhist Meditation"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        test_database.create_expression(ExpressionModelInput.model_validate(expr_en_data))

        expr_bo_data = {
            "type": "root",
            "title": {"bo": "སངས་རྒྱས་ཀྱི་སྒོམ།"},
            "language": "bo",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        test_database.create_expression(ExpressionModelInput.model_validate(expr_bo_data))

        # Fuzzy search with language filter - should only return English expression
        response = client.get("/v2/texts?title=Buddist&language=en")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1
        assert data[0]["language"] == "en"

    def test_get_all_metadata_fuzzy_tibetan_title_search(self, client, test_database, test_person_data):
        """Test fuzzy search with Tibetan titles"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'དཔེ་རྩ།'}
        )

        # Create expressions with Tibetan titles
        titles = [
            {"bo": "བྱང་ཆུབ་ལམ་རིམ་ཆེན་མོ།"},  # Lamrim Chenmo
            {"bo": "དབུ་མ་རྩ་བའི་ཚིག་ལེའུར་བྱས་པ།"},  # Mulamadhyamakakarika
            {"bo": "ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པའི་སྙིང་པོ།"},  # Heart Sutra
        ]

        expression_ids = []
        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "bo",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(root_data)
            expression_ids.append(test_database.create_expression(expression))

        # Test exact Tibetan search
        response = client.get("/v2/texts?title=བྱང་ཆུབ་ལམ་རིམ")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1
        assert any("བྱང་ཆུབ་ལམ་རིམ" in item["title"].get("bo", "") for item in data)

        # Test partial Tibetan title search
        response = client.get("/v2/texts?title=ཤེས་རབ")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1

    def test_get_all_metadata_fuzzy_tibetan_author_search(self, client, test_database):
        """Test fuzzy search with Tibetan author names"""
        # Create author with Tibetan name
        author = PersonModelInput.model_validate({
            "name": {"bo": "ཙོང་ཁ་པ་བློ་བཟང་གྲགས་པ།"},  # Tsongkhapa Lobzang Drakpa
            "alt_names": [
                {"bo": "རྗེ་རིན་པོ་ཆེ།"},  # Je Rinpoche
                {"en": "Tsongkhapa"}
            ],
            "bdrc": "P555555",
            "wiki": "Q555555",
        })
        author_id = test_database.create_person(author)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expr_data = {
            "type": "root",
            "title": {"bo": "བྱང་ཆུབ་ལམ་རིམ་ཆེན་མོ།"},
            "language": "bo",
            "category_id": category_id,
            "contributions": [{"person_id": author_id, "role": "author"}],
        }
        test_database.create_expression(ExpressionModelInput.model_validate(expr_data))

        # Test search by Tibetan author name
        response = client.get("/v2/texts?author=ཙོང་ཁ་པ")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

        # Test search by Tibetan alternate name
        response = client.get("/v2/texts?author=རྗེ་རིན་པོ་ཆེ")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) == 1

    def test_get_all_metadata_fuzzy_sanskrit_title_search(self, client, test_database, test_person_data):
        """Test fuzzy search with Sanskrit titles"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        # Create expressions with Sanskrit titles
        titles = [
            {"sa": "प्रज्ञापारमिताहृदयसूत्र"},  # Prajnaparamita Heart Sutra
            {"sa": "मूलमध्यमककारिका"},  # Mulamadhyamakakarika
            {"sa": "अभिधर्मकोश"},  # Abhidharmakosha
        ]

        expression_ids = []
        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "sa",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(root_data)
            expression_ids.append(test_database.create_expression(expression))

        # Test Sanskrit search
        response = client.get("/v2/texts?title=प्रज्ञापारमिता")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1

        # Test another Sanskrit term
        response = client.get("/v2/texts?title=मध्यमक")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1

    def test_get_all_metadata_fuzzy_chinese_title_search(self, client, test_database, test_person_data):
        """Test fuzzy search with Chinese titles"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        # Create expressions with Chinese titles
        titles = [
            {"zh": "般若波羅蜜多心經"},  # Heart Sutra
            {"zh": "中論"},  # Mulamadhyamakakarika
            {"zh": "大智度論"},  # Mahaprajnaparamita Shastra
        ]

        expression_ids = []
        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "zh",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            expression = ExpressionModelInput.model_validate(root_data)
            expression_ids.append(test_database.create_expression(expression))

        # Test Chinese search
        response = client.get("/v2/texts?title=般若波羅蜜")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1

        # Test another Chinese term
        response = client.get("/v2/texts?title=中論")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1

    def test_get_all_metadata_fuzzy_mixed_language_search(self, client, test_database):
        """Test fuzzy search across expressions with titles in multiple languages"""
        # Create author
        author = PersonModelInput.model_validate({
            "name": {"en": "Nagarjuna", "bo": "ཀླུ་སྒྲུབ།", "sa": "नागार्जुन"},
            "alt_names": [],
            "bdrc": "P666666",
            "wiki": "Q666666",
        })
        author_id = test_database.create_person(author)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        # Create same text with titles in different languages
        expr_en = {
            "type": "root",
            "title": {"en": "Fundamental Wisdom of the Middle Way"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": author_id, "role": "author"}],
        }
        en_id = test_database.create_expression(ExpressionModelInput.model_validate(expr_en))

        expr_bo = {
            "type": "root",
            "title": {"bo": "དབུ་མ་རྩ་བའི་ཚིག་ལེའུར་བྱས་པ།"},
            "language": "bo",
            "category_id": category_id,
            "contributions": [{"person_id": author_id, "role": "author"}],
        }
        bo_id = test_database.create_expression(ExpressionModelInput.model_validate(expr_bo))

        expr_sa = {
            "type": "root",
            "title": {"sa": "मूलमध्यमककारिका"},
            "language": "sa",
            "category_id": category_id,
            "contributions": [{"person_id": author_id, "role": "author"}],
        }
        sa_id = test_database.create_expression(ExpressionModelInput.model_validate(expr_sa))

        # Search by English author name - should find all three
        response = client.get("/v2/texts?author=Nagarjuna")
        assert response.status_code == 200
        data = json.loads(response.data)
        found_ids = {item["id"] for item in data}
        assert en_id in found_ids
        assert bo_id in found_ids
        assert sa_id in found_ids

        # Search by Tibetan author name - should find all three
        response = client.get("/v2/texts?author=ཀླུ་སྒྲུབ")
        assert response.status_code == 200
        data = json.loads(response.data)
        found_ids = {item["id"] for item in data}
        assert en_id in found_ids
        assert bo_id in found_ids
        assert sa_id in found_ids

        # Search by Sanskrit author name - should find all three
        response = client.get("/v2/texts?author=नागार्जुन")
        assert response.status_code == 200
        data = json.loads(response.data)
        found_ids = {item["id"] for item in data}
        assert en_id in found_ids
        assert bo_id in found_ids
        assert sa_id in found_ids

    def test_get_all_metadata_fuzzy_romanized_tibetan_search(self, client, test_database, test_person_data):
        """Test fuzzy search with romanized Tibetan (Wylie) that might have typos"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        # Create expressions with romanized Tibetan titles
        titles = [
            {"en": "Byang chub lam rim chen mo"},  # Lamrim Chenmo in Wylie
            {"en": "Dbu ma rtsa ba'i tshig le'ur byas pa"},  # MMK in Wylie
        ]

        for title in titles:
            root_data = {
                "type": "root",
                "title": title,
                "language": "en",
                "category_id": category_id,
                "contributions": [{"person_id": person_id, "role": "author"}],
            }
            test_database.create_expression(ExpressionModelInput.model_validate(root_data))

        # Test fuzzy search with slight typo in Wylie
        response = client.get("/v2/texts?title=Byang%20chub%20lam%20rim")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1

        # Test with typo - "Byang chub" vs "Byang chubb"
        response = client.get("/v2/texts?title=Byang%20chubb")
        assert response.status_code == 200
        data = json.loads(response.data)
        assert len(data) >= 1


class TestGetSingleTextV2:
    """Tests for GET /v2/texts/{text_id} endpoint (get single text)"""

    def test_get_single_metadata_by_text_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.get(f"/v2/texts/{expression_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["id"] == expression_id
        assert data["title"]["en"] == "Test Expression"
        assert data["title"]["bo"] == "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"
        assert data["language"] == "en"
        assert data["date"] == "2024-01-01"
        assert data["bdrc"] == "W123456"
        assert data["wiki"] == "Q789012"
        assert len(data["contributions"]) == 1
        assert data["contributions"][0]["role"] == "author"
        assert data["contributions"][0]["person_name"]["en"] == test_person_data["name"]["en"]
        assert data["contributions"][0]["person_name"]["bo"] == test_person_data["name"]["bo"]
        assert len(data["contributions"][0]["alt_names"]) >= 1
        assert data["contributions"][0]["alt_names"][0]["en"] == test_person_data["alt_names"][0]["en"]
        assert data["contributions"][0]["alt_names"][0]["bo"] == test_person_data["alt_names"][0]["bo"]
        assert data["target"] is None
        assert data["category_id"] == category_id

    def test_get_single_metadata_by_bdrc_id_success(self, client, test_database, test_person_data, test_expression_data):
        """Test successfully retrieving a single expression"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create test expression
        test_expression_data["contributions"] = [{"person_id": person_id, "role": "author"}]
        test_expression_data["category_id"] = category_id
        expression = ExpressionModelInput.model_validate(test_expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.get(f"/v2/texts/{test_expression_data['bdrc']}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["bdrc"] == test_expression_data['bdrc']
        assert data["title"]["en"] == "Test Expression"
        assert data["title"]["bo"] == "བརྟག་དཔྱད་ཚིག་སྒྲུབ།"
        assert data["language"] == "en"
        assert data["date"] == "2024-01-01"
        assert data["wiki"] == "Q789012"
        assert len(data["contributions"]) == 1
        assert data["contributions"][0]["role"] == "author"
        assert data["contributions"][0]["person_name"]["en"] == test_person_data["name"]["en"]
        assert data["contributions"][0]["person_name"]["bo"] == test_person_data["name"]["bo"]
        assert len(data["contributions"][0]["alt_names"]) >= 1
        assert data["contributions"][0]["alt_names"][0]["en"] == test_person_data["alt_names"][0]["en"]
        assert data["contributions"][0]["alt_names"][0]["bo"] == test_person_data["alt_names"][0]["bo"]
        assert data["target"] is None
        assert data["category_id"] == category_id

    def test_get_single_metadata_translation_expression(self, client, test_database, test_person_data):
        """Test retrieving TRANSLATION expression with target relationship"""

        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create target ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Target Root Expression"},
            "language": "en",
            "category_id": category_id,
            "contributions": [{"person_id": person_id, "role": "author"}],
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        target_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།", "en": "Translation Expression"},
            "language": "bo",
            "category_id": category_id,
            "target": target_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
        }
        translation_expression = ExpressionModelInput.model_validate(translation_data)
        translation_id = test_database.create_expression(translation_expression)

        response = client.get(f"/v2/texts/{translation_id}")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data["type"] == "translation"
        assert data["target"] == target_id
        assert data["language"] == "bo"
        assert data["contributions"][0]["role"] == "translator"
        assert data["contributions"][0]["person_name"]["en"] == test_person_data["name"]["en"]
        assert data["contributions"][0]["person_name"]["bo"] == test_person_data["name"]["bo"]
        assert len(data["contributions"][0]["alt_names"]) >= 1
        assert data["contributions"][0]["alt_names"][0]["en"] == test_person_data["alt_names"][0]["en"]
        assert data["contributions"][0]["alt_names"][0]["bo"] == test_person_data["alt_names"][0]["bo"]

    def test_get_single_metadata_invalid_id(self, client, test_database):
        """Test retrieving invalid expression id"""

        response = client.get("/v2/texts/invalid_id")

        assert response.status_code == 404
        data = json.loads(response.data)
        assert "not found" in data["error"].lower()


class TestPostTextV2:
    """Tests for POST /v2/texts/ endpoint (create text)"""

    def test_create_root_expression_success(self, client, test_database, test_person_data):
        """Test successfully creating a ROOT expression"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        expression_data = {
            "type": "root",
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "copyright": "Public domain",
            "license": "CC0"
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]
        assert "id" in data

        # Verify the expression was created by retrieving it
        created_id = data["id"]
        verify_response = client.get(f"/v2/texts/{created_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "New Root Expression"
        assert verify_data["target"] is None

    def test_create_root_expression_with_empty_contributions(self, client, test_database):
        """Test creating a ROOT expression with empty contributions"""
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Root With No Contributors"},
            "language": "en",
            "contributions": [],
            "category_id": category_id,
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "id" in data

        verify_response = client.get(f"/v2/texts/{data['id']}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["contributions"] == []

    def test_create_expression_missing_json(self, client):
        """Test POST with no JSON data"""

        response = client.post("/v2/texts", content_type="application/json")

        assert response.status_code == 500  # Flask returns 500 for empty JSON
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_invalid_json(self, client):
        """Test POST with invalid JSON"""

        response = client.post("/v2/texts", data="invalid json", content_type="application/json")

        assert response.status_code == 500
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_missing_required_fields(self, client):
        """Test POST with missing required fields"""

        # Missing title field
        expression_data = {"type": "root", "language": "en", "contributions": []}

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data

    def test_create_expression_invalid_type(self, client):
        """Test POST with invalid expression type"""
        expression_data = {"type": "invalid_type", "title": {"en": "Test"}, "language": "en", "contributions": []}

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status
        data = json.loads(response.data)
        assert "error" in data

    def test_create_root_expression_with_target_fails(self, client):
        """Test that ROOT expression with target fails validation"""
        expression_data = {
            "type": "root",
            "title": {"en": "Test"},
            "language": "en",
            "target": "some_target_id",
            "contributions": [],
            "copyright": "Public domain",
            "license": "CC0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_commentary_without_target_fails(self, client):
        """Test that COMMENTARY expression without target fails validation"""
        expression_data = {
            "type": "commentary",
            "title": {"en": "Test Commentary"},
            "language": "en",
            "contributions": [],
            "copyright": "Public domain",
            "license": "CC0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422  # Proper validation error status

    def test_create_translation_without_target_fails(self, client):
        """Test that TRANSLATION expression without target fails validation"""
        expression_data = {
            "type": "translation",
            "title": {"en": "Test Translation"},
            "language": "en",
            "contributions": [],
            "copyright": "Public domain",
            "license": "CC0",
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_standalone_commentary_with_na_target_not_implemented(self, client, test_database, test_person_data):
        """Test that standalone COMMENTARY with target='N/A' returns Not Implemented error"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Try to create standalone TRANSLATION expression
        expression_data = {
            "type": "translation",
            "title": {"en": "Standalone Translation", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "target": None
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 422
        data = json.loads(response.data)
        assert "error" in data
        assert "target must be provided" in data["error"]

    def test_create_standalone_translation_with_na_target_success(self, client, test_person_data, test_database):
        """Test successfully creating a standalone TRANSLATION with target='N/A'"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create standalone TRANSLATION expression
        expression_data = {
            "type": "translation",
            "title": {"en": "Standalone Translation", "bo": "སྒྱུར་བ་རང་དབང་།"},
            "language": "bo",
            "target": "N/A",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id,
        }

        response = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]
        assert "id" in data

        # Verify the expression was created by retrieving it
        created_id = data["id"]
        verify_response = client.get(f"/v2/texts/{created_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Standalone Translation"

    def test_create_translation_with_valid_root_target_surcess(self, client, test_database, test_person_data):
        """Test successfully creating a TRANSLATION with a valid root target"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(translation_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]

    def test_create_commentary_with_valid_root_target_surcess(self, client, test_database, test_person_data):
        """Test successfully creating a TRANSLATION with a valid root target"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
        }
        root_expression = ExpressionModelInput.model_validate(root_data)
        root_id = test_database.create_expression(root_expression)

        # Create COMMENTARY expression
        commentary_data = {
            "type": "commentary",
            "title": {"en": "Commentary Expression", "bo": "འགྲེལ་པ།"},
            "language": "bo",
            "target": root_id,
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(commentary_data), content_type="application/json")

        assert response.status_code == 201
        data = json.loads(response.data)
        assert "message" in data
        assert "Text created successfully" in data["message"]

    def test_create_translation_with_invalid_root_target(self, client, test_database, test_person_data):
        """Test creating a TRANSLATION with an invalid root target"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create TRANSLATION expression
        translation_data = {
            "type": "translation",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "target": "invalid_target",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(translation_data), content_type="application/json")

        assert response.status_code == 404

    def test_create_commentary_with_invalid_root_target(self, client, test_database, test_person_data):
        """Test creating a COMMENTARY with an invalid root target"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create COMMENTARY expression
        commentary_data = {
            "type": "commentary",
            "title": {"en": "Translation Expression", "bo": "སྒྱུར་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "bo",
            "target": "invalid_target",
            "contributions": [{"person_id": person_id, "role": "translator"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(commentary_data), content_type="application/json")

        assert response.status_code == 404
    
    def test_create_text_without_category_id(self, client, test_database, test_person_data):
        """Test creating a text without a category ID"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}]
        }

        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_text_with_invalid_person_role(self, client, test_database, test_person_data):
        """Test creating a text with an invalid person role"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "invalid_role"}],
            "category_id": category_id
        }
        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422
    
    def test_create_text_with_contributionmodel_both_bdrc_and_person_id(self, client, test_database, test_person_data):
        """Test creating a text with a ContributionModel containing both person_id and person_bdrc_id"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        root_data = {
            "type": "root",
            "title": {"en": "Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "person_bdrc_id": "P123456", "role": "author"}],
            "category_id": category_id
        }

        response = client.post("/v2/texts", data=json.dumps(root_data), content_type="application/json")

        assert response.status_code == 422

    def test_create_text_with_existing_bdrc_id(self, client, test_database, test_person_data):
        """Test creating a text with an existing BDRC ID"""
        # Create test person first
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create ROOT expression
        expression_data = {
            "type": "root",
            "bdrc": "T1234567",
            "title": {"en": "New Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "copyright": "Public domain",
            "license": "CC0"
        }
        response_1 = client.post("/v2/texts", data=json.dumps(expression_data), content_type="application/json")

        assert response_1.status_code == 201

        duplicate_expression_data = {
            "type": "root",
            "bdrc": "T1234567",
            "title": {"en": "Duplicate Root Expression", "bo": "རྩ་བའི་ཚིག་སྒྲུབ་གསར་པ།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id,
            "copyright": "Public domain",
            "license": "CC0"
        }

        response_2 = client.post("/v2/texts", data=json.dumps(duplicate_expression_data), content_type="application/json")

        assert response_2.status_code == 500

class TestUpdateTitleV2:
    """Tests for PUT /v2/texts/{expression_id}/title endpoint (update title)"""

    def test_update_title_preserves_other_languages(self, client, test_database, test_person_data):
        """Test that updating a title in one language preserves other language versions"""
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create expression with multiple language titles
        expression_data = {
            "type": "root",
            "title": {"en": "Original English Title", "bo": "བོད་ཡིག་མཚན་བྱང་།"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Verify both language versions exist
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Original English Title"
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"

        # Update only the English title
        update_data = {"title": {"en": "Updated English Title"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200
        data = json.loads(response.data)
        assert "message" in data
        assert "Title updated successfully" in data["message"]

        # Verify the English title was updated AND the Tibetan title was preserved
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Updated English Title"
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"  # Should still be present!

    def test_update_title_adds_new_language(self, client, test_database, test_person_data):
        """Test that updating a title with a new language adds it without removing existing ones"""
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)
        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )
        # Create expression with only English title
        expression_data = {
            "type": "root",
            "title": {"en": "English Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Add a Tibetan title
        update_data = {"title": {"bo": "བོད་ཡིག་མཚན་བྱང་།"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200

        # Verify both titles now exist
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "English Title"  # Original should be preserved
        assert verify_data["title"]["bo"] == "བོད་ཡིག་མཚན་བྱང་།"  # New should be added

    def test_update_title_updates_existing_language(self, client, test_database, test_person_data):
        """Test that updating an existing language version modifies it correctly"""
        # Create test person
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create expression with English title
        expression_data = {
            "type": "root",
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update the English title (same language)
        update_data = {"title": {"en": "Modified Title"}}
        response = client.put(f"/v2/texts/{expression_id}/title", data=json.dumps(update_data), content_type="application/json")

        assert response.status_code == 200

        # Verify the title was updated
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Modified Title"

    def test_update_alt_title_adds_new_language(self, client, test_database, test_person_data):
        """Test adding a new alt title language without removing existing ones"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Primary Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {"alt_title": {"bo": "མཚན་བྱང་གཞན།"}}
        response = client.put(
            f"/v2/texts/{expression_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 200

        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        alt_titles = verify_data.get("alt_titles") or []
        assert any(alt.get("bo") == "མཚན་བྱང་གཞན།" for alt in alt_titles)

    def test_update_alt_title_adds_second_title_for_language(self, client, test_database, test_person_data):
        """Test adding another alt title for the same language while preserving others"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Primary Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {
            "alt_title": {
                "bo": [
                "འཕགས་པ་མདོ་སྡུད་པ།",
                "འཕགས་པ་ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ་ཡན་ཏ་རིན་པོ་ཆེ་སྡུད་པ།",
                "འཕགས་པ་ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ་ཡོན་ཏན་རིན་པོ་ཆེ་སྡུད་པ།",
                "བཅོམ་ལྡན་འདས་མ་ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ་སྡུད་པ་ཚིགས་སུ་བཅད་པ།",
                "མདོ་སྡུད་པ།",
                "སྡུད་པ་ཚིགས་སུ་བཅད་པ།"
                ],
                "cmg": [
                "ᠬᠣᠳᠣᠬ ᠳᠣ ᠪᠢᠯᠢᠺ ᠦᠨ ᠴᠢᠨᠠᠳᠣ ᠭᠢᠵᠠᠭᠠᠷ᠎ᠠ ᠭᠥᠷᠥᠭᠰᠡᠨ ᠬᠣᠷᠢᠶᠠᠩᠭᠣᠢ ᠰᠢᠯᠥᠭ",
                "qutug tu bilig ün cinadu kijagar a kürügsen quriyanggui silüg"
                ]
            }
            }
        response = client.put(
            f"/v2/texts/{expression_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 200

        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        alt_titles = verify_data.get("alt_titles") or []

        # Extract all bo and cmg titles from the response
        bo_titles = [alt.get("bo") for alt in alt_titles if "bo" in alt]
        cmg_titles = [alt.get("cmg") for alt in alt_titles if "cmg" in alt]

        # Assert all bo titles are present
        assert "འཕགས་པ་མདོ་སྡུད་པ།" in bo_titles
        assert "འཕགས་པ་ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ་ཡན་ཏ་རིན་པོ་ཆེ་སྡུད་པ།" in bo_titles
        assert "འཕགས་པ་ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ་ཡོན་ཏན་རིན་པོ་ཆེ་སྡུད་པ།" in bo_titles
        assert "བཅོམ་ལྡན་འདས་མ་ཤེས་རབ་ཀྱི་ཕ་རོལ་ཏུ་ཕྱིན་པ་སྡུད་པ་ཚིགས་སུ་བཅད་པ།" in bo_titles
        assert "མདོ་སྡུད་པ།" in bo_titles
        assert "སྡུད་པ་ཚིགས་སུ་བཅད་པ།" in bo_titles

        # Assert all cmg titles are present
        assert "ᠬᠣᠳᠣᠬ ᠳᠣ ᠪᠢᠯᠢᠺ ᠦᠨ ᠴᠢᠨᠠᠳᠣ ᠭᠢᠵᠠᠭᠠᠷ᠎ᠠ ᠭᠥᠷᠥᠭᠰᠡᠨ ᠬᠣᠷᠢᠶᠠᠩᠭᠣᠢ ᠰᠢᠯᠥᠭ" in cmg_titles
        assert "qutug tu bilig ün cinadu kijagar a kürügsen quriyanggui silüg" in cmg_titles

        # Verify total count
        assert len(bo_titles) == 6
        assert len(cmg_titles) == 2

    def test_update_title_and_alt_title_together(self, client, test_database, test_person_data):
        """Test updating title and alt title in the same request"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {
            "title": {"en": "Updated Title"},
            "alt_title": {"bo": "མཚན་བྱང་གསར་པ།"},
        }
        response = client.put(
            f"/v2/texts/{expression_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 200

        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data["title"]["en"] == "Updated Title"
        alt_titles = verify_data.get("alt_titles") or []
        assert any(alt.get("bo") == "མཚན་བྱང་གསར་པ།" for alt in alt_titles)

    def test_update_alt_title_non_existing_language(self, client, test_database, test_person_data):
        """Test overwriting an existing alt title language while preserving others"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Primary Title"},
            "alt_titles": [{"bo": "མཚན་བྱང་རྙིང་།"}, {"en": "Alt English"}],
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {"alt_title": {"non-existing": "མཚན་བྱང་གསར་པ།"}}
        response = client.put(
            f"/v2/texts/{expression_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 400


    def test_update_title_nonexistent_expression(self, client):
        """Updating title on a non-existent expression should return 404, not 200 or 500."""
        fake_id = "nonexistent_expression_id"

        update_data = {"title": {"en": "Should Not Work"}}
        response = client.put(
            f"/v2/texts/{fake_id}/title",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 400

    def test_update_title_missing_json_body(
        self, client, test_database, test_person_data
    ):
        """PUT with no JSON body should return an error (like POST)."""
        # Create a minimal expression to update
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            "type": "root",
            "title": {"en": "Original Title"},
            "language": "en",
            "contributions": [{"person_id": person_id, "role": "author"}],
            "category_id": category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f"/v2/texts/{expression_id}/title",
            content_type="application/json",
        )

        # Mirror your POST tests (500 + {"error": ...})
        assert response.status_code == 400

class TestUpdateLicenseV2:
    """Tests for PUT /v2/texts/{expression_id}/license endpoint (update license)"""

    def test_update_license_success(self, client, test_database, test_person_data):
        """Happy path: updates license and persists it (verify via GET)."""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {'license': 'CC0'}
        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['message'] == 'License updated successfully'

        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['license'] == 'CC0'

    def test_update_license_missing_json_body(self, client, test_database, test_person_data):
        """PUT with no JSON body should return 400 + error."""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Missing Body'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            content_type='application/json',
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'Request body is required'

    def test_update_license_missing_license_field(self, client, test_database, test_person_data):
        """PUT with JSON but no 'license' should return 400 + error."""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Missing Field'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps({'something_else': 'value'}),
            content_type='application/json',
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert data['error'] == 'License is required'

    def test_update_license_invalid_license_value(self, client, test_database, test_person_data):
        """PUT with invalid license should return 400 and list valid values."""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'License Invalid Value'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f'/v2/texts/{expression_id}/license',
            data=json.dumps({'license': 'NOT_A_LICENSE'}),
            content_type='application/json',
        )

        assert response.status_code == 400
        data = json.loads(response.data)
        assert 'Invalid license type' in data['error']
        # Spot-check a couple known allowed values so the error remains helpful.
        assert 'CC0' in data['error']
        assert 'Public Domain Mark' in data['error']

    def test_update_license_nonexistent_expression_returns_404(self, client, test_database):
        """Nonexistent expression_id should return 404 (DataNotFound)."""
        response = client.put(
            '/v2/texts/nonexistent_expression_id/license',
            data=json.dumps({'license': 'CC0'}),
            content_type='application/json',
        )

        assert response.status_code == 404
        data = json.loads(response.data)
        assert 'not found' in data['error'].lower()


class TestUpdateTextV2:
    """Tests for PUT /v2/texts/{expression_id} endpoint (unified update)"""

    def test_update_text_bdrc_and_wiki(self, client, test_database, test_person_data):
        """Test updating bdrc and wiki identifiers"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update bdrc and wiki
        update_data = {
            'bdrc': 'W12345',
            'wiki': 'Q67890'
        }
        response = client.put(
            f'/v2/texts/{expression_id}',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200
        data = json.loads(response.data)
        assert data['message'] == 'Text updated successfully'
        assert data['id'] == expression_id

        # Verify the update
        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['bdrc'] == 'W12345'
        assert verify_data['wiki'] == 'Q67890'

    def test_update_text_date(self, client, test_database, test_person_data):
        """Test updating date field"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update date
        update_data = {
            'date': '15th century'
        }
        response = client.put(
            f'/v2/texts/{expression_id}',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200

        # Verify the update
        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['date'] == '15th century'

    def test_update_text_copyright_and_license(self, client, test_database, test_person_data):
        """Test updating copyright and license"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update copyright and license
        update_data = {
            'copyright': 'In copyright',
            'license': 'CC BY'
        }
        response = client.put(
            f'/v2/texts/{expression_id}',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200

        # Verify the update
        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['copyright'] == 'In copyright'
        assert verify_data['license'] == 'CC BY'

    def test_update_text_title(self, client, test_database, test_person_data):
        """Test updating title"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Original Title'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update title
        update_data = {
            'title': {'en': 'Updated Title'}
        }
        response = client.put(
            f'/v2/texts/{expression_id}',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200

        # Verify the update
        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['title']['en'] == 'Updated Title'

    def test_update_text_alt_title_dict_of_lists(self, client, test_database, test_person_data):
        """Test updating alt titles via `alt_title` (dict-of-lists), like /title endpoint."""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Primary Title'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        update_data = {
            "alt_title": {
                "bo": ["མཚན་བྱང་གཞན།-1", "མཚན་བྱང་གཞན།-2"],
                "en": ["Alternative Title 1", "Alternative Title 2"],
            }
        }
        response = client.put(
            f"/v2/texts/{expression_id}",
            data=json.dumps(update_data),
            content_type="application/json",
        )

        assert response.status_code == 200

        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        alt_titles = verify_data.get("alt_titles") or []

        assert any(alt.get("bo") == "མཚན་བྱང་གཞན།-1" for alt in alt_titles)
        assert any(alt.get("bo") == "མཚན་བྱང་གཞན།-2" for alt in alt_titles)
        assert any(alt.get("en") == "Alternative Title 1" for alt in alt_titles)
        assert any(alt.get("en") == "Alternative Title 2" for alt in alt_titles)

    def test_update_text_alt_titles_replaces_existing(self, client, test_database, test_person_data):
        """Test that updating alt_titles replaces all existing alt_titles (not appends)."""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category', 'bo': 'ཚིག་སྒྲུབ་གསར་པ།'}
        )

        # Create expression with initial alt_titles
        expression_data = {
            'type': 'root',
            'title': {'en': 'Primary Title'},
            'alt_titles': [{'en': 'Old Alt Title 1'}, {'bo': 'གཞན་མཚན་བྱང་རྙིང་པ།'}],
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Verify initial alt_titles exist
        initial_response = client.get(f"/v2/texts/{expression_id}")
        assert initial_response.status_code == 200
        initial_data = json.loads(initial_response.data)
        initial_alt_titles = initial_data.get("alt_titles") or []
        assert any(alt.get("en") == "Old Alt Title 1" for alt in initial_alt_titles)
        assert any(alt.get("bo") == "གཞན་མཚན་བྱང་རྙིང་པ།" for alt in initial_alt_titles)

        # Update with new alt_titles (should replace, not append)
        update_data = {
            "alt_title": {
                "en": ["New Alt Title 1", "New Alt Title 2"],
            }
        }
        response = client.put(
            f"/v2/texts/{expression_id}",
            data=json.dumps(update_data),
            content_type="application/json",
        )
        assert response.status_code == 200

        # Verify old alt_titles are removed and only new ones exist
        verify_response = client.get(f"/v2/texts/{expression_id}")
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        alt_titles = verify_data.get("alt_titles") or []

        # New alt_titles should exist
        assert any(alt.get("en") == "New Alt Title 1" for alt in alt_titles)
        assert any(alt.get("en") == "New Alt Title 2" for alt in alt_titles)

        # Old alt_titles should be gone (replaced, not appended)
        assert not any(alt.get("en") == "Old Alt Title 1" for alt in alt_titles)
        assert not any(alt.get("bo") == "གཞན་མཚན་བྱང་རྙིང་པ།" for alt in alt_titles)

    def test_update_text_multiple_fields(self, client, test_database, test_person_data):
        """Test updating multiple fields at once"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Update multiple fields
        update_data = {
            'bdrc': 'W99999',
            'wiki': 'Q88888',
            'date': '14th century',
            'title': {'en': 'Multi-field Updated Title'}
        }
        response = client.put(
            f'/v2/texts/{expression_id}',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 200

        # Verify all updates
        verify_response = client.get(f'/v2/texts/{expression_id}')
        assert verify_response.status_code == 200
        verify_data = json.loads(verify_response.data)
        assert verify_data['bdrc'] == 'W99999'
        assert verify_data['wiki'] == 'Q88888'
        assert verify_data['date'] == '14th century'
        assert verify_data['title']['en'] == 'Multi-field Updated Title'

    def test_update_text_no_fields_returns_error(self, client, test_database, test_person_data):
        """Test that updating with no fields returns an error"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        # Try to update with no fields
        update_data = {}
        response = client.put(
            f'/v2/texts/{expression_id}',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 400

    def test_update_text_nonexistent_returns_404(self, client, test_database):
        """Test that updating nonexistent expression returns 404"""
        update_data = {
            'bdrc': 'W12345'
        }
        response = client.put(
            '/v2/texts/nonexistent_id',
            data=json.dumps(update_data),
            content_type='application/json',
        )

        assert response.status_code == 404

    def test_update_text_no_body_returns_error(self, client, test_database, test_person_data):
        """Test that updating with no body returns an error"""
        person = PersonModelInput.model_validate(test_person_data)
        person_id = test_database.create_person(person)

        category_id = test_database.create_category(
            application='test_application',
            title={'en': 'Test Category'}
        )

        expression_data = {
            'type': 'root',
            'title': {'en': 'Test Text'},
            'language': 'en',
            'contributions': [{'person_id': person_id, 'role': 'author'}],
            'category_id': category_id
        }
        expression = ExpressionModelInput.model_validate(expression_data)
        expression_id = test_database.create_expression(expression)

        response = client.put(
            f'/v2/texts/{expression_id}',
            content_type='application/json',
        )

        assert response.status_code == 400

