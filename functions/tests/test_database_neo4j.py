# pylint: disable=redefined-outer-name
"""
Integration tests using real cloud Neo4j instance.

Requires environment variables:
- NEO4J_TEST_URI: Neo4j test instance URI
- NEO4J_TEST_PASSWORD: Password for test instance

Environment variables can be set via:
1. Shell environment (export NEO4J_TEST_URI=...)
2. .env file in project root (automatically loaded)
"""
import os

import pytest
from dotenv import load_dotenv
from exceptions import DataNotFound
from metadata_model_v2 import LocalizedString, PersonModel
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

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
    # Create Neo4j database with test connection
    db = Neo4JDatabase(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Create constraints
        session.run("CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE")
        session.run("CREATE CONSTRAINT expression_id IF NOT EXISTS FOR (e:Expression) REQUIRE e.id IS UNIQUE")

        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
        session.run("MERGE (l:Language {code: 'en', name: 'English'})")
        session.run("MERGE (l:Language {code: 'sa', name: 'Sanskrit'})")

        # Create test expression types
        session.run("MERGE (t:ExpressionType {name: 'translation'})")
        session.run("MERGE (t:ExpressionType {name: 'original'})")
        session.run("MERGE (t:ExpressionType {name: 'commentary'})")

        # Create test role types
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'editor'})")

    yield db

    # Cleanup after tests
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    db.close_driver()


class TestDatabaseNeo4jReal:
    """Integration tests with real Neo4j database using new Queries structure"""

    def test_env_loading(self):
        """Test that .env file is loaded correctly"""
        test_uri = os.environ.get("NEO4J_TEST_URI")
        test_password = os.environ.get("NEO4J_TEST_PASSWORD")

        # This test will pass if .env is loaded, skip if not
        if not test_uri or not test_password:
            pytest.skip("Neo4j test credentials not found in environment")

        # Basic validation that the values look correct
        assert test_uri.startswith(("neo4j://", "neo4j+s://", "bolt://")), f"Invalid Neo4j URI format: {test_uri}"
        assert len(test_password) > 0, "Neo4j password is empty"

    def test_create_and_retrieve_person(self, test_database):
        """Test full person creation and retrieval cycle"""
        db = test_database

        # Create a person with Tibetan and English names
        person = PersonModel(
            id="temp-id",  # Temporary ID, will be replaced by create_person_neo4j
            name=LocalizedString({"bo": "རིན་ཆེན་སྡེ།", "en": "Rinchen De"}),
            alt_names=[LocalizedString({"bo": "རིན་ཆེན་སྡེ་བ།"}), LocalizedString({"en": "Rinchen Dewa"})],
        )

        # Create in database
        person_id = db.create_person_neo4j(person)
        assert person_id is not None
        assert len(person_id) == 16  # Should be 16-character ID

        # Retrieve the person
        retrieved_person = db.get_person_neo4j(person_id)
        assert retrieved_person.id == person_id

        # Compare LocalizedString objects properly
        assert retrieved_person.name.root == {"bo": "རིན་ཆེན་སྡེ།", "en": "Rinchen De"}
        assert len(retrieved_person.alt_names) == 2

        # Check alt_names content regardless of order
        alt_names_roots = [alt_name.root for alt_name in retrieved_person.alt_names]
        assert {"bo": "རིན་ཆེན་སྡེ་བ།"} in alt_names_roots
        assert {"en": "Rinchen Dewa"} in alt_names_roots

    def test_get_all_persons(self, test_database):
        """Test retrieving all persons"""
        db = test_database

        person1 = PersonModel(id="temp-1", name=LocalizedString({"en": "John Doe"}))
        person2 = PersonModel(id="temp-2", name=LocalizedString({"bo": "རིན་ཆེན་སྡེ།"}))
        person3 = PersonModel(id="temp-3", name=LocalizedString({"sa": "मञ्जुश्री", "en": "Manjushri"}))

        id1 = db.create_person_neo4j(person1)
        id2 = db.create_person_neo4j(person2)
        id3 = db.create_person_neo4j(person3)

        # Retrieve all
        all_persons = db.get_all_persons_neo4j()
        assert len(all_persons) == 3

        # Check that our created persons are in the results
        person_ids = [p.id for p in all_persons]
        assert id1 in person_ids
        assert id2 in person_ids
        assert id3 in person_ids

    def test_person_not_found(self, test_database):
        """Test retrieving non-existent person"""
        db = test_database

        with pytest.raises(DataNotFound, match="Person with ID 'nonexistent' not found"):
            db.get_person_neo4j("nonexistent")

    def test_queries_class_structure(self, test_database):
        """Test that the new Queries class structure works correctly"""
        db = test_database

        # Test that queries are accessible via dictionary structure
        assert "fetch_all" in Queries.persons
        assert "fetch_by_id" in Queries.persons
        assert "create" in Queries.persons

        assert "fetch_all" in Queries.expressions
        assert "fetch_by_id" in Queries.expressions
        assert "fetch_related" in Queries.expressions

        # Test that the queries actually work with real Neo4j
        with db.get_session() as session:
            # Test persons query
            result = session.run(Queries.persons["fetch_all"])
            list(result)  # Should not raise an error

            # Test expressions query with parameters
            params = {"offset": 0, "limit": 10, "type": None, "language": None}
            result = session.run(Queries.expressions["fetch_all"], params)
            list(result)  # Should not raise an error

    def test_person_with_bdrc_and_wiki_fields(self, test_database):
        """Test person creation and retrieval"""
        db = test_database

        # Create a person with bdrc and wiki values using the actual API
        person = PersonModel(
            id="temp-id",  # Will be replaced by create_person_neo4j
            bdrc="P123456",
            wiki="W123456",
            name=LocalizedString({"en": "Test Person", "bo": "བསྟན་པ་མི་"}),
            alt_names=[LocalizedString({"en": "Alternative Name"})],
        )

        # Create person using the actual create_person_neo4j method
        person_id = db.create_person_neo4j(person)
        assert person_id is not None
        assert len(person_id) == 16  # Should be 16-character ID

        # Retrieve the person and validate ALL fields
        retrieved_person = db.get_person_neo4j(person_id)

        # Validate that bdrc and wiki are properly stored and retrieved
        assert retrieved_person.id == person_id
        assert retrieved_person.bdrc == "P123456"
        assert retrieved_person.wiki == "W123456"
        assert retrieved_person.name.root == {"en": "Test Person", "bo": "བསྟན་པ་མི་"}
        assert len(retrieved_person.alt_names) == 1
        assert retrieved_person.alt_names[0].root == {"en": "Alternative Name"}  # Still empty

    def test_person_name_fragments_in_real_query(self, test_database):
        """Test that the person name fragment methods work in real queries"""
        db = test_database

        # Create a person to test fragments
        person = PersonModel(
            id="temp-fragment-test",
            name=LocalizedString({"bo": "རིན་ཆེན་སྡེ།", "en": "Rinchen De"}),
            alt_names=[LocalizedString({"bo": "རིན་ཆེན་སྡེ་བ།"})],
        )
        person_id = db.create_person_neo4j(person)

        # Test that the fragments work in actual queries
        with db.get_session() as session:
            # Use the person fragment in a real query
            query = f"""
            MATCH (person:Person)
            WHERE person.id = $person_id
            RETURN {Queries.person_fragment('person')} AS person
            """
            result = session.run(query, person_id=person_id)
            record = result.single()

            assert record is not None
            person_data = record.data()["person"]
            assert person_data["id"] == person_id
            assert len(person_data["name"]) > 0  # Should have primary name
            assert len(person_data["alt_names"]) > 0  # Should have alternative names

    def test_expressions_query_structure(self, test_database):
        """Test expressions query with empty database"""
        db = test_database

        # Test expressions query with no data
        result = db.get_all_expressions_neo4j(offset=0, limit=10, filters={})
        assert isinstance(result, list)
        assert len(result) == 0  # Empty database

        # Test with filters
        filters = {"type": "translation", "language": "bo"}
        result = db.get_all_expressions_neo4j(offset=0, limit=10, filters=filters)
        assert isinstance(result, list)
        assert len(result) == 0  # Still empty

    def test_expression_not_found(self, test_database):
        """Test retrieving non-existent expression"""
        db = test_database

        with pytest.raises(DataNotFound, match="Expression with ID 'nonexistent' not found"):
            db.get_expression_neo4j("nonexistent")

    def test_manifestation_not_found(self, test_database):
        """Test retrieving non-existent manifestation"""
        db = test_database

        with pytest.raises(DataNotFound, match="Manifestation with ID 'nonexistent' not found"):
            db.get_manifestation_neo4j("nonexistent")

    def test_helper_methods_accessibility(self):
        """Test that helper methods are now public and accessible"""
        # Test that the static methods are accessible and return strings
        primary_name_fragment = Queries.person_primary_name("p")
        alt_names_fragment = Queries.person_alternative_names("p")
        title_primary_fragment = Queries.title_primary("e")
        title_alt_fragment = Queries.title_alternative("e")
        person_fragment = Queries.person_fragment("p")
        expression_fragment = Queries.expression_fragment("e")

        # All should be strings containing Cypher patterns
        assert isinstance(primary_name_fragment, str)
        assert isinstance(alt_names_fragment, str)
        assert isinstance(title_primary_fragment, str)
        assert isinstance(title_alt_fragment, str)
        assert isinstance(person_fragment, str)
        assert isinstance(expression_fragment, str)

        # Should contain expected Cypher patterns
        assert "HAS_NAME" in primary_name_fragment
        assert "ALTERNATIVE_OF" in alt_names_fragment
        assert "HAS_TITLE" in title_primary_fragment
        assert "HAS_TITLE" in title_alt_fragment
        assert "id:" in person_fragment
        assert "id:" in expression_fragment

    def test_database_connection_parameters(self, test_database):
        """Test that database is using the test connection parameters"""
        db = test_database

        # Verify we can connect and perform operations
        with db.get_session() as session:
            # Simple connectivity test
            result = session.run("RETURN 1 as test")
            record = result.single()
            assert record.data()["test"] == 1
