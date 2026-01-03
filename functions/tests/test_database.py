# pylint: disable=redefined-outer-name,too-many-lines
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
from pathlib import Path

import pytest
from dotenv import load_dotenv
from exceptions import DataNotFoundError
from identifier import generate_id
from models import (
    ContributionInput,
    ContributorRole,
    ExpressionInput,
    LocalizedString,
    ManifestationInput,
    ManifestationType,
    PersonInput,
)
from database.database import Database
from exceptions import DataValidationError

# Load .env file if it exists
load_dotenv()


def load_constraints_file():
    """Load and return the contents of the Neo4j constraints file"""
    constraints_file = Path(__file__).parent.parent / "neo4j_constraints.cypher"
    if not constraints_file.exists():
        raise FileNotFoundError(f"Constraints file not found: {constraints_file}")

    with open(constraints_file, encoding="utf-8") as f:
        content = f.read()

    # Split by semicolons and filter out empty lines and comments-only lines
    statements = []
    for line in content.split(";"):
        line = line.strip()
        if line and not line.startswith("//") and "CREATE CONSTRAINT" in line:
            statements.append(line + ";")

    return statements


@pytest.fixture(scope="session")
def neo4j_connection():
    """Get Neo4j connection details from environment variables"""
    test_uri = os.environ.get("NEO4J_TEST_URI")
    test_password = os.environ.get("NEO4J_TEST_PASSWORD")

    if not test_uri or not test_password:
        pytest.skip(
            "Neo4j test credentials not provided. Set NEO4J_TEST_URI and NEO4J_TEST_PASSWORD environment variables."
        )

    return {"uri": test_uri, "auth": ("neo4j", test_password)}


@pytest.fixture
def test_database(neo4j_connection):
    """Create a Neo4JDatabase instance connected to the test Neo4j instance"""
    # Create Neo4j database with test connection
    db = Database(neo4j_uri=neo4j_connection["uri"], neo4j_auth=neo4j_connection["auth"])

    # Setup test schema and basic data
    with db.get_session() as session:
        # Clean up any existing data first
        session.run("MATCH (n) DETACH DELETE n")

        # Load and apply all constraints from the comprehensive constraints file
        constraint_statements = load_constraints_file()
        for statement in constraint_statements:
            session.run(statement)

        # Create test languages
        session.run("MERGE (l:Language {code: 'bo', name: 'Tibetan'})")
        session.run("MERGE (l:Language {code: 'en', name: 'English'})")
        session.run("MERGE (l:Language {code: 'sa', name: 'Sanskrit'})")
        session.run("MERGE (l:Language {code: 'zh', name: 'Chinese'})")

        # Create test text types (TextType enum values)
        session.run("MERGE (t:TextType {name: 'root'})")
        session.run("MERGE (t:TextType {name: 'commentary'})")
        session.run("MERGE (t:TextType {name: 'translation'})")

        # Create test role types (only allowed values per constraints)
        session.run("MERGE (r:RoleType {name: 'translator'})")
        session.run("MERGE (r:RoleType {name: 'author'})")
        session.run("MERGE (r:RoleType {name: 'reviser'})")

        # Create license type nodes
        session.run("MERGE (l:LicenseType {name: 'public'})")

    yield db

    # Cleanup after tests
    with db.get_session() as session:
        session.run("MATCH (n) DETACH DELETE n")

    # Close the database driver to avoid deprecation warning
    db.close()


class TestDatabase:
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
        person = PersonInput(
            name=LocalizedString({"bo": "རིན་ཆེན་སྡེ།", "en": "Rinchen De"}),
            alt_names=[LocalizedString({"bo": "རིན་ཆེན་སྡེ་བ།"}), LocalizedString({"en": "Rinchen Dewa"})],
        )

        # Create in database
        person_id = db.person.create(person)
        assert person_id is not None
        assert len(person_id) == 21  # NanoID length

        # Retrieve the person
        retrieved_person = db.person.get(person_id)
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

        person1 = PersonInput(name=LocalizedString({"en": "John Doe"}))
        person2 = PersonInput(name=LocalizedString({"bo": "རིན་ཆེན་སྡེ།"}))
        person3 = PersonInput(name=LocalizedString({"sa": "मञ्जुश्री", "en": "Manjushri"}))

        id1 = db.person.create(person1)
        id2 = db.person.create(person2)
        id3 = db.person.create(person3)

        # Retrieve all
        all_persons = db.person.get_all()
        assert len(all_persons) == 3

        # Check that our created persons are in the results
        person_ids = [p.id for p in all_persons]
        assert id1 in person_ids
        assert id2 in person_ids
        assert id3 in person_ids

    def test_person_not_found(self, test_database):
        """Test retrieving non-existent person"""
        db = test_database

        with pytest.raises(DataNotFoundError, match="Person with ID 'nonexistent' not found"):
            db.person.get("nonexistent")

    def test_person_with_bdrc_and_wiki_fields(self, test_database):
        """Test person creation and retrieval"""
        db = test_database

        # Create a person with bdrc and wiki values using the actual API
        person = PersonInput(
            bdrc="P123456",
            wiki="W123456",
            name=LocalizedString({"en": "Test Person", "bo": "བསྟན་པ་མི་"}),
            alt_names=[LocalizedString({"en": "Alternative Name"})],
        )

        # Create person using the actual create_person_neo4j method
        person_id = db.person.create(person)
        assert person_id is not None
        assert len(person_id) == 21  # NanoID length

        # Retrieve the person and validate ALL fields
        retrieved_person = db.person.get(person_id)

        # Validate that bdrc and wiki are properly stored and retrieved
        assert retrieved_person.id == person_id
        assert retrieved_person.bdrc == "P123456"
        assert retrieved_person.wiki == "W123456"
        assert retrieved_person.name.root == {"en": "Test Person", "bo": "བསྟན་པ་མི་"}
        assert len(retrieved_person.alt_names) == 1
        assert retrieved_person.alt_names[0].root == {"en": "Alternative Name"}  # Still empty

    def test_expressions_query_structure(self, test_database):
        """Test expressions query with empty database"""
        db = test_database

        # Test expressions query with no data
        result = db.expression.get_all(offset=0, limit=10, filters=None)
        assert isinstance(result, list)
        assert len(result) == 0  # Empty database

    def test_expression_not_found(self, test_database):
        """Test retrieving non-existent expression"""
        db = test_database

        with pytest.raises(DataNotFoundError, match="Expression with ID 'nonexistent' not found"):
            db.expression.get("nonexistent")

    def test_manifestation_not_found(self, test_database):
        """Test retrieving manifestations for non-existent expression"""
        # Test that getting manifestations for non-existent expression returns empty list
        manifestations = test_database.manifestation.get_all("nonexistent-expression-id")
        assert isinstance(manifestations, list)
        assert len(manifestations) == 0

    def test_database_connection_parameters(self, test_database):
        """Test that database is using the test connection parameters"""
        # This is more of a sanity check to ensure we're connected to the test database
        # We can't easily check the exact connection details, but we can verify
        # that the database responds to queries
        with test_database.get_session() as session:
            result = session.run("RETURN 'test connection' as message")
            record = result.single()
            assert record["message"] == "test connection"

    def test_create_root_expression_success(self, test_database):
        """Test successful creation of ROOT type expression with all components"""

        # First create a person to reference in contributions
        person = PersonInput(
            bdrc="P123456",
            wiki="Q123456",
            name=LocalizedString({"en": "Test Author", "bo": "རྩོམ་པ་པོ་"}),
            alt_names=[LocalizedString({"en": "Alternative Author Name"})],
        )
        person_id = test_database.person.create(person)

        # Create ROOT expression (no commentary_of or translation_of)
        expression = ExpressionInput(
            bdrc="W789012",
            wiki="Q789012",
            date="2024-01-15",
            title=LocalizedString({"bo": "དམ་པའི་ཆོས་པདྨ་དཀར་པོ།", "en": "The Sacred White Lotus Dharma"}),
            alt_titles=[LocalizedString({"bo": "པདྨ་དཀར་པོའི་མདོ།", "en": "White Lotus Sutra"})],
            language="bo",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )

        # Create the expression
        expression_id = test_database.expression.create(expression)

        # Verify the expression was created
        assert expression_id is not None
        assert len(expression_id) > 0

        # Verify we can retrieve the expression
        retrieved_expression = test_database.expression.get(expression_id)
        assert retrieved_expression.id == expression_id
        assert retrieved_expression.bdrc == "W789012"
        assert retrieved_expression.wiki == "Q789012"
        assert retrieved_expression.date == "2024-01-15"
        assert retrieved_expression.language == "bo"

        # Verify title
        assert "bo" in retrieved_expression.title.root
        assert "en" in retrieved_expression.title.root
        assert retrieved_expression.title.root["bo"] == "དམ་པའི་ཆོས་པདྨ་དཀར་པོ།"

        # Verify alt_titles
        assert retrieved_expression.alt_titles is not None
        assert len(retrieved_expression.alt_titles) == 1
        assert "bo" in retrieved_expression.alt_titles[0].root
        assert retrieved_expression.alt_titles[0].root["bo"] == "པདྨ་དཀར་པོའི་མདོ།"

        # Verify contributions
        assert len(retrieved_expression.contributions) == 1
        contribution = retrieved_expression.contributions[0]
        assert contribution.person_id == person_id
        assert contribution.role == ContributorRole.AUTHOR

    def test_create_root_expression_missing_person(self, test_database):
        """Test that creating expression with non-existent person fails and rolls back"""

        expression = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id="non-existent-person-id", role=ContributorRole.AUTHOR)],
        )

        # Should raise DataValidationError for missing person
        with pytest.raises(DataValidationError) as exc_info:
            test_database.expression.create(expression)

        assert "non-existent-person-id" in str(exc_info.value)

    def test_create_root_expression_language_support(self, test_database):
        """Test that various language codes including BCP47 variants are properly supported"""

        # Create a person
        person = PersonInput(
            name=LocalizedString({"en": "Test Person"}),
        )
        person_id = test_database.person.create(person)

        # Test various language inputs - BCP47 tags should be preserved
        test_cases = [
            ("bo-Latn", {"bo-Latn": "བོད་སྐད།", "en": "Tibetan Text"}),
            ("zh-Hans-CN", {"zh-Hans-CN": "中文", "en": "Chinese Text"}),
            ("en-US", {"en-US": "American English"}),
            ("bo", {"bo": "བོད་སྐད།", "en": "Tibetan Text"}),
        ]

        for input_lang, title_dict in test_cases:
            expression = ExpressionInput(
                title=LocalizedString(title_dict),
                language=input_lang,
                contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
            )

            # Should create successfully
            expression_id = test_database.expression.create(expression)
            assert expression_id is not None

            # Verify BCP47 language code is preserved
            retrieved = test_database.expression.get(expression_id)
            assert retrieved.language == input_lang

    def test_create_root_expression_multiple_contributions(self, test_database):
        """Test creating expression with multiple contributors"""
        # Create multiple persons
        author = PersonInput(
            name=LocalizedString({"en": "Primary Author"}),
        )
        author_id = test_database.person.create(author)

        reviser = PersonInput(
            name=LocalizedString({"en": "Reviser"}),
        )
        reviser_id = test_database.person.create(reviser)

        expression = ExpressionInput(
            title=LocalizedString({"en": "Multi-Contributor Work"}),
            language="en",
            contributions=[
                ContributionInput(person_id=author_id, role=ContributorRole.AUTHOR),
                ContributionInput(person_id=reviser_id, role=ContributorRole.REVISER),
            ],
        )

        expression_id = test_database.expression.create(expression)
        retrieved = test_database.expression.get(expression_id)

        # Verify both contributions
        assert len(retrieved.contributions) == 2

        # Check that we have both roles
        roles = {contrib.role for contrib in retrieved.contributions}
        assert ContributorRole.AUTHOR in roles
        assert ContributorRole.REVISER in roles

        # Check that we have both persons
        person_ids = {contrib.person_id for contrib in retrieved.contributions}
        assert author_id in person_ids
        assert reviser_id in person_ids

    def test_create_root_expression_minimal_data(self, test_database):
        """Test creating expression with minimal required data"""
        # Create a person
        person = PersonInput(
            name=LocalizedString({"en": "Minimal Person"}),
        )
        person_id = test_database.person.create(person)

        # Minimal expression (no bdrc, wiki, date, alt_titles)
        expression = ExpressionInput(
            title=LocalizedString({"en": "Minimal Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )

        expression_id = test_database.expression.create(expression)
        retrieved = test_database.expression.get(expression_id)

        assert retrieved.id == expression_id
        assert retrieved.bdrc is None
        assert retrieved.wiki is None
        assert retrieved.date is None
        assert retrieved.alt_titles is None or len(retrieved.alt_titles) == 0
        assert retrieved.title.root["en"] == "Minimal Expression"
        assert len(retrieved.contributions) == 1

    def test_create_root_expression_with_bdrc_id(self, test_database):
        """Test creating expression with contribution using person_bdrc_id instead of person_id"""
        # Create a person with BDRC ID
        person = PersonInput(
            name=LocalizedString({"en": "BDRC Person", "bo": "བདྲ་ཅ་མི་སྣ།"}),
            bdrc="P123456",  # This is the BDRC ID we'll use for lookup
        )
        person_id = test_database.person.create(person)

        # Create expression using person_bdrc_id instead of person_id
        expression = ExpressionInput(
            title=LocalizedString({"en": "Expression with BDRC Contributor"}),
            language="en",
            contributions=[
                ContributionInput(
                    person_bdrc_id="P123456",
                    role=ContributorRole.AUTHOR,
                )
            ],
        )

        # Should successfully create expression using BDRC ID
        expression_id = test_database.expression.create(expression)
        retrieved = test_database.expression.get(expression_id)

        # Verify the expression was created correctly
        assert retrieved.id == expression_id
        assert retrieved.title.root["en"] == "Expression with BDRC Contributor"
        assert len(retrieved.contributions) == 1

        # Verify the contribution is linked to the correct person
        contribution = retrieved.contributions[0]
        assert contribution.person_id == person_id  # Should resolve to the actual person_id
        assert contribution.person_bdrc_id == "P123456"  # Should also include the BDRC ID
        assert contribution.role == ContributorRole.AUTHOR

    def test_create_root_expression_missing_person_bdrc_id(self, test_database):
        """Test that creating expression with non-existent person_bdrc_id fails"""
        expression = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[
                ContributionInput(person_bdrc_id="P999999", role=ContributorRole.AUTHOR)
            ],
        )

        # Should raise DataValidationError for missing person
        with pytest.raises(DataValidationError) as exc_info:
            test_database.expression.create(expression)

        assert "P999999" in str(exc_info.value)

    def test_create_translation_expression_success(self, test_database):
        """Test creating a translation expression that links to parent"""
        # First create a root expression (parent)
        person = PersonInput(
            name=LocalizedString({"en": "Original Author"}),
        )
        person_id = test_database.person.create(person)

        root_expression = ExpressionInput(
            title=LocalizedString({"en": "Original Text"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_expression_id = test_database.expression.create(root_expression)

        # Create translator person
        translator = PersonInput(
            name=LocalizedString({"en": "Translator Name"}),
        )
        translator_id = test_database.person.create(translator)

        # Now create a translation expression using translation_of
        translation_expression = ExpressionInput(
            title=LocalizedString({"bo": "བསྒྱུར་བ།"}),
            language="bo",
            translation_of=root_expression_id,
            contributions=[ContributionInput(person_id=translator_id, role=ContributorRole.TRANSLATOR)],
        )

        translation_id = test_database.expression.create(translation_expression)
        retrieved = test_database.expression.get(translation_id)

        # Verify the translation was created correctly
        assert retrieved.id == translation_id
        assert retrieved.translation_of == root_expression_id
        assert retrieved.title.root["bo"] == "བསྒྱུར་བ།"
        assert retrieved.language == "bo"
        assert len(retrieved.contributions) == 1
        assert retrieved.contributions[0].role == ContributorRole.TRANSLATOR

    def test_create_expression_both_commentary_and_translation_fails(self, test_database):
        """Test that creating expression with both commentary_of and translation_of fails validation"""
        # Create a person for the contribution
        person = PersonInput(
            name=LocalizedString({"en": "Author"}),
        )
        person_id = test_database.person.create(person)

        # Create a root expression first
        root_expression = ExpressionInput(
            title=LocalizedString({"en": "Root Text"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_id = test_database.expression.create(root_expression)

        # Try to create expression with both commentary_of and translation_of - should fail validation
        with pytest.raises(ValueError, match="Cannot be both a commentary and translation"):
            ExpressionInput(
                title=LocalizedString({"bo": "བསྒྱུར་བ།"}),
                language="bo",
                commentary_of=root_id,
                translation_of=root_id,
                contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
            )

    def test_create_translation_expression_nonexistent_target(self, test_database):
        """Test that creating translation with non-existent target fails"""
        # Create a person for the contribution
        person = PersonInput(
            name=LocalizedString({"en": "Translator"}),
        )
        person_id = test_database.person.create(person)

        # Create translation with non-existent target
        translation_expression = ExpressionInput(
            title=LocalizedString({"bo": "བསྒྱུར་བ།"}),
            language="bo",
            translation_of="nonexistent-target-id",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.TRANSLATOR)],
        )

        # Should fail when trying to create in database
        with pytest.raises(Exception):
            test_database.expression.create(translation_expression)

    def test_create_commentary_expression_success(self, test_database):
        """Test creating a commentary expression using commentary_of"""
        # First create a root expression (parent)
        person = PersonInput(
            name=LocalizedString({"en": "Original Author"}),
        )
        person_id = test_database.person.create(person)

        root_expression = ExpressionInput(
            title=LocalizedString({"en": "Original Text"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_expression_id = test_database.expression.create(root_expression)

        # Create commentator person
        commentator = PersonInput(
            name=LocalizedString({"en": "Commentator Name"}),
        )
        commentator_id = test_database.person.create(commentator)

        # Now create a commentary expression using commentary_of
        commentary_expression = ExpressionInput(
            title=LocalizedString({"bo": "འགྲེལ་པ།"}),
            language="bo",
            commentary_of=root_expression_id,
            contributions=[ContributionInput(person_id=commentator_id, role=ContributorRole.AUTHOR)],
        )

        commentary_id = test_database.expression.create(commentary_expression)
        retrieved = test_database.expression.get(commentary_id)

        # Verify the commentary was created correctly
        assert retrieved.id == commentary_id
        assert retrieved.commentary_of == root_expression_id
        assert retrieved.bdrc is None
        assert retrieved.wiki is None
        assert retrieved.date is None
        assert retrieved.title.root["bo"] == "འགྲེལ་པ།"
        assert retrieved.language == "bo"
        assert len(retrieved.contributions) == 1
        assert retrieved.contributions[0].person_id == commentator_id
        assert retrieved.contributions[0].role == ContributorRole.AUTHOR

    def test_create_commentary_expression_nonexistent_target(self, test_database):
        """Test that creating commentary with non-existent target fails"""
        # Create a person for the contribution
        person = PersonInput(
            name=LocalizedString({"en": "Commentator"}),
        )
        person_id = test_database.person.create(person)

        # Create commentary expression with non-existent target
        commentary_expression = ExpressionInput(
            title=LocalizedString({"bo": "འགྲེལ་པ།"}),
            language="bo",
            commentary_of="nonexistent-target-id",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )

        # Should fail when trying to create in database
        with pytest.raises(Exception):
            test_database.expression.create(commentary_expression)

    def test_create_commentary_expression_with_multiple_contributions(self, test_database):
        """Test creating commentary expression with multiple contributors"""
        # Create target expression
        author = PersonInput(
            name=LocalizedString({"en": "Original Author"}),
        )
        author_id = test_database.person.create(author)

        root_expression = ExpressionInput(
            title=LocalizedString({"en": "Original Text"}),
            language="en",
            contributions=[ContributionInput(person_id=author_id, role=ContributorRole.AUTHOR)],
        )
        root_expression_id = test_database.expression.create(root_expression)

        # Create multiple contributors for commentary
        commentator = PersonInput(
            name=LocalizedString({"en": "Commentator"}),
        )
        commentator_id = test_database.person.create(commentator)

        reviser = PersonInput(
            name=LocalizedString({"en": "Reviser"}),
        )
        reviser_id = test_database.person.create(reviser)

        # Create commentary with multiple contributions
        commentary_expression = ExpressionInput(
            title=LocalizedString({"bo": "འགྲེལ་པ།"}),
            language="bo",
            commentary_of=root_expression_id,
            contributions=[
                ContributionInput(person_id=commentator_id, role=ContributorRole.AUTHOR),
                ContributionInput(person_id=reviser_id, role=ContributorRole.REVISER),
            ],
        )

        commentary_id = test_database.expression.create(commentary_expression)
        retrieved = test_database.expression.get(commentary_id)

        # Verify multiple contributions
        assert len(retrieved.contributions) == 2
        contribution_roles = {contrib.role for contrib in retrieved.contributions}
        assert ContributorRole.AUTHOR in contribution_roles
        assert ContributorRole.REVISER in contribution_roles

    # Manifestation Tests
    def test_get_manifestations_by_expression_empty(self, test_database):
        """Test getting manifestations for expression with no manifestations."""
        # Create a basic expression first
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        # Get manifestations for expression with no manifestations
        manifestations = test_database.manifestation.get_all(expression_id)
        assert manifestations == []

    def test_get_manifestations_by_expression_with_different_types(self, test_database):
        """Test getting manifestations for different expression types (ROOT, TRANSLATION, COMMENTARY)."""
        # Create person
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        # Test ROOT expression
        root_expression = ExpressionInput(
            title=LocalizedString({"en": "Root Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_id = test_database.expression.create(root_expression)
        root_manifestations = test_database.manifestation.get_all(root_id)
        assert isinstance(root_manifestations, list)

        # Test TRANSLATION expression
        translation_expression = ExpressionInput(
            title=LocalizedString({"bo": "འགྱུར་བ།"}),
            language="bo",
            translation_of=root_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.TRANSLATOR)],
        )
        translation_id = test_database.expression.create(translation_expression)
        translation_manifestations = test_database.manifestation.get_all(translation_id)
        assert isinstance(translation_manifestations, list)

        # Test COMMENTARY expression
        commentary_expression = ExpressionInput(
            title=LocalizedString({"bo": "འགྲེལ་པ།"}),
            language="bo",
            commentary_of=root_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        commentary_id = test_database.expression.create(commentary_expression)
        commentary_manifestations = test_database.manifestation.get_all(commentary_id)
        assert isinstance(commentary_manifestations, list)

    def test_create_manifestation_basic(self, test_database):
        """Test creating a basic manifestation."""
        # Create expression first
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="Test Source",
            colophon="Test colophon",
        )

        # Create manifestation in database
        manifestation_id = generate_id()
        test_database.manifestation.create(manifestation, manifestation_id, expression_id)

        # Verify we can get raw manifestations list
        retrieved_manifestations = test_database.manifestation.get_all(expression_id)
        assert isinstance(retrieved_manifestations, list)
        assert len(retrieved_manifestations) == 1
        assert retrieved_manifestations[0].id == manifestation_id

    def test_create_and_retrieve_manifestation(self, test_database):
        """Test creating a manifestation and retrieving it."""
        # Create expression first
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="Test Source",
        )

        # Create manifestation in database
        manifestation_id = generate_id()
        test_database.manifestation.create(manifestation, manifestation_id, expression_id)

        # Retrieve and verify
        retrieved_manifestations = test_database.manifestation.get_all(expression_id)
        assert len(retrieved_manifestations) == 1

        retrieved = retrieved_manifestations[0]
        assert retrieved.id == manifestation_id
        assert retrieved.type == ManifestationType.CRITICAL

    def test_create_multiple_manifestations_for_expression(self, test_database):
        """Test creating multiple manifestations for the same expression."""
        # Create expression first
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        # Create first manifestation (CRITICAL)
        manifestation1 = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="Test Source 1",
            colophon="First manifestation",
        )
        manifestation1_id = generate_id()
        test_database.manifestation.create(manifestation1, manifestation1_id, expression_id)

        # Create second manifestation (DIPLOMATIC - requires bdrc)
        manifestation2 = ManifestationInput(
            type=ManifestationType.DIPLOMATIC,
            bdrc="W12345",
            source="Test Source 2",
            colophon="Second manifestation",
        )
        manifestation2_id = generate_id()
        test_database.manifestation.create(manifestation2, manifestation2_id, expression_id)

        # Retrieve all manifestations
        retrieved_manifestations = test_database.manifestation.get_all(expression_id)
        assert len(retrieved_manifestations) == 2

        # Verify both manifestations are present
        retrieved_ids = {m.id for m in retrieved_manifestations}
        assert manifestation1_id in retrieved_ids
        assert manifestation2_id in retrieved_ids

        # Verify types
        retrieved_types = {m.type for m in retrieved_manifestations}
        assert ManifestationType.CRITICAL in retrieved_types
        assert ManifestationType.DIPLOMATIC in retrieved_types

    def test_create_manifestation_nonexistent_expression(self, test_database):
        """Test that creating manifestation for non-existent expression fails."""
        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="Test Source",
        )

        # Should raise DataValidationError for non-existent expression
        with pytest.raises(DataValidationError, match="Expression nonexistent-id does not exist"):
            test_database.manifestation.create(manifestation, generate_id(), "nonexistent-id")
