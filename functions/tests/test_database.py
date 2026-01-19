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

import pytest
from exceptions import DataNotFoundError, DataValidationError
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
            category_id="category",
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
            category_id="category",
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
                category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
            title=LocalizedString({"en": "Root Text"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_id = test_database.expression.create(root_expression)

        # Try to create expression with both commentary_of and translation_of - should fail validation
        with pytest.raises(ValueError, match="Cannot be both a commentary and translation"):
            ExpressionInput(
                category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
            title=LocalizedString({"en": "Root Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_id = test_database.expression.create(root_expression)
        root_manifestations = test_database.manifestation.get_all(root_id)
        assert isinstance(root_manifestations, list)

        # Test TRANSLATION expression
        translation_expression = ExpressionInput(
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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
            category_id="category",
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

    def test_create_manifestation_with_source(self, test_database):
        """Test that source field is stored in Source node and retrieved correctly."""
        # Create expression first
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        # Create manifestation with source
        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="BDRC Library",
            colophon="Test colophon",
        )
        manifestation_id = generate_id()
        test_database.manifestation.create(manifestation, manifestation_id, expression_id)

        # Retrieve and verify source is returned
        retrieved = test_database.manifestation.get(manifestation_id)
        assert retrieved.id == manifestation_id
        assert retrieved.source == "BDRC Library"
        assert retrieved.colophon == "Test colophon"
        assert retrieved.type == ManifestationType.CRITICAL

    def test_create_manifestation_without_source(self, test_database):
        """Test that manifestation without source works correctly."""
        # Create expression first
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        # Create manifestation without source
        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
        )
        manifestation_id = generate_id()
        test_database.manifestation.create(manifestation, manifestation_id, expression_id)

        # Retrieve and verify source is None
        retrieved = test_database.manifestation.get(manifestation_id)
        assert retrieved.id == manifestation_id
        assert retrieved.source is None
        assert retrieved.type == ManifestationType.CRITICAL


class TestSpanDatabase:
    """Tests for SpanDatabase span adjustment functionality."""

    def _create_test_setup(self, test_database):
        """Helper to create expression, manifestation, segmentation, and segment for testing."""
        person = PersonInput(name=LocalizedString({"en": "Test Author"}))
        person_id = test_database.person.create(person)

        expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "Test Expression"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        expression_id = test_database.expression.create(expression)

        manifestation = ManifestationInput(type=ManifestationType.CRITICAL)
        manifestation_id = generate_id()
        test_database.manifestation.create(manifestation, manifestation_id, expression_id)

        segment_id = generate_id()
        segmentation_id = generate_id()
        with test_database.get_session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (m:Manifestation {id: $manifestation_id})
                    CREATE (segmentation:Segmentation {id: $segmentation_id})-[:SEGMENTATION_OF]->(m)
                    CREATE (seg:Segment {id: $segment_id})-[:SEGMENT_OF]->(segmentation)
                    CREATE (span:Span {start: $start, end: $end})-[:SPAN_OF]->(seg)
                    """,
                    manifestation_id=manifestation_id,
                    segmentation_id=segmentation_id,
                    segment_id=segment_id,
                    start=0,
                    end=12,
                )
            )

        return {
            "expression_id": expression_id,
            "manifestation_id": manifestation_id,
            "segment_id": segment_id,
            "segmentation_id": segmentation_id,
        }

    def _add_note(self, test_database, manifestation_id: str, start: int, end: int) -> str:
        """Helper to add a note with a span."""
        note_id = generate_id()
        with test_database.get_session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (m:Manifestation {id: $manifestation_id}), (nt:NoteType {name: 'durchen'})
                    CREATE (span:Span {start: $start, end: $end})-[:SPAN_OF]->(n:Note {id: $note_id, text: 'test'})
                    CREATE (n)-[:NOTE_OF]->(m)
                    CREATE (n)-[:HAS_TYPE]->(nt)
                    """,
                    manifestation_id=manifestation_id,
                    note_id=note_id,
                    start=start,
                    end=end,
                )
            )
        return note_id

    def _add_second_segment(self, test_database, manifestation_id: str, start: int, end: int) -> str:
        """Helper to add a second segment."""
        segment_id = generate_id()
        with test_database.get_session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (m:Manifestation {id: $manifestation_id})<-[:SEGMENTATION_OF]-(segmentation:Segmentation)
                    CREATE (seg:Segment {id: $segment_id})-[:SEGMENT_OF]->(segmentation)
                    CREATE (span:Span {start: $start, end: $end})-[:SPAN_OF]->(seg)
                    """,
                    manifestation_id=manifestation_id,
                    segment_id=segment_id,
                    start=start,
                    end=end,
                )
            )
        return segment_id

    def _get_span(self, test_database, entity_id: str) -> tuple[int, int] | None:
        """Helper to get span for any entity."""
        with test_database.get_session() as session:
            result = session.run(
                "MATCH (s:Span)-[:SPAN_OF]->(e {id: $id}) RETURN s.start AS start, s.end AS end",
                id=entity_id,
            ).single()
            if result is None:
                return None
            return (result["start"], result["end"])

    def test_update_span_end(self, test_database):
        """Test updating a segment's span end position."""
        setup = self._create_test_setup(test_database)

        test_database.span.update_span_end(setup["segment_id"], 20)

        span = self._get_span(test_database, setup["segment_id"])
        assert span == (0, 20)

    def test_adjust_shifts_subsequent_segment(self, test_database):
        """Test that subsequent segments are shifted when content grows."""
        setup = self._create_test_setup(test_database)
        second_segment_id = self._add_second_segment(test_database, setup["manifestation_id"], 13, 28)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=20,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, second_segment_id)
        assert span == (21, 36)

    def test_adjust_shifts_subsequent_note(self, test_database):
        """Test that subsequent notes are shifted when content grows."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 15, 20)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=20,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span == (23, 28)

    def test_adjust_deletes_encompassed_note(self, test_database):
        """Test that notes fully encompassed by replacement are deleted."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 2, 8)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=5,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span is None

    def test_adjust_trims_overlapping_note_end(self, test_database):
        """Test that notes overlapping the replacement end are trimmed."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 5, 15)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=12,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span == (12, 15)

    def test_adjust_no_change_for_span_after_replacement(self, test_database):
        """Test that spans completely after replacement with same length stay unchanged."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 20, 30)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=12,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span == (20, 30)

    def test_adjust_shrinking_content_shifts_left(self, test_database):
        """Test that subsequent spans shift left when content shrinks."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 20, 30)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=4,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span == (12, 22)

    def test_adjust_trims_overlapping_start(self, test_database):
        """Test that spans overlapping replacement start are adjusted."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 5, 20)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=10,
            replace_end=15,
            new_length=5,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span == (5, 20)

    def test_adjust_inside_span_expands(self, test_database):
        """Test that replacement inside a span expands it."""
        setup = self._create_test_setup(test_database)
        note_id = self._add_note(test_database, setup["manifestation_id"], 0, 30)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=10,
            replace_end=15,
            new_length=10,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, note_id)
        assert span == (0, 35)

    def _add_page(self, test_database, manifestation_id: str, start: int, end: int) -> str:
        """Helper to add a page with a span."""
        page_id = generate_id()
        with test_database.get_session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (m:Manifestation {id: $manifestation_id})
                    CREATE (span:Span {start: $start, end: $end})-[:SPAN_OF]->(p:Page {id: $page_id, index: 1})
                    CREATE (p)-[:PAGE_OF]->(m)
                    """,
                    manifestation_id=manifestation_id,
                    page_id=page_id,
                    start=start,
                    end=end,
                )
            )
        return page_id

    def test_adjust_shifts_page(self, test_database):
        """Test that pages are shifted when content grows."""
        setup = self._create_test_setup(test_database)
        page_id = self._add_page(test_database, setup["manifestation_id"], 15, 40)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=20,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, page_id)
        assert span == (23, 48)

    def _add_bibliographic(self, test_database, manifestation_id: str, start: int, end: int) -> str:
        """Helper to add bibliographic metadata with a span."""
        bib_id = generate_id()
        with test_database.get_session() as session:
            session.execute_write(
                lambda tx: tx.run(
                    """
                    MATCH (m:Manifestation {id: $manifestation_id})
                    CREATE (span:Span {start: $start, end: $end})-[:SPAN_OF]->(b:BibliographicMetadata {id: $bib_id})
                    CREATE (b)-[:BIBLIOGRAPHY_OF]->(m)
                    """,
                    manifestation_id=manifestation_id,
                    bib_id=bib_id,
                    start=start,
                    end=end,
                )
            )
        return bib_id

    def test_adjust_shifts_bibliographic(self, test_database):
        """Test that bibliographic metadata is shifted when content grows."""
        setup = self._create_test_setup(test_database)
        bib_id = self._add_bibliographic(test_database, setup["manifestation_id"], 15, 25)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=20,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, bib_id)
        assert span == (23, 33)

    def test_adjust_deletes_encompassed_bibliographic(self, test_database):
        """Test that bibliographic metadata fully inside replacement is deleted."""
        setup = self._create_test_setup(test_database)
        bib_id = self._add_bibliographic(test_database, setup["manifestation_id"], 2, 10)

        test_database.span.adjust_affected_spans(
            manifestation_id=setup["manifestation_id"],
            replace_start=0,
            replace_end=12,
            new_length=5,
            exclude_entity_id=setup["segment_id"],
        )

        span = self._get_span(test_database, bib_id)
        assert span is None
