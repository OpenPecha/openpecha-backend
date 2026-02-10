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

    def test_create_manifestation_with_expression(self, test_database):
        """Test creating manifestation with expression in same transaction."""
        # Create a person for the expression contribution
        person = PersonInput(
            name=LocalizedString({"en": "Test Author"}),
        )
        person_id = test_database.person.create(person)

        # Create expression input (not yet in database)
        expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "New Expression Created With Manifestation"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )

        # Create manifestation input
        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="Test Source",
            colophon="Test colophon",
        )

        # Create both in same transaction
        manifestation_id = generate_id()
        expression_id = generate_id()
        test_database.manifestation.create(
            manifestation, manifestation_id, expression_id, expression=expression
        )

        # Verify expression was created
        retrieved_expression = test_database.expression.get(expression_id)
        assert retrieved_expression.id == expression_id
        assert retrieved_expression.title.root["en"] == "New Expression Created With Manifestation"
        assert len(retrieved_expression.contributions) == 1

        # Verify manifestation was created and linked
        retrieved_manifestation = test_database.manifestation.get(manifestation_id)
        assert retrieved_manifestation.id == manifestation_id
        assert retrieved_manifestation.source == "Test Source"
        assert retrieved_manifestation.type == ManifestationType.CRITICAL

        # Verify manifestation is linked to expression
        manifestations = test_database.manifestation.get_all(expression_id)
        assert len(manifestations) == 1
        assert manifestations[0].id == manifestation_id

    def test_create_manifestation_with_expression_rollback_on_invalid_person(self, test_database):
        """Test that transaction rolls back if expression creation fails due to invalid person."""
        # Create expression with non-existent person
        expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "Expression With Invalid Person"}),
            language="en",
            contributions=[ContributionInput(person_id="nonexistent-person-id", role=ContributorRole.AUTHOR)],
        )

        manifestation = ManifestationInput(
            type=ManifestationType.CRITICAL,
            source="Test Source",
        )

        manifestation_id = generate_id()
        expression_id = generate_id()

        # Should fail due to invalid person in expression
        with pytest.raises(DataValidationError, match="nonexistent-person-id"):
            test_database.manifestation.create(
                manifestation, manifestation_id, expression_id, expression=expression
            )

        # Verify nothing was created (transaction rolled back)
        with pytest.raises(DataNotFoundError):
            test_database.expression.get(expression_id)

        with pytest.raises(DataNotFoundError):
            test_database.manifestation.get(manifestation_id)

    def test_create_manifestation_with_translation_expression(self, test_database):
        """Test creating manifestation with translation expression in same transaction."""
        # Create person
        person = PersonInput(
            name=LocalizedString({"en": "Author"}),
        )
        person_id = test_database.person.create(person)

        # Create root expression first (parent)
        root_expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"en": "Root Text"}),
            language="en",
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.AUTHOR)],
        )
        root_expression_id = test_database.expression.create(root_expression)

        # Create translation expression input
        translation_expression = ExpressionInput(
            category_id="category",
            title=LocalizedString({"bo": "བསྒྱུར་བ།"}),
            language="bo",
            translation_of=root_expression_id,
            contributions=[ContributionInput(person_id=person_id, role=ContributorRole.TRANSLATOR)],
        )

        manifestation = ManifestationInput(
            type=ManifestationType.DIPLOMATIC,
            bdrc="W12345",
        )

        # Create both in same transaction
        manifestation_id = generate_id()
        translation_id = generate_id()
        test_database.manifestation.create(
            manifestation, manifestation_id, translation_id, expression=translation_expression
        )

        # Verify translation expression was created with parent link
        retrieved_expression = test_database.expression.get(translation_id)
        assert retrieved_expression.id == translation_id
        assert retrieved_expression.translation_of == root_expression_id
        assert retrieved_expression.language == "bo"

        # Verify manifestation was created
        retrieved_manifestation = test_database.manifestation.get(manifestation_id)
        assert retrieved_manifestation.id == manifestation_id
        assert retrieved_manifestation.type == ManifestationType.DIPLOMATIC


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


class TestSpanAdjustmentFunctions:
    """Unit tests for the new span adjustment helper functions."""

    def test_continuous_insert_at_position_zero_expands_first_segment(self):
        """Insert at position 0 should expand first continuous span (start=0)."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=0, end=10, insert_pos=0, insert_len=5)
        assert result == (0, 15)

    def test_continuous_insert_at_position_zero_shifts_non_first_segment(self):
        """Insert at position 0 should shift continuous spans that don't start at 0."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=5, end=15, insert_pos=0, insert_len=5)
        assert result == (10, 20)

    def test_continuous_insert_at_start_boundary_shifts(self):
        """Insert at continuous span start boundary should shift it."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=10, end=20, insert_pos=10, insert_len=5)
        assert result == (15, 25)

    def test_continuous_insert_at_end_boundary_expands(self):
        """Insert at continuous span end boundary should expand it."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=10, end=20, insert_pos=20, insert_len=5)
        assert result == (10, 25)

    def test_continuous_insert_inside_expands(self):
        """Insert inside continuous span should expand it."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=10, end=20, insert_pos=15, insert_len=5)
        assert result == (10, 25)

    def test_continuous_insert_after_unchanged(self):
        """Insert after continuous span should leave it unchanged."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=10, end=20, insert_pos=25, insert_len=5)
        assert result == (10, 20)

    def test_annotation_insert_at_start_boundary_shifts(self):
        """Insert at annotation start boundary should shift it."""
        from database.span_database import _adjust_annotation_for_insert

        result = _adjust_annotation_for_insert(start=10, end=20, insert_pos=10, insert_len=5)
        assert result == (15, 25)

    def test_annotation_insert_at_end_boundary_unchanged(self):
        """Insert at annotation end boundary should leave it unchanged."""
        from database.span_database import _adjust_annotation_for_insert

        result = _adjust_annotation_for_insert(start=10, end=20, insert_pos=20, insert_len=5)
        assert result == (10, 20)

    def test_annotation_insert_inside_expands(self):
        """Insert strictly inside annotation should expand it."""
        from database.span_database import _adjust_annotation_for_insert

        result = _adjust_annotation_for_insert(start=10, end=20, insert_pos=15, insert_len=5)
        assert result == (10, 25)

    def test_delete_fully_encompasses_returns_none(self):
        """Delete that fully encompasses span should return None."""
        from database.span_database import _adjust_span_for_delete

        result = _adjust_span_for_delete(start=10, end=20, del_start=5, del_end=25)
        assert result is None

    def test_delete_before_shifts_left(self):
        """Delete before span should shift it left."""
        from database.span_database import _adjust_span_for_delete

        result = _adjust_span_for_delete(start=20, end=30, del_start=5, del_end=10)
        assert result == (15, 25)

    def test_delete_after_unchanged(self):
        """Delete after span should leave it unchanged."""
        from database.span_database import _adjust_span_for_delete

        result = _adjust_span_for_delete(start=10, end=20, del_start=25, del_end=30)
        assert result == (10, 20)

    def test_delete_overlaps_start_trims(self):
        """Delete overlapping start should trim the span and shift."""
        from database.span_database import _adjust_span_for_delete

        # Span (10,20), delete [5,15): del_start <= start < del_end < end
        # Result: (del_start, end - del_len) = (5, 20 - 10) = (5, 10)
        result = _adjust_span_for_delete(start=10, end=20, del_start=5, del_end=15)
        assert result == (5, 10)

    def test_delete_overlaps_end_trims(self):
        """Delete overlapping end should trim the span."""
        from database.span_database import _adjust_span_for_delete

        result = _adjust_span_for_delete(start=10, end=20, del_start=15, del_end=25)
        assert result == (10, 15)

    def test_delete_inside_shrinks(self):
        """Delete inside span should shrink it."""
        from database.span_database import _adjust_span_for_delete

        result = _adjust_span_for_delete(start=10, end=30, del_start=15, del_end=20)
        assert result == (10, 25)

    def test_continuous_replace_exact_match_preserves(self):
        """Replace exact match should preserve continuous span."""
        from database.span_database import _adjust_continuous_for_replace

        result = _adjust_continuous_for_replace(
            start=10, end=20, replace_start=10, replace_end=20, new_len=15, is_first_encompassed=False
        )
        assert result == (10, 25)

    def test_continuous_replace_encompasses_first_keeps(self):
        """Replace encompassing first continuous span should keep it."""
        from database.span_database import _adjust_continuous_for_replace

        result = _adjust_continuous_for_replace(
            start=10, end=20, replace_start=5, replace_end=25, new_len=10, is_first_encompassed=True
        )
        assert result == (5, 15)

    def test_continuous_replace_encompasses_subsequent_deletes(self):
        """Replace encompassing subsequent continuous spans should delete them."""
        from database.span_database import _adjust_continuous_for_replace

        result = _adjust_continuous_for_replace(
            start=10, end=20, replace_start=5, replace_end=25, new_len=10, is_first_encompassed=False
        )
        assert result is None

    def test_annotation_replace_exact_match_deletes(self):
        """Replace exact match should delete annotation."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=10, end=20, replace_start=10, replace_end=20, new_len=15)
        assert result is None

    def test_annotation_replace_encompasses_deletes(self):
        """Replace encompassing annotation should delete it."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=10, end=20, replace_start=5, replace_end=25, new_len=10)
        assert result is None

    def test_replace_partial_overlap_start_trims(self):
        """Replace overlapping start should trim the span."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=10, end=20, replace_start=5, replace_end=15, new_len=3)
        assert result == (8, 13)

    def test_replace_partial_overlap_end_trims(self):
        """Replace overlapping end should trim the span."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=10, end=20, replace_start=15, replace_end=25, new_len=3)
        assert result == (10, 18)

    def test_replace_inside_span_adjusts(self):
        """Replace inside span should adjust its size."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=10, end=30, replace_start=15, replace_end=20, new_len=3)
        assert result == (10, 28)


class TestSpanAdjustmentEdgeCases:
    """Edge case tests from text-editing-edge-cases document."""

    def test_insert_at_boundary_between_adjacent_continuous_spans(self):
        """Insert at boundary between two adjacent continuous spans should not cause overlap.

        Setup: S1 [0, 10), S2 [10, 20)
        Operation: Insert at position 10
        Expected: S1 expands to [0, 15), S2 shifts to [15, 25) - no overlap
        """
        from database.span_database import _adjust_continuous_for_insert

        s1_result = _adjust_continuous_for_insert(start=0, end=10, insert_pos=10, insert_len=5)
        s2_result = _adjust_continuous_for_insert(start=10, end=20, insert_pos=10, insert_len=5)

        assert s1_result == (0, 15)
        assert s2_result == (15, 25)
        assert s1_result[1] == s2_result[0]

    def test_insert_at_segment_boundary_with_gap(self):
        """Insert at boundary between segments with gap.

        Setup: B1 [0, 3), space at 3, B2 [4, 9)
        Operation: Insert "very " at position 4
        Expected: B1 unchanged, B2 shifts
        """
        from database.span_database import _adjust_continuous_for_insert

        b1_result = _adjust_continuous_for_insert(start=0, end=3, insert_pos=4, insert_len=5)
        b2_result = _adjust_continuous_for_insert(start=4, end=9, insert_pos=4, insert_len=5)

        assert b1_result == (0, 3)
        assert b2_result == (9, 14)

    def test_delete_exact_match_returns_none(self):
        """Delete that exactly matches span should return None.

        Operation: Delete [4, 9) on span [4, 9)
        """
        from database.span_database import _adjust_span_for_delete

        result = _adjust_span_for_delete(start=4, end=9, del_start=4, del_end=9)
        assert result is None

    def test_delete_across_multiple_spans(self):
        """Delete crossing multiple spans should trim both.

        Setup: B2 [4, 9), B3 [10, 15)
        Operation: Delete [7, 12) - crosses both spans
        Expected: B2 trims to [4, 7), B3 trims and shifts to [7, 10)
        """
        from database.span_database import _adjust_span_for_delete

        b2_result = _adjust_span_for_delete(start=4, end=9, del_start=7, del_end=12)
        b3_result = _adjust_span_for_delete(start=10, end=15, del_start=7, del_end=12)

        assert b2_result == (4, 7)
        assert b3_result == (7, 10)

    def test_delete_creates_continuous_segmentation(self):
        """Delete entire segment should maintain continuity.

        Setup: S1 [0, 10), S2 [10, 20), S3 [20, 30)
        Operation: Delete [10, 20) (exactly S2)
        Expected: S1 stays [0, 10), S2 deleted, S3 shifts to [10, 20)
        """
        from database.span_database import _adjust_span_for_delete

        s1_result = _adjust_span_for_delete(start=0, end=10, del_start=10, del_end=20)
        s2_result = _adjust_span_for_delete(start=10, end=20, del_start=10, del_end=20)
        s3_result = _adjust_span_for_delete(start=20, end=30, del_start=10, del_end=20)

        assert s1_result == (0, 10)
        assert s2_result is None
        assert s3_result == (10, 20)
        assert s1_result is not None and s3_result is not None
        assert s1_result[1] == s3_result[0]

    def test_delete_partial_overlap_maintains_continuity(self):
        """Delete partial overlap should maintain continuity.

        Setup: S1 [0, 10), S2 [10, 20), S3 [20, 30)
        Operation: Delete [5, 15) (partial S1, partial S2)
        Expected: S1 [0, 5), S2 [5, 10), S3 [10, 20)
        """
        from database.span_database import _adjust_span_for_delete

        s1_result = _adjust_span_for_delete(start=0, end=10, del_start=5, del_end=15)
        s2_result = _adjust_span_for_delete(start=10, end=20, del_start=5, del_end=15)
        s3_result = _adjust_span_for_delete(start=20, end=30, del_start=5, del_end=15)

        assert s1_result == (0, 5)
        assert s2_result == (5, 10)
        assert s3_result == (10, 20)
        assert s1_result is not None and s2_result is not None and s3_result is not None
        assert s1_result[1] == s2_result[0]
        assert s2_result[1] == s3_result[0]

    def test_replace_same_length_exact_match_continuous(self):
        """Replace with same length on exact match should preserve continuous span.

        Operation: Replace [4, 9) with "QUICK" (5 chars) on span [4, 9)
        """
        from database.span_database import _adjust_continuous_for_replace

        result = _adjust_continuous_for_replace(
            start=4, end=9, replace_start=4, replace_end=9, new_len=5, is_first_encompassed=False
        )
        assert result == (4, 9)

    def test_replace_longer_text_exact_match_continuous(self):
        """Replace with longer text on exact match should expand continuous span.

        Operation: Replace [4, 9) with "VERY QUICK" (10 chars) on span [4, 9)
        """
        from database.span_database import _adjust_continuous_for_replace

        result = _adjust_continuous_for_replace(
            start=4, end=9, replace_start=4, replace_end=9, new_len=10, is_first_encompassed=False
        )
        assert result == (4, 14)

    def test_replace_shorter_text_exact_match_continuous(self):
        """Replace with shorter text on exact match should shrink continuous span.

        Operation: Replace [4, 9) with "QK" (2 chars) on span [4, 9)
        """
        from database.span_database import _adjust_continuous_for_replace

        result = _adjust_continuous_for_replace(
            start=4, end=9, replace_start=4, replace_end=9, new_len=2, is_first_encompassed=False
        )
        assert result == (4, 6)

    def test_replace_encompasses_multiple_continuous_keeps_first(self):
        """Replace encompassing multiple continuous spans should keep first, delete others.

        Setup: B2 [4, 9), B3 [10, 15)
        Operation: Replace [4, 15) with "FAST" (4 chars)
        Expected: B2 (first) becomes [4, 8), B3 deleted
        """
        from database.span_database import _adjust_continuous_for_replace

        b2_result = _adjust_continuous_for_replace(
            start=4, end=9, replace_start=4, replace_end=15, new_len=4, is_first_encompassed=True
        )
        b3_result = _adjust_continuous_for_replace(
            start=10, end=15, replace_start=4, replace_end=15, new_len=4, is_first_encompassed=False
        )

        assert b2_result == (4, 8)
        assert b3_result is None

    def test_replace_encompasses_annotation_deletes(self):
        """Replace encompassing annotation (not exact) should delete it.

        Operation: Replace [3, 10) on annotation [4, 9)
        """
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=4, end=9, replace_start=3, replace_end=10, new_len=7)
        assert result is None

    def test_multiple_segmentations_affected_correctly(self):
        """Multiple segmentations should be adjusted correctly.

        Setup:
        - Segmentation A: S_A1 [0, 20), S_A2 [20, 40)
        - Segmentation B: S_B1 [0, 10), S_B2 [10, 20), S_B3 [20, 40)

        Operation: Replace [5, 15) with "XXX" (3 chars), delta = -7

        Expected:
        - S_A1: inside replace -> [0, 13)
        - S_A2: shift -> [13, 33)
        - S_B1: trim end -> [0, 8)
        - S_B2: trim start, shift -> [8, 13)
        - S_B3: shift -> [13, 33)
        """
        from database.span_database import _adjust_continuous_for_replace

        s_a1 = _adjust_continuous_for_replace(
            start=0, end=20, replace_start=5, replace_end=15, new_len=3, is_first_encompassed=False
        )
        s_a2 = _adjust_continuous_for_replace(
            start=20, end=40, replace_start=5, replace_end=15, new_len=3, is_first_encompassed=False
        )
        s_b1 = _adjust_continuous_for_replace(
            start=0, end=10, replace_start=5, replace_end=15, new_len=3, is_first_encompassed=False
        )
        s_b2 = _adjust_continuous_for_replace(
            start=10, end=20, replace_start=5, replace_end=15, new_len=3, is_first_encompassed=False
        )
        s_b3 = _adjust_continuous_for_replace(
            start=20, end=40, replace_start=5, replace_end=15, new_len=3, is_first_encompassed=False
        )

        assert s_a1 == (0, 13)
        assert s_a2 == (13, 33)
        assert s_b1 == (0, 8)
        assert s_b2 == (8, 13)
        assert s_b3 == (13, 33)
        assert s_a1 is not None and s_a2 is not None and s_b1 is not None and s_b2 is not None and s_b3 is not None
        assert s_a1[1] == s_a2[0]
        assert s_b1[1] == s_b2[0]
        assert s_b2[1] == s_b3[0]

    def test_overlapping_annotations_handled_correctly(self):
        """Overlapping annotations should be handled correctly.

        Setup: N1 [5, 15), N2 [10, 20)
        Operation: Delete [12, 18)
        Expected: N1 trims to [5, 12), N2 shrinks to [10, 14)
        """
        from database.span_database import _adjust_span_for_delete

        n1_result = _adjust_span_for_delete(start=5, end=15, del_start=12, del_end=18)
        n2_result = _adjust_span_for_delete(start=10, end=20, del_start=12, del_end=18)

        assert n1_result == (5, 12)
        assert n2_result == (10, 14)

    def test_nested_spans_handled_correctly(self):
        """Nested spans should be handled correctly.

        Setup: Outer [0, 40), Inner [10, 20)
        Operation: Delete [5, 25)
        Expected: Outer shrinks to [0, 20), Inner deleted
        """
        from database.span_database import _adjust_span_for_delete

        outer_result = _adjust_span_for_delete(start=0, end=40, del_start=5, del_end=25)
        inner_result = _adjust_span_for_delete(start=10, end=20, del_start=5, del_end=25)

        assert outer_result == (0, 20)
        assert inner_result is None

    def test_annotation_insert_before_shifts(self):
        """Insert before annotation should shift it."""
        from database.span_database import _adjust_annotation_for_insert

        result = _adjust_annotation_for_insert(start=10, end=20, insert_pos=5, insert_len=5)
        assert result == (15, 25)

    def test_annotation_insert_after_unchanged(self):
        """Insert after annotation should leave it unchanged."""
        from database.span_database import _adjust_annotation_for_insert

        result = _adjust_annotation_for_insert(start=10, end=20, insert_pos=25, insert_len=5)
        assert result == (10, 20)

    def test_continuous_insert_before_shifts(self):
        """Insert before continuous span should shift it."""
        from database.span_database import _adjust_continuous_for_insert

        result = _adjust_continuous_for_insert(start=10, end=20, insert_pos=5, insert_len=5)
        assert result == (15, 25)

    def test_replace_before_shifts(self):
        """Replace before span should shift it by delta."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=20, end=30, replace_start=5, replace_end=10, new_len=3)
        assert result == (18, 28)

    def test_replace_after_unchanged(self):
        """Replace after span should leave it unchanged."""
        from database.span_database import _adjust_annotation_for_replace

        result = _adjust_annotation_for_replace(start=10, end=20, replace_start=25, replace_end=30, new_len=3)
        assert result == (10, 20)
