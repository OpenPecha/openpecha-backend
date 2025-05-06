# type: ignore
import json

import pytest
from category_model import CategoryModel
from metadata_model import LocalizedString
from pydantic import ValidationError


class TestValidCategoryModel:
    """Tests for valid category model instances."""

    def test_minimal_category(self):
        """Test creation of a category with only required fields."""
        input_data = {"name": {"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}}

        model = CategoryModel.model_validate(input_data)
        assert model.name["en"] == "Test Category"
        assert model.name["bo"] == "ཚོད་ལེན་སྡེ་ཚན།"
        assert model.description is None
        assert model.short_description is None
        assert model.parent is None

    def test_category_with_description(self):
        """Test creation of a category with description."""
        input_data = {
            "name": {"en": "Prasangika", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
            "description": {"en": "Category of Prasangika pechas", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
        }

        model = CategoryModel.model_validate(input_data)
        assert model.name["en"] == "Prasangika"
        assert model.description is not None
        assert model.description["en"] == "Category of Prasangika pechas"
        assert model.short_description is None
        assert model.parent is None

    def test_category_with_short_description(self):
        """Test creation of a category with short description."""
        input_data = {
            "name": {"en": "Prasangika", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
            "short_description": {"en": "Prasangika category", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
        }

        model = CategoryModel.model_validate(input_data)
        assert model.name["en"] == "Prasangika"
        assert model.description is None
        assert model.short_description is not None
        assert model.short_description["en"] == "Prasangika category"
        assert model.parent is None

    def test_category_with_parent(self):
        """Test creation of a category with parent reference."""
        input_data = {"name": {"en": "Madhyamaka", "bo": "དབུ་མ།"}, "parent": "CAT001"}

        model = CategoryModel.model_validate(input_data)
        assert model.name["en"] == "Madhyamaka"
        assert model.parent == "CAT001"
        assert model.description is None
        assert model.short_description is None

    def test_complete_category(self):
        """Test category with all fields provided."""
        input_data = {
            "name": {"en": "Prasangika", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
            "description": {"en": "Category of Prasangika pechas", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
            "short_description": {"en": "Prasangika category", "bo": "པར་སངས་གི་སྲིད་སྤེས་ས།"},
            "parent": "CAT001",
        }

        model = CategoryModel.model_validate(input_data)
        assert model.name["en"] == "Prasangika"
        assert model.name["bo"] == "པར་སངས་གི་སྲིད་སྤེས་ས།"
        assert model.description is not None
        assert model.description["en"] == "Category of Prasangika pechas"
        assert model.short_description is not None
        assert model.short_description["en"] == "Prasangika category"
        assert model.parent == "CAT001"


class TestInvalidCategoryModel:
    """Tests for invalid category model instances."""

    def test_missing_name(self):
        """Test validation error when name is missing."""
        with pytest.raises(ValidationError) as excinfo:
            CategoryModel.model_validate({})
        assert "name" in str(excinfo.value)
        assert "Field required" in str(excinfo.value)

    def test_empty_name(self):
        """Test validation error when name is empty."""
        with pytest.raises(ValidationError) as excinfo:
            CategoryModel.model_validate({"name": {}})

        error_details = [e for e in excinfo.value.errors()]
        assert any("name" in str(e) for e in error_details)

    def test_invalid_name_type(self):
        """Test validation error when name is not a dictionary."""
        with pytest.raises(ValidationError) as excinfo:
            CategoryModel.model_validate({"name": "Not a dictionary"})

        error_details = [e for e in excinfo.value.errors()]
        assert any("name" in str(e) for e in error_details)

    def test_invalid_description_type(self):
        """Test validation error when description is not a dictionary."""
        with pytest.raises(ValidationError) as excinfo:
            CategoryModel.model_validate(
                {"name": {"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}, "description": "Not a dictionary"}
            )

        error_details = [e for e in excinfo.value.errors()]
        assert any("description" in str(e) for e in error_details)

    def test_invalid_parent_type(self):
        """Test validation error when parent is not a string."""
        with pytest.raises(ValidationError) as excinfo:
            CategoryModel.model_validate(
                {"name": {"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}, "parent": 123}  # Should be a string
            )

        error_details = [e for e in excinfo.value.errors()]
        assert any("parent" in str(e) for e in error_details)

    def test_extra_fields(self):
        """Test validation error when extra fields are provided."""
        with pytest.raises(ValidationError) as excinfo:
            CategoryModel.model_validate(
                {"name": {"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}, "extra_field": "This should not be allowed"}
            )

        assert "extra_forbidden" in str(excinfo.value)


class TestCategoryModelSerialization:
    """Tests for serialization of CategoryModel instances."""

    def test_serialization(self):
        """Test serialization to JSON."""
        category = CategoryModel(
            name=LocalizedString({"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}),
            description=LocalizedString({"en": "Test Description", "bo": "ཚོད་ལེན་འགྲེལ་བཤད།"}),
            short_description=LocalizedString({"en": "Short Desc", "bo": "འགྲེལ་བཤད་ཐུང་ངུ་།"}),
            parent="CAT001",
        )

        serialized = json.loads(category.model_dump_json())

        # Check all fields are serialized correctly
        assert serialized["name"] == {"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}
        assert serialized["description"] == {"en": "Test Description", "bo": "ཚོད་ལེན་འགྲེལ་བཤད།"}
        assert serialized["short_description"] == {"en": "Short Desc", "bo": "འགྲེལ་བཤད་ཐུང་ངུ་།"}
        assert serialized["parent"] == "CAT001"

    def test_serialization_with_nulls(self):
        """Test serialization with null values."""
        category = CategoryModel(
            name=LocalizedString({"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}),
        )

        serialized = json.loads(category.model_dump_json())

        # Check optional fields are serialized as null
        assert serialized["name"] == {"en": "Test Category", "bo": "ཚོད་ལེན་སྡེ་ཚན།"}
        assert serialized["description"] is None
        assert serialized["short_description"] is None
        assert serialized["parent"] is None
