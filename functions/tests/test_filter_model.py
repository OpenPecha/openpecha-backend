import pytest
from filter_model import FilterModel
from pydantic import ValidationError


@pytest.mark.parametrize(
    "invalid_data, expected_error",
    [
        # Invalid: Missing required field in single filter
        ({"filter": {"operator": "==", "value": "en"}}, "field"),
        ({"filter": {"field": "language", "value": "en"}}, "operator"),
        ({"filter": {"field": "language", "operator": "=="}}, "value"),
        # Invalid: Unsupported operator
        ({"filter": {"field": "author", "operator": ">", "value": "Alice"}}, "operator"),
        # Invalid: `and` filter is empty
        ({"filter": {"and": []}}, "and"),
        # Invalid: `or` filter is empty
        ({"filter": {"or": []}}, "or"),
        # Invalid: `and` filter contains an invalid condition
        ({"filter": {"and": [{"field": "language", "operator": ">", "value": "en"}]}}, "operator"),
        # Invalid: `or` filter contains a missing field
        ({"filter": {"or": [{"operator": "==", "value": "en"}]}}, "field"),
        # Invalid: Random extra field
        ({"filter": {"field": "author", "operator": "==", "value": "Alice", "extra": "unexpected"}}, "extra_forbidden"),
        (
            {
                "filter": {
                    "and": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "author", "operator": "!=", "value": "Bob"},
                    ],
                    "or": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "author", "operator": "!=", "value": "Bob"},
                    ],
                }
            },
            "or",
        ),
    ],
)
def test_invalid_filter_model(invalid_data, expected_error):
    """Test invalid filter should raise ValidationError."""
    with pytest.raises(ValidationError) as excinfo:
        FilterModel.model_validate(invalid_data)

    assert expected_error in str(excinfo.value)


@pytest.mark.parametrize(
    "input_data, expected_dict",
    [
        ({}, {"filter": None}),
        ({"filter": None}, {"filter": None}),
        (
            {"filter": {"field": "language", "operator": "==", "value": "en"}},
            {"filter": {"field": "language", "operator": "==", "value": "en"}},
        ),
        (
            {"filter": {"field": "language", "operator": "==", "value": None}},
            {"filter": {"field": "language", "operator": "==", "value": None}},
        ),
        (
            {
                "filter": {
                    "and": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "author", "operator": "!=", "value": "Bob"},
                    ]
                }
            },
            {
                "filter": {
                    "and": [
                        {"field": "language", "operator": "==", "value": "en"},
                        {"field": "author", "operator": "!=", "value": "Bob"},
                    ]
                }
            },
        ),
        (
            {
                "filter": {
                    "or": [
                        {"field": "language", "operator": "==", "value": "zh"},
                        {"field": "author", "operator": "==", "value": "Alice"},
                    ]
                }
            },
            {
                "filter": {
                    "or": [
                        {"field": "language", "operator": "==", "value": "zh"},
                        {"field": "author", "operator": "==", "value": "Alice"},
                    ]
                }
            },
        ),
    ],
)
def test_filter_model_serialization(input_data, expected_dict):
    """Test serialization of FilterModel instances."""
    model = FilterModel(**input_data)
    assert model.model_dump(by_alias=True) == expected_dict
