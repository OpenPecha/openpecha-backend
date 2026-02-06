"""Tests for TextOperation model validation."""

import pytest
from pydantic import ValidationError

from models import DeleteOperation, InsertOperation, ReplaceOperation, TextOperation


class TestTextOperationValidation:
    """Test TextOperation model validation."""

    def test_insert_valid(self):
        """Valid INSERT operation should pass validation."""
        op = TextOperation.model_validate({"type": "insert", "position": 10, "text": "hello"})
        assert op.operation.type == "insert"
        assert isinstance(op.operation, InsertOperation)
        assert op.operation.position == 10
        assert op.operation.text == "hello"

    def test_insert_missing_position_fails(self):
        """INSERT without position should fail."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "insert", "text": "hello"})

    def test_insert_missing_text_fails(self):
        """INSERT without text should fail."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "insert", "position": 10})

    def test_insert_with_start_end_ignored(self):
        """INSERT with start/end should ignore extra fields (extra=forbid on model)."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "insert", "position": 10, "text": "hello", "start": 5, "end": 15})

    def test_delete_valid(self):
        """Valid DELETE operation should pass validation."""
        op = TextOperation.model_validate({"type": "delete", "start": 10, "end": 20})
        assert op.operation.type == "delete"
        assert isinstance(op.operation, DeleteOperation)
        assert op.operation.start == 10
        assert op.operation.end == 20

    def test_delete_missing_start_fails(self):
        """DELETE without start should fail."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "delete", "end": 20})

    def test_delete_missing_end_fails(self):
        """DELETE without end should fail."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "delete", "start": 10})

    def test_delete_start_gte_end_fails(self):
        """DELETE with start >= end should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TextOperation.model_validate({"type": "delete", "start": 20, "end": 10})
        assert "'start' must be less than 'end'" in str(exc_info.value)

    def test_delete_with_position_fails(self):
        """DELETE with position should fail (extra=forbid)."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "delete", "start": 10, "end": 20, "position": 15})

    def test_delete_with_text_fails(self):
        """DELETE with text should fail (extra=forbid)."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "delete", "start": 10, "end": 20, "text": "hello"})

    def test_replace_valid(self):
        """Valid REPLACE operation should pass validation."""
        op = TextOperation.model_validate({"type": "replace", "start": 10, "end": 20, "text": "replacement"})
        assert op.operation.type == "replace"
        assert isinstance(op.operation, ReplaceOperation)
        assert op.operation.start == 10
        assert op.operation.end == 20
        assert op.operation.text == "replacement"

    def test_replace_missing_start_fails(self):
        """REPLACE without start should fail."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "replace", "end": 20, "text": "replacement"})

    def test_replace_missing_text_fails(self):
        """REPLACE without text should fail."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "replace", "start": 10, "end": 20})

    def test_replace_start_gte_end_fails(self):
        """REPLACE with start >= end should fail."""
        with pytest.raises(ValidationError) as exc_info:
            TextOperation.model_validate({"type": "replace", "start": 20, "end": 10, "text": "replacement"})
        assert "'start' must be less than 'end'" in str(exc_info.value)

    def test_replace_with_position_fails(self):
        """REPLACE with position should fail (extra=forbid)."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "replace", "start": 10, "end": 20, "text": "replacement", "position": 15})

    def test_insert_position_zero_valid(self):
        """INSERT at position 0 should be valid."""
        op = TextOperation.model_validate({"type": "insert", "position": 0, "text": "prefix"})
        assert isinstance(op.operation, InsertOperation)
        assert op.operation.position == 0

    def test_text_empty_string_fails(self):
        """Empty text string should fail validation (min_length=1)."""
        with pytest.raises(ValidationError):
            TextOperation.model_validate({"type": "insert", "position": 10, "text": ""})
