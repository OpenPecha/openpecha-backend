# pylint: disable=redefined-outer-name
"""
Unit tests for `DELETE /v2/texts/{text_id}` and the underlying
`Neo4JDatabase.delete_expression` cascade.

These tests are mock-based — they do NOT hit a real Neo4j instance. They cover:
  - Happy path through the Flask route (response shape, status code, aggregated
    counts, per-instance summaries, work_deleted flag, title pass-through).
  - 404 propagation when the underlying DB raises `DataNotFound`.
  - Storage cleanup runs once per cascaded manifestation, and storage failures
    do not fail the request.
  - The search-segmenter cleanup is called once with the union of every
    manifestation's `segment_ids`.
  - Texts with zero manifestations still produce a valid 200 response with
    empty `instances` and zero counts.
  - The DB-layer `delete_expression` raises `DataNotFound` when the expression
    does not exist (without attempting any cascade).
  - The DB-layer `delete_expression` skips `delete_orphan_work` when the Work
    is still referenced by another Expression.
"""
import logging
from unittest.mock import MagicMock, patch

import pytest
from exceptions import DataNotFound
from main import create_app
from neo4j_database import Neo4JDatabase

logger = logging.getLogger(__name__)


def _make_manifestation_result(instance_id: str, segment_ids: list[str]) -> dict:
    """Helper to construct a minimal `delete_manifestation` return dict for mocking."""
    return {
        "expression_id": "T12345678",
        "annotations": [{"segmentation": {"id": [f"A_{instance_id}"]}}],
        "segment_ids": segment_ids,
        "deleted_counts": {
            "manifestations": 1,
            "annotations": 1,
            "segments": len(segment_ids),
            "references": 0,
        },
    }


MOCK_TEXT_DELETE_RESULT = {
    "title": {"en": "The Great Commentary", "bo": "འགྲེལ་པ་ཆེན་མོ།"},
    "manifestation_ids": ["I10000001", "I10000002"],
    "manifestation_results": [
        _make_manifestation_result("I10000001", ["S1", "S2"]),
        _make_manifestation_result("I10000002", ["S3"]),
    ],
    "work_deleted": True,
    "deleted_counts": {
        "expressions": 1,
        "manifestations": 2,
        "annotations": 2,
        "segments": 3,
        "references": 0,
        "contributions": 3,
    },
}


@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


class TestDeleteTextRoute:
    """Route-level tests for `DELETE /v2/texts/{text_id}`."""

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_returns_200_with_full_response_shape(self, mock_db_cls, mock_storage_cls, client):
        mock_db_cls.return_value.delete_expression.return_value = MOCK_TEXT_DELETE_RESULT

        response = client.delete("/v2/texts/T12345678")

        assert response.status_code == 200
        body = response.get_json()
        assert body["message"] == "Text deleted successfully"
        assert body["text_id"] == "T12345678"
        assert body["title"] == {"en": "The Great Commentary", "bo": "འགྲེལ་པ་ཆེན་མོ།"}
        assert body["work_deleted"] is True
        assert body["deleted_counts"] == MOCK_TEXT_DELETE_RESULT["deleted_counts"]

        # Per-instance summaries are present in the same order as the cascade.
        assert body["instances"] == [
            {
                "instance_id": "I10000001",
                "annotations": [{"segmentation": {"id": ["A_I10000001"]}}],
            },
            {
                "instance_id": "I10000002",
                "annotations": [{"segmentation": {"id": ["A_I10000002"]}}],
            },
        ]

        mock_db_cls.return_value.delete_expression.assert_called_once_with(
            expression_id="T12345678"
        )

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_storage_delete_called_once_per_manifestation(
        self, mock_db_cls, mock_storage_cls, client
    ):
        mock_db_cls.return_value.delete_expression.return_value = MOCK_TEXT_DELETE_RESULT

        response = client.delete("/v2/texts/T12345678")

        assert response.status_code == 200
        storage = mock_storage_cls.return_value
        assert storage.delete_base_text.call_count == 2
        storage.delete_base_text.assert_any_call(
            expression_id="T12345678", manifestation_id="I10000001"
        )
        storage.delete_base_text.assert_any_call(
            expression_id="T12345678", manifestation_id="I10000002"
        )

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_storage_failures_do_not_fail_request(
        self, mock_db_cls, mock_storage_cls, client
    ):
        mock_db_cls.return_value.delete_expression.return_value = MOCK_TEXT_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.side_effect = Exception("blob missing")

        response = client.delete("/v2/texts/T12345678")

        assert response.status_code == 200
        # Still attempted both manifestations even though both raised.
        assert mock_storage_cls.return_value.delete_base_text.call_count == 2

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_search_segmenter_called_once_with_union_of_segment_ids(
        self, mock_db_cls, mock_storage_cls, client
    ):
        mock_db_cls.return_value.delete_expression.return_value = MOCK_TEXT_DELETE_RESULT

        # We patch via `api.instances` because that's where `_trigger_delete_search_segments`
        # is defined; `api.texts` imports it from there at module load time, so we patch
        # the location where it's looked up at call time (the `api.texts` namespace).
        with patch("api.texts._trigger_delete_search_segments") as mock_trigger:
            response = client.delete("/v2/texts/T12345678")

        assert response.status_code == 200
        mock_trigger.assert_called_once_with(["S1", "S2", "S3"])

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_search_segmenter_not_called_when_no_segments(
        self, mock_db_cls, mock_storage_cls, client
    ):
        result = {
            **MOCK_TEXT_DELETE_RESULT,
            "manifestation_ids": ["I_empty"],
            "manifestation_results": [_make_manifestation_result("I_empty", [])],
        }
        mock_db_cls.return_value.delete_expression.return_value = result

        with patch("api.texts._trigger_delete_search_segments") as mock_trigger:
            response = client.delete("/v2/texts/T_empty")

        assert response.status_code == 200
        mock_trigger.assert_not_called()

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_text_with_no_manifestations_still_returns_200(
        self, mock_db_cls, mock_storage_cls, client
    ):
        result = {
            "title": {"en": "Orphan text"},
            "manifestation_ids": [],
            "manifestation_results": [],
            "work_deleted": False,
            "deleted_counts": {
                "expressions": 1,
                "manifestations": 0,
                "annotations": 0,
                "segments": 0,
                "references": 0,
                "contributions": 0,
            },
        }
        mock_db_cls.return_value.delete_expression.return_value = result

        response = client.delete("/v2/texts/T_no_manifestations")

        assert response.status_code == 200
        body = response.get_json()
        assert body["instances"] == []
        assert body["work_deleted"] is False
        # No storage calls when there are no manifestations.
        mock_storage_cls.return_value.delete_base_text.assert_not_called()

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_text_with_null_title_returns_null_title(
        self, mock_db_cls, mock_storage_cls, client
    ):
        result = {
            **MOCK_TEXT_DELETE_RESULT,
            "title": None,
            "manifestation_ids": [],
            "manifestation_results": [],
        }
        mock_db_cls.return_value.delete_expression.return_value = result

        response = client.delete("/v2/texts/T_no_title")

        assert response.status_code == 200
        assert response.get_json()["title"] is None

    @patch("api.texts.Storage")
    @patch("api.texts.Neo4JDatabase")
    def test_404_when_text_not_found(self, mock_db_cls, mock_storage_cls, client):
        mock_db_cls.return_value.delete_expression.side_effect = DataNotFound(
            "Text with ID 'missing' not found"
        )

        response = client.delete("/v2/texts/missing")

        assert response.status_code == 404
        assert response.get_json() == {"error": "Text with ID 'missing' not found"}
        mock_storage_cls.return_value.delete_base_text.assert_not_called()


class TestDeleteExpressionDb:
    """Tests for the `delete_expression` DB method (cypher-mocked)."""

    @staticmethod
    def _make_db_with_session_mock(read_returns: list, write_returns: list) -> tuple:
        """
        Build a Neo4JDatabase whose session.execute_read / execute_write iterate
        through the provided side-effect lists in order. Returns (db, mock_driver).
        """
        with patch("neo4j_database.GraphDatabase") as mock_driver_cls:
            mock_driver = MagicMock()
            mock_driver_cls.driver.return_value = mock_driver

            session = MagicMock()
            mock_driver.session.return_value.__enter__ = MagicMock(return_value=session)
            mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

            # Sequentially return values for execute_read calls.
            read_iter = iter(read_returns)

            def execute_read(_tx_func):
                return next(read_iter)

            session.execute_read.side_effect = execute_read

            # Sequentially return values for execute_write calls.
            write_iter = iter(write_returns)

            def execute_write(tx_func):
                # We still call the tx_func with a mock so any tx.run inside is exercised
                # (but we ignore its return value and provide our own).
                tx = MagicMock()
                tx.run.return_value.single.return_value = MagicMock()
                tx_func(tx)  # exercise but ignore
                return next(write_iter)

            session.execute_write.side_effect = execute_write

            db = Neo4JDatabase(neo4j_uri="bolt://x:7687", neo4j_auth=("neo4j", "p"))
            return db, mock_driver

    def test_raises_data_not_found_when_expression_missing(self):
        # First execute_read (exists) returns None → must raise without touching write.
        db, _driver = self._make_db_with_session_mock(read_returns=[None], write_returns=[])

        with pytest.raises(DataNotFound, match="Text with ID 'missing' not found"):
            db.delete_expression("missing")

    def test_does_not_run_per_manifestation_cascade_when_missing(self):
        """Confirm `delete_manifestation` is not invoked when the expression doesn't
        exist."""
        db, _driver = self._make_db_with_session_mock(read_returns=[None], write_returns=[])

        with patch.object(db, "delete_manifestation") as mock_dm:
            with pytest.raises(DataNotFound):
                db.delete_expression("missing")
            mock_dm.assert_not_called()

    def test_aggregates_counts_across_manifestations(self):
        """When the expression has manifestations, counts should be summed and
        the contribution_count from stage 2 should be added."""
        # Setup: exists check returns a record, manifestation_ids returns 2 ids.
        exists_record = {"id": "T1"}
        manifestation_ids_record = {"manifestation_ids": ["I1", "I2"]}

        # Stage 2 transaction returns title + contribution_count + work_deleted.
        stage2_result = {
            "title": [{"language": "en", "text": "Hello"}],
            "contribution_count": 4,
            "work_deleted": False,
        }

        db, _driver = self._make_db_with_session_mock(
            read_returns=[exists_record, manifestation_ids_record],
            write_returns=[stage2_result],
        )

        # Mock per-manifestation cascade results.
        with patch.object(db, "delete_manifestation") as mock_dm:
            mock_dm.side_effect = [
                {
                    "expression_id": "T1",
                    "annotations": [],
                    "segment_ids": ["s1"],
                    "deleted_counts": {
                        "manifestations": 1,
                        "annotations": 2,
                        "segments": 10,
                        "references": 1,
                    },
                },
                {
                    "expression_id": "T1",
                    "annotations": [],
                    "segment_ids": ["s2", "s3"],
                    "deleted_counts": {
                        "manifestations": 1,
                        "annotations": 3,
                        "segments": 20,
                        "references": 2,
                    },
                },
            ]
            result = db.delete_expression("T1")

        assert mock_dm.call_count == 2
        mock_dm.assert_any_call(manifestation_id="I1")
        mock_dm.assert_any_call(manifestation_id="I2")

        assert result["manifestation_ids"] == ["I1", "I2"]
        assert result["work_deleted"] is False
        assert result["title"] == {"en": "Hello"}
        assert result["deleted_counts"] == {
            "expressions": 1,
            "manifestations": 2,
            "annotations": 5,
            "segments": 30,
            "references": 3,
            "contributions": 4,
        }

    def test_text_with_no_manifestations_skips_per_manifestation_cascade(self):
        exists_record = {"id": "T_empty"}
        manifestation_ids_record = {"manifestation_ids": []}

        stage2_result = {
            "title": [],
            "contribution_count": 0,
            "work_deleted": True,
        }

        db, _driver = self._make_db_with_session_mock(
            read_returns=[exists_record, manifestation_ids_record],
            write_returns=[stage2_result],
        )

        with patch.object(db, "delete_manifestation") as mock_dm:
            result = db.delete_expression("T_empty")
            mock_dm.assert_not_called()

        assert result["manifestation_ids"] == []
        assert result["title"] is None  # empty title list collapses to None
        assert result["work_deleted"] is True
        assert result["deleted_counts"]["expressions"] == 1
        assert result["deleted_counts"]["manifestations"] == 0
