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
  - The DB-layer `delete_expression` performs graph deletes in a single write
    transaction and skips `delete_orphan_work` when the Work is still referenced
    by another Expression.
"""
import logging
from unittest.mock import MagicMock, patch

import pytest
from exceptions import DataNotFound
from main import create_app
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

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
    def _make_db_with_write_tx(single_returns: list) -> tuple:
        """
        Build a Neo4JDatabase whose session.execute_write runs the transaction
        function against one mock tx. `tx.run(...).single()` iterates through
        `single_returns` for the read portions inside that single write tx.
        """
        with patch("neo4j_database.GraphDatabase") as mock_driver_cls:
            mock_driver = MagicMock()
            mock_driver_cls.driver.return_value = mock_driver

            session = MagicMock()
            mock_driver.session.return_value.__enter__ = MagicMock(return_value=session)
            mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

            tx = MagicMock()
            tx.run.return_value.single.side_effect = single_returns

            def execute_write(tx_func):
                return tx_func(tx)

            session.execute_write.side_effect = execute_write

            db = Neo4JDatabase(neo4j_uri="bolt://x:7687", neo4j_auth=("neo4j", "p"))
            return db, session, tx

    def test_raises_data_not_found_when_expression_missing(self):
        # The single write tx first runs `get_manifestation_ids_for_delete`; None means
        # the Expression does not exist and the transaction function raises.
        db, session, _tx = self._make_db_with_write_tx(single_returns=[None])

        with pytest.raises(DataNotFound, match="Text with ID 'missing' not found"):
            db.delete_expression("missing")

        session.execute_write.assert_called_once()
        session.execute_read.assert_not_called()

    def test_does_not_run_per_manifestation_cascade_when_missing(self):
        """Confirm no manifestation cascade starts when the expression doesn't exist."""
        db, _session, _tx = self._make_db_with_write_tx(single_returns=[None])

        with patch.object(db, "_delete_manifestation_in_tx") as mock_delete_in_tx:
            with pytest.raises(DataNotFound):
                db.delete_expression("missing")
            mock_delete_in_tx.assert_not_called()

    def test_aggregates_counts_across_manifestations(self):
        """When the expression has manifestations, counts should be summed and
        the contribution_count from the expression info should be added."""
        manifestation_ids_record = {"manifestation_ids": ["I1", "I2"]}
        expression_info_record = {
            "title": [{"language": "en", "text": "Hello"}],
            "contribution_count": 4,
            "work_id": "W1",
            "work_is_orphan": False,
        }

        db, session, tx = self._make_db_with_write_tx(
            single_returns=[manifestation_ids_record, expression_info_record]
        )

        raw_manifestation_results = {
            "I1": {
                "expression_id": "T1",
                "annotation_info": [],
                "segment_ids": ["s1"],
                "deleted_counts": {
                    "manifestations": 1,
                    "annotations": 2,
                    "segments": 10,
                    "references": 1,
                },
            },
            "I2": {
                "expression_id": "T1",
                "annotation_info": [],
                "segment_ids": ["s2", "s3"],
                "deleted_counts": {
                    "manifestations": 1,
                    "annotations": 3,
                    "segments": 20,
                    "references": 2,
                },
            },
        }

        def delete_manifestation_in_tx(tx_arg, manifestation_id):
            assert tx_arg is tx
            return raw_manifestation_results[manifestation_id]

        with patch.object(db, "delete_manifestation") as mock_public_dm:
            with patch.object(
                db, "_delete_manifestation_in_tx", side_effect=delete_manifestation_in_tx
            ) as mock_delete_in_tx:
                result = db.delete_expression("T1")

        session.execute_write.assert_called_once()
        session.execute_read.assert_not_called()
        mock_public_dm.assert_not_called()
        assert mock_delete_in_tx.call_count == 2
        assert mock_delete_in_tx.call_args_list[0].kwargs["manifestation_id"] == "I1"
        assert mock_delete_in_tx.call_args_list[1].kwargs["manifestation_id"] == "I2"

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
        # Expression exists but has zero manifestations → list is empty (not None).
        manifestation_ids_record = {"manifestation_ids": []}
        expression_info_record = {
            "title": [],
            "contribution_count": 0,
            "work_id": "W_empty",
            "work_is_orphan": True,
        }

        db, session, tx = self._make_db_with_write_tx(
            single_returns=[manifestation_ids_record, expression_info_record]
        )

        with patch.object(db, "_delete_manifestation_in_tx") as mock_delete_in_tx:
            result = db.delete_expression("T_empty")
            mock_delete_in_tx.assert_not_called()

        session.execute_write.assert_called_once()
        session.execute_read.assert_not_called()
        assert result["manifestation_ids"] == []
        assert result["title"] is None  # empty title list collapses to None
        assert result["work_deleted"] is True
        assert result["deleted_counts"]["expressions"] == 1
        assert result["deleted_counts"]["manifestations"] == 0

        run_queries = [args[0] for args, _kwargs in tx.run.call_args_list]
        assert run_queries.index(Queries.expressions["delete_node"]) < run_queries.index(
            Queries.expressions["delete_orphan_work"]
        )
