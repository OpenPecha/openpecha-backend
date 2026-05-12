# pylint: disable=redefined-outer-name
"""
Unit tests for `DELETE /v2/instances/{instance_id}` and the helpers it relies on.

These tests are mock-based — they do NOT hit a real Neo4j instance. They cover:
  - Happy path through the Flask route (response shape, status code).
  - 404 propagation when the underlying DB raises `DataNotFound`.
  - Search-segmenter cleanup is triggered with the segment ids returned by the DB.
  - Storage cleanup failures are swallowed and do not fail the request.
  - The DB-layer `delete_manifestation` raises `DataNotFound` when the
    `get_delete_info` cypher returns no record.
  - `_build_annotation_summary` correctly groups annotations by type and handles
    both source-side and target-side alignment annotations.
"""
import logging
from unittest.mock import MagicMock, patch

import pytest
from exceptions import DataNotFound
from main import create_app
from neo4j_database import Neo4JDatabase, _build_annotation_summary

logger = logging.getLogger(__name__)


MOCK_DELETE_RESULT = {
    "expression_id": "test-expression-id",
    "annotations": [
        {"segmentation": {"id": ["A1001"]}},
        {
            "alignment": {
                "aligned_from_id": ["A1002", "A1003"],
                "aligned_to_id": ["A2001", "A2002"],
            }
        },
        {"bibliography": {"id": ["A1004"]}},
    ],
    "segment_ids": ["S001", "S002", "S003"],
    "deleted_counts": {
        "manifestations": 1,
        "annotations": 4,
        "segments": 152,
        "references": 0,
    },
}


@pytest.fixture
def client():
    app = create_app()
    app.config["TESTING"] = True
    return app.test_client()


class TestDeleteInstanceRoute:
    """Route-level tests for `DELETE /v2/instances/{instance_id}`."""

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_returns_200_with_full_response_shape(self, mock_db_cls, mock_storage_cls, client):
        mock_db_cls.return_value.delete_manifestation.return_value = MOCK_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.return_value = None

        response = client.delete("/v2/instances/I12345678")

        assert response.status_code == 200
        body = response.get_json()
        assert body["message"] == "Instance deleted successfully"
        assert body["instance_id"] == "I12345678"
        assert body["annotations"] == MOCK_DELETE_RESULT["annotations"]
        assert body["deleted_counts"] == MOCK_DELETE_RESULT["deleted_counts"]
        mock_db_cls.return_value.delete_manifestation.assert_called_once_with(
            manifestation_id="I12345678"
        )

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_storage_delete_called_with_expression_and_manifestation_id(
        self, mock_db_cls, mock_storage_cls, client
    ):
        mock_db_cls.return_value.delete_manifestation.return_value = MOCK_DELETE_RESULT

        response = client.delete("/v2/instances/I12345678")

        assert response.status_code == 200
        mock_storage_cls.return_value.delete_base_text.assert_called_once_with(
            expression_id="test-expression-id", manifestation_id="I12345678"
        )

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_storage_skipped_when_expression_id_is_none(
        self, mock_db_cls, mock_storage_cls, client
    ):
        # An orphan manifestation (no Expression) should not call storage at all.
        result = {**MOCK_DELETE_RESULT, "expression_id": None}
        mock_db_cls.return_value.delete_manifestation.return_value = result

        response = client.delete("/v2/instances/I12345678")

        assert response.status_code == 200
        mock_storage_cls.return_value.delete_base_text.assert_not_called()

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_404_when_manifestation_not_found(self, mock_db_cls, mock_storage_cls, client):
        mock_db_cls.return_value.delete_manifestation.side_effect = DataNotFound(
            "Manifestation with ID 'missing-id' not found"
        )

        response = client.delete("/v2/instances/missing-id")

        assert response.status_code == 404
        assert response.get_json() == {"error": "Manifestation with ID 'missing-id' not found"}
        # Storage cleanup must not happen on 404.
        mock_storage_cls.return_value.delete_base_text.assert_not_called()

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_search_segmenter_called_with_returned_segment_ids(
        self, mock_db_cls, mock_storage_cls, client
    ):
        mock_db_cls.return_value.delete_manifestation.return_value = MOCK_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.return_value = None

        with patch("api.instances._trigger_delete_search_segments") as mock_trigger:
            response = client.delete("/v2/instances/I12345678")

        assert response.status_code == 200
        mock_trigger.assert_called_once_with(MOCK_DELETE_RESULT["segment_ids"])

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_search_segmenter_not_called_when_no_segments(
        self, mock_db_cls, mock_storage_cls, client
    ):
        result = {**MOCK_DELETE_RESULT, "segment_ids": []}
        mock_db_cls.return_value.delete_manifestation.return_value = result
        mock_storage_cls.return_value.delete_base_text.return_value = None

        with patch("api.instances._trigger_delete_search_segments") as mock_trigger:
            response = client.delete("/v2/instances/I12345678")

        assert response.status_code == 200
        mock_trigger.assert_not_called()

    @patch("api.instances.Storage")
    @patch("api.instances.Neo4JDatabase")
    def test_storage_failure_does_not_fail_request(
        self, mock_db_cls, mock_storage_cls, client
    ):
        mock_db_cls.return_value.delete_manifestation.return_value = MOCK_DELETE_RESULT
        mock_storage_cls.return_value.delete_base_text.side_effect = Exception("blob not found")

        response = client.delete("/v2/instances/I12345678")

        assert response.status_code == 200
        assert response.get_json()["message"] == "Instance deleted successfully"


class TestDeleteManifestationDb:
    """Tests for the `delete_manifestation` DB method (cypher-mocked)."""

    def test_raises_data_not_found_when_get_delete_info_returns_none(self):
        """When the manifestation does not exist, `get_delete_info` returns no record
        and `delete_manifestation` must raise DataNotFound."""
        with patch("neo4j_database.GraphDatabase") as mock_driver_cls:
            mock_driver = MagicMock()
            mock_driver_cls.driver.return_value = mock_driver
            mock_session = MagicMock()
            mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
            mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)

            # The transaction function calls `tx.run(...).single()` which we mock to
            # return None (no record), simulating a missing manifestation.
            def execute_write(tx_func):
                tx = MagicMock()
                tx.run.return_value.single.return_value = None
                return tx_func(tx)

            mock_session.execute_write.side_effect = execute_write

            db = Neo4JDatabase(neo4j_uri="bolt://x:7687", neo4j_auth=("neo4j", "p"))
            with pytest.raises(DataNotFound, match="not found"):
                db.delete_manifestation("missing-id")


class TestBuildAnnotationSummary:
    """
    Unit tests for the `_build_annotation_summary` helper that converts raw
    `get_delete_info` rows into the API `annotations` response shape.
    """

    def test_empty_input_returns_empty_list(self):
        assert _build_annotation_summary([]) == []

    def test_skips_rows_with_missing_required_fields(self):
        rows = [
            {"annotation_id": None, "annotation_type": "segmentation", "partner_id": None},
            {"annotation_id": "A1", "annotation_type": None, "partner_id": None},
        ]
        assert _build_annotation_summary(rows) == []

    def test_groups_non_alignment_annotations_by_type(self):
        rows = [
            {"annotation_id": "A1", "annotation_type": "segmentation", "partner_id": None},
            {"annotation_id": "A2", "annotation_type": "bibliography", "partner_id": None},
            {"annotation_id": "A3", "annotation_type": "segmentation", "partner_id": None},
        ]
        result = _build_annotation_summary(rows)
        assert {"segmentation": {"id": ["A1", "A3"]}} in result
        assert {"bibliography": {"id": ["A2"]}} in result
        assert len(result) == 2

    def test_alignment_source_side_groups_from_and_to(self):
        """An alignment annotation on m with outgoing ALIGNED_TO is the source side."""
        rows = [
            {"annotation_id": "A_src", "annotation_type": "alignment", "partner_id": "A_tgt"},
        ]
        result = _build_annotation_summary(rows)
        assert result == [
            {"alignment": {"aligned_from_id": ["A_src"], "aligned_to_id": ["A_tgt"]}}
        ]

    def test_alignment_target_side_still_appears_in_aligned_from(self):
        """An alignment annotation on m WITHOUT a partner (target side via incoming
        ALIGNED_TO that the cypher does NOT capture) must still appear in
        `aligned_from_id` so it isn't silently dropped from the response.

        This is the bug fix vs the original implementation, which dropped these
        annotations entirely.
        """
        rows = [
            {"annotation_id": "A_src", "annotation_type": "alignment", "partner_id": "A_tgt"},
            {"annotation_id": "A_target_side", "annotation_type": "alignment", "partner_id": None},
        ]
        result = _build_annotation_summary(rows)
        assert result == [
            {
                "alignment": {
                    "aligned_from_id": ["A_src", "A_target_side"],
                    "aligned_to_id": ["A_tgt"],
                }
            }
        ]

    def test_alignment_with_undirected_partner_match_dedupes(self):
        """When the cypher matches ALIGNED_TO undirected, the same annotation row
        can appear with both directions populated. Dedup should keep the response
        clean."""
        rows = [
            {"annotation_id": "A1", "annotation_type": "alignment", "partner_id": "B1"},
            {"annotation_id": "A1", "annotation_type": "alignment", "partner_id": "B1"},
            {"annotation_id": "A1", "annotation_type": "alignment", "partner_id": "B2"},
        ]
        result = _build_annotation_summary(rows)
        assert result == [
            {
                "alignment": {
                    "aligned_from_id": ["A1"],
                    "aligned_to_id": ["B1", "B2"],
                }
            }
        ]

    def test_mixed_annotation_types_all_present(self):
        rows = [
            {"annotation_id": "A1", "annotation_type": "segmentation", "partner_id": None},
            {"annotation_id": "A2", "annotation_type": "alignment", "partner_id": "B2"},
            {"annotation_id": "A3", "annotation_type": "bibliography", "partner_id": None},
            {"annotation_id": "A4", "annotation_type": "search_segmentation", "partner_id": None},
        ]
        result = _build_annotation_summary(rows)
        assert {"segmentation": {"id": ["A1"]}} in result
        assert {"bibliography": {"id": ["A3"]}} in result
        assert {"search_segmentation": {"id": ["A4"]}} in result
        assert {
            "alignment": {"aligned_from_id": ["A2"], "aligned_to_id": ["B2"]}
        } in result
        assert len(result) == 4
