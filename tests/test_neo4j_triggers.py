# ruff: noqa: ANN001, S101, SLF001
import pytest

from database.neo4j_triggers import audit_triggers


@pytest.mark.asyncio(loop_scope="session")
async def test_audit_detects_multiple_display_segmentations(test_database) -> None:
    async with test_database.get_session() as session:
        await session.run("""
            CREATE (edition:Edition {id: 'edition_with_duplicate_display'})
            CREATE (:Segmentation:Display {id: 'display_1'})-[:SEGMENTATION_OF]->(edition)
            CREATE (:Segmentation:Display {id: 'display_2'})-[:SEGMENTATION_OF]->(edition)
        """)

    violations = await audit_triggers(test_database._driver)

    assert "enforce_edition_single_display_segmentation" in violations
    assert any(
        "edition_with_duplicate_display" in item
        for item in violations["enforce_edition_single_display_segmentation"]
    )
