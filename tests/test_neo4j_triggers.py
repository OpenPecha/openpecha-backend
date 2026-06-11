# ruff: noqa: ANN001, S101, SLF001
import pytest

from database.neo4j_triggers import audit_triggers


@pytest.mark.asyncio(loop_scope="session")
async def test_audit_detects_self_aligned_segment(test_database) -> None:
    async with test_database.get_session() as session:
        await session.run("""
            CREATE (segment:Segment {id: 'self_aligned_segment'})
            CREATE (segment)-[:ALIGNED_TO]->(segment)
        """)

    violations = await audit_triggers(test_database._driver)

    assert "enforce_no_self_aligned_to" in violations
    assert any(
        "self_aligned_segment" in item
        for item in violations["enforce_no_self_aligned_to"]
    )
