# ruff: noqa: ANN001, S101, SLF001
import pytest

from database.neo4j_triggers import TRIGGERS, audit_triggers


def _trigger_query(name: str) -> str:
    return next(trigger["query"] for trigger in TRIGGERS if trigger["name"] == name)


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


@pytest.mark.asyncio(loop_scope="session")
async def test_text_title_unique_trigger_uses_title_language_not_text_language(test_database) -> None:
    async with test_database.get_session() as session:
        await session.run("""
            MATCH (title_lang:Language {code: 'en'})
            MATCH (bo:Language {code: 'bo'})
            MATCH (sa:Language {code: 'sa'})
            CREATE (text_a:Text {id: 'title_unique_text_a'})-[:HAS_LANGUAGE {bcp47: 'bo'}]->(bo)
            CREATE (title_a:Nomen {id: 'title_unique_title_a'})
            CREATE (text_a)-[:HAS_TITLE]->(title_a)
            CREATE (lt_a:LocalizedText {text: 'Shared Trigger Title'})
            CREATE (title_a)-[:HAS_LOCALIZATION]->(lt_a)-[:HAS_LANGUAGE {bcp47: 'en'}]->(title_lang)
            CREATE (text_b:Text {id: 'title_unique_text_b'})-[:HAS_LANGUAGE {bcp47: 'sa'}]->(sa)
            CREATE (title_b:Nomen {id: 'title_unique_title_b'})
            CREATE (text_b)-[:HAS_TITLE]->(title_b)
            CREATE (lt_b:LocalizedText {text: 'Shared Trigger Title'})
            CREATE (title_b)-[:HAS_LOCALIZATION]->(lt_b)-[:HAS_LANGUAGE {bcp47: 'en'}]->(title_lang)
        """)

        with pytest.raises(Exception, match="enforce_text_title_unique"):
            result = await session.run(
                """
                MATCH (created:LocalizedText {text: 'Shared Trigger Title'})
                WITH collect(created) AS createdNodes
                CALL apoc.cypher.run(
                    $statement,
                    {
                        createdNodes: createdNodes,
                        createdRelationships: [],
                        deletedRelationships: []
                    }
                ) YIELD value
                RETURN value
                """,
                statement=_trigger_query("enforce_text_title_unique"),
            )
            await result.consume()


@pytest.mark.asyncio(loop_scope="session")
async def test_text_title_unique_trigger_allows_distinct_bcp47_title_languages(test_database) -> None:
    async with test_database.get_session() as session:
        await session.run("""
            MATCH (bo:Language {code: 'bo'})
            CREATE (text_a:Text {id: 'title_bcp47_text_a'})-[:HAS_LANGUAGE {bcp47: 'bo'}]->(bo)
            CREATE (title_a:Nomen {id: 'title_bcp47_title_a'})
            CREATE (text_a)-[:HAS_TITLE]->(title_a)
            CREATE (lt_a:LocalizedText {text: 'Shared BCP47 Title'})
            CREATE (title_a)-[:HAS_LOCALIZATION]->(lt_a)-[:HAS_LANGUAGE {bcp47: 'bo'}]->(bo)
            CREATE (text_b:Text {id: 'title_bcp47_text_b'})-[:HAS_LANGUAGE {bcp47: 'bo'}]->(bo)
            CREATE (title_b:Nomen {id: 'title_bcp47_title_b'})
            CREATE (text_b)-[:HAS_TITLE]->(title_b)
            CREATE (lt_b:LocalizedText {text: 'Shared BCP47 Title'})
            CREATE (title_b)-[:HAS_LOCALIZATION]->(lt_b)-[:HAS_LANGUAGE {bcp47: 'bo-x-ewts'}]->(bo)
        """)

        result = await session.run(
            """
            MATCH (created:LocalizedText {text: 'Shared BCP47 Title'})
            WITH collect(created) AS createdNodes
            CALL apoc.cypher.run(
                $statement,
                {
                    createdNodes: createdNodes,
                    createdRelationships: [],
                    deletedRelationships: []
                }
            ) YIELD value
            RETURN count(value) AS rows
            """,
            statement=_trigger_query("enforce_text_title_unique"),
        )
        await result.consume()

    violations = await audit_triggers(test_database._driver)

    assert "enforce_text_title_unique" not in violations
