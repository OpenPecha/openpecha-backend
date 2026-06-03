from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, LiteralString

from database.database_validator import DatabaseValidator
from database.nomen_database import NomenDatabase
from exceptions import DataNotFoundError
from identifier import generate_id
from models.annotation import (
    AnnotationMetadata,
    Span,
    TableOfContentsInput,
    TableOfContentsOutput,
    TableOfContentsSectionInput,
    TableOfContentsSectionOutput,
)
from models.base import LocalizedString

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database


class TableOfContentsDatabase:
    CREATE_TOC_QUERY: LiteralString = """
    MATCH (edition:Edition {id: $edition_id})
    CREATE (toc:TableOfContents {id: $toc_id})-[:TOC_OF]->(edition)
    WITH toc
    CALL (*) {
        WHEN $metadata_id IS NOT NULL THEN {
            CREATE (metadata:AnnotationMetadata {id: $metadata_id, name: $metadata_name})
            CREATE (toc)-[:HAS_METADATA]->(metadata)
        }
    }
    RETURN toc.id AS id
    """

    CREATE_SECTIONS_QUERY: LiteralString = """
    MATCH (toc:TableOfContents {id: $toc_id})
    UNWIND $sections AS section_data
    MATCH (title:Nomen {id: section_data.title_nomen_id})
    OPTIONAL MATCH (summary:Nomen {id: section_data.summary_nomen_id})
    CREATE (section:TableOfContentsSection {id: section_data.id})-[:SECTION_OF]->(toc)
    CREATE (section)-[:HAS_TITLE]->(title)
    CREATE (:Span {start: section_data.span_start, end: section_data.span_end})-[:SPAN_OF]->(section)
    WITH section, summary
    CALL (*) {
        WHEN summary IS NOT NULL THEN {
            CREATE (section)-[:HAS_SUMMARY]->(summary)
        }
    }
    RETURN count(section) AS count
    """

    CREATE_HIERARCHY_QUERY: LiteralString = """
    UNWIND $sections AS section_data
    WITH section_data
    WHERE section_data.parent_id IS NOT NULL
    MATCH (section:TableOfContentsSection {id: section_data.id})
    MATCH (parent:TableOfContentsSection {id: section_data.parent_id})
    CREATE (section)-[:SUBSECTION_OF]->(parent)
    RETURN count(section) AS count
    """

    GET_BY_ID_QUERY: LiteralString = """
    MATCH (toc:TableOfContents {id: $toc_id})-[:TOC_OF]->(edition:Edition)-[:EDITION_OF]->(text:Text)
    OPTIONAL MATCH (toc)-[:HAS_METADATA]->(metadata:AnnotationMetadata)
    RETURN toc.id AS id,
           edition.id AS edition_id,
           text.id AS text_id,
           metadata.name AS metadata_name
    """

    GET_BY_EDITION_ID_QUERY: LiteralString = """
    MATCH (edition:Edition {id: $edition_id})-[:EDITION_OF]->(text:Text)
    MATCH (toc:TableOfContents)-[:TOC_OF]->(edition)
    OPTIONAL MATCH (toc)-[:HAS_METADATA]->(metadata:AnnotationMetadata)
    RETURN toc.id AS id,
           edition.id AS edition_id,
           text.id AS text_id,
           metadata.name AS metadata_name
    ORDER BY toc.id
    """

    GET_SECTIONS_QUERY: LiteralString = """
    MATCH (section:TableOfContentsSection)-[:SECTION_OF]->(:TableOfContents {id: $toc_id})
    MATCH (span:Span)-[:SPAN_OF]->(section)
    OPTIONAL MATCH (section)-[:SUBSECTION_OF]->(parent:TableOfContentsSection)
    RETURN section.id AS id,
           parent.id AS parent_id,
           span.start AS span_start,
           span.end AS span_end,
           apoc.map.fromPairs([
               (section)-[:HAS_TITLE]->(:Nomen)-[:HAS_LOCALIZATION]->(title:LocalizedText)
                   -[title_rel:HAS_LANGUAGE]->(title_lang:Language) |
               [coalesce(title_rel.bcp47, title_lang.code), title.text]
           ]) AS title,
           apoc.map.fromPairs([
               (section)-[:HAS_SUMMARY]->(:Nomen)-[:HAS_LOCALIZATION]->(summary:LocalizedText)
                   -[summary_rel:HAS_LANGUAGE]->(summary_lang:Language) |
               [coalesce(summary_rel.bcp47, summary_lang.code), summary.text]
           ]) AS summary
    ORDER BY parent_id, span.start, span.end, section.id
    """

    DELETE_QUERY: LiteralString = """
    MATCH (toc:TableOfContents {id: $toc_id})
    OPTIONAL MATCH (toc)-[:HAS_METADATA]->(metadata:AnnotationMetadata)
    OPTIONAL MATCH (section:TableOfContentsSection)-[:SECTION_OF]->(toc)
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(section)
    OPTIONAL MATCH (section)-[:HAS_TITLE|HAS_SUMMARY]->(nomen:Nomen)
    OPTIONAL MATCH (nomen)-[:HAS_LOCALIZATION]->(localized:LocalizedText)
    DETACH DELETE span, localized, nomen, section, metadata, toc
    FINISH
    """

    DELETE_ALL_QUERY: LiteralString = """
    MATCH (toc:TableOfContents)-[:TOC_OF]->(:Edition {id: $edition_id})
    OPTIONAL MATCH (toc)-[:HAS_METADATA]->(metadata:AnnotationMetadata)
    OPTIONAL MATCH (section:TableOfContentsSection)-[:SECTION_OF]->(toc)
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(section)
    OPTIONAL MATCH (section)-[:HAS_TITLE|HAS_SUMMARY]->(nomen:Nomen)
    OPTIONAL MATCH (nomen)-[:HAS_LOCALIZATION]->(localized:LocalizedText)
    DETACH DELETE span, localized, nomen, section, metadata, toc
    FINISH
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    async def _flatten_sections(
        tx: AsyncManagedTransaction,
        sections: list[TableOfContentsSectionInput],
        *,
        parent_id: str | None = None,
    ) -> list[dict[str, str | int | None]]:
        flattened: list[dict[str, str | int | None]] = []
        for section in sections:
            section_id = generate_id()
            title_nomen_id = await NomenDatabase.create_with_transaction(tx, section.title.root, None)
            summary_nomen_id = (
                await NomenDatabase.create_with_transaction(tx, section.summary.root, None)
                if section.summary is not None
                else None
            )
            flattened.append(
                {
                    "id": section_id,
                    "parent_id": parent_id,
                    "title_nomen_id": title_nomen_id,
                    "summary_nomen_id": summary_nomen_id,
                    "span_start": section.span.start,
                    "span_end": section.span.end,
                }
            )
            flattened.extend(
                await TableOfContentsDatabase._flatten_sections(tx, section.subsections, parent_id=section_id)
            )
        return flattened

    @staticmethod
    def _parse_metadata(record: dict[str, Any] | Record) -> AnnotationMetadata | None:
        if record["metadata_name"] is None:
            return None
        return AnnotationMetadata(name=record["metadata_name"])

    @staticmethod
    def _parse_section_record(record: dict[str, Any] | Record) -> TableOfContentsSectionOutput:
        summary = dict(record["summary"] or {})
        return TableOfContentsSectionOutput(
            id=record["id"],
            title=LocalizedString(dict(record["title"] or {})),
            summary=LocalizedString(summary) if summary else None,
            span=Span(start=record["span_start"], end=record["span_end"]),
            subsections=[],
        )

    @staticmethod
    def _build_sections(records: Sequence[dict[str, Any] | Record]) -> list[TableOfContentsSectionOutput]:
        nodes: dict[str, TableOfContentsSectionOutput] = {}
        parent_ids: dict[str, str | None] = {}
        spans: dict[str, tuple[int, int]] = {}

        for record in records:
            node = TableOfContentsDatabase._parse_section_record(record)
            nodes[node.id] = node
            parent_ids[node.id] = record["parent_id"]
            spans[node.id] = (record["span_start"], record["span_end"])

        roots: list[TableOfContentsSectionOutput] = []
        for node_id, node in nodes.items():
            parent_id = parent_ids[node_id]
            if parent_id and parent_id in nodes:
                nodes[parent_id].subsections.append(node)
            else:
                roots.append(node)

        for node in nodes.values():
            node.subsections.sort(key=lambda item: (*spans[item.id], item.id))
        roots.sort(key=lambda item: (*spans[item.id], item.id))
        return roots

    @staticmethod
    async def _parse_toc(tx: AsyncManagedTransaction, record: dict[str, Any] | Record) -> TableOfContentsOutput:
        result = await tx.run(TableOfContentsDatabase.GET_SECTIONS_QUERY, toc_id=record["id"])
        sections = TableOfContentsDatabase._build_sections(await result.data())
        return TableOfContentsOutput(
            id=record["id"],
            edition_id=record["edition_id"],
            text_id=record["text_id"],
            metadata=TableOfContentsDatabase._parse_metadata(record),
            sections=sections,
        )

    async def get(self, toc_id: str) -> TableOfContentsOutput:
        async with self._db.get_session() as session:
            return await session.execute_read(lambda tx: TableOfContentsDatabase.get_with_transaction(tx, toc_id))

    async def get_all(self, edition_id: str) -> list[TableOfContentsOutput]:
        async with self._db.get_session() as session:
            return await session.execute_read(
                lambda tx: TableOfContentsDatabase.get_all_with_transaction(tx, edition_id)
            )

    async def add(self, edition_id: str, toc: TableOfContentsInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: TableOfContentsDatabase.add_with_transaction(tx, edition_id, toc)
            )

    async def delete(self, toc_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: TableOfContentsDatabase.delete_with_transaction(tx, toc_id))

    @staticmethod
    async def get_with_transaction(tx: AsyncManagedTransaction, toc_id: str) -> TableOfContentsOutput:
        result = await tx.run(TableOfContentsDatabase.GET_BY_ID_QUERY, toc_id=toc_id)
        record = await result.single()
        if record is None:
            raise DataNotFoundError(f"Table of contents with ID '{toc_id}' not found")
        return await TableOfContentsDatabase._parse_toc(tx, record)

    @staticmethod
    async def get_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> list[TableOfContentsOutput]:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)
        result = await tx.run(TableOfContentsDatabase.GET_BY_EDITION_ID_QUERY, edition_id=edition_id)
        return [await TableOfContentsDatabase._parse_toc(tx, record) for record in await result.data()]

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        edition_id: str,
        toc: TableOfContentsInput,
    ) -> str:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)

        toc_id = generate_id()
        metadata_id = generate_id() if toc.metadata is not None else None
        metadata_name = toc.metadata.name if toc.metadata is not None else None
        sections = await TableOfContentsDatabase._flatten_sections(tx, toc.sections)

        result = await tx.run(
            TableOfContentsDatabase.CREATE_TOC_QUERY,
            edition_id=edition_id,
            toc_id=toc_id,
            metadata_id=metadata_id,
            metadata_name=metadata_name,
        )
        record = await result.single()
        if record is None:
            raise DataNotFoundError(f"Edition with ID '{edition_id}' not found")

        sections_result = await tx.run(TableOfContentsDatabase.CREATE_SECTIONS_QUERY, toc_id=toc_id, sections=sections)
        sections_record = await sections_result.single()
        if not sections_record or sections_record["count"] != len(sections):
            raise DataNotFoundError(f"Failed to create table of contents sections for table of contents '{toc_id}'")

        await tx.run(TableOfContentsDatabase.CREATE_HIERARCHY_QUERY, sections=sections)
        return toc_id

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, toc_id: str) -> None:
        await tx.run(TableOfContentsDatabase.DELETE_QUERY, toc_id=toc_id)

    @staticmethod
    async def delete_all_with_transaction(tx: AsyncManagedTransaction, edition_id: str) -> None:
        await tx.run(TableOfContentsDatabase.DELETE_ALL_QUERY, edition_id=edition_id)
