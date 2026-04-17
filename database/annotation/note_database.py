from typing import TYPE_CHECKING

from database.database_validator import DatabaseValidator
from exceptions import DataNotFoundError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from identifier import generate_id
from models.annotation import NoteInput, NoteOutput, Span


class NoteDatabase:
    GET_BY_ID_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(n:Note {id: $note_id})
    WHERE span.start < span.end
    RETURN n.id AS note_id, n.text AS text, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    GET_BY_EDITION_ID_QUERY = """
    MATCH (:Edition {id: $edition_id})<-[:NOTE_OF]-(n:Note)-[:HAS_TYPE]->(:NoteType {name: $note_type})
    MATCH (span:Span)-[:SPAN_OF]->(n)
    WHERE span.start < span.end
    RETURN n.id AS note_id, n.text AS text, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    CREATE_QUERY = """
    MATCH (m:Edition {id: $edition_id}), (nt:NoteType {name: $note_type})
    CREATE (span:Span {start: $span_start, end: $span_end})
        -[:SPAN_OF]->(n:Note {id: $note_id, text: $text})
        -[:NOTE_OF]->(m),
        (n)-[:HAS_TYPE]->(nt)
    RETURN n.id AS note_id
    """

    DELETE_QUERY = """
    MATCH (n:Note {id: $note_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(n)
    DETACH DELETE span, n
    FINISH
    """

    DELETE_ALL_QUERY = """
    MATCH (n:Note)-[:NOTE_OF]->(:Edition {id: $edition_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(n)
    DETACH DELETE span, n
    FINISH
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def _parse_record(record: dict | Record) -> NoteOutput:
        return NoteOutput(
            id=record["note_id"],
            span=Span(start=record["span_start"], end=record["span_end"]),
            text=record["text"],
        )

    async def get(self, note_id: str) -> NoteOutput:
        async def read(tx: AsyncManagedTransaction) -> NoteOutput:
            result = await tx.run(NoteDatabase.GET_BY_ID_QUERY, note_id=note_id)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Note with ID '{note_id}' not found")
            return self._parse_record(record)

        async with self._db.get_session() as session:
            return await session.execute_read(read)

    async def get_all(self, edition_id: str, note_type: str = "durchen") -> list[NoteOutput]:
        async def read(tx: AsyncManagedTransaction) -> list[NoteOutput]:
            result = await tx.run(
                NoteDatabase.GET_BY_EDITION_ID_QUERY,
                edition_id=edition_id,
                note_type=note_type,
            )
            return [self._parse_record(record) for record in await result.data()]

        async with self._db.get_session() as session:
            return await session.execute_read(read)

    async def add_durchen(self, edition_id: str, note: NoteInput) -> str:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: NoteDatabase.add_with_transaction(tx, edition_id, note, "durchen")
            )

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        edition_id: str,
        note: NoteInput,
        note_type: str,
    ) -> str:
        await DatabaseValidator.validate_edition_exists(tx, edition_id)

        generated_id = generate_id()

        await tx.run(
            NoteDatabase.CREATE_QUERY,
            edition_id=edition_id,
            note_id=generated_id,
            text=note.text,
            span_start=note.span.start,
            span_end=note.span.end,
            note_type=note_type,
        )

        return generated_id

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, note_id: str) -> None:
        await tx.run(NoteDatabase.DELETE_QUERY, note_id=note_id)

    async def delete(self, note_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: NoteDatabase.delete_with_transaction(tx, note_id))

    @staticmethod
    async def delete_all_with_transaction(
        tx: AsyncManagedTransaction, edition_id: str, _note_type: str = "durchen"
    ) -> None:
        await tx.run(NoteDatabase.DELETE_ALL_QUERY, edition_id=edition_id)
