from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, Record

    from database.database import Database
from identifier import generate_id
from models.annotation import NoteInput, NoteOutput, Span


class NoteDatabase:
    GET_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(n:Note)
    WHERE ($note_id IS NOT NULL AND n.id = $note_id)
       OR ($edition_id IS NOT NULL
           AND EXISTS { (n)-[:NOTE_OF]->(:Edition {id: $edition_id}) }
           AND EXISTS { (n)-[:HAS_TYPE]->(:NoteType {name: $note_type}) })
    RETURN n.id AS note_id, n.text AS text, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    CREATE_QUERY = """
    MATCH (m:Edition {id: $edition_id}), (nt:NoteType {name: $note_type})
    UNWIND $notes AS note
    CREATE (span:Span {start: note.span_start, end: note.span_end})
        -[:SPAN_OF]->(n:Note {id: note.note_id, text: note.text})
        -[:NOTE_OF]->(m),
        (n)-[:HAS_TYPE]->(nt)
    RETURN collect(n.id) AS note_ids
    """

    DELETE_QUERY = """
    MATCH (n:Note {id: $note_id})
    OPTIONAL MATCH (span:Span)-[:SPAN_OF]->(n)
    DETACH DELETE span, n
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
        async with self._db.get_session() as session:
            result = await session.run(NoteDatabase.GET_QUERY, note_id=note_id, edition_id=None, note_type=None)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Note with ID '{note_id}' not found")
            return self._parse_record(record)

    async def get_all(self, edition_id: str, note_type: str = "durchen") -> list[NoteOutput]:
        async with self._db.get_session() as session:
            result = await session.run(NoteDatabase.GET_QUERY, note_id=None, edition_id=edition_id, note_type=note_type)
            records = await result.data()
            return [self._parse_record(record) for record in records]

    async def add_durchen(self, edition_id: str, durchen_notes: list[NoteInput]) -> list[str]:
        async with self._db.get_session() as session:
            return await session.execute_write(
                lambda tx: NoteDatabase.add_with_transaction(tx, edition_id, durchen_notes, "durchen")
            )

    @staticmethod
    async def add_with_transaction(
        tx: AsyncManagedTransaction,
        edition_id: str,
        notes: list[NoteInput],
        note_type: str,
    ) -> list[str]:
        notes_data = [
            {
                "note_id": generate_id(),
                "text": note.text,
                "span_start": note.span.start,
                "span_end": note.span.end,
            }
            for note in notes
        ]

        result = await tx.run(
            NoteDatabase.CREATE_QUERY,
            edition_id=edition_id,
            notes=notes_data,
            note_type=note_type,
        )
        record = await result.single()
        if not record or not record["note_ids"]:
            raise DataNotFoundError(f"Edition with ID '{edition_id}' not found")
        return record["note_ids"]

    @staticmethod
    async def delete_with_transaction(tx: AsyncManagedTransaction, note_id: str) -> None:
        await tx.run(NoteDatabase.DELETE_QUERY, note_id=note_id)

    async def delete(self, note_id: str) -> None:
        async with self._db.get_session() as session:
            await session.execute_write(lambda tx: NoteDatabase.delete_with_transaction(tx, note_id))

    @staticmethod
    async def delete_all_with_transaction(
        tx: AsyncManagedTransaction, edition_id: str, note_type: str = "durchen"
    ) -> None:
        result = await tx.run(NoteDatabase.GET_QUERY, note_id=None, edition_id=edition_id, note_type=note_type)
        records = await result.data()

        for record in records:
            await NoteDatabase.delete_with_transaction(tx, record["note_id"])
