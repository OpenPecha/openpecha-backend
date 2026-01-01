from database.database import Database
from exceptions import DataNotFoundError
from identifier import generate_id
from models import (
    NoteInput,
    NoteOutput,
    SpanModel,
)
from neo4j import ManagedTransaction, Record


class NoteDatabase:
    GET_QUERY = """
    MATCH (span:Span)-[:SPAN_OF]->(n:Note)
    WHERE ($note_id IS NOT NULL AND n.id = $note_id)
       OR ($manifestation_id IS NOT NULL
           AND (n)-[:NOTE_OF]->(:Manifestation {id: $manifestation_id})
           AND (n)-[:HAS_TYPE]->(:NoteType {name: $note_type}))
    RETURN n.id AS note_id, n.text AS text, span.start AS span_start, span.end AS span_end
    ORDER BY span.start
    """

    CREATE_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id}), (nt:NoteType {name: $note_type})
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
            span=SpanModel(start=record["span_start"], end=record["span_end"]),
            text=record["text"],
        )

    def get(self, note_id: str) -> NoteOutput:
        with self._db.get_session() as session:
            result = session.run(
                NoteDatabase.GET_QUERY, note_id=note_id, manifestation_id=None, note_type=None
            ).single()
            if result is None:
                raise DataNotFoundError(f"Note with ID '{note_id}' not found")
            return self._parse_record(result)

    def get_all(self, manifestation_id: str, note_type: str = "durchen") -> list[NoteOutput]:
        with self._db.get_session() as session:
            result = session.run(
                NoteDatabase.GET_QUERY, note_id=None, manifestation_id=manifestation_id, note_type=note_type
            ).data()
            return [self._parse_record(record) for record in result]

    @staticmethod
    def add_with_transaction(
        tx: ManagedTransaction,
        manifestation_id: str,
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

        result = tx.run(
            NoteDatabase.CREATE_QUERY,
            manifestation_id=manifestation_id,
            notes=notes_data,
            note_type=note_type,
        )
        record = result.single()
        if not record:
            raise DataNotFoundError(f"Manifestation with ID '{manifestation_id}' not found")
        return record["note_ids"]

    def add_durchen(self, manifestation_id: str, durchen_notes: list[NoteInput]) -> list[str]:
        with self._db.get_session() as session:
            return session.execute_write(
                lambda tx: NoteDatabase.add_with_transaction(tx, manifestation_id, durchen_notes, "durchen")
            )

    @staticmethod
    def delete_with_transaction(tx: ManagedTransaction, note_id: str) -> None:
        tx.run(NoteDatabase.DELETE_QUERY, note_id=note_id)

    def delete(self, note_id: str) -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: NoteDatabase.delete_with_transaction(tx, note_id))

    @staticmethod
    def delete_all_with_transaction(tx: ManagedTransaction, manifestation_id: str, note_type: str = "durchen") -> None:
        result = tx.run(
            NoteDatabase.GET_QUERY, note_id=None, manifestation_id=manifestation_id, note_type=note_type
        ).data()

        for record in result:
            NoteDatabase.delete_with_transaction(tx, record["note_id"])

    def delete_all(self, manifestation_id: str, note_type: str = "durchen") -> None:
        with self._db.get_session() as session:
            session.execute_write(lambda tx: NoteDatabase.delete_all_with_transaction(tx, manifestation_id, note_type))
