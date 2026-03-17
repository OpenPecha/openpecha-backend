from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError, DataValidationError
from identifier import generate_id

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from models import TagInput, TagOutput
    from neo4j import ManagedTransaction, Session

    from .database import Database


class TagDatabase:
    GET_ALL_QUERY = """
    MATCH (t:Tag)-[:BELONGS_TO]->(app:Application {id: $application})
    RETURN {
        id: t.id,
        title: [(t)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(l:Language) | {language: l.code, text: lt.text}],
        description: [(t)-[:HAS_DESCRIPTION]->(dn:Nomen)-[:HAS_LOCALIZATION]->(dlt:LocalizedText)
            -[:HAS_LANGUAGE]->(dl:Language) | {language: dl.code, text: dlt.text}]
    } AS tag
    """

    CREATE_QUERY = """
        MATCH (n:Nomen {id: $nomen_id})
        MATCH (app:Application {id: $application})
        CREATE (t:Tag {id: $tag_id})
        CREATE (t)-[:HAS_TITLE]->(n)
        CREATE (t)-[:BELONGS_TO]->(app)
        WITH t
        OPTIONAL MATCH (desc_nomen:Nomen {id: $description_nomen_id})
        WITH t, desc_nomen
        CALL (*) { WHEN desc_nomen IS NOT NULL THEN { CREATE (t)-[:HAS_DESCRIPTION]->(desc_nomen) } }
        RETURN t.id AS tag_id
    """

    FIND_EXISTING_QUERY = """
        MATCH (t:Tag)-[:BELONGS_TO]->(app:Application {id: $application})
        MATCH (t)-[:HAS_TITLE]->(:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(:Language {code: $language})
        WHERE toLower(lt.text) = toLower($title_text)
        RETURN t.id AS tag_id
        LIMIT 1
    """

    DELETE_QUERY = """
        MATCH (t:Tag {id: $tag_id})-[:BELONGS_TO]->(app:Application {id: $application})
        OPTIONAL MATCH (t)-[:HAS_TITLE]->(title_nomen:Nomen)
        OPTIONAL MATCH (title_nomen)-[:HAS_LOCALIZATION]->(title_lt:LocalizedText)
        OPTIONAL MATCH (t)-[:HAS_DESCRIPTION]->(desc_nomen:Nomen)
        OPTIONAL MATCH (desc_nomen)-[:HAS_LOCALIZATION]->(desc_lt:LocalizedText)
        DETACH DELETE t, title_nomen, title_lt, desc_nomen, desc_lt
        RETURN count(*) AS deleted
    """

    TAG_WORK_QUERY = """
        MATCH (w:Work {id: $work_id})
        MATCH (t:Tag {id: $tag_id})
        MERGE (w)-[:HAS_TAG]->(t)
        RETURN w.id AS work_id
    """

    UNTAG_WORK_QUERY = """
        MATCH (w:Work {id: $work_id})-[r:HAS_TAG]->(t:Tag {id: $tag_id})
        DELETE r
        RETURN w.id AS work_id
    """

    TAG_SEGMENT_QUERY = """
        MATCH (s:Segment {id: $segment_id})
        MATCH (t:Tag {id: $tag_id})
        MERGE (s)-[:HAS_TAG]->(t)
        RETURN s.id AS segment_id
    """

    UNTAG_SEGMENT_QUERY = """
        MATCH (s:Segment {id: $segment_id})-[r:HAS_TAG]->(t:Tag {id: $tag_id})
        DELETE r
        RETURN s.id AS segment_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def get_all(self, application: str) -> list[TagOutput]:
        with self.session as session:
            result = session.run(
                TagDatabase.GET_ALL_QUERY,
                application=application,
            )
            return [DataAdapter.tag(record.data()["tag"]) for record in result]

    def create(self, tag: TagInput, application: str) -> str:
        def create_transaction(tx: ManagedTransaction) -> str:
            self._validate_not_exists_tx(tx, application, tag.title.root)

            tag_id = generate_id()
            nomen_id = NomenDatabase.create_with_transaction(tx, tag.title.root, None)
            description_nomen_id = None
            if tag.description is not None:
                description_nomen_id = NomenDatabase.create_with_transaction(tx, tag.description.root, None)

            result = tx.run(
                TagDatabase.CREATE_QUERY,
                tag_id=tag_id,
                application=application,
                nomen_id=nomen_id,
                description_nomen_id=description_nomen_id,
            )
            record = result.single(strict=True)
            return record["tag_id"]

        with self.session as session:
            return str(session.execute_write(create_transaction))

    def delete(self, tag_id: str, application: str) -> None:
        with self.session as session:
            result = session.run(
                TagDatabase.DELETE_QUERY,
                tag_id=tag_id,
                application=application,
            ).single()
            if not result or result["deleted"] == 0:
                raise DataNotFoundError(f"Tag with ID '{tag_id}' not found in application '{application}'")

    def tag_work(self, work_id: str, tag_id: str) -> None:
        with self.session as session:
            result = session.run(
                TagDatabase.TAG_WORK_QUERY,
                work_id=work_id,
                tag_id=tag_id,
            ).single()
            if not result:
                raise DataNotFoundError(f"Work '{work_id}' or Tag '{tag_id}' not found")

    def untag_work(self, work_id: str, tag_id: str) -> None:
        with self.session as session:
            result = session.run(
                TagDatabase.UNTAG_WORK_QUERY,
                work_id=work_id,
                tag_id=tag_id,
            ).single()
            if not result:
                raise DataNotFoundError(f"Tag '{tag_id}' is not attached to Work '{work_id}'")

    def tag_segment(self, segment_id: str, tag_id: str) -> None:
        with self.session as session:
            result = session.run(
                TagDatabase.TAG_SEGMENT_QUERY,
                segment_id=segment_id,
                tag_id=tag_id,
            ).single()
            if not result:
                raise DataNotFoundError(f"Segment '{segment_id}' or Tag '{tag_id}' not found")

    def untag_segment(self, segment_id: str, tag_id: str) -> None:
        with self.session as session:
            result = session.run(
                TagDatabase.UNTAG_SEGMENT_QUERY,
                segment_id=segment_id,
                tag_id=tag_id,
            ).single()
            if not result:
                raise DataNotFoundError(f"Tag '{tag_id}' is not attached to Segment '{segment_id}'")

    @staticmethod
    def tag_work_with_transaction(tx: ManagedTransaction, work_id: str, tag_id: str) -> None:
        result = tx.run(
            TagDatabase.TAG_WORK_QUERY,
            work_id=work_id,
            tag_id=tag_id,
        ).single()
        if not result:
            raise DataNotFoundError(f"Work '{work_id}' or Tag '{tag_id}' not found")

    def _validate_not_exists_tx(self, tx: ManagedTransaction, application: str, title: dict[str, str]) -> None:
        for language, title_text in title.items():
            result = tx.run(
                TagDatabase.FIND_EXISTING_QUERY,
                application=application,
                language=language,
                title_text=title_text,
            )
            record = result.single()
            if record:
                raise DataValidationError(
                    f"Tag with title '{title_text}' in language '{language}' "
                    f"already exists for application '{application}'"
                )
