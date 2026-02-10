from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataValidationError
from identifier import generate_id

from .database_validator import DatabaseValidator

if TYPE_CHECKING:
    from neo4j import ManagedTransaction

    from .database import Database


class NomenDatabase:
    CREATE_QUERY = """
    OPTIONAL MATCH (primary:Nomen {id: $primary_nomen_id})
    CREATE (n:Nomen {id: $nomen_id})
    WITH n, primary
    CALL (*) {
        WHEN primary IS NOT NULL THEN { CREATE (n)-[:ALTERNATIVE_OF]->(primary) }
    }
    WITH n
    CALL (n) {
        UNWIND $localized_texts AS lt
        MERGE (l:Language {code: lt.base_lang_code})
        CREATE (n)-[:HAS_LOCALIZATION]->(locText:LocalizedText {text: lt.text})
            -[:HAS_LANGUAGE {bcp47: lt.bcp47_tag}]->(l)
        RETURN count(*) AS _
    }
    RETURN n.id as nomen_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @staticmethod
    def create_with_transaction(
        tx: ManagedTransaction, primary_text: dict[str, str], alternative_texts: list[dict[str, str]] | None = None
    ) -> str:
        base_codes = {tag.split("-")[0].lower() for tag in primary_text}
        for alt_text in alternative_texts or []:
            base_codes.update(tag.split("-")[0].lower() for tag in alt_text)
        DatabaseValidator.validate_language_codes_exist(tx, list(base_codes))

        primary_localized_texts = [
            {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
            for bcp47_tag, text in primary_text.items()
        ]

        primary_nomen_id = generate_id()
        result = tx.run(
            NomenDatabase.CREATE_QUERY,
            nomen_id=primary_nomen_id,
            primary_nomen_id=None,
            localized_texts=primary_localized_texts,
        )
        record = result.single()
        if not record:
            raise DataValidationError(f"Failed to create Nomen with texts: {list(primary_text.keys())}")

        for alt_text in alternative_texts or []:
            localized_texts = [
                {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
                for bcp47_tag, text in alt_text.items()
            ]

            tx.run(
                NomenDatabase.CREATE_QUERY,
                nomen_id=generate_id(),
                primary_nomen_id=primary_nomen_id,
                localized_texts=localized_texts,
            )

        return primary_nomen_id
