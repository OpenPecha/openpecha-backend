from identifier import generate_id
from neo4j import ManagedTransaction, Session
from neo4j_queries import Queries

from .database import Database
from .database_validator import DatabaseValidator


class NomenDatabase:
    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    @staticmethod
    def create_with_transaction(
        tx: ManagedTransaction, primary_text: dict[str, str], alternative_texts: list[dict[str, str]] | None = None
    ) -> str:
        # Validate all base language codes from primary and alternative titles in one go (lowercased)
        base_codes = {tag.split("-")[0].lower() for tag in primary_text}
        for alt_text in alternative_texts or []:
            base_codes.update(tag.split("-")[0].lower() for tag in alt_text)
        DatabaseValidator.validate_language_codes_exist(tx, list(base_codes))
        # Build localized payloads
        primary_localized_texts = [
            {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
            for bcp47_tag, text in primary_text.items()
        ]

        # Create primary nomen
        primary_nomen_id = generate_id()
        result = tx.run(
            Queries.nomens["create"],
            nomen_id=primary_nomen_id,
            primary_nomen_id=None,
            localized_texts=primary_localized_texts,
        )
        result.single(strict=True)

        # Create alternative nomens
        for alt_text in alternative_texts or []:
            localized_texts = [
                {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
                for bcp47_tag, text in alt_text.items()
            ]

            tx.run(
                Queries.nomens["create"],
                nomen_id=generate_id(),
                primary_nomen_id=primary_nomen_id,
                localized_texts=localized_texts,
            )

        return primary_nomen_id
