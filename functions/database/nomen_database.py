from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

from .database_validator import DatabaseValidator


class NomenDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    @staticmethod
    def create_with_transaction(
        tx, primary_text: dict[str, str], alternative_texts: list[dict[str, str]] = None
    ) -> str:
        # Validate all base language codes from primary and alternative titles in one go (lowercased)
        base_codes = {tag.split("-")[0].lower() for tag in primary_text.keys()}
        for alt_text in alternative_texts or []:
            base_codes.update(tag.split("-")[0].lower() for tag in alt_text.keys())
        DatabaseValidator.validate_language_codes_exist(tx, list(base_codes))
        # Build localized payloads
        primary_localized_texts = [
            {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
            for bcp47_tag, text in primary_text.items()
        ]

        # Create primary nomen
        result = tx.run(
            Queries.nomens["create"],
            primary_name_element_id=None,
            localized_texts=primary_localized_texts,
        )
        primary_nomen_element_id = result.single()["element_id"]

        # Create alternative nomens
        for alt_text in alternative_texts or []:
            localized_texts = [
                {"base_lang_code": bcp47_tag.split("-")[0].lower(), "bcp47_tag": bcp47_tag, "text": text}
                for bcp47_tag, text in alt_text.items()
            ]

            tx.run(
                Queries.nomens["create"],
                primary_name_element_id=primary_nomen_element_id,
                localized_texts=localized_texts,
            )

        return primary_nomen_element_id
