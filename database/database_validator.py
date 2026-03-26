import logging

from neo4j import AsyncManagedTransaction

from exceptions import DataNotFoundError, DataValidationError, InvalidRequestError
from models.contribution import ContributionInput
from models.text import TextInput

logger = logging.getLogger(__name__)


class DatabaseValidator:
    def __init__(self) -> None:
        pass

    @staticmethod
    async def validate_person_references(tx: AsyncManagedTransaction, person_ids: list[str]) -> None:
        if not person_ids:
            return

        query = """
        UNWIND $person_ids as person_id
        OPTIONAL MATCH (p:Person {id: person_id})
        RETURN person_id, p IS NOT NULL as exists
        """

        result = await tx.run(query, person_ids=person_ids)
        records = await result.data()
        missing_persons = [record["person_id"] for record in records if not record["exists"]]

        if missing_persons:
            raise DataValidationError(f"Referenced persons do not exist: {', '.join(missing_persons)}")

    @staticmethod
    async def validate_person_bdrc_references(tx: AsyncManagedTransaction, person_bdrc_ids: list[str]) -> None:
        if not person_bdrc_ids:
            return

        query = """
        UNWIND $person_bdrc_ids as bdrc_id
        OPTIONAL MATCH (p:Person {bdrc: bdrc_id})
        RETURN bdrc_id, p IS NOT NULL as exists
        """

        result = await tx.run(query, person_bdrc_ids=person_bdrc_ids)
        records = await result.data()
        missing_persons = [record["bdrc_id"] for record in records if not record["exists"]]

        if missing_persons:
            raise DataValidationError(f"Referenced person BDRC IDs do not exist: {', '.join(missing_persons)}")

    @staticmethod
    async def validate_text_creation(tx: AsyncManagedTransaction, text: TextInput, work_id: str) -> None:
        if not text.commentary_of and not text.translation_of:
            await DatabaseValidator.validate_original_text_uniqueness(tx, work_id)

        if text.contributions:
            person_ids = [
                contrib.person_id
                for contrib in text.contributions
                if isinstance(contrib, ContributionInput) and contrib.person_id
            ]
            person_bdrc_ids = [
                contrib.person_bdrc_id
                for contrib in text.contributions
                if isinstance(contrib, ContributionInput) and contrib.person_bdrc_id
            ]

            await DatabaseValidator.validate_person_references(tx, person_ids)
            await DatabaseValidator.validate_person_bdrc_references(tx, person_bdrc_ids)

    @staticmethod
    async def validate_original_text_uniqueness(tx: AsyncManagedTransaction, work_id: str) -> None:
        query = """
        MATCH (w:Work {id: $work_id})
        RETURN count { (w)<-[:TEXT_OF {original: true}]-(:Text) } AS existing_count
        """

        result = await tx.run(query, work_id=work_id)
        record = await result.single()

        if record and record["existing_count"] > 0:
            raise DataValidationError(
                f"Work {work_id} already has an original text. Only one original text per work is allowed."
            )

    @staticmethod
    async def validate_text_exists(tx: AsyncManagedTransaction, text_id: str) -> None:
        query = """
        RETURN EXISTS { (e:Text {id: $text_id}) } AS exists
        """

        result = await tx.run(query, text_id=text_id)
        record = await result.single()

        if not record or not record["exists"]:
            raise DataValidationError(f"Text {text_id} does not exist. Cannot create edition for non-existent text.")

    @staticmethod
    async def validate_edition_exists(tx: AsyncManagedTransaction, edition_id: str) -> None:
        query = """
        RETURN EXISTS { (m:Edition {id: $edition_id}) } AS exists
        """

        result = await tx.run(query, edition_id=edition_id)
        record = await result.single()

        if not record or not record["exists"]:
            raise DataNotFoundError(f"Edition with ID '{edition_id}' not found")

    @staticmethod
    async def validate_language_code_exists(tx: AsyncManagedTransaction, language_code: str) -> None:
        """Validate that a given base language code exists.

        Uses direct pattern matching for efficiency, only collecting all codes on failure.
        Raises InvalidRequest with the available codes listed if not found.
        """
        query = """
        OPTIONAL MATCH (l:Language {code: $code})
        CALL () { MATCH (lang:Language) RETURN collect(lang.code) AS all_codes }
        RETURN l IS NOT NULL AS exists,
               CASE WHEN l IS NULL THEN all_codes ELSE null END AS codes
        """

        result = await tx.run(query, code=language_code)
        record = await result.single()

        if not record or not record["exists"]:
            codes = record["codes"] if record else []
            if not codes:
                raise InvalidRequestError(f"Language '{language_code}' is not present in Neo4j. No languages found.")
            raise InvalidRequestError(
                f"Language '{language_code}' is not present in Neo4j. Available languages: {', '.join(codes)}"
            )

    @staticmethod
    async def validate_language_codes_exist(tx: AsyncManagedTransaction, language_codes: list[str]) -> None:
        """Validate that all given base language codes exist. Raises InvalidRequest listing missing and available."""
        query = """
        UNWIND $codes_to_check AS code
        OPTIONAL MATCH (l:Language {code: code})
        WITH code, l IS NOT NULL AS exists
        WITH collect(CASE WHEN NOT exists THEN code END) AS missing
        CALL () { MATCH (lang:Language) RETURN collect(lang.code) AS codes }
        RETURN missing, codes
        """
        result = await tx.run(query, codes_to_check=[c.lower() for c in language_codes])
        record = await result.single()
        if not record:
            raise InvalidRequestError("No languages found in Neo4j database")
        missing = [c for c in (record["missing"] or []) if c]
        if missing:
            raise InvalidRequestError(
                f"Languages {', '.join(missing)} are not present in Neo4j. "
                f"Available languages: {', '.join(record['codes'])}"
            )

    @staticmethod
    async def validate_category_exists(tx: AsyncManagedTransaction, category_id: str) -> None:
        """Validate that a category with the given ID exists.

        Raises DataValidationError if the category does not exist.
        """
        query = """
        RETURN EXISTS { (c:Category {id: $category_id}) } AS exists
        """

        result = await tx.run(query, category_id=category_id)
        record = await result.single()

        if not record or not record["exists"]:
            raise DataValidationError(
                f"Category with ID '{category_id}' does not exist. Please provide a valid category_id."
            )

    @staticmethod
    async def validate_tags_exist(tx: AsyncManagedTransaction, tag_ids: list[str]) -> None:
        """Validate that all given tag IDs exist. Raises DataValidationError listing missing IDs."""
        if not tag_ids:
            return

        query = """
        UNWIND $tag_ids AS tag_id
        OPTIONAL MATCH (t:Tag {id: tag_id})
        RETURN tag_id, t IS NOT NULL AS exists
        """

        result = await tx.run(query, tag_ids=tag_ids)
        records = await result.data()
        missing_tags = [record["tag_id"] for record in records if not record["exists"]]

        if missing_tags:
            raise DataValidationError(f"Referenced tags do not exist: {', '.join(missing_tags)}")

    @staticmethod
    async def validate_text_title_unique(tx: AsyncManagedTransaction, title: dict[str, str]) -> None:
        """Ensure no text exists with the same title text and language combination."""
        if not title:
            return

        query = """
        UNWIND $titles AS item
        RETURN EXISTS {
            MATCH (e:Text)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText {text: item.text})
                  -[:HAS_LANGUAGE]->(lang:Language {code: item.lang})
        } AS exists
        """

        titles_list = [{"text": text, "lang": lang} for lang, text in title.items()]
        result = await tx.run(query, titles=titles_list)
        records = await result.data()

        for record in records:
            if record["exists"]:
                raise DataValidationError("Text with the same title and language already exists")
