import logging

from exceptions import InvalidRequestError
from models import (
    AnnotationType,
    ContributionInput,
    ExpressionInput,
)
from neo4j import ManagedTransaction

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    pass


class DatabaseValidator:
    def __init__(self) -> None:
        pass

    @staticmethod
    def validate_original_expression_uniqueness(tx: ManagedTransaction, work_id: str) -> None:
        query = """
        MATCH (w:Work {id: $work_id})<-[:EXPRESSION_OF {original: true}]-(e:Expression)
        RETURN count(e) as existing_count
        """

        result = tx.run(query, work_id=work_id)
        record = result.single()

        if record and record["existing_count"] > 0:
            raise DataValidationError(
                f"Work {work_id} already has an original expression. Only one original expression per work is allowed."
            )

    @staticmethod
    def validate_person_references(tx: ManagedTransaction, person_ids: list[str]) -> None:
        if not person_ids:
            return

        query = """
        UNWIND $person_ids as person_id
        OPTIONAL MATCH (p:Person {id: person_id})
        RETURN person_id, p IS NOT NULL as exists
        """

        result = tx.run(query, person_ids=person_ids)
        missing_persons = [record["person_id"] for record in result if not record["exists"]]

        if missing_persons:
            raise DataValidationError(f"Referenced persons do not exist: {', '.join(missing_persons)}")

    @staticmethod
    def validate_person_bdrc_references(tx: ManagedTransaction, person_bdrc_ids: list[str]) -> None:
        if not person_bdrc_ids:
            return

        query = """
        UNWIND $person_bdrc_ids as bdrc_id
        OPTIONAL MATCH (p:Person {bdrc: bdrc_id})
        RETURN bdrc_id, p IS NOT NULL as exists
        """

        result = tx.run(query, person_bdrc_ids=person_bdrc_ids)
        missing_persons = [record["bdrc_id"] for record in result if not record["exists"]]

        if missing_persons:
            raise DataValidationError(f"Referenced person BDRC IDs do not exist: {', '.join(missing_persons)}")

    @staticmethod
    def validate_expression_creation(tx: ManagedTransaction, expression: ExpressionInput, work_id: str) -> None:
        # Validate uniqueness for root expressions (no parent relationships)
        if not expression.commentary_of and not expression.translation_of:
            DatabaseValidator.validate_original_expression_uniqueness(tx, work_id)

        if expression.contributions:
            person_ids = [
                contrib.person_id
                for contrib in expression.contributions
                if isinstance(contrib, ContributionInput) and contrib.person_id
            ]
            person_bdrc_ids = [
                contrib.person_bdrc_id
                for contrib in expression.contributions
                if isinstance(contrib, ContributionInput) and contrib.person_bdrc_id
            ]

            DatabaseValidator.validate_person_references(tx, person_ids)
            DatabaseValidator.validate_person_bdrc_references(tx, person_bdrc_ids)

    @staticmethod
    def validate_expression_exists(tx: ManagedTransaction, expression_id: str) -> None:
        query = """
        MATCH (e:Expression {id: $expression_id})
        RETURN count(e) as expression_count
        """

        result = tx.run(query, expression_id=expression_id)
        record = result.single()

        if not record or record["expression_count"] == 0:
            raise DataValidationError(
                f"Expression {expression_id} does not exist. Cannot create manifestation for non-existent expression."
            )

    @staticmethod
    def validate_language_code_exists(tx: ManagedTransaction, language_code: str) -> None:
        """Validate that a given base language code exists.

        Uses a single query to both check existence and obtain the available codes.
        Raises InvalidRequest with the available codes listed if not found.
        """
        query = """
        MATCH (l:Language)
        WITH collect(l.code) AS codes
        RETURN $code IN codes AS exists, codes
        """

        record = tx.run(
            query,
            code=language_code,
        ).single()

        if not record:
            raise InvalidRequestError(f"Language '{language_code}' is not present in Neo4j. No languages found.")
        if not record["exists"]:
            raise InvalidRequestError(
                f"Language '{language_code}' is not present in Neo4j. Available languages: {', '.join(record['codes'])}"
            )

    @staticmethod
    def validate_language_codes_exist(tx: ManagedTransaction, language_codes: list[str]) -> None:
        """Validate that all given base language codes exist. Raises InvalidRequest listing missing and available."""
        query = """
        MATCH (l:Language)
        WITH collect(l.code) AS codes
        UNWIND $codes_to_check AS code
        WITH codes, code, code IN codes AS exists
        RETURN collect(CASE WHEN exists THEN NULL ELSE code END) AS missing, codes
        """
        record = tx.run(query, codes_to_check=[c.lower() for c in language_codes]).single()
        if not record:
            raise InvalidRequestError("No languages found in Neo4j database")
        missing = [c for c in (record["missing"] or []) if c]
        if missing:
            raise InvalidRequestError(
                f"Languages {', '.join(missing)} are not present in Neo4j. "
                f"Available languages: {', '.join(record['codes'])}"
            )

    @staticmethod
    def validate_category_exists(tx: ManagedTransaction, category_id: str) -> None:
        """Validate that a category with the given ID exists.

        Raises DataValidationError if the category does not exist.
        """
        query = """
        MATCH (c:Category {id: $category_id})
        RETURN count(c) as count
        """

        result = tx.run(query, category_id=category_id)
        record = result.single()

        if not record or record["count"] == 0:
            raise DataValidationError(
                f"Category with ID '{category_id}' does not exist. Please provide a valid category_id."
            )

    @staticmethod
    def validate_no_annotation_type_exists(
        tx: ManagedTransaction, manifestation_id: str, annotation_type: AnnotationType
    ) -> None:
        """Ensure annotation type doesn't already exist for manifestation."""
        query = """
        MATCH (m:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(a:Annotation)-[:HAS_TYPE]->
        (t:AnnotationType {name: $type})
        RETURN count(a) as count
        """
        result = tx.run(query, manifestation_id=manifestation_id, type=annotation_type.value)
        record = result.single()
        if record and record["count"] > 0:
            raise DataValidationError(
                f"Annotation of type '{annotation_type.value}' already exists for manifestation '{manifestation_id}'"
            )

    @staticmethod
    def validate_no_alignment_exists(
        tx: ManagedTransaction, manifestation_id: str, target_manifestation_id: str
    ) -> None:
        """Ensure alignment relationship doesn't already exist between manifestations."""
        query = """
        MATCH (m1:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(a1:Annotation)-[:HAS_TYPE]->
        (t:AnnotationType {name: 'alignment'})
        MATCH (m2:Manifestation {id: $target_manifestation_id})<-[:ANNOTATION_OF]-(a2:Annotation {aligned_to: a1.id})
        RETURN count(*) as count
        """
        result = tx.run(query, manifestation_id=manifestation_id, target_manifestation_id=target_manifestation_id)
        record = result.single()
        if record and record["count"] > 0:
            raise DataValidationError(
                f"Alignment relationship already exists between manifestation '{manifestation_id}' "
                f"and target manifestation '{target_manifestation_id}'"
            )

    @staticmethod
    def validate_expression_title_unique(tx: ManagedTransaction, title: dict[str, str]) -> None:
        """Ensure no expression exists with the same title text and language combination."""
        if not title:
            return

        query = """
        UNWIND $titles AS item
        RETURN EXISTS {
            MATCH (e:Expression)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText {text: item.text})
                  -[:HAS_LANGUAGE]->(lang:Language {code: item.lang})
        } AS exists
        """

        titles_list = [{"text": text, "lang": lang} for lang, text in title.items()]
        result = tx.run(query, titles=titles_list)

        for record in result:
            if record["exists"]:
                raise DataValidationError("Expression with the same title and language already exists")
