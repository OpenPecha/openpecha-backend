import logging

from models import ExpressionModelInput, TextType, ManifestationType
from exceptions import InvalidRequest
from neo4j_queries import Queries

logger = logging.getLogger(__name__)


class DataValidationError(Exception):
    pass


class Neo4JDatabaseValidator:
    def __init__(self):
        pass

    def validate_original_expression_uniqueness(self, session, work_id: str) -> None:
        query = """
        MATCH (w:Work {id: $work_id})<-[:EXPRESSION_OF {original: true}]-(e:Expression)
        RETURN count(e) as existing_count
        """

        result = session.run(query, work_id=work_id)
        record = result.single()

        if record and record["existing_count"] > 0:
            raise DataValidationError(
                f"Work {work_id} already has an original expression. "
                "Only one original expression per work is allowed."
            )

    def validate_person_references(self, session, person_ids: List[str]) -> None:
        if not person_ids:
            return

        query = """
        UNWIND $person_ids as person_id
        OPTIONAL MATCH (p:Person {id: person_id})
        RETURN person_id, p IS NOT NULL as exists
        """

        result = session.run(query, person_ids=person_ids)
        missing_persons = []

        for record in result:
            if not record["exists"]:
                missing_persons.append(record["person_id"])

        if missing_persons:
            raise DataValidationError(f"Referenced persons do not exist: {', '.join(missing_persons)}")

    def validate_person_bdrc_references(self, session, person_bdrc_ids: List[str]) -> None:
        if not person_bdrc_ids:
            return

        query = """
        UNWIND $person_bdrc_ids as bdrc_id
        OPTIONAL MATCH (p:Person {bdrc: bdrc_id})
        RETURN bdrc_id, p IS NOT NULL as exists
        """

        result = session.run(query, person_bdrc_ids=person_bdrc_ids)
        missing_persons = []

        for record in result:
            if not record["exists"]:
                missing_persons.append(record["bdrc_id"])

        if missing_persons:
            raise DataValidationError(f"Referenced person BDRC IDs do not exist: {', '.join(missing_persons)}")

    def validate_expression_creation(self, session, expression: ExpressionModelInput, work_id: str) -> None:
        if expression.type == TextType.ROOT:
            self.validate_original_expression_uniqueness(session, work_id)

        if expression.contributions:
            person_ids = [
                contrib.person_id
                for contrib in expression.contributions
                if hasattr(contrib, "person_id") and contrib.person_id
            ]
            person_bdrc_ids = [
                contrib.person_bdrc_id
                for contrib in expression.contributions
                if hasattr(contrib, "person_bdrc_id") and contrib.person_bdrc_id
            ]

            self.validate_person_references(session, person_ids)
            self.validate_person_bdrc_references(session, person_bdrc_ids)

    def validate_expression_exists(self, session, expression_id: str) -> None:
        query = """
        MATCH (e:Expression {id: $expression_id})
        RETURN count(e) as expression_count
        """

        result = session.run(query, expression_id=expression_id)
        record = result.single()

        if not record or record["expression_count"] == 0:
            raise DataValidationError(
                f"Expression {expression_id} does not exist. "
                "Cannot create manifestation for non-existent expression."
            )

    def has_manifestation_of_type_for_expression_id(self, session, expression_id: str, type: ManifestationType) -> bool:

        query = """
        MATCH (e:Expression {id: $expression_id})
        MATCH (m:Manifestation)-[:MANIFESTATION_OF]->(e)
        MATCH (m)-[:HAS_TYPE]->(mt:ManifestationType {name: $type})
        RETURN count(m) AS count
        """

        result = session.run(query, expression_id=expression_id, type=type.value)
        record = result.single()

        return bool(record and record.get("count", 0) > 0)

    def validate_language_code_exists(self, session, language_code: str) -> None:
        """Validate that a given base language code exists.

        Uses a single query to both check existence and obtain the available codes.
        Raises InvalidRequest with the available codes listed if not found.
        """
        query = """
        MATCH (l:Language)
        WITH collect(l.code) AS codes
        RETURN $code IN codes AS exists, codes
        """

        record = session.run(
            query,
            code=language_code,
        ).single()

        if not record or not record["exists"]:
            raise InvalidRequest(f"Language '{language_code}' is not present in Neo4j. Available languages: {', '.join(record['codes'])}")

    def validate_bibliography_type_exists(self, session, bibliography_types: list[str]) -> None:
        """Validate that all given bibliography type names exist. Raises InvalidRequest listing missing and available."""
        query = """
        MATCH (bt:BibliographyType)
        WITH collect(bt.name) AS names
        UNWIND $names_to_check AS name
        WITH names, name, name IN names AS exists
        RETURN collect(CASE WHEN exists THEN NULL ELSE name END) AS missing, names
        """
        record = session.run(query, names_to_check=[n.lower() for n in bibliography_types]).single()
        missing = [n for n in (record["missing"] or []) if n]
        if missing:
            available_list = ", ".join(sorted(record["names"])) if record["names"] else "none"
            raise InvalidRequest(
                f"Bibliography type(s) not found: {', '.join(sorted(missing))}. "
                f"Available bibliography types: {available_list}"
            )

    def validate_language_codes_exist(self, session, language_codes: list[str]) -> None:
        """Validate that all given base language codes exist. Raises InvalidRequest listing missing and available."""
        query = """
        MATCH (l:Language)
        WITH collect(l.code) AS codes
        UNWIND $codes_to_check AS code
        WITH codes, code, code IN codes AS exists
        RETURN collect(CASE WHEN exists THEN NULL ELSE code END) AS missing, codes
        """
        record = session.run(query, codes_to_check=[c.lower() for c in language_codes]).single()
        missing = [c for c in (record["missing"] or []) if c]
        if missing:
            raise InvalidRequest(
                f"Languages {', '.join(missing)} are not present in Neo4j. Available languages: {', '.join(record['codes'])}"
            )

    def validate_category_exists(self, session, category_id: str) -> None:
        """Validate that a category with the given ID exists.
        
        Raises DataValidationError if the category does not exist.
        """
        query = """
        MATCH (c:Category {id: $category_id})
        RETURN count(c) as count
        """
        
        result = session.run(query, category_id=category_id)
        record = result.single()
        
        if not record or record["count"] == 0:
            raise DataValidationError(
                f"Category with ID '{category_id}' does not exist. "
                "Please provide a valid category_id."
            )