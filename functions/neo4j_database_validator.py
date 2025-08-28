import logging
from typing import List

from metadata_model_v2 import ExpressionModelInput, TextType

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
            person_ids = [contrib.person_id for contrib in expression.contributions if hasattr(contrib, 'person_id') and contrib.person_id]
            person_bdrc_ids = [contrib.person_bdrc_id for contrib in expression.contributions if hasattr(contrib, 'person_bdrc_id') and contrib.person_bdrc_id]

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
