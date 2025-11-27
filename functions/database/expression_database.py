from exceptions import DataNotFound
from identifier import generate_id
from models import AIContributionModel, ContributionModel, ExpressionModelInput, ExpressionModelOutput, TextType
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

from .data_adapter import DataAdapter
from .database_validator import DatabaseValidator
from .nomen_database import NomenDatabase


class ExpressionDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    def get(self, expression_id: str) -> ExpressionModelOutput:
        with self.session as session:
            result = session.run(Queries.expressions["fetch_by_id"], id=expression_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")

            return DataAdapter.expression(data=record.data()["expression"])

    def get_all(
        self,
        offset: int = 0,
        limit: int = 20,
        filters: dict[str, str] | None = None,
    ) -> list[ExpressionModelOutput]:
        if filters is None:
            filters = {}

        params = {
            "offset": offset,
            "limit": limit,
            "type": filters.get("type"),
            "language": filters.get("language"),
            "title": filters.get("title"),
        }

        with self.session as session:
            if params.get("language"):
                DatabaseValidator.validate_language_code_exists(session, params["language"])

            result = session.run(Queries.expressions["fetch_all"], params)
            expressions = []

            for record in result:
                expression_data = record.data()["expression"]
                expression = DataAdapter.expression(data=expression_data)
                expressions.append(expression)

            return expressions

    def get_by_bdrc(self, bdrc_id: str) -> ExpressionModelOutput:
        with self.session as session:
            result = session.run(Queries.expressions["fetch_by_bdrc"], bdrc_id=bdrc_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with BDRC ID '{bdrc_id}' not found")

            return DataAdapter.expression(data=record.data()["expression"])

    def get_by_ids(self, expression_ids: list[str]) -> dict[str, ExpressionModelOutput]:
        if not expression_ids:
            return {}

        with self.session as session:
            result = session.execute_read(
                lambda tx: list(tx.run(Queries.expressions["get_expressions_by_ids"], expression_ids=expression_ids))
            )
            return {
                record["expression_id"]: DataAdapter.expression(data=record.data()["expression"]) for record in result
            }

    def get_id_by_manifestation(self, manifestation_id: str) -> str:
        result = self.get_ids_by_manifestations([manifestation_id])
        return result.get(manifestation_id)

    def get_ids_by_manifestations(self, manifestation_ids: list[str]) -> dict[str, str]:
        if not manifestation_ids:
            return {}

        with self.session as session:
            result = session.execute_read(
                lambda tx: list(
                    tx.run(
                        Queries.manifestations["get_expression_ids_by_manifestation_ids"],
                        manifestation_ids=manifestation_ids,
                    )
                )
            )
            return {record["manifestation_id"]: record["expression_id"] for record in result}

    def create(self, expression: ExpressionModelInput) -> str:
        with self.session as session:
            return session.execute_write(lambda tx: self.create_with_transaction(tx, expression))

    def validate_create(self, expression: ExpressionModelInput):
        with self.session as session:
            session.execute_read(lambda tx: self._validate_create(tx, expression))

    @staticmethod
    def _validate_create(tx, expression: ExpressionModelInput):
        DatabaseValidator.validate_expression_title_unique(tx, expression.title)

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

            DatabaseValidator.validate_person_references(tx, person_ids)
            DatabaseValidator.validate_person_bdrc_references(tx, person_bdrc_ids)

    @staticmethod
    def create_with_transaction(tx, expression: ExpressionModelInput, expression_id: str | None = None) -> str:
        # TODO: move the validation based on language to the database validator
        expression_id = expression_id or generate_id()
        target_id = expression.target if expression.target != "N/A" else None
        if target_id and expression.type == TextType.TRANSLATION:
            result = tx.run(Queries.expressions["fetch_by_id"], id=target_id).single()
            target_language = result.data()["expression"]["language"] if result else None
            if target_language == expression.language:
                raise ValueError("Translation must have a different language than the target expression")

        work_id = generate_id()
        DatabaseValidator.validate_expression_creation(tx, expression, work_id)
        base_lang_code = expression.language.split("-")[0].lower()
        # Validate base language exists (single-query validator)
        DatabaseValidator.validate_language_code_exists(tx, base_lang_code)
        # Validate category exists if category_id is provided
        if expression.category_id:
            DatabaseValidator.validate_category_exists(tx, expression.category_id)
        alt_titles_data = [alt_title.root for alt_title in expression.alt_titles] if expression.alt_titles else None
        expression_title_element_id = NomenDatabase.create_with_transaction(tx, expression.title.root, alt_titles_data)

        common_params = {
            "expression_id": expression_id,
            "bdrc": expression.bdrc,
            "wiki": expression.wiki,
            "date": expression.date,
            "language_code": base_lang_code,
            "bcp47_tag": expression.language,
            "title_nomen_element_id": expression_title_element_id,
            "target_id": target_id,
            "copyright": expression.copyright.value,
            "license": expression.license.value,
        }

        match expression.type:
            case TextType.ROOT:
                tx.run(Queries.expressions["create_standalone"], work_id=work_id, original=True, **common_params)
            case TextType.TRANSLATION:
                if expression.target == "N/A":
                    tx.run(Queries.expressions["create_standalone"], work_id=work_id, original=False, **common_params)
                else:
                    tx.run(Queries.expressions["create_translation"], **common_params)
            case TextType.COMMENTARY:
                if expression.target == "N/A":
                    raise NotImplementedError("Standalone COMMENTARY texts (target='N/A') are not yet supported")
                tx.run(Queries.expressions["create_commentary"], work_id=work_id, **common_params)

        # Link work to category if category_id is provided
        if expression.category_id:
            tx.run(Queries.works["link_to_category"], work_id=work_id, category_id=expression.category_id)

        if expression.contributions:
            for contribution in expression.contributions:
                # Validate that the role exists in the database
                if isinstance(contribution, ContributionModel):
                    result = tx.run(
                        Queries.expressions["create_contribution"],
                        expression_id=expression_id,
                        person_id=contribution.person_id,
                        person_bdrc_id=contribution.person_bdrc_id,
                        role_name=contribution.role.value,
                    )

                    if not result.single():
                        raise DataNotFound(
                            f"Person or Role not found. "
                            f"Person: id={contribution.person_id}, bdrc_id={contribution.person_bdrc_id}; "
                            f"Role: {contribution.role.value}"
                        )
                elif isinstance(contribution, AIContributionModel):
                    ai_result = tx.run(
                        Queries.ai["find_or_create"],
                        ai_id=contribution.ai_id,
                    )
                    record = ai_result.single()
                    if not record:
                        raise DataNotFound("Failed to find or create AI node")

                    result = tx.run(
                        Queries.expressions["create_ai_contribution"],
                        expression_id=expression_id,
                        ai_element_id=record["ai_element_id"],
                        role_name=contribution.role.value,
                    )
                    if not result.single():
                        raise DataNotFound(
                            f"AI contribution creation failed. "
                            f"AI: {contribution.ai_id}; Role: {contribution.role.value}"
                        )
                else:
                    raise ValueError(f"Unknown contribution type: {type(contribution)}")

        return expression_id
