import logging
import os

from exceptions import DataNotFound
from identifier import generate_id
from models import (
    AIContributionModel,
    AnnotationModel,
    AnnotationType,
    ContributionModel,
    CopyrightStatus,
    ExpressionModelInput,
    ExpressionModelOutput,
    LocalizedString,
    ManifestationModelInput,
    ManifestationModelOutput,
    ManifestationType,
    PersonModelInput,
    PersonModelOutput,
    TextType,
)
from neo4j import GraphDatabase
from neo4j_database_validator import Neo4JDatabaseValidator
from neo4j_queries import Queries

logger = logging.getLogger(__name__)


class Neo4JDatabase:
    def __init__(self, neo4j_uri: str = None, neo4j_auth: tuple = None) -> None:
        if neo4j_uri and neo4j_auth:
            self.__driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        else:
            self.__driver = GraphDatabase.driver(
                os.environ.get("NEO4J_URI"),
                auth=("neo4j", os.environ.get("NEO4J_PASSWORD")),
            )
        self.__driver.verify_connectivity()
        self.__validator = Neo4JDatabaseValidator()
        logger.info("Connection to neo4j established.")

    def __del__(self):
        """Destructor to automatically close the driver when object is garbage collected"""
        self.__close_driver()

    def get_session(self):
        return self.__driver.session()

    def __close_driver(self):
        """Private method to close the Neo4j driver"""
        if self.__driver:
            self.__driver.close()

    def get_expression(self, expression_id: str) -> ExpressionModelOutput:
        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_by_id"], id=expression_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")

            expression = record.data()["expression"]

            return ExpressionModelOutput(
                id=expression.get("id"),
                bdrc=expression.get("bdrc"),
                wiki=expression.get("wiki"),
                type=TextType(expression.get("type")),
                contributions=self._build_contributions(expression.get("contributors")),
                date=expression.get("date"),
                title=self.__convert_to_localized_text(expression.get("title")),
                alt_titles=[self.__convert_to_localized_text(alt) for alt in expression.get("alt_titles")],
                language=expression.get("language"),
                parent=expression.get("parent"),
            )

    def _process_manifestation_data(self, manifestation_data: dict) -> ManifestationModelOutput:
        annotations = [
            AnnotationModel(
                id=annotation.get("id"),
                type=AnnotationType(annotation.get("type")),
                aligned_to=annotation.get("aligned_to"),
            )
            for annotation in manifestation_data.get("annotations", [])
        ]

        incipit_title = self.__convert_to_localized_text(manifestation_data.get("incipit_title"))
        alt_incipit_titles = (
            [self.__convert_to_localized_text(alt) for alt in manifestation_data.get("alt_incipit_titles", [])]
            if manifestation_data.get("alt_incipit_titles")
            else None
        )

        return ManifestationModelOutput(
            id=manifestation_data["id"],
            bdrc=manifestation_data.get("bdrc"),
            wiki=manifestation_data.get("wiki"),
            type=ManifestationType(manifestation_data["type"]),
            annotations=annotations,
            copyright=CopyrightStatus(manifestation_data["copyright"]),
            colophon=manifestation_data.get("colophon"),
            incipit_title=incipit_title,
            alt_incipit_titles=alt_incipit_titles,
            alignment_sources=manifestation_data.get("alignment_sources"),
            alignment_targets=manifestation_data.get("alignment_targets"),
        )

    def get_manifestations_by_expression(self, expression_id: str) -> list[ManifestationModelOutput]:
        with self.__driver.session() as session:
            rows = session.execute_read(
                lambda tx: [
                    r.data()
                    for r in tx.run(Queries.manifestations["fetch"], expression_id=expression_id, manifestation_id=None)
                ]
            )
            return [self._process_manifestation_data(row["manifestation"]) for row in rows]

    def get_manifestation(self, manifestation_id: str) -> tuple[ManifestationModelOutput, str]:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch"], manifestation_id=manifestation_id, expression_id=None
                ).single()
            )
            if record is None:
                raise DataNotFound(f"Manifestation '{manifestation_id}' not found")
            d = record.data()
            return self._process_manifestation_data(d["manifestation"]), d["expression_id"]

    def get_manifestation_by_annotation(self, annotation_id: str) -> tuple[ManifestationModelOutput, str] | None:
        with self.__driver.session() as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.manifestations["fetch_by_annotation"], annotation_id=annotation_id).single()
            )
            if record is None:
                return None
            d = record.data()
            return self._process_manifestation_data(d["manifestation"]), d["expression_id"]

    def create_manifestation(
        self, manifestation: ManifestationModelInput, annotation: AnnotationModel, expression_id: str
    ) -> str:
        def transaction_function(tx):
            manifestation_id = self._execute_create_manifestation(tx, manifestation, expression_id)
            self._execute_add_annotation(tx, manifestation_id, annotation)
            return manifestation_id

        with self.__driver.session() as session:
            return session.execute_write(transaction_function)

    def get_all_persons(self) -> list[PersonModelOutput]:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_all"])
            return [
                person_model
                for record in result
                if (person_model := self._create_person_model(record.data()["person"])) is not None
            ]

    def get_person(self, person_id: str) -> PersonModelOutput:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_by_id"], id=person_id)
            record = result.single()
            if not record:
                raise DataNotFound(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            person_model = self._create_person_model(person_data)
            if person_model is None:
                raise DataNotFound(f"Person with ID '{person_id}' has invalid data and cannot be retrieved")
            return person_model

    def create_person(self, person: PersonModelInput) -> str:
        def create_transaction(tx):
            person_id = generate_id()
            alt_names_data = [alt_name.root for alt_name in person.alt_names] if person.alt_names else None
            primary_name_element_id = self._create_nomens(tx, person.name.root, alt_names_data)

            tx.run(
                Queries.persons["create"],
                id=person_id,
                bdrc=person.bdrc,
                wiki=person.wiki,
                primary_name_element_id=primary_name_element_id,
            )

            return person_id

        with self.__driver.session() as session:
            return session.execute_write(create_transaction)

    def create_expression(self, expression: ExpressionModelInput) -> str:
        with self.__driver.session() as session:
            return session.execute_write(lambda tx: self._execute_create_expression(tx, expression))

    def add_annotation(self, manifestation_id: str, annotation: AnnotationModel) -> str:
        with self.__driver.session() as session:
            return session.execute_write(lambda tx: self._execute_add_annotation(tx, manifestation_id, annotation))

    def create_commentary(
        self,
        expression: ExpressionModelInput,
        expression_id: str,
        manifestation: ManifestationModelInput,
        annotation: AnnotationModel,
        original_manifestation_id: str,
        original_annotation: AnnotationModel = None,
    ) -> str:
        def transaction_function(tx):
            _ = self._execute_create_expression(tx, expression, expression_id)
            manifestation_id = self._execute_create_manifestation(tx, manifestation, expression_id)

            if original_annotation:
                _ = self._execute_add_annotation(tx, original_manifestation_id, original_annotation)

            _ = self._execute_add_annotation(tx, manifestation_id, annotation)

            return manifestation_id

        with self.__driver.session() as session:
            return session.execute_write(transaction_function)

    def create_translation(
        self,
        expression_id: str,
        expression: ExpressionModelInput,
        manifestation: ManifestationModelInput,
        annotation: AnnotationModel,
        original_manifestation_id: str,
        original_annotation: AnnotationModel = None,
    ) -> str:
        def transaction_function(tx):
            _ = self._execute_create_expression(tx, expression, expression_id)
            manifestation_id = self._execute_create_manifestation(tx, manifestation, expression_id)

            if original_annotation:
                _ = self._execute_add_annotation(tx, original_manifestation_id, original_annotation)

            _ = self._execute_add_annotation(tx, manifestation_id, annotation)

            return manifestation_id

        with self.__driver.session() as session:
            return session.execute_write(transaction_function)

    def _create_nomens(self, tx, primary_text: dict[str, str], alternative_texts: list[dict[str, str]] = None) -> str:
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

    def get_all_expressions(
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
        }

        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_all"], params)
            expressions = []

            for record in result:
                expression_data = record.data()["expression"]
                parent = expression_data["parent"]

                expression_type = expression_data["type"]
                if expression_type is None:
                    raise ValueError(f"Expression type invalid for expression {expression_data['id']}")

                expression = ExpressionModelOutput(
                    id=expression_data["id"],
                    bdrc=expression_data["bdrc"],
                    wiki=expression_data["wiki"],
                    type=TextType(expression_type),
                    contributions=self._build_contributions(expression_data.get("contributors")),
                    date=expression_data["date"],
                    title=self.__convert_to_localized_text(expression_data["title"]),
                    alt_titles=[self.__convert_to_localized_text(alt) for alt in expression_data["alt_titles"]],
                    language=expression_data["language"],
                    parent=parent,
                )
                expressions.append(expression)

            return expressions

    def __convert_to_localized_text(self, entries: list[dict[str, str]] | None) -> dict[str, str] | None:
        if entries is None:
            return None
        result = {entry["language"]: entry["text"] for entry in entries if "language" in entry and "text" in entry}
        return result or None

    def _create_person_model(self, person_data, person_id=None) -> PersonModelOutput | None:
        try:
            person = PersonModelOutput(
                id=person_id or person_data.get("id"),
                bdrc=person_data.get("bdrc"),
                wiki=person_data.get("wiki"),
                name=LocalizedString(self.__convert_to_localized_text(person_data["name"])),
                alt_names=(
                    [LocalizedString(self.__convert_to_localized_text(alt)) for alt in person_data["alt_names"]]
                    if person_data.get("alt_names")
                    else None
                ),
            )
        except Exception as e:
            logger.error("Failed to create person (data: %s) model: %s", person_id or person_data, e)
            raise  # temprorarily so we know if data is corrupted in the db
            # return None

        return person

    def _build_contributions(self, items: list[dict] | None) -> list[ContributionModel | AIContributionModel]:
        out: list[ContributionModel | AIContributionModel] = []
        for c in items or []:
            if c.get("ai_id"):
                out.append(AIContributionModel(ai_id=c["ai_id"], role=c["role"]))
            else:
                out.append(
                    ContributionModel(
                        person_id=c.get("person_id"),
                        person_bdrc_id=c.get("person_bdrc_id"),
                        role=c["role"],
                    )
                )
        return out

    def _execute_create_expression(self, tx, expression: ExpressionModelInput, expression_id: str | None = None) -> str:
        # TODO: move the validation based on language to the database validator
        expression_id = expression_id or generate_id()
        parent_id = expression.parent if expression.parent != "N/A" else None
        if parent_id and expression.type == TextType.TRANSLATION:
            result = tx.run(Queries.expressions["fetch_by_id"], id=parent_id).single()
            parent_language = result.data()["expression"]["language"] if result else None
            if parent_language == expression.language:
                raise ValueError("Translation must have a different language than the parent expression")

        work_id = generate_id()
        self.__validator.validate_expression_creation(tx, expression, work_id)
        base_lang_code = expression.language.split("-")[0].lower()
        alt_titles_data = [alt_title.root for alt_title in expression.alt_titles] if expression.alt_titles else None
        expression_title_element_id = self._create_nomens(tx, expression.title.root, alt_titles_data)

        common_params = {
            "expression_id": expression_id,
            "bdrc": expression.bdrc,
            "wiki": expression.wiki,
            "date": expression.date,
            "language_code": base_lang_code,
            "bcp47_tag": expression.language,
            "title_nomen_element_id": expression_title_element_id,
            "parent_id": parent_id,
        }

        match expression.type:
            case TextType.ROOT:
                tx.run(Queries.expressions["create_standalone"], work_id=work_id, original=True, **common_params)
            case TextType.TRANSLATION:
                if expression.parent == "N/A":
                    tx.run(Queries.expressions["create_standalone"], work_id=work_id, original=False, **common_params)
                else:
                    tx.run(Queries.expressions["create_translation"], **common_params)
            case TextType.COMMENTARY:
                tx.run(Queries.expressions["create_commentary"], work_id=work_id, **common_params)

        for contribution in expression.contributions:
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
                        f"Person (id: {contribution.person_id} bdrc_id: {contribution.person_bdrc_id}) not found"
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
                    raise DataNotFound("AI contribution creation failed")
            else:
                raise ValueError(f"Unknown contribution type: {type(contribution)}")

        return expression_id

    def _execute_create_manifestation(self, tx, manifestation: ManifestationModelInput, expression_id: str) -> str:
        self.__validator.validate_expression_exists(tx, expression_id)

        manifestation_id = generate_id()

        incipit_element_id = None
        if manifestation.incipit_title:
            alt_incipit_data = (
                [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
            )
            incipit_element_id = self._create_nomens(tx, manifestation.incipit_title.root, alt_incipit_data)

        result = tx.run(
            Queries.manifestations["create"],
            manifestation_id=manifestation_id,
            expression_id=expression_id,
            bdrc=manifestation.bdrc,
            wiki=manifestation.wiki,
            type=manifestation.type.value if manifestation.type else None,
            copyright=manifestation.copyright.value if manifestation.copyright else "public",
            colophon=manifestation.colophon,
            incipit_element_id=incipit_element_id,
        )

        if not result.single():
            raise DataNotFound(f"Expression '{expression_id}' not found")

        return manifestation_id

    def _execute_add_annotation(self, tx, manifestation_id: str, annotation: AnnotationModel) -> str:
        tx.run(
            Queries.annotations["create"],
            manifestation_id=manifestation_id,
            annotation_id=annotation.id,
            type=annotation.type.value,
            aligned_to_id=annotation.aligned_to,
        )
        return annotation.id
