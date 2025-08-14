import logging
import os

from exceptions import DataNotFound
from identifier import generate_id
from metadata_model_v2 import (
    AnnotationModel,
    AnnotationType,
    ContributionModel,
    CopyrightStatus,
    ExpressionModel,
    LocalizedString,
    ManifestationModel,
    ManifestationType,
    PersonModel,
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

    def get_session(self):
        return self.__driver.session()

    def close_driver(self):
        if self.__driver:
            self.__driver.close()

    def get_expression(self, expression_id: str) -> ExpressionModel:
        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_by_id"], id=expression_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")

            expression = record.data()["expression"]

            contributions = [
                ContributionModel(
                    person_id=contributor.get("person_id"),
                    person_bdrc_id=contributor.get("person_bdrc_id"),
                    role=contributor.get("role"),
                )
                for contributor in expression.get("contributors", [])
            ]

            return ExpressionModel(
                id=expression.get("id"),
                bdrc=expression.get("bdrc"),
                wiki=expression.get("wiki"),
                type=TextType(expression.get("type")),
                contributions=contributions,
                date=expression.get("date"),
                title=self.__convert_to_localized_text(expression.get("title")),
                alt_titles=[self.__convert_to_localized_text(alt) for alt in expression.get("alt_titles")],
                language=expression.get("language"),
                parent=expression.get("parent"),
            )

    def _process_manifestation_data(self, manifestation_data: dict) -> ManifestationModel:
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

        return ManifestationModel(
            id=manifestation_data["id"],
            bdrc=manifestation_data.get("bdrc"),
            wiki=manifestation_data.get("wiki"),
            type=ManifestationType(manifestation_data["type"]),
            annotations=annotations,
            copyright=CopyrightStatus(manifestation_data["copyright"]),
            colophon=manifestation_data.get("colophon"),
            incipit_title=incipit_title,
            alt_incipit_titles=alt_incipit_titles,
        )

    def get_manifestations_by_expression(self, expression_id: str) -> list[ManifestationModel]:
        with self.__driver.session() as session:
            result = session.run(Queries.manifestations["fetch"], expression_id=expression_id, manifestation_id=None)
            manifestations = []

            for record in result:
                manifestation_data = record.data()["manifestation"]
                manifestation = self._process_manifestation_data(manifestation_data)
                manifestations.append(manifestation)

            return manifestations

    def get_manifestation(self, manifestation_id: str) -> tuple[ManifestationModel, str]:
        with self.__driver.session() as session:
            result = session.run(Queries.manifestations["fetch"], manifestation_id=manifestation_id, expression_id=None)
            record = result.single()
            if record is None:
                raise DataNotFound(f"Manifestation '{manifestation_id}' not found")

            record_data = record.data()
            manifestation_data = record_data["manifestation"]
            expression_id = record_data["expression_id"]
            manifestation = self._process_manifestation_data(manifestation_data)
            return manifestation, expression_id

    def create_manifestation(self, manifestation: ManifestationModel, expression_id: str) -> str:
        def create_transaction(tx):
            self.__validator.validate_expression_exists(tx, expression_id)

            manifestation_id = generate_id()

            title_nomen_element_id = None
            if manifestation.incipit_title:
                alt_titles_data = (
                    [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
                )
                title_nomen_element_id = self._create_nomens(tx, manifestation.incipit_title.root, alt_titles_data)

            result = tx.run(
                Queries.manifestations["create"],
                manifestation_id=manifestation_id,
                expression_id=expression_id,
                bdrc=manifestation.bdrc,
                wiki=manifestation.wiki,
                type=manifestation.type.value if manifestation.type else None,
                copyright=manifestation.copyright.value if manifestation.copyright else "public",
                colophon=manifestation.colophon,
                title_nomen_element_id=title_nomen_element_id,
            )

            if not result.single():
                raise DataNotFound(f"Expression '{expression_id}' not found")

            for annotation in manifestation.annotations or []:
                annotation_id = generate_id()
                tx.run(
                    Queries.annotations["create"],
                    annotation_id=annotation_id,
                    manifestation_id=manifestation_id,
                    type=annotation.type.value if annotation.type else None,
                    aligned_to_id=annotation.aligned_to,
                )

            return manifestation_id

        with self.__driver.session() as session:
            return session.execute_write(create_transaction)

    def get_all_persons(self) -> list[PersonModel]:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_all"])
            persons = []
            for record in result:
                person_data = record.data()["person"]
                persons.append(self._create_person_model(person_data))
            return persons

    def get_person(self, person_id: str) -> PersonModel:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_by_id"], id=person_id)
            record = result.single()
            if not record:
                raise DataNotFound(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            return self._create_person_model(person_data)

    def create_person(self, person: PersonModel) -> str:
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

    def create_expression(self, expression: ExpressionModel) -> str:
        if expression.parent:
            parent_expression = self.get_expression(expression.parent)
            if expression.type == TextType.TRANSLATION:
                if parent_expression.language == expression.language:
                    raise ValueError("Translation must have a different language than the parent expression")

        def create_transaction(tx):
            work_id = generate_id() if expression.type == TextType.ROOT else None

            self.__validator.validate_expression_creation(tx, expression, work_id)
            expression_id = generate_id()
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
                "parent_id": expression.parent,
            }

            match expression.type:
                case TextType.ROOT:
                    tx.run(Queries.expressions["create_root"], work_id=work_id, **common_params)
                case TextType.TRANSLATION:
                    tx.run(Queries.expressions["create_translation"], **common_params)
                case TextType.COMMENTARY:
                    tx.run(Queries.expressions["create_commentary"], work_id=generate_id(), **common_params)

            for contribution in expression.contributions:
                person_link_result = tx.run(
                    Queries.expressions["create_contribution"],
                    expression_id=expression_id,
                    person_id=contribution.person_id,
                    person_bdrc_id=contribution.person_bdrc_id,
                    role_name=contribution.role.value,
                )
                if not person_link_result.single():
                    raise DataNotFound(
                        f"Person (id: {contribution.person_id} bdrc_id: {contribution.person_bdrc_id}) not found"
                    )

            return expression_id

        with self.__driver.session() as session:
            return session.execute_write(create_transaction)

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
    ) -> list[ExpressionModel]:
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

                contributions = [
                    ContributionModel(
                        person_id=contributor["person_id"],
                        person_bdrc_id=contributor.get("person_bdrc_id"),
                        role=contributor["role"],
                    )
                    for contributor in expression_data.get("contributors", [])
                ]

                parent = expression_data["parent"]

                expression_type = expression_data["type"]
                if expression_type is None:
                    # Skip expressions that don't have a valid type
                    continue

                expression = ExpressionModel(
                    id=expression_data["id"],
                    bdrc=expression_data["bdrc"],
                    wiki=expression_data["wiki"],
                    type=TextType(expression_type),
                    contributions=contributions,
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

    def _create_person_model(self, person_data, person_id=None):
        return PersonModel(
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
