import logging
import os

from exceptions import DataNotFound
from identifier import generate_id
from metadata_model_v2 import (
    AnnotationModel,
    ContributionModel,
    ExpressionModel,
    LocalizedString,
    ManifestationModel,
    PersonModel,
    TextType,
)
from neo4j import GraphDatabase
from neo4j_queries import Queries

logger = logging.getLogger(__name__)


class Neo4JDatabase:
    """Neo4j database operations for OpenPecha backend"""

    def __init__(self, neo4j_uri: str = None, neo4j_auth: tuple = None) -> None:
        # Use provided Neo4j connection or default to production
        if neo4j_uri and neo4j_auth:
            self.__driver = GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
        else:
            # Production Neo4j connection
            self.__driver = GraphDatabase.driver(
                os.environ.get("NEO4J_URI"),
                auth=("neo4j", os.environ.get("NEO4J_PASSWORD")),
            )
        self.__driver.verify_connectivity()
        logger.info("Connection to neo4j established.")

    def get_session(self):
        """Get a Neo4j session for direct database operations (primarily for testing)"""
        return self.__driver.session()

    def close_driver(self):
        """Close the Neo4j driver (primarily for testing cleanup)"""
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
                    person_id=contributor["person_id"],
                    person_bdrc_id=contributor.get("person_bdrc_id"),
                    role=contributor["role"],
                )
                for contributor in expression.get("contributors", [])
            ]

            parent = expression.get("parent")

            return ExpressionModel(
                id=expression["id"],
                bdrc=expression["bdrc"],
                wiki=expression["wiki"],
                type=expression["type"],
                contributions=contributions,
                date=expression["date"],
                title=self.__convert_to_localized_text(expression["title"]),
                alt_titles=[self.__convert_to_localized_text(alt) for alt in expression["alt_titles"]],
                language=expression["language"],
                parent=parent,
            )

    def get_manifestation(self, manifestation_id: str) -> ManifestationModel:
        with self.__driver.session() as session:
            result = session.run(Queries.manifestations["fetch_by_id"], id=manifestation_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Manifestation with ID '{manifestation_id}' not found")

            manifestation = record.data()["manifestation"]

            annotations = [
                AnnotationModel.model_validate(annotation_data)
                for annotation_data in manifestation.get("annotations", [])
            ]

            incipit_title = None
            if manifestation.get("incipit_title"):
                incipit_title = self.__convert_to_localized_text(manifestation["incipit_title"])

            alt_incipit_titles = None
            if manifestation.get("alt_incipit_titles"):
                alt_incipit_titles = [
                    self.__convert_to_localized_text(alt) for alt in manifestation["alt_incipit_titles"]
                ]

            return ManifestationModel(
                id=manifestation["id"],
                bdrc=manifestation["bdrc"],
                type=manifestation["type"],
                manifestation_of=manifestation["manifestation_of"],
                annotations=annotations,
                copyright=manifestation["copyright"],
                colophon=manifestation.get("colophon", ""),
                incipit_title=incipit_title,
                alt_incipit_titles=alt_incipit_titles,
            )

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
        def create_transaction(tx):
            expression_id = generate_id()
            base_lang_code = expression.language.split("-")[0].lower()
            alt_titles_data = [alt_title.root for alt_title in expression.alt_titles] if expression.alt_titles else None
            expression_title_element_id = self._create_nomens(tx, expression.title.root, alt_titles_data)

            # Choose query based on expression type
            if expression.type == TextType.ROOT:
                work_id = generate_id()
                tx.run(
                    Queries.expressions["create_root"],
                    work_id=work_id,
                    expression_id=expression_id,
                    bdrc=expression.bdrc,
                    wiki=expression.wiki,
                    date=expression.date,
                    language_code=base_lang_code,
                    bcp47_tag=expression.language,
                    title_nomen_element_id=expression_title_element_id,
                )
            elif expression.type == TextType.TRANSLATION:
                tx.run(
                    Queries.expressions["create_translation"],
                    parent_id=expression.parent,
                    expression_id=expression_id,
                    bdrc=expression.bdrc,
                    wiki=expression.wiki,
                    date=expression.date,
                    language_code=base_lang_code,
                    bcp47_tag=expression.language,
                    title_nomen_element_id=expression_title_element_id,
                )
            else:  # TextType.COMMENTARY
                work_id = generate_id()
                tx.run(
                    Queries.expressions["create_commentary"],
                    work_id=work_id,
                    parent_id=expression.parent,
                    expression_id=expression_id,
                    bdrc=expression.bdrc,
                    wiki=expression.wiki,
                    date=expression.date,
                    language_code=base_lang_code,
                    bcp47_tag=expression.language,
                    title_nomen_element_id=expression_title_element_id,
                )

            for contribution in expression.contributions:
                person_link_result = tx.run(
                    Queries.expressions["create_contribution"],
                    expression_id=expression_id,
                    person_id=contribution.person_id,
                    person_bdrc_id=contribution.person_bdrc_id,
                    role_name=contribution.role.value,
                )
                if not person_link_result.single():
                    if contribution.person_id:
                        raise DataNotFound(f"Person with ID '{contribution.person_id}' not found")
                    else:
                        raise DataNotFound(f"Person with BDRC ID '{contribution.person_bdrc_id}' not found")

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

                parent = None
                # TODO: this should get the parent (root, in case of commentary, and original in case of translation)
                # if expression_data["type"] == "translation":
                #     related_expressions = expression_data.get("related", [])
                #     for related in related_expressions:
                #         if related.get("type") == "original":
                #             parent = related["id"]
                #             break

                expression = ExpressionModel(
                    id=expression_data["id"],
                    bdrc=expression_data["bdrc"],
                    wiki=expression_data["wiki"],
                    type=expression_data["type"],
                    contributions=contributions,
                    date=expression_data["date"],
                    title=self.__convert_to_localized_text(expression_data["title"]),
                    alt_titles=[self.__convert_to_localized_text(alt) for alt in expression_data["alt_titles"]],
                    language=expression_data["language"],
                    parent=parent,
                )
                expressions.append(expression)

            return expressions

    def __convert_to_localized_text(self, entries: list[dict[str, str]]) -> dict[str, str]:
        return {entry["language"]: entry["text"] for entry in entries if "language" in entry and "text" in entry}

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
