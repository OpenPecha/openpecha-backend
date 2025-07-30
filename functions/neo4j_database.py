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
                "neo4j+s://de3b3d5d.databases.neo4j.io",
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

    def _create_person_model(self, person_data, person_id=None):
        """Helper to create PersonModel from Neo4j person data"""
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

    def get_expression_neo4j(self, expression_id: str) -> ExpressionModel:
        with self.__driver.session() as session:
            result = session.run(Queries.expressions["fetch_by_id"], id=expression_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")

            expression = record.data()["expression"]

            contributions = [
                ContributionModel(
                    person=self._create_person_model(contributor["person"]),
                    role=contributor["role"],
                )
                for contributor in expression.get("contributors", [])
            ]

            parent = None
            # TODO
            # if expression["type"] == "translation":
            #     related_expressions = expression.get("related", [])
            #     for related in related_expressions:
            #         if related.get("type") == "original":
            #             parent = related["id"]
            #             break

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

    def get_manifestation_neo4j(self, manifestation_id: str) -> ManifestationModel:
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

    def get_all_persons_neo4j(self) -> list[PersonModel]:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_all"])
            persons = []
            for record in result:
                person_data = record.data()["person"]
                persons.append(self._create_person_model(person_data))
            return persons

    def get_person_neo4j(self, person_id: str) -> PersonModel:
        with self.__driver.session() as session:
            result = session.run(Queries.persons["fetch_by_id"], id=person_id)
            record = result.single()
            if not record:
                raise DataNotFound(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            return self._create_person_model(person_data)

    def create_person_neo4j(self, person: PersonModel) -> str:
        def create_transaction(tx):
            person_id = generate_id()
            tx.run(Queries.persons["create"], id=person_id, bdrc=person.bdrc, wiki=person.wiki)
            primary_name_id = self._create_name(tx, person.name.root)

            tx.run(Queries.nomens["link_to_person"], person_id=person_id, primary_name_id=primary_name_id)

            for alt_name in person.alt_names or []:
                alt_name_id = self._create_name(tx, alt_name.root)
                tx.run(Queries.nomens["link_alternative"], primary_name_id=primary_name_id, alt_name_id=alt_name_id)

            return person_id

        with self.__driver.session() as session:
            return session.execute_write(create_transaction)

    def _create_name(self, tx, localized_text: dict[str, str]) -> str:
        nomen_id = generate_id()
        tx.run(Queries.nomens["create"], id=nomen_id)

        for bcp47_tag, text in localized_text.items():
            base_lang_code = bcp47_tag.split("-")[0].lower()

            tx.run(Queries.languages["create_or_find"], lang_code=base_lang_code)

            tx.run(
                Queries.nomens["create_localized_text"],
                id=nomen_id,
                base_lang_code=base_lang_code,
                bcp47_tag=bcp47_tag,
                text=text,
            )

        return nomen_id

    def get_all_expressions_neo4j(
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
                        person=PersonModel(
                            id=contributor["person"]["id"],
                            name=LocalizedString(self.__convert_to_localized_text(contributor["person"]["name"])),
                            alt_names=[
                                LocalizedString(self.__convert_to_localized_text(alt))
                                for alt in contributor["person"].get("alt_names", [])
                            ],
                        ),
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
