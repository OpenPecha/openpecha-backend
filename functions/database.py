import logging
import os
from typing import Any

from category_model import CategoryModel
from exceptions import DataNotFound, InvalidRequest
from filter_model import AndFilter, Condition, FilterModel, OrFilter
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or
from identifier import generate_id
from metadata_model import MetadataModel, TextType
from metadata_model_v2 import AnnotationModel, ContributionModel, ExpressionModel, ManifestationModel, PersonModel
from neo4j import GraphDatabase
from neo4j_queries import (
    CREATE_LOCALIZED_TEXT_QUERY,
    CREATE_NOMEN_QUERY,
    CREATE_OR_FIND_LANGUAGE_QUERY,
    CREATE_PERSON_QUERY,
    FETCH_ALL_PERSONS_QUERY,
    FETCH_EXPRESSION_QUERY,
    FETCH_MANIFESTATION_QUERY,
    FETCH_PERSON_QUERY,
    LINK_ALTERNATIVE_NOMEN_QUERY,
    LINK_PERSON_TO_NOMEN_QUERY,
)
from openpecha.pecha.annotations import AnnotationModel as OpenPechaAnnotationModel

logger = logging.getLogger(__name__)


class Database:
    __driver: GraphDatabase.driver

    def __init__(self) -> None:
        self.db = firestore.client()
        self.metadata_ref = self.db.collection("metadata")
        self.category_ref = self.db.collection("category")
        self.languages_ref = self.db.collection("language")
        self.annotation_ref = self.db.collection("annotation")

        self.__driver = GraphDatabase.driver(
            "neo4j+s://de3b3d5d.databases.neo4j.io",
            auth=("neo4j", os.environ.get("NEO4J_PASSWORD")),
        )
        with self.__driver as driver:
            driver.verify_connectivity()
            logger.info("Connection to neo4j established.")

    def metadata_exists(self, pecha_id: str) -> bool:
        doc = self.metadata_ref.document(pecha_id).get()
        return doc.exists

    def count_metadata(self) -> int:
        return self.metadata_ref.count().get()[0][0].value

    def get_metadata(self, pecha_id: str) -> MetadataModel:
        doc = self.metadata_ref.document(pecha_id).get()
        if not doc.exists:
            raise DataNotFound(f"Metadata with ID '{pecha_id}' not found")
        return MetadataModel.model_validate(doc.to_dict())

    def get_expression_neo4j(self, expression_id: str) -> MetadataModel:
        with self.__driver.session() as session:
            result = session.run(FETCH_EXPRESSION_QUERY, expressionElementId=expression_id)

            if (record := result.single()) is None:
                raise DataNotFound(f"Expression with ID '{expression_id}' not found")

            expression = record.data()["expression"]

            contributions = [
                ContributionModel(
                    person=PersonModel(
                        name=self.__convert_to_localized_text(contributor["person"]["name"]),
                        alt_names=[
                            self.__convert_to_localized_text(alt) for alt in contributor["person"].get("alt_names", [])
                        ],
                    ),
                    role=contributor["role"],
                )
                for contributor in expression.get("contributors", [])
            ]

            parent = None
            if expression["type"] == "translation":
                # Find first original expression in related array
                related_expressions = expression.get("related", [])
                for related in related_expressions:
                    if related.get("type") == "original":
                        parent = related["id"]
                        break

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
            result = session.run(FETCH_MANIFESTATION_QUERY, manifestationElementId=manifestation_id)

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
            result = session.run(FETCH_ALL_PERSONS_QUERY)
            persons = []
            for record in result:
                person_data = record.data()["person"]
                persons.append(
                    PersonModel(
                        id=person_data["id"],
                        name=self.__convert_to_localized_text(person_data["name"]),
                        alt_names=[self.__convert_to_localized_text(alt) for alt in person_data["alt_names"]],
                    )
                )
            return persons

    def get_person_neo4j(self, person_id: str) -> PersonModel:
        with self.__driver.session() as session:
            result = session.run(FETCH_PERSON_QUERY, personId=person_id)
            if (record := result.single()) is None:
                raise DataNotFound(f"Person with ID '{person_id}' not found")

            person_data = record.data()["person"]
            return PersonModel(
                id=person_data["id"],
                name=self.__convert_to_localized_text(person_data["name"]),
                alt_names=[self.__convert_to_localized_text(alt) for alt in person_data["alt_names"]],
            )

    def create_person_neo4j(self, person: PersonModel) -> str:
        def create_transaction(tx):
            person_id = generate_id()
            tx.run(CREATE_PERSON_QUERY, person_id=person_id)
            primary_name_id = self._create_name(tx, person.name.root)

            tx.run(LINK_PERSON_TO_NOMEN_QUERY, person_id=person_id, primary_name_id=primary_name_id)

            for alt_name in person.alt_names or []:
                alt_name_id = self._create_name(tx, alt_name.root)
                tx.run(LINK_ALTERNATIVE_NOMEN_QUERY, primary_name_id=primary_name_id, alt_name_id=alt_name_id)

            return person_id

        with self.__driver.session() as session:
            return session.execute_write(create_transaction)

    def _create_name(self, tx, localized_text: dict[str, str]) -> str:
        nomen_id = generate_id()
        tx.run(CREATE_NOMEN_QUERY, nomen_id=nomen_id)

        for bcp47_tag, text in localized_text.items():
            # Extract base language code (everything before first '-')
            base_lang_code = bcp47_tag.split('-')[0].lower()

            # Ensure Language node exists
            tx.run(CREATE_OR_FIND_LANGUAGE_QUERY, lang_code=base_lang_code)

            # Create LocalizedText with HAS_LANGUAGE relationship storing full BCP47 tag
            tx.run(CREATE_LOCALIZED_TEXT_QUERY,
                   nomen_id=nomen_id,
                   base_lang_code=base_lang_code,
                   bcp47_tag=bcp47_tag,
                   text=text)

        return nomen_id

    def get_metadata_by_field(self, field: str, value: Any) -> dict[str, MetadataModel]:
        query = self.metadata_ref.where(filter=FieldFilter(field, "==", value))
        docs = query.stream()
        return {doc.id: MetadataModel.model_validate(doc.to_dict()) for doc in docs}

    def set_metadata(self, pecha_id: str, metadata: MetadataModel) -> None:
        self.metadata_ref.document(pecha_id).set(metadata.model_dump())

    def update_metadata(self, pecha_id: str, fields: dict[str, Any]) -> None:
        self.metadata_ref.document(pecha_id).update(fields)

    def delete_metadata(self, pecha_id: str) -> None:
        self.metadata_ref.document(pecha_id).delete()

    def get_children_metadata(self, pecha_id: str, relationships: list[TextType]) -> dict[str, MetadataModel]:
        ref_fields = [r.value for r in relationships]

        docs = self.metadata_ref.where(filter=Or([FieldFilter(f, "==", pecha_id) for f in ref_fields])).stream()

        return {doc.id: MetadataModel.model_validate(doc.to_dict()) for doc in docs}

    def filter_metadata(
        self, filter_model: FilterModel | None, offset: int = 0, limit: int = 20
    ) -> dict[str, MetadataModel]:
        query = self.metadata_ref

        if filter_model is not None:
            if not (f := filter_model.root):
                raise InvalidRequest("Invalid filters provided")

            if isinstance(f, OrFilter):
                query = query.where(filter=Or([FieldFilter(c.field, c.operator, c.value) for c in f.conditions]))
            elif isinstance(f, AndFilter):
                for c in f.conditions:
                    query = query.where(filter=FieldFilter(c.field, c.operator, c.value))
            elif isinstance(f, Condition):
                query = query.where(filter=FieldFilter(f.field, f.operator, f.value))
            else:
                raise InvalidRequest("No valid filters provided")

        query = query.limit(limit).offset(offset)

        results = {}
        for doc in query.stream():
            results[doc.id] = MetadataModel.model_validate(doc.to_dict())

        return results

    def category_exists(self, category_id: str) -> bool:
        doc = self.category_ref.document(category_id).get()
        return doc.exists

    def delete_all_categories(self) -> None:
        docs = self.category_ref.stream()
        for doc in docs:
            self.category_ref.document(doc.id).delete()

    def get_category(self, category_id: str) -> CategoryModel:
        doc = self.category_ref.document(category_id).get()
        if not doc.exists:
            raise DataNotFound(f"Category with ID '{category_id}' not found")
        return CategoryModel.model_validate(doc.to_dict())

    def set_category(self, category_id: str, category: CategoryModel) -> None:
        doc_ref = self.category_ref.document(category_id)
        doc_ref.set(category.model_dump())

    def get_all_categories(self) -> dict[str, dict[str, Any]]:
        return {doc.id: doc.to_dict() for doc in self.category_ref.stream()}

    def get_languages(self) -> list[dict[str, str]]:
        languages_ref = self.languages_ref.stream()
        return [{"code": doc.id, "name": doc.to_dict().get("name")} for doc in languages_ref]

    def get_annotation(self, annotation_id: str) -> OpenPechaAnnotationModel:
        doc = self.annotation_ref.document(annotation_id).get()
        if not doc.exists:
            raise DataNotFound(f"Annotation with ID '{annotation_id}' not found")
        return OpenPechaAnnotationModel.model_validate(doc.to_dict())

    def get_annotation_by_field(self, field: str, value: Any) -> dict[str, OpenPechaAnnotationModel]:
        query = self.annotation_ref.where(filter=FieldFilter(field, "==", value))
        docs = query.stream()
        return {doc.id: OpenPechaAnnotationModel.model_validate(doc.to_dict()) for doc in docs}

    def add_annotation(self, annotation: OpenPechaAnnotationModel) -> str:
        doc_ref = self.annotation_ref.add(annotation.model_dump())
        return doc_ref[1].id

    def __convert_to_localized_text(self, entries: list[dict[str, str]]) -> dict[str, str]:
        return {entry["language"]: entry["text"] for entry in entries if "language" in entry and "text" in entry}
