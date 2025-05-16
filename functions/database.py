from typing import Any

from category_model import CategoryModel
from exceptions import DataNotFound, InvalidRequest
from filter_model import AndFilter, Condition, FilterModel, OrFilter
from firebase_admin import firestore
from google.cloud.firestore_v1.base_query import FieldFilter, Or
from metadata_model import MetadataModel, Relationship
from openpecha.pecha.annotations import AnnotationModel


class Database:
    def __init__(self):
        self.db = firestore.client()
        self.metadata_ref = self.db.collection("metadata")
        self.category_ref = self.db.collection("category")
        self.languages_ref = self.db.collection("language")
        self.annotation_ref = self.db.collection("annotation")

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

    def get_metadata_by_field(self, field: str, value: Any) -> dict[str, MetadataModel]:
        query = self.metadata_ref.where(filter=FieldFilter(field, "==", value))
        docs = query.stream()
        return {doc.id: MetadataModel.model_validate(doc.to_dict()) for doc in docs}

    def set_metadata(self, pecha_id: str, metadata: MetadataModel):
        self.metadata_ref.document(pecha_id).set(metadata.model_dump())

    def update_metadata(self, pecha_id: str, fields: dict[str, Any]):
        self.metadata_ref.document(pecha_id).update(fields)

    def delete_metadata(self, pecha_id: str):
        self.metadata_ref.document(pecha_id).delete()

    def get_children_metadata(self, pecha_id: str, relationships: list[Relationship]) -> dict[str, MetadataModel]:
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

    def category_exists(self, category_id: str):
        doc = self.category_ref.document(category_id).get()
        return doc.exists

    def delete_all_categories(self):
        docs = self.category_ref.stream()
        for doc in docs:
            doc.reference.delete()
        
    def get_category(self, category_id: str) -> CategoryModel:
        doc = self.category_ref.document(category_id).get()
        if not doc.exists:
            raise DataNotFound(f"Category with ID '{category_id}' not found")
        return CategoryModel.model_validate(doc.to_dict())

    def set_category(self, category_id: str, category: CategoryModel):
        doc_ref = self.category_ref.document(category_id)
        doc = doc_ref.get()
        if not doc.exists:
            raise DataNotFound(f"Category with ID '{category_id}' not found")

        doc_ref.set(category.model_dump())

    def get_all_categories(self) -> dict[str, dict[str, Any]]:
        return {doc.id: doc.to_dict() for doc in self.category_ref.stream()}

    def get_languages(self) -> list[dict[str, str]]:
        languages_ref = self.languages_ref.stream()
        return [{"code": doc.id, "name": doc.to_dict().get("name")} for doc in languages_ref]

    def get_annotation(self, annotation_id: str) -> AnnotationModel:
        doc = self.annotation_ref.document(annotation_id).get()
        if not doc.exists:
            raise DataNotFound(f"Annotation with ID '{annotation_id}' not found")
        return AnnotationModel.model_validate(doc.to_dict())

    def get_annotation_by_field(self, field: str, value: Any) -> dict[str, AnnotationModel]:
        query = self.annotation_ref.where(filter=FieldFilter(field, "==", value))
        docs = query.stream()
        return {doc.id: AnnotationModel.model_validate(doc.to_dict()) for doc in docs}

    def add_annotation(self, annotation: AnnotationModel) -> str:
        doc_ref = self.annotation_ref.add(annotation.model_dump())
        return doc_ref[1].id
