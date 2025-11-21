from exceptions import DataNotFound
from models import AnnotationModel, ExpressionModelInput, ManifestationModelInput, ManifestationModelOutput
from neo4j_database import Neo4JDatabase
from neo4j_queries import Queries

from functions.database.database_validator import DatabaseValidator
from functions.database.nomen_database import NomenDatabase
from functions.database.segment_database import SegmentDatabase

from .annotation_database import AnnotationDatabase
from .data_adapter import DataAdapter
from .expression_database import ExpressionDatabase


class ManifestationDatabase:
    def __init__(self, db: Neo4JDatabase):
        self._db = db

    @property
    def session(self):
        return self._db.get_session()

    def get(self, manifestation_id: str) -> tuple[ManifestationModelOutput, str]:
        with self.session as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch"],
                    manifestation_id=manifestation_id,
                    expression_id=None,
                    manifestation_type=None,
                ).single()
            )
            if record is None:
                raise DataNotFound(f"Manifestation '{manifestation_id}' not found")
            d = record.data()
            return DataAdapter.mainfestation(d["manifestation"]), d["expression_id"]

    def get_by_expression(
        self, expression_id: str, manifestation_type: str | None = None
    ) -> list[ManifestationModelOutput]:
        with self.session as session:
            rows = session.execute_read(
                lambda tx: [
                    r.data()
                    for r in tx.run(
                        Queries.manifestations["fetch"],
                        expression_id=expression_id,
                        manifestation_id=None,
                        manifestation_type=manifestation_type,
                    )
                ]
            )
            return [DataAdapter.mainfestation(row["manifestation"]) for row in rows]

    def get_by_annotation(self, annotation_id: str) -> tuple[ManifestationModelOutput, str] | None:
        with self.session as session:
            record = session.execute_read(
                lambda tx: tx.run(Queries.manifestations["fetch_by_annotation"], annotation_id=annotation_id).single()
            )
            if record is None:
                return None
            d = record.data()
            return DataAdapter.mainfestation(d["manifestation"]), d["expression_id"]

    def get_id_by_annotation(self, annotation_id: str) -> str:
        with self.session as session:
            record = session.execute_read(
                lambda tx: tx.run(
                    Queries.manifestations["fetch_by_annotation_id"], annotation_id=annotation_id
                ).single()
            )
            if record is None:
                return None
            d = record.data()
            return d["manifestation_id"]

    def create(
        self,
        manifestation: ManifestationModelInput,
        expression_id: str,
        manifestation_id: str,
        annotation: AnnotationModel = None,
        annotation_segments: list[dict] = None,
        expression: ExpressionModelInput = None,
        bibliography_annotation: AnnotationModel = None,
        bibliography_segments: list[dict] = None,
    ) -> str:
        def transaction_function(tx):
            if expression:
                ExpressionDatabase.create_with_transaction(tx, expression, expression_id)

            self.create_with_transaction(tx, manifestation, expression_id, manifestation_id)

            if annotation:
                AnnotationDatabase.create_with_transaction(tx, manifestation_id, annotation)
                SegmentDatabase.create_with_transaction(tx, annotation.id, annotation_segments)
                if annotation_segments:
                    if "reference" in annotation_segments[0]:
                        SegmentDatabase.create_and_link_with_transaction(tx, annotation_segments)
                    if "type" in annotation_segments[0]:
                        SegmentDatabase.link_bibliography_type_with_transaction(tx, annotation_segments)

            # Add bibliography annotation in the same transaction
            if bibliography_annotation:
                AnnotationDatabase.create_with_transaction(tx, manifestation_id, bibliography_annotation)
                SegmentDatabase.create_with_transaction(tx, bibliography_annotation.id, bibliography_segments)
                if bibliography_segments:
                    SegmentDatabase.link_bibliography_type_with_transaction(tx, bibliography_segments)

        with self.session as session:
            return session.execute_write(transaction_function)

    @staticmethod
    def create_with_transaction(
        tx, manifestation: ManifestationModelInput, expression_id: str, manifestation_id: str
    ) -> str:
        DatabaseValidator.validate_expression_exists(tx, expression_id)

        incipit_element_id = None
        if manifestation.incipit_title:
            alt_incipit_data = (
                [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
            )
            incipit_element_id = NomenDatabase.create_with_transaction(
                tx, manifestation.incipit_title.root, alt_incipit_data
            )

        result = tx.run(
            Queries.manifestations["create"],
            manifestation_id=manifestation_id,
            expression_id=expression_id,
            bdrc=manifestation.bdrc,
            wiki=manifestation.wiki,
            type=manifestation.type.value if manifestation.type else None,
            source=manifestation.source,
            colophon=manifestation.colophon,
            incipit_element_id=incipit_element_id,
        )

        if not result.single():
            raise DataNotFound(f"Expression '{expression_id}' not found")
