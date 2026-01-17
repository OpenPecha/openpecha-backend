from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from exceptions import DataNotFoundError
from models import (
    ExpressionInput,
    ManifestationInput,
    ManifestationOutput,
    ManifestationType,
    PaginationInput,
    SegmentationInput,
)

from .annotation.alignment_database import AlignmentDatabase
from .annotation.bibliographic_database import BibliographicDatabase
from .annotation.note_database import NoteDatabase
from .annotation.pagination_database import PaginationDatabase
from .annotation.segmentation_database import SegmentationDatabase
from .data_adapter import DataAdapter
from .database_validator import DatabaseValidator, DataValidationError
from .expression_database import ExpressionDatabase
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from neo4j import ManagedTransaction, Session

    from .database import Database


logger = logging.getLogger(__name__)


class ManifestationDatabase:
    DELETE_QUERY = """
    MATCH (m:Manifestation {id: $manifestation_id})
    OPTIONAL MATCH (m)-[:HAS_INCIPIT_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
    OPTIONAL MATCH (n)<-[:ALTERNATIVE_OF]-(alt:Nomen)-[:HAS_LOCALIZATION]->(alt_lt:LocalizedText)
    OPTIONAL MATCH (m)-[:HAS_SOURCE]->(s:Source)
    WITH m, n, lt, alt, alt_lt, s, size([(s)<-[:HAS_SOURCE]-(:Manifestation) | 1]) AS source_refs
    DETACH DELETE m, n, lt, alt, alt_lt
    WITH s, source_refs WHERE s IS NOT NULL AND source_refs <= 1
    DELETE s
    """

    _MANIFESTATION_RETURN = """
    RETURN {
        id: m.id, bdrc: m.bdrc, wiki: m.wiki, colophon: m.colophon,
        source: [(m)-[:HAS_SOURCE]->(s:Source) | s.name][0],
        type: [(m)-[:HAS_TYPE]->(mt:ManifestationType) | mt.name][0],
        incipit_title: [(m)-[:HAS_INCIPIT_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->
            (lt:LocalizedText)-[r:HAS_LANGUAGE]->(l:Language) |
            {language: coalesce(r.bcp47, l.code), text: lt.text}],
        alt_incipit_titles: [(m)-[:HAS_INCIPIT_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-(an:Nomen) |
            [(an)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[r:HAS_LANGUAGE]->(l:Language) |
                {language: coalesce(r.bcp47, l.code), text: lt.text}]],
        expression_id: e.id
    } as manifestation
    """

    GET_QUERY = f"""
    MATCH (m:Manifestation)-[:MANIFESTATION_OF]->(e:Expression)
    WHERE ($manifestation_id IS NOT NULL AND m.id = $manifestation_id)
       OR ($expression_id IS NOT NULL AND e.id = $expression_id)
    WITH m, e
    WHERE $manifestation_type IS NULL
       OR EXISTS {{ (m)-[:HAS_TYPE]->(:ManifestationType {{name: $manifestation_type}}) }}
    {_MANIFESTATION_RETURN}
    """

    GET_RELATED_QUERY = f"""
    // Related via segment alignment (bidirectional)
    MATCH (source:Manifestation {{id: $manifestation_id}})<-[:SEGMENTATION_OF]-(:Segmentation)<-[:SEGMENT_OF]-(:Segment)
          -[:ALIGNED_TO]-(:Segment)-[:SEGMENT_OF]->(:Segmentation)-[:SEGMENTATION_OF]->(m:Manifestation)
          -[:MANIFESTATION_OF]->(e:Expression)
    {_MANIFESTATION_RETURN}

    UNION

    // Related via expression relationships
    MATCH (source:Manifestation {{id: $manifestation_id}})-[:MANIFESTATION_OF]->(:Expression)
          -[:TRANSLATION_OF|:COMMENTARY_OF]-(e:Expression)<-[:MANIFESTATION_OF]-(m:Manifestation)
    WHERE m.id <> $manifestation_id
    {_MANIFESTATION_RETURN}
    """

    CREATE_QUERY = """
    MATCH (e:Expression {id: $expression_id})
    OPTIONAL MATCH (it:Nomen {id: $incipit_nomen_id})
    MERGE (mt:ManifestationType {name: $type})
    CREATE (m:Manifestation {id: $manifestation_id, bdrc: $bdrc, wiki: $wiki, colophon: $colophon})
    WITH m, e, mt, it
    CREATE (m)-[:MANIFESTATION_OF]->(e), (m)-[:HAS_TYPE]->(mt)
    CALL (*) { WHEN it IS NOT NULL THEN { CREATE (m)-[:HAS_INCIPIT_TITLE]->(it) } }
    CALL (*) { WHEN $source IS NOT NULL THEN { MERGE (s:Source {name: $source}) CREATE (m)-[:HAS_SOURCE]->(s) } }
    RETURN m.id AS manifestation_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def get(self, manifestation_id: str) -> ManifestationOutput:
        with self.session as session:
            result = session.run(
                ManifestationDatabase.GET_QUERY,
                manifestation_id=manifestation_id,
                expression_id=None,
                manifestation_type=None,
            ).single()
            if result is None:
                raise DataNotFoundError(f"Manifestation '{manifestation_id}' not found")
            return self._parse_record(result.data())

    def get_all(
        self, expression_id: str, manifestation_type: ManifestationType | None = None
    ) -> list[ManifestationOutput]:
        with self.session as session:
            return session.execute_read(
                lambda tx: ManifestationDatabase.get_all_with_transaction(tx, expression_id, manifestation_type)
            )

    @staticmethod
    def get_all_with_transaction(
        tx: ManagedTransaction, expression_id: str, manifestation_type: ManifestationType | None = None
    ) -> list[ManifestationOutput]:
        result = tx.run(
            ManifestationDatabase.GET_QUERY,
            manifestation_id=None,
            expression_id=expression_id,
            manifestation_type=manifestation_type.value if manifestation_type else None,
        )
        return [ManifestationDatabase._parse_record(r.data()) for r in result]

    def get_related(self, manifestation_id: str) -> list[ManifestationOutput]:
        """Find all manifestations related through alignment or expression relationships."""
        with self.session as session:
            result = session.execute_read(
                lambda tx: list(tx.run(ManifestationDatabase.GET_RELATED_QUERY, manifestation_id=manifestation_id))
            )
            return [self._parse_record(r.data()) for r in result]

    def create(
        self,
        manifestation: ManifestationInput,
        manifestation_id: str,
        expression_id: str,
        expression: ExpressionInput | None = None,
        pagination: PaginationInput | None = None,
        segmentation: SegmentationInput | None = None,
    ) -> str:
        def transaction_function(tx: ManagedTransaction) -> None:
            if expression:
                ExpressionDatabase.create_with_transaction(tx, expression, expression_id)

            self.create_with_transaction(tx, manifestation, expression_id, manifestation_id)

            if segmentation is not None:
                SegmentationDatabase.add_with_transaction(tx, manifestation_id, segmentation)

            if pagination is not None:
                PaginationDatabase.add_with_transaction(tx, manifestation_id, pagination)

        with self.session as session:
            return str(session.execute_write(transaction_function))

    def delete(self, manifestation_id: str) -> None:
        with self.session as session:
            session.execute_write(lambda tx: self.delete_with_transaction(tx, manifestation_id))

    @staticmethod
    def delete_with_transaction(tx: ManagedTransaction, manifestation_id: str) -> None:
        AlignmentDatabase.delete_all_with_transaction(tx, manifestation_id)
        SegmentationDatabase.delete_all_with_transaction(tx, manifestation_id)
        PaginationDatabase.delete_all_with_transaction(tx, manifestation_id)
        BibliographicDatabase.delete_all_with_transaction(tx, manifestation_id)
        NoteDatabase.delete_all_with_transaction(tx, manifestation_id)

        tx.run(ManifestationDatabase.DELETE_QUERY, manifestation_id=manifestation_id)

    def validate_create(
        self,
        manifestation: ManifestationInput,
        expression_id: str,
    ) -> None:
        with self.session as session:
            session.execute_read(lambda tx: self._validate_create(tx, manifestation, expression_id))

    @staticmethod
    def create_with_transaction(
        tx: ManagedTransaction, manifestation: ManifestationInput, expression_id: str, manifestation_id: str
    ) -> str:
        ManifestationDatabase._validate_create(tx, manifestation, expression_id)

        incipit_nomen_id = None
        if manifestation.incipit_title:
            alt_incipit_data = (
                [alt.root for alt in manifestation.alt_incipit_titles] if manifestation.alt_incipit_titles else None
            )
            incipit_nomen_id = NomenDatabase.create_with_transaction(
                tx, manifestation.incipit_title.root, alt_incipit_data
            )

        result = tx.run(
            ManifestationDatabase.CREATE_QUERY,
            manifestation_id=manifestation_id,
            expression_id=expression_id,
            bdrc=manifestation.bdrc,
            wiki=manifestation.wiki,
            type=manifestation.type.value if manifestation.type else None,
            colophon=manifestation.colophon,
            incipit_nomen_id=incipit_nomen_id,
            source=manifestation.source,
        )

        record = result.single()
        if not record:
            raise DataNotFoundError(f"Expression '{expression_id}' not found")

        return manifestation_id

    @staticmethod
    def _validate_create(
        tx: ManagedTransaction,
        manifestation: ManifestationInput,
        expression_id: str,
    ) -> None:
        DatabaseValidator.validate_expression_exists(tx, expression_id)

        if manifestation.type == ManifestationType.CRITICAL:
            ManifestationDatabase._validate_no_critical_exists(tx, expression_id)

    @staticmethod
    def _validate_no_critical_exists(tx: ManagedTransaction, expression_id: str) -> None:
        """Ensure only one critical manifestation exists for an expression."""
        manifestations = ManifestationDatabase.get_all_with_transaction(tx, expression_id, ManifestationType.CRITICAL)
        if manifestations:
            raise DataValidationError("Critical manifestation already present for this expression")

    @staticmethod
    def _parse_record(data: dict, key: str = "manifestation") -> ManifestationOutput:
        return DataAdapter.manifestation(data[key])
