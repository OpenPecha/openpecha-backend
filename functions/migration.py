"""Database migration script for OpenPecha backend."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING

from dotenv import load_dotenv
from identifier import generate_id
from models import AttributeType, LicenseType, NoteType

if TYPE_CHECKING:
    from neo4j import Session

logger = logging.getLogger(__name__)
load_dotenv()


def migrate_work_category_relationship_rename(session: Session) -> int:
    """Rename (Work)-[:BELONGS_TO]->(Category) to (Work)-[:HAS_CATEGORY]->(Category)."""
    result = session.run("""
        MATCH (w:Work)-[old:BELONGS_TO]->(c:Category)
        CREATE (w)-[:HAS_CATEGORY]->(c)
        DELETE old
        RETURN count(*) AS count
    """).single()
    count = result["count"] if result else 0
    logger.info("Renamed %d Work→Category relationships to HAS_CATEGORY", count)
    return count


def migrate_category_application_extraction(session: Session) -> dict[str, int]:
    """Extract 'application' property from Category nodes into Application nodes with BELONGS_TO."""
    result = session.run("""
        MATCH (c:Category)
        WHERE c.application IS NOT NULL
        AND NOT EXISTS { (c)-[:BELONGS_TO]->(:Application) }
        WITH c.application AS app_id, collect(c) AS categories
        MERGE (a:Application {id: app_id})
        ON CREATE SET a.name = app_id
        WITH a, categories
        UNWIND categories AS c
        CREATE (c)-[:BELONGS_TO]->(a)
        REMOVE c.application
        RETURN count(DISTINCT a) AS apps_created, count(c) AS rels_created
    """).single()

    stats = {"apps_created": 0, "rels_created": 0}
    if result:
        stats["apps_created"] = result["apps_created"]
        stats["rels_created"] = result["rels_created"]
    logger.info(
        "Created %d Application nodes, %d BELONGS_TO relationships", stats["apps_created"], stats["rels_created"]
    )
    return stats


def migrate_nomen_add_ids(session: Session, batch_size: int = 1000) -> int:
    """Add ID field to all Nomen nodes that don't have one."""
    total = 0
    while True:
        # Get a batch of element IDs
        result = session.run(
            "MATCH (n:Nomen) WHERE n.id IS NULL RETURN elementId(n) AS eid LIMIT $batch_size",
            batch_size=batch_size,
        )
        batch = [(record["eid"], generate_id()) for record in result]
        if not batch:
            break

        # Update batch in single query using UNWIND
        session.run(
            """
            UNWIND $updates AS update
            MATCH (n:Nomen) WHERE elementId(n) = update[0]
            SET n.id = update[1]
            """,
            updates=batch,
        )
        total += len(batch)
        logger.info("Processed batch of %d Nomen nodes (total: %d)", len(batch), total)

    logger.info("Added ID to %d Nomen nodes", total)
    return total


def migrate_create_license_types(session: Session) -> int:
    """Create LicenseType enum nodes."""
    result = session.run(
        """
        UNWIND $names AS name
        MERGE (lt:LicenseType {name: name})
        RETURN count(*) AS count
        """,
        names=[lt.value for lt in LicenseType],
    ).single()
    count = result["count"] if result else 0
    logger.info("Created/verified %d LicenseType nodes", count)
    return count


def migrate_copyright_to_license(session: Session) -> dict[str, int]:
    """Convert HAS_COPYRIGHT→Copyright to HAS_LICENSE→LicenseType based on Copyright.status."""
    result = session.run("""
        MATCH (e:Expression)-[old:HAS_COPYRIGHT]->(c:Copyright)
        WITH e, c, old,
             CASE c.status
                 WHEN 'Public domain' THEN 'public'
                 WHEN 'Unknown' THEN 'unknown'
                 ELSE 'unknown'
             END AS license_name
        MATCH (lt:LicenseType {name: license_name})
        MERGE (e)-[:HAS_LICENSE]->(lt)
        DELETE old
        WITH c
        WHERE NOT EXISTS { ()-[:HAS_COPYRIGHT]->(c) }
        DELETE c
        RETURN count(DISTINCT c) AS count
    """).single()

    stats = {"expressions_updated": result["count"] if result else 0}
    logger.info("Converted %d Expression HAS_COPYRIGHT to HAS_LICENSE", stats["expressions_updated"])
    return stats


def migrate_license_to_license_type(session: Session) -> dict[str, int]:
    """Convert HAS_LICENSE→License to HAS_LICENSE→LicenseType with name mapping."""
    result = session.run("""
        MATCH (e:Expression)-[old:HAS_LICENSE]->(l:License)
        WITH e, l, old,
             CASE l.name
                 WHEN 'CC0' THEN 'cc0'
                 WHEN 'Public Domain Mark' THEN 'public'
                 WHEN 'CC BY' THEN 'cc-by'
                 WHEN 'CC BY-SA' THEN 'cc-by-sa'
                 WHEN 'CC BY-ND' THEN 'cc-by-nd'
                 WHEN 'CC BY-NC' THEN 'cc-by-nc'
                 WHEN 'CC BY-NC-SA' THEN 'cc-by-nc-sa'
                 WHEN 'CC BY-NC-ND' THEN 'cc-by-nc-nd'
                 WHEN 'unknown' THEN 'unknown'
                 ELSE 'unknown'
             END AS license_type_name
        MATCH (lt:LicenseType {name: license_type_name})
        MERGE (e)-[:HAS_LICENSE]->(lt)
        DELETE old
        WITH l
        WHERE NOT EXISTS { ()-[:HAS_LICENSE]->(l) }
        DELETE l
        RETURN count(DISTINCT l) AS count
    """).single()

    stats = {"expressions_updated": result["count"] if result else 0}
    logger.info("Converted %d Expression HAS_LICENSE→License to HAS_LICENSE→LicenseType", stats["expressions_updated"])
    return stats


def migrate_create_note_types(session: Session) -> int:
    """Create NoteType enum nodes."""
    result = session.run(
        """
        UNWIND $names AS name
        MERGE (nt:NoteType {name: name})
        RETURN count(*) AS count
        """,
        names=[nt.value for nt in NoteType],
    ).single()
    count = result["count"] if result else 0
    logger.info("Created/verified %d NoteType nodes", count)
    return count


def migrate_create_attribute_types(session: Session) -> int:
    """Create AttributeType enum nodes."""
    result = session.run(
        """
        UNWIND $names AS name
        MERGE (at:AttributeType {name: name})
        RETURN count(*) AS count
        """,
        names=[at.value for at in AttributeType],
    ).single()
    count = result["count"] if result else 0
    logger.info("Created/verified %d AttributeType nodes", count)
    return count


def migrate_durchen_annotations_to_notes(session: Session, batch_size: int = 500) -> dict[str, int]:
    """Convert durchen Segment+DurchenNote to Note with NOTE_OF→Manifestation, HAS_TYPE→NoteType(durchen), Span→Note."""
    total_notes = 0
    total_spans = 0

    while True:
        result = session.run(
            """
            MATCH (s:Segment)-[:HAS_DURCHEN_NOTE]->(dn:DurchenNote)
            RETURN elementId(s) AS eid, s.span_start IS NOT NULL AS has_span
            LIMIT $batch_size
            """,
            batch_size=batch_size,
        )
        records = list(result)
        if not records:
            break

        logger.info("Processing batch of %d durchen notes...", len(records))
        ids = [generate_id() for _ in records]
        session.run(
            """
            UNWIND $data AS row
            MATCH (s:Segment)-[r:HAS_DURCHEN_NOTE]->(dn:DurchenNote) WHERE elementId(s) = row.eid
            MATCH (s)-[:SEGMENTATION_OF]->(a:Annotation)-[:ANNOTATION_OF]->(m:Manifestation)
            MATCH (nt:NoteType {name: 'durchen'})
            CREATE (n:Note {id: row.id, text: dn.note})-[:NOTE_OF]->(m)
            CREATE (n)-[:HAS_TYPE]->(nt)
            FOREACH (_ IN CASE WHEN s.span_start IS NOT NULL THEN [1] ELSE [] END |
                CREATE (:Span {start: s.span_start, end: s.span_end})-[:SPAN_OF]->(n)
            )
            DELETE r, dn
            DETACH DELETE s
            """,
            data=[{"eid": r["eid"], "id": ids[i]} for i, r in enumerate(records)],
        )
        total_notes += len(records)
        total_spans += sum(1 for r in records if r["has_span"])
        logger.info("Batch complete. Total: %d notes, %d spans", total_notes, total_spans)

    # Clean up orphaned Annotations separately
    orphan_result = session.run("""
        MATCH (a:Annotation)
        WHERE NOT EXISTS { (:Segment)-[:SEGMENTATION_OF]->(a) }
        DETACH DELETE a
        RETURN count(a) AS deleted
    """).single()
    orphans_deleted = orphan_result["deleted"] if orphan_result else 0
    logger.info("Deleted %d orphaned Annotation nodes", orphans_deleted)

    logger.info("Created %d Notes and %d Spans from durchen annotations", total_notes, total_spans)
    return {"notes_created": total_notes, "spans_created": total_spans}


def migrate_bibliography_annotations(session: Session, batch_size: int = 500) -> dict[str, int]:
    """Convert bibliography Annotation+Segment to BibliographicMetadata with BIBLIOGRAPHY_OF→Manifestation."""
    total_bib = 0
    total_spans = 0

    while True:
        result = session.run(
            """
            MATCH (s:Segment)-[:SEGMENTATION_OF]->(a:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'bibliography'})
            MATCH (s)-[:HAS_TYPE]->(bt:BibliographyType)
            RETURN elementId(s) AS eid, s.span_start IS NOT NULL AS has_span, bt.name AS bib_type
            LIMIT $batch_size
            """,
            batch_size=batch_size,
        )
        records = list(result)
        if not records:
            break

        logger.info("Processing batch of %d bibliography segments...", len(records))
        ids = [generate_id() for _ in records]
        session.run(
            """
            UNWIND $data AS row
            MATCH (s:Segment)-[:SEGMENTATION_OF]->(a:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'bibliography'})
            WHERE elementId(s) = row.eid
            MATCH (a)-[:ANNOTATION_OF]->(m:Manifestation)
            MATCH (bt:BibliographyType {name: row.bib_type})
            CREATE (bm:BibliographicMetadata {id: row.id})-[:BIBLIOGRAPHY_OF]->(m)
            CREATE (bm)-[:HAS_TYPE]->(bt)
            FOREACH (_ IN CASE WHEN s.span_start IS NOT NULL THEN [1] ELSE [] END |
                CREATE (:Span {start: s.span_start, end: s.span_end})-[:SPAN_OF]->(bm)
            )
            DETACH DELETE s
            """,
            data=[{"eid": r["eid"], "id": ids[i], "bib_type": r["bib_type"]} for i, r in enumerate(records)],
        )
        total_bib += len(records)
        total_spans += sum(1 for r in records if r["has_span"])
        logger.info("Batch complete. Total: %d bib metadata, %d spans", total_bib, total_spans)

    # Clean up orphaned bibliography Annotations
    orphan_result = session.run("""
        MATCH (a:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'bibliography'})
        WHERE NOT EXISTS { (:Segment)-[:SEGMENTATION_OF]->(a) }
        DETACH DELETE a
        RETURN count(a) AS deleted
    """).single()
    orphans_deleted = orphan_result["deleted"] if orphan_result else 0
    logger.info("Deleted %d orphaned bibliography Annotation nodes", orphans_deleted)

    logger.info("Created %d BibliographicMetadata and %d Spans", total_bib, total_spans)
    return {"bib_metadata_created": total_bib, "spans_created": total_spans}


def migrate_segmentation_annotations(session: Session, batch_size: int = 50) -> dict[str, int]:
    """Convert segmentation/alignment Annotations to Segmentation nodes."""
    total_segmentations = 0
    total_spans = 0

    # Step 1: Remove Annotation-to-Annotation ALIGNED_TO relationships
    aligned_result = session.run("""
        MATCH (a1:Annotation)-[r:ALIGNED_TO]->(a2:Annotation)
        DELETE r
        RETURN count(r) AS deleted
    """).single()
    aligned_deleted = aligned_result["deleted"] if aligned_result else 0
    logger.info("Deleted %d Annotation-to-Annotation ALIGNED_TO relationships", aligned_deleted)

    # Check for invalid annotations that will be skipped
    invalid_result = session.run("""
        MATCH (a:Annotation)-[:HAS_TYPE]->(at:AnnotationType)
        WHERE at.name IN ['segmentation', 'alignment']
        AND (a.id IS NULL OR NOT EXISTS { (a)-[:ANNOTATION_OF]->(:Manifestation) })
        RETURN count(a) AS count
    """).single()
    invalid_count = invalid_result["count"] if invalid_result else 0
    if invalid_count > 0:
        logger.warning("Skipping %d annotations without id or ANNOTATION_OF relationship", invalid_count)

    # Step 2: Process Annotations in batches
    while True:
        result = session.run(
            """
            MATCH (a:Annotation)-[:HAS_TYPE]->(at:AnnotationType)
            WHERE at.name IN ['segmentation', 'alignment']
            AND a.id IS NOT NULL
            AND EXISTS { (a)-[:ANNOTATION_OF]->(:Manifestation) }
            RETURN elementId(a) AS eid
            LIMIT $batch_size
            """,
            batch_size=batch_size,
        )
        records = list(result)
        if not records:
            break

        logger.info("Processing batch of %d segmentation/alignment annotations...", len(records))

        # Create Segmentation nodes and link to Manifestation
        session.run(
            """
            UNWIND $data AS row
            MATCH (a:Annotation) WHERE elementId(a) = row.eid
            MATCH (a)-[:ANNOTATION_OF]->(m:Manifestation)
            CREATE (seg:Segmentation {id: a.id})-[:SEGMENTATION_OF]->(m)
            """,
            data=[{"eid": r["eid"]} for r in records],
        )

        # Update Segments: create SEGMENT_OF to new Segmentation
        session.run(
            """
            UNWIND $data AS row
            MATCH (a:Annotation) WHERE elementId(a) = row.eid
            MATCH (seg:Segmentation {id: a.id})
            MATCH (s:Segment)-[:SEGMENTATION_OF]->(a)
            CREATE (s)-[:SEGMENT_OF]->(seg)
            """,
            data=[{"eid": r["eid"]} for r in records],
        )

        # Extract spans from Segments
        spans_result = session.run(
            """
            UNWIND $data AS row
            MATCH (a:Annotation) WHERE elementId(a) = row.eid
            MATCH (s:Segment)-[:SEGMENTATION_OF]->(a)
            WHERE s.span_start IS NOT NULL
            CREATE (:Span {start: s.span_start, end: s.span_end})-[:SPAN_OF]->(s)
            RETURN count(*) AS spans_created
            """,
            data=[{"eid": r["eid"]} for r in records],
        ).single()
        total_spans += spans_result["spans_created"] if spans_result else 0

        # Remove span properties from Segments
        session.run(
            """
            UNWIND $data AS row
            MATCH (a:Annotation) WHERE elementId(a) = row.eid
            MATCH (s:Segment)-[:SEGMENTATION_OF]->(a)
            REMOVE s.span_start, s.span_end
            """,
            data=[{"eid": r["eid"]} for r in records],
        )

        # Delete old SEGMENTATION_OF relationships (Segment→Annotation)
        session.run(
            """
            UNWIND $data AS row
            MATCH (a:Annotation) WHERE elementId(a) = row.eid
            MATCH (s:Segment)-[r:SEGMENTATION_OF]->(a)
            DELETE r
            """,
            data=[{"eid": r["eid"]} for r in records],
        )

        # Delete Annotation nodes (DETACH removes HAS_TYPE and ANNOTATION_OF)
        session.run(
            """
            UNWIND $data AS row
            MATCH (a:Annotation) WHERE elementId(a) = row.eid
            DETACH DELETE a
            """,
            data=[{"eid": r["eid"]} for r in records],
        )

        total_segmentations += len(records)
        logger.info("Batch complete. Total: %d segmentations, %d spans", total_segmentations, total_spans)

    logger.info("Converted %d Annotations to Segmentations, %d Spans created", total_segmentations, total_spans)
    return {
        "segmentations_created": total_segmentations,
        "spans_created": total_spans,
        "aligned_to_deleted": aligned_deleted,
    }


def migrate_delete_search_segmentation_annotations(session: Session) -> int:
    """Delete Annotations with HAS_TYPE pointing to 'search_segmentation' AnnotationType."""
    result = session.run("""
        MATCH (a:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'search_segmentation'})
        DETACH DELETE a
        RETURN count(a) AS deleted
    """).single()
    deleted = result["deleted"] if result else 0
    logger.info("Deleted %d search_segmentation Annotations", deleted)
    return deleted


def migrate_delete_obsolete_nodes(session: Session) -> dict[str, int]:
    """Delete obsolete AnnotationType, License, Copyright, CopyrightStatus nodes."""
    counts: dict[str, int] = {}

    result = session.run("MATCH (n:AnnotationType) DETACH DELETE n RETURN count(n) AS deleted").single()
    counts["AnnotationType"] = result["deleted"] if result else 0
    logger.info("Deleted %d AnnotationType nodes", counts["AnnotationType"])

    result = session.run("MATCH (n:License) DETACH DELETE n RETURN count(n) AS deleted").single()
    counts["License"] = result["deleted"] if result else 0
    logger.info("Deleted %d License nodes", counts["License"])

    result = session.run("MATCH (n:Copyright) DETACH DELETE n RETURN count(n) AS deleted").single()
    counts["Copyright"] = result["deleted"] if result else 0
    logger.info("Deleted %d Copyright nodes", counts["Copyright"])

    result = session.run("MATCH (n:CopyrightStatus) DETACH DELETE n RETURN count(n) AS deleted").single()
    counts["CopyrightStatus"] = result["deleted"] if result else 0
    logger.info("Deleted %d CopyrightStatus nodes", counts["CopyrightStatus"])

    return counts


def run_all_migrations(session: Session) -> dict[str, int | dict[str, int]]:
    """Run all migrations in order."""
    logger.info("Starting migrations...")
    results: dict[str, int | dict[str, int]] = {
        "work_category_renamed": migrate_work_category_relationship_rename(session),
        "category_application": migrate_category_application_extraction(session),
        "nomen_ids_added": migrate_nomen_add_ids(session),
        "license_types_created": migrate_create_license_types(session),
        "copyright_to_license": migrate_copyright_to_license(session),
        "license_to_license_type": migrate_license_to_license_type(session),
        "note_types_created": migrate_create_note_types(session),
        "attribute_types_created": migrate_create_attribute_types(session),
        "durchen_to_notes": migrate_durchen_annotations_to_notes(session),
        "bibliography_to_metadata": migrate_bibliography_annotations(session),
        "segmentation_annotations": migrate_segmentation_annotations(session),
        "search_segmentation_deleted": migrate_delete_search_segmentation_annotations(session),
        "obsolete_nodes_deleted": migrate_delete_obsolete_nodes(session),
    }
    logger.info("All migrations complete.")
    return results


if __name__ == "__main__":
    import os

    from neo4j import GraphDatabase

    logging.basicConfig(level=logging.INFO)

    uri = os.environ.get("NEO4J_URI")
    username = os.environ.get("NEO4J_USERNAME", "neo4j")
    password = os.environ.get("NEO4J_PASSWORD")

    if not uri or not password:
        logger.error("NEO4J_URI and NEO4J_PASSWORD environment variables must be set")
        sys.exit(1)

    driver = GraphDatabase.driver(uri, auth=(username, password))  # type: ignore[arg-type]
    try:
        with driver.session() as session:
            results = run_all_migrations(session)
            for name, result in results.items():
                logger.info("%s: %s", name, result)
    finally:
        driver.close()
