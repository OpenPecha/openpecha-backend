"""Migrate a SINGLE text (and its related subgraph) from production to dev.

Production (`main`) runs the old Neo4j schema (Expression / Manifestation /
Annotation / Segment …). Dev runs the new schema (Text / Edition /
Segmentation / Segment / Span …). This script reads one text's subgraph from
the production Neo4j, transforms it to the new schema, and writes it into the
dev Neo4j — preserving the original IDs (Text, Edition, Segmentation and,
importantly, every Segment ID stays identical).

Design decisions (chosen as sensible defaults — adjust if your situation differs):

  * Scope = the text itself + its editions + their segmentations / segments /
    spans + durchen notes + bibliographic metadata. ALIGNED_TO edges are
    recreated only between segments that both exist in dev after the load
    (so cross-text alignment edges to texts you haven't migrated are skipped,
    not dangling). TRANSLATION_OF / COMMENTARY_OF links to other texts are
    created only if the other Text already exists in dev.
  * Connectivity = the script talks to BOTH Neo4j instances at once
    (SOURCE_* for prod, TARGET_* for dev).
  * Idempotent = everything is MERGEd on a stable key (preserved `id` for
    entity nodes; a `_src_eid` provenance key for property-only nodes such as
    Nomen / LocalizedText). Re-running for the same text will not duplicate.
  * pagination annotations are NOT migrated (the in-place migration.py doesn't
    handle them either — see the project migration notes). They are reported.

Environment variables (a .env file is loaded automatically):

  SOURCE_NEO4J_URI, SOURCE_NEO4J_USERNAME, SOURCE_NEO4J_PASSWORD   (production)
  TARGET_NEO4J_URI, TARGET_NEO4J_USERNAME, TARGET_NEO4J_PASSWORD   (dev)

Usage:

  # Dry run — extract from prod and print what WOULD be written, change nothing:
  python scripts/migrate_single_text.py --text-id E12345678 --dry-run

  # Real run:
  python scripts/migrate_single_text.py --text-id E12345678

  # Also copy the base-text files (prod Firebase Storage -> dev S3):
  python scripts/migrate_single_text.py --text-id E12345678 \
      --copy-content --firebase-bucket pecha-backend.appspot.com

Run it from the repository root (so `identifier`, `database.search_text` import).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from dotenv import load_dotenv

from database.search_text import normalize_search_text
from identifier import generate_id

if TYPE_CHECKING:
    from neo4j import Driver, Session

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


# --------------------------------------------------------------------------- #
# Value mappings (old schema -> new schema)
# --------------------------------------------------------------------------- #

# Old ManifestationType.name -> new EditionType.name
EDITION_TYPE_MAP = {
    "diplomatic": "diplomatic",
    "critical": "critical",
    "collated": "collated",
}

# Old CopyrightStatus.name -> new LicenseType.name
LICENSE_MAP = {
    "public": "public",
    "Public domain": "public",
    "copyrighted": "copyrighted",
    "unknown": "unknown",
}
DEFAULT_LICENSE = "unknown"


# --------------------------------------------------------------------------- #
# In-memory model of the extracted subgraph
# --------------------------------------------------------------------------- #


@dataclass
class Nomen:
    """A name structure: localizations + (optional) alternative names."""

    src_eid: str
    localizations: list[dict[str, Any]] = field(default_factory=list)  # {text, code, name, bcp47}
    alternatives: list["Nomen"] = field(default_factory=list)


@dataclass
class Contribution:
    role: str | None
    person_id: str | None
    person_bdrc: str | None
    person_wiki: str | None
    person_name: Nomen | None
    ai_id: str | None


@dataclass
class Segment:
    id: str
    spans: list[dict[str, int]]  # {start, end}
    aligned_to: list[str]  # target segment ids (old)
    has_out: bool
    has_in: bool


@dataclass
class Segmentation:
    id: str
    labels: list[str]  # subset of Display / Aligned / Target
    segments: list[Segment]


@dataclass
class Note:
    text: str
    span: dict[str, int] | None


@dataclass
class Bibliographic:
    type: str | None
    span: dict[str, int] | None


@dataclass
class Edition:
    id: str
    colophon: str | None
    bdrc: str | None
    wiki: str | None
    edition_type: str | None
    incipit: Nomen | None
    segmentations: list[Segmentation] = field(default_factory=list)
    notes: list[Note] = field(default_factory=list)
    bibliographic: list[Bibliographic] = field(default_factory=list)
    pagination_skipped: int = 0


@dataclass
class TextModel:
    id: str
    wiki: str | None
    bdrc: str | None
    work_id: str | None
    work_wiki: str | None
    work_bdrc: str | None
    original: bool
    lang_code: str | None
    lang_name: str | None
    bcp47: str | None
    translation_of: str | None
    commentary_of: str | None
    category_id: str | None
    license_name: str
    title: Nomen | None
    contributions: list[Contribution] = field(default_factory=list)
    editions: list[Edition] = field(default_factory=list)


# --------------------------------------------------------------------------- #
# Extraction (PRODUCTION / old schema)
# --------------------------------------------------------------------------- #


def _extract_nomen(session: Session, eid: str, _seen: set[str] | None = None) -> Nomen | None:
    """Read a Nomen subtree (localizations + alternatives) from the source DB."""
    if eid is None:
        return None
    seen = _seen if _seen is not None else set()
    if eid in seen:  # guard against ALTERNATIVE_OF cycles
        return None
    seen.add(eid)

    record = session.run(
        """
        MATCH (n) WHERE elementId(n) = $eid
        OPTIONAL MATCH (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[r:HAS_LANGUAGE]->(l:Language)
        OPTIONAL MATCH (n)-[:ALTERNATIVE_OF]->(alt:Nomen)
        RETURN
          collect(DISTINCT CASE WHEN lt IS NULL THEN NULL ELSE
            {text: lt.text, code: l.code, name: l.name, bcp47: r.bcp47} END) AS locs,
          collect(DISTINCT elementId(alt)) AS alt_eids
        """,
        eid=eid,
    ).single()
    if record is None:
        return None

    locs = [loc for loc in record["locs"] if loc and loc.get("text") is not None]
    alternatives: list[Nomen] = []
    for alt_eid in record["alt_eids"]:
        if alt_eid is None:
            continue
        alt = _extract_nomen(session, alt_eid, seen)
        if alt is not None:
            alternatives.append(alt)

    return Nomen(src_eid=eid, localizations=locs, alternatives=alternatives)


def _extract_license(session: Session, text_id: str) -> str:
    record = session.run(
        """
        MATCH (m:Manifestation)-[:MANIFESTATION_OF]->(:Expression {id: $tid})
        OPTIONAL MATCH (m)-[:HAS_COPYRIGHT]->(cs:CopyrightStatus)
        RETURN collect(DISTINCT cs.name) AS statuses
        """,
        tid=text_id,
    ).single()
    statuses = [s for s in (record["statuses"] if record else []) if s]
    for status in statuses:
        if status in LICENSE_MAP:
            return LICENSE_MAP[status]
    return DEFAULT_LICENSE


def _extract_segmentation(session: Session, annotation_id: str, ann_type: str) -> Segmentation:
    rows = session.run(
        """
        MATCH (s:Segment)-[:SEGMENTATION_OF]->(:Annotation {id: $aid})
        RETURN s.id AS id, s.span_start AS start, s.span_end AS end,
               EXISTS { (s)-[:ALIGNED_TO]->(:Segment) } AS has_out,
               EXISTS { (s)<-[:ALIGNED_TO]-(:Segment) } AS has_in,
               [(s)-[:ALIGNED_TO]->(t:Segment) | t.id] AS aligned_to
        """,
        aid=annotation_id,
    ).data()

    segments: list[Segment] = []
    any_out = any_in = False
    for row in rows:
        spans: list[dict[str, int]] = []
        if row["start"] is not None and row["end"] is not None:
            spans.append({"start": row["start"], "end": row["end"]})
        segments.append(
            Segment(
                id=row["id"],
                spans=spans,
                aligned_to=[t for t in row["aligned_to"] if t is not None],
                has_out=bool(row["has_out"]),
                has_in=bool(row["has_in"]),
            )
        )
        any_out = any_out or bool(row["has_out"])
        any_in = any_in or bool(row["has_in"])

    labels: list[str] = []
    if any_out:
        labels.append("Aligned")
    if any_in:
        labels.append("Target")
    if not labels:
        labels.append("Display")
    # Pure 'segmentation' annotations with no alignment are always Display.
    if ann_type == "segmentation" and not any_out and not any_in:
        labels = ["Display"]

    return Segmentation(id=annotation_id, labels=labels, segments=segments)


def _extract_edition(session: Session, edition_row: dict[str, Any]) -> Edition:
    edition = Edition(
        id=edition_row["id"],
        colophon=edition_row["colophon"],
        bdrc=edition_row["bdrc"],
        wiki=edition_row["wiki"],
        edition_type=edition_row["type"],
        incipit=_extract_nomen(session, edition_row["incipit_eid"]) if edition_row["incipit_eid"] else None,
    )

    annotations = session.run(
        """
        MATCH (a:Annotation)-[:ANNOTATION_OF]->(:Manifestation {id: $mid})
        MATCH (a)-[:HAS_TYPE]->(at:AnnotationType)
        RETURN a.id AS id, at.name AS type
        """,
        mid=edition.id,
    ).data()

    for ann in annotations:
        ann_type = ann["type"]
        if ann["id"] is None:
            logger.warning("Skipping annotation without id on edition %s (type=%s)", edition.id, ann_type)
            continue
        if ann_type in ("segmentation", "alignment"):
            edition.segmentations.append(_extract_segmentation(session, ann["id"], ann_type))
        elif ann_type == "pagination":
            edition.pagination_skipped += 1
        # bibliography handled below via its own structure; durchen via HAS_DURCHEN_NOTE

    # Durchen notes (old: Segment-[:HAS_DURCHEN_NOTE]->DurchenNote)
    durchen = session.run(
        """
        MATCH (s:Segment)-[:HAS_DURCHEN_NOTE]->(dn:DurchenNote)
        MATCH (s)-[:SEGMENTATION_OF]->(:Annotation)-[:ANNOTATION_OF]->(:Manifestation {id: $mid})
        RETURN dn.note AS text, s.span_start AS start, s.span_end AS end
        """,
        mid=edition.id,
    ).data()
    for row in durchen:
        if row["text"] is None:
            continue
        span = {"start": row["start"], "end": row["end"]} if row["start"] is not None else None
        edition.notes.append(Note(text=row["text"], span=span))

    # Bibliographic metadata (old: 'bibliography' annotation; type via HAS_BIBLIOGRAPHY_TYPE or HAS_TYPE)
    biblio = session.run(
        """
        MATCH (s:Segment)-[:SEGMENTATION_OF]->(a:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'bibliography'})
        MATCH (a)-[:ANNOTATION_OF]->(:Manifestation {id: $mid})
        OPTIONAL MATCH (s)-[:HAS_BIBLIOGRAPHY_TYPE]->(bt1:BibliographyType)
        OPTIONAL MATCH (s)-[:HAS_TYPE]->(bt2:BibliographyType)
        RETURN s.span_start AS start, s.span_end AS end,
               coalesce(bt1.type, bt1.name, bt2.type, bt2.name) AS type
        """,
        mid=edition.id,
    ).data()
    for row in biblio:
        span = {"start": row["start"], "end": row["end"]} if row["start"] is not None else None
        edition.bibliographic.append(Bibliographic(type=row["type"], span=span))

    return edition


def extract(session: Session, text_id: str) -> TextModel:
    core = session.run(
        """
        MATCH (e:Expression {id: $tid})
        OPTIONAL MATCH (e)-[rw:EXPRESSION_OF]->(w:Work)
        OPTIONAL MATCH (e)-[rlang:HAS_LANGUAGE]->(lang:Language)
        OPTIONAL MATCH (e)-[:TRANSLATION_OF]->(tr:Expression)
        OPTIONAL MATCH (e)-[:COMMENTARY_OF]->(co:Expression)
        OPTIONAL MATCH (w)-[:BELONGS_TO]->(cat:Category)
        RETURN e.id AS id, e.wiki AS wiki, e.bdrc AS bdrc,
               w.id AS work_id, w.wiki AS work_wiki, w.bdrc AS work_bdrc, rw.original AS original,
               lang.code AS lang_code, lang.name AS lang_name, rlang.bcp47 AS bcp47,
               tr.id AS translation_of, co.id AS commentary_of,
               cat.id AS category_id
        """,
        tid=text_id,
    ).single()

    if core is None:
        raise SystemExit(f"No Expression found in source with id '{text_id}'")

    title_eid_record = session.run(
        "MATCH (:Expression {id: $tid})-[:HAS_TITLE]->(n:Nomen) RETURN elementId(n) AS eid",
        tid=text_id,
    ).single()
    title = _extract_nomen(session, title_eid_record["eid"]) if title_eid_record else None

    model = TextModel(
        id=core["id"],
        wiki=core["wiki"],
        bdrc=core["bdrc"],
        work_id=core["work_id"],
        work_wiki=core["work_wiki"],
        work_bdrc=core["work_bdrc"],
        original=bool(core["original"]) if core["original"] is not None else True,
        lang_code=core["lang_code"],
        lang_name=core["lang_name"],
        bcp47=core["bcp47"],
        translation_of=core["translation_of"],
        commentary_of=core["commentary_of"],
        category_id=core["category_id"],
        license_name=_extract_license(session, text_id),
        title=title,
    )

    contributions = session.run(
        """
        MATCH (:Expression {id: $tid})-[:HAS_CONTRIBUTION]->(c:Contribution)
        OPTIONAL MATCH (c)-[:BY]->(p:Person)
        OPTIONAL MATCH (c)-[:BY]->(ai:AI)
        OPTIONAL MATCH (c)-[:WITH_ROLE]->(rt:RoleType)
        OPTIONAL MATCH (p)-[:HAS_NAME]->(pn:Nomen)
        RETURN rt.name AS role,
               p.id AS person_id, p.bdrc AS person_bdrc, p.wiki AS person_wiki,
               elementId(pn) AS person_nomen_eid, ai.id AS ai_id
        """,
        tid=text_id,
    ).data()
    for row in contributions:
        model.contributions.append(
            Contribution(
                role=row["role"],
                person_id=row["person_id"],
                person_bdrc=row["person_bdrc"],
                person_wiki=row["person_wiki"],
                person_name=_extract_nomen(session, row["person_nomen_eid"]) if row["person_nomen_eid"] else None,
                ai_id=row["ai_id"],
            )
        )

    editions = session.run(
        """
        MATCH (m:Manifestation)-[:MANIFESTATION_OF]->(:Expression {id: $tid})
        OPTIONAL MATCH (m)-[:HAS_TYPE]->(mt:ManifestationType)
        OPTIONAL MATCH (m)-[:HAS_INCIPIT_TITLE]->(it:Nomen)
        RETURN m.id AS id, m.colophon AS colophon, m.bdrc AS bdrc, m.wiki AS wiki,
               mt.name AS type, elementId(it) AS incipit_eid
        """,
        tid=text_id,
    ).data()
    for edition_row in editions:
        model.editions.append(_extract_edition(session, edition_row))

    return model


# --------------------------------------------------------------------------- #
# Loading (DEV / new schema)
# --------------------------------------------------------------------------- #


def _load_nomen(session: Session, nomen: Nomen) -> str:
    """MERGE a Nomen (+ localizations + alternatives) into dev, return its id."""
    record = session.run(
        """
        MERGE (n:Nomen {_src_eid: $eid})
        ON CREATE SET n.id = $new_id
        RETURN n.id AS id
        """,
        eid=nomen.src_eid,
        new_id=generate_id(),
    ).single()
    nomen_id = record["id"]

    for loc in nomen.localizations:
        session.run(
            """
            MATCH (n:Nomen {id: $nomen_id})
            MERGE (lang:Language {code: $code})
            ON CREATE SET lang.name = coalesce($name, $code)
            MERGE (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText {_src_eid: $eid, lang_code: $code})
            SET lt.text = $text, lt.search_text = $search_text
            MERGE (lt)-[r:HAS_LANGUAGE]->(lang)
            SET r.bcp47 = $bcp47
            """,
            nomen_id=nomen_id,
            code=loc["code"],
            name=loc.get("name"),
            text=loc["text"],
            search_text=normalize_search_text(loc["text"]),
            bcp47=loc.get("bcp47"),
            eid=f"{nomen.src_eid}:{loc['code']}",
        )

    for alt in nomen.alternatives:
        alt_id = _load_nomen(session, alt)
        session.run(
            """
            MATCH (n:Nomen {id: $nomen_id}), (alt:Nomen {id: $alt_id})
            MERGE (n)-[:ALTERNATIVE_OF]->(alt)
            """,
            nomen_id=nomen_id,
            alt_id=alt_id,
        )

    return nomen_id


def _load_text_core(session: Session, model: TextModel) -> None:
    session.run(
        """
        MERGE (t:Text {id: $id})
        SET t.wiki = $wiki, t.bdrc = $bdrc
        WITH t
        MERGE (lt:LicenseType {name: $license})
        MERGE (t)-[:HAS_LICENSE]->(lt)
        """,
        id=model.id,
        wiki=model.wiki,
        bdrc=model.bdrc,
        license=model.license_name,
    )

    if model.work_id:
        session.run(
            """
            MATCH (t:Text {id: $id})
            MERGE (w:Work {id: $work_id})
            SET w.wiki = $work_wiki, w.bdrc = $work_bdrc
            MERGE (t)-[r:TEXT_OF]->(w)
            SET r.original = $original
            """,
            id=model.id,
            work_id=model.work_id,
            work_wiki=model.work_wiki,
            work_bdrc=model.work_bdrc,
            original=model.original,
        )
        if model.category_id:
            linked = session.run(
                """
                MATCH (w:Work {id: $work_id})
                MATCH (c:Category {id: $category_id})
                MERGE (w)-[:HAS_CATEGORY]->(c)
                RETURN count(*) AS n
                """,
                work_id=model.work_id,
                category_id=model.category_id,
            ).single()
            if not linked or linked["n"] == 0:
                logger.warning(
                    "Category '%s' not found in dev; skipped Work->Category link", model.category_id
                )

    if model.lang_code:
        session.run(
            """
            MATCH (t:Text {id: $id})
            MERGE (lang:Language {code: $code})
            ON CREATE SET lang.name = coalesce($name, $code)
            MERGE (t)-[r:HAS_LANGUAGE]->(lang)
            SET r.bcp47 = $bcp47
            """,
            id=model.id,
            code=model.lang_code,
            name=model.lang_name,
            bcp47=model.bcp47,
        )

    if model.title is not None:
        title_id = _load_nomen(session, model.title)
        session.run(
            "MATCH (t:Text {id: $id}), (n:Nomen {id: $nid}) MERGE (t)-[:HAS_TITLE]->(n)",
            id=model.id,
            nid=title_id,
        )

    for rel_field, rel_type in (("translation_of", "TRANSLATION_OF"), ("commentary_of", "COMMENTARY_OF")):
        target = getattr(model, rel_field)
        if not target:
            continue
        linked = session.run(
            f"""
            MATCH (t:Text {{id: $id}})
            MATCH (other:Text {{id: $target}})
            MERGE (t)-[:{rel_type}]->(other)
            RETURN count(*) AS n
            """,
            id=model.id,
            target=target,
        ).single()
        if not linked or linked["n"] == 0:
            logger.warning(
                "Related text '%s' (%s) not yet in dev; skipped relationship", target, rel_type
            )


def _load_contributions(session: Session, model: TextModel) -> None:
    for contrib in model.contributions:
        if contrib.person_id:
            session.run(
                """
                MATCH (t:Text {id: $id})
                MERGE (p:Person {id: $person_id})
                SET p.bdrc = $bdrc, p.wiki = $wiki
                MERGE (c:Contribution {_src_eid: $contrib_key})
                MERGE (t)-[:HAS_CONTRIBUTION]->(c)
                MERGE (c)-[:BY]->(p)
                """,
                id=model.id,
                person_id=contrib.person_id,
                bdrc=contrib.person_bdrc,
                wiki=contrib.person_wiki,
                contrib_key=f"{model.id}:{contrib.person_id}:{contrib.role}",
            )
            if contrib.person_name is not None:
                name_id = _load_nomen(session, contrib.person_name)
                session.run(
                    "MATCH (p:Person {id: $pid}), (n:Nomen {id: $nid}) MERGE (p)-[:HAS_NAME]->(n)",
                    pid=contrib.person_id,
                    nid=name_id,
                )
        elif contrib.ai_id:
            session.run(
                """
                MATCH (t:Text {id: $id})
                MERGE (ai:AI {id: $ai_id})
                MERGE (c:Contribution {_src_eid: $contrib_key})
                MERGE (t)-[:HAS_CONTRIBUTION]->(c)
                MERGE (c)-[:BY]->(ai)
                """,
                id=model.id,
                ai_id=contrib.ai_id,
                contrib_key=f"{model.id}:ai:{contrib.ai_id}:{contrib.role}",
            )
        else:
            continue

        if contrib.role:
            session.run(
                """
                MATCH (t:Text {id: $id})-[:HAS_CONTRIBUTION]->(c:Contribution {_src_eid: $contrib_key})
                MERGE (rt:RoleType {name: $role})
                MERGE (c)-[:WITH_ROLE]->(rt)
                """,
                id=model.id,
                contrib_key=(
                    f"{model.id}:{contrib.person_id}:{contrib.role}"
                    if contrib.person_id
                    else f"{model.id}:ai:{contrib.ai_id}:{contrib.role}"
                ),
                role=contrib.role,
            )


def _load_edition(session: Session, text_id: str, edition: Edition) -> None:
    edition_type = EDITION_TYPE_MAP.get(edition.edition_type or "", "critical")
    session.run(
        """
        MATCH (t:Text {id: $text_id})
        MERGE (e:Edition {id: $id})
        SET e.colophon = $colophon, e.bdrc = $bdrc, e.wiki = $wiki
        MERGE (e)-[:EDITION_OF]->(t)
        MERGE (et:EditionType {name: $edition_type})
        MERGE (e)-[:HAS_TYPE]->(et)
        """,
        text_id=text_id,
        id=edition.id,
        colophon=edition.colophon,
        bdrc=edition.bdrc,
        wiki=edition.wiki,
        edition_type=edition_type,
    )

    if edition.incipit is not None:
        incipit_id = _load_nomen(session, edition.incipit)
        session.run(
            "MATCH (e:Edition {id: $eid}), (n:Nomen {id: $nid}) MERGE (e)-[:HAS_INCIPIT_TITLE]->(n)",
            eid=edition.id,
            nid=incipit_id,
        )

    for seg in edition.segmentations:
        label_clause = "".join(f":{label}" for label in seg.labels)
        session.run(
            f"""
            MATCH (e:Edition {{id: $eid}})
            MERGE (sgn:Segmentation {{id: $sgn_id}})
            SET sgn{label_clause}
            MERGE (sgn)-[:SEGMENTATION_OF]->(e)
            """,
            eid=edition.id,
            sgn_id=seg.id,
        )
        segments_payload = [{"id": s.id, "spans": s.spans} for s in seg.segments]
        session.run(
            """
            MATCH (sgn:Segmentation {id: $sgn_id})
            UNWIND $segments AS seg
            MERGE (s:Segment {id: seg.id})
            MERGE (s)-[:SEGMENT_OF]->(sgn)
            WITH s, seg
            UNWIND seg.spans AS span
            MERGE (sp:Span {_src_seg: seg.id, start: span.start, end: span.end})
            MERGE (sp)-[:SPAN_OF]->(s)
            """,
            sgn_id=seg.id,
            segments=segments_payload,
        )

    for note in edition.notes:
        note_id = generate_id()
        session.run(
            """
            MATCH (e:Edition {id: $eid})
            MERGE (nt:NoteType {name: 'durchen'})
            CREATE (n:Note {id: $note_id, text: $text})-[:NOTE_OF]->(e)
            CREATE (n)-[:HAS_TYPE]->(nt)
            FOREACH (_ IN CASE WHEN $has_span THEN [1] ELSE [] END |
                CREATE (:Span {start: $start, end: $end})-[:SPAN_OF]->(n)
            )
            """,
            eid=edition.id,
            note_id=note_id,
            text=note.text,
            has_span=note.span is not None,
            start=note.span["start"] if note.span else None,
            end=note.span["end"] if note.span else None,
        )

    for bib in edition.bibliographic:
        bib_id = generate_id()
        bib_type = bib.type or "colophon"
        session.run(
            """
            MATCH (e:Edition {id: $eid})
            MERGE (bt:BibliographyType {name: $bib_type})
            CREATE (bm:BibliographicMetadata {id: $bib_id})-[:BIBLIOGRAPHY_OF]->(e)
            CREATE (bm)-[:HAS_TYPE]->(bt)
            FOREACH (_ IN CASE WHEN $has_span THEN [1] ELSE [] END |
                CREATE (:Span {start: $start, end: $end})-[:SPAN_OF]->(bm)
            )
            """,
            eid=edition.id,
            bib_id=bib_id,
            bib_type=bib_type,
            has_span=bib.span is not None,
            start=bib.span["start"] if bib.span else None,
            end=bib.span["end"] if bib.span else None,
        )

    if edition.pagination_skipped:
        logger.warning(
            "Edition %s: skipped %d pagination annotation(s) (not migrated)",
            edition.id,
            edition.pagination_skipped,
        )


def _load_aligned_to(session: Session, model: TextModel) -> int:
    """Recreate ALIGNED_TO edges, but only where BOTH segments exist in dev."""
    pairs = [
        {"from": seg.id, "to": target}
        for edition in model.editions
        for sgn in edition.segmentations
        for seg in sgn.segments
        for target in seg.aligned_to
    ]
    if not pairs:
        return 0
    record = session.run(
        """
        UNWIND $pairs AS pair
        MATCH (a:Segment {id: pair.from})
        MATCH (b:Segment {id: pair.to})
        MERGE (a)-[:ALIGNED_TO]->(b)
        RETURN count(*) AS created
        """,
        pairs=pairs,
    ).single()
    created = record["created"] if record else 0
    if created < len(pairs):
        logger.warning(
            "ALIGNED_TO: %d/%d edges created; %d skipped (counterpart segment not in dev)",
            created,
            len(pairs),
            len(pairs) - created,
        )
    return created


def load(session: Session, model: TextModel) -> None:
    _load_text_core(session, model)
    _load_contributions(session, model)
    for edition in model.editions:
        _load_edition(session, model.id, edition)
    _load_aligned_to(session, model)


# --------------------------------------------------------------------------- #
# Optional content copy (Firebase Storage -> S3)
# --------------------------------------------------------------------------- #


def copy_content(model: TextModel, firebase_bucket: str, *, dry_run: bool) -> None:
    import hashlib

    import boto3  # noqa: PLC0415
    import firebase_admin  # noqa: PLC0415
    from firebase_admin import credentials, storage  # noqa: PLC0415

    s3_bucket = os.environ.get("AWS_S3_BUCKET", "")
    s3_region = os.environ.get("AWS_REGION", "")
    if not s3_bucket or not s3_region:
        logger.error("AWS_S3_BUCKET and AWS_REGION must be set for --copy-content")
        return

    try:
        firebase_admin.get_app()
    except ValueError:
        firebase_admin.initialize_app(credentials.ApplicationDefault(), {"storageBucket": firebase_bucket})
    src_bucket = storage.bucket(firebase_bucket)

    s3 = boto3.Session(region_name=s3_region).client("s3")

    for edition in model.editions:
        key = f"base_texts/{model.id}/{edition.id}.txt"
        blob = src_bucket.blob(key)
        if not blob.exists():
            logger.warning("Content not found in Firebase: %s", key)
            continue
        if dry_run:
            logger.info("[DRY RUN] would copy %s -> s3://%s/%s", key, s3_bucket, key)
            continue
        data = blob.download_as_bytes()
        try:
            head = s3.head_object(Bucket=s3_bucket, Key=key)
            if head["ETag"].strip('"') == hashlib.md5(data).hexdigest():  # noqa: S324
                logger.info("SKIP (identical): %s", key)
                continue
        except s3.exceptions.ClientError as err:
            if err.response.get("Error", {}).get("Code") != "404":
                raise
        s3.put_object(
            Bucket=s3_bucket,
            Key=key,
            Body=data,
            ContentType="text/plain; charset=utf-8",
            CacheControl="public, max-age=0, must-revalidate",
        )
        logger.info("UPLOADED: %s (%d bytes)", key, len(data))


# --------------------------------------------------------------------------- #
# Reporting / CLI
# --------------------------------------------------------------------------- #


def summarize(model: TextModel) -> dict[str, Any]:
    return {
        "text_id": model.id,
        "license": model.license_name,
        "language": model.lang_code,
        "work_id": model.work_id,
        "translation_of": model.translation_of,
        "commentary_of": model.commentary_of,
        "contributions": len(model.contributions),
        "editions": [
            {
                "edition_id": e.id,
                "type": EDITION_TYPE_MAP.get(e.edition_type or "", "critical"),
                "segmentations": [
                    {"id": s.id, "labels": s.labels, "segments": len(s.segments)} for s in e.segmentations
                ],
                "notes": len(e.notes),
                "bibliographic": len(e.bibliographic),
                "pagination_skipped": e.pagination_skipped,
            }
            for e in model.editions
        ],
    }


def _driver(prefix: str) -> "Driver":
    from neo4j import GraphDatabase  # noqa: PLC0415

    uri = os.environ.get(f"{prefix}_NEO4J_URI")
    user = os.environ.get(f"{prefix}_NEO4J_USERNAME", "neo4j")
    password = os.environ.get(f"{prefix}_NEO4J_PASSWORD")
    if not uri or not password:
        logger.error("%s_NEO4J_URI and %s_NEO4J_PASSWORD must be set", prefix, prefix)
        sys.exit(1)
    return GraphDatabase.driver(uri, auth=(user, password))


def main() -> None:
    parser = argparse.ArgumentParser(description="Migrate a single text from prod to dev (old schema -> new schema)")
    parser.add_argument("--text-id", required=True, help="The Expression/Text id to migrate (kept identical in dev)")
    parser.add_argument("--dry-run", action="store_true", help="Extract and report only; write nothing to dev")
    parser.add_argument("--copy-content", action="store_true", help="Also copy base-text files (Firebase -> S3)")
    parser.add_argument("--firebase-bucket", help="Source Firebase Storage bucket (required with --copy-content)")
    args = parser.parse_args()

    source = _driver("SOURCE")
    try:
        with source.session() as src_session:
            model = extract(src_session, args.text_id)
    finally:
        source.close()

    logger.info("Extracted subgraph:\n%s", json.dumps(summarize(model), indent=2, ensure_ascii=False))

    if args.dry_run:
        logger.info("Dry run — nothing written to dev.")
    else:
        target = _driver("TARGET")
        try:
            with target.session() as tgt_session:
                load(tgt_session, model)
        finally:
            target.close()
        logger.info("Loaded text '%s' into dev.", model.id)

    if args.copy_content:
        if not args.firebase_bucket:
            logger.error("--copy-content requires --firebase-bucket")
            sys.exit(1)
        copy_content(model, args.firebase_bucket, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
