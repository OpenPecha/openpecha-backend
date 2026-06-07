import logging

from neo4j import AsyncDriver, AsyncManagedTransaction

logger = logging.getLogger(__name__)

DATABASE_NAME = "neo4j"

# Each trigger is a dict with:
#   name: unique trigger name
#   query: Cypher to execute on each transaction
#   phase: "before" or "after" commit
#   audit: standalone Cypher that checks the same invariant against ALL existing data
#
# Naming convention: enforce_{source_label}_{relationship}
# These cover every "required: true" relationship in neo4j_schema.yaml,
# plus target-type enforcement for optional typed relationships.


def _required_rel_trigger(
    name: str,
    description: str,
    source_label: str,
    rel_type: str,
    target_label: str,
    cardinality: str = "one",
) -> list[dict]:
    """Generate trigger + audit pairs for a required relationship.

    Returns a list of dicts:
      - Existence: a single trigger that fires on both node creation and
        relationship deletion, using CALL () { UNION ALL } to collect affected nodes.
      - If cardinality is 'one', also includes a uniqueness trigger (at most one).
    """
    triggers = [
        {
            "name": name,
            "description": description,
            "phase": "before",
            "query": f"""
                CALL () {{
                    UNWIND $createdNodes AS n
                    WITH n WHERE n:{source_label}
                    RETURN n AS node
                  UNION ALL
                    UNWIND $deletedRelationships AS rel
                    WITH rel WHERE type(rel) = '{rel_type}'
                    WITH startNode(rel) AS n
                    WHERE n:{source_label}
                    RETURN n AS node
                }}
                WITH DISTINCT node
                WHERE NOT (node)-[:{rel_type}]->(:{target_label})
                WITH collect(node.id) AS ids
                WHERE size(ids) > 0
                CALL apoc.util.validate(
                    true,
                    '{name}: {source_label} must have {rel_type}->{target_label}. IDs: %s',
                    [apoc.text.join(ids, ', ')]
                )
                RETURN null
            """,
            "audit": f"""
                MATCH (n:{source_label})
                WHERE NOT (n)-[:{rel_type}]->(:{target_label})
                RETURN n.id AS violating_id
            """,
        },
    ]

    if cardinality == "one":
        cardinality_name = name + "_max_one"
        triggers.append(
            {
                "name": cardinality_name,
                "description": f"{source_label} must have at most one {rel_type}->{target_label}",
                "phase": "before",
                "query": f"""
                CALL () {{
                    UNWIND $createdNodes AS n
                    WITH n WHERE n:{source_label}
                    RETURN n AS node
                  UNION ALL
                    UNWIND $createdRelationships AS rel
                    WITH rel WHERE type(rel) = '{rel_type}'
                    WITH startNode(rel) AS n
                    WHERE n:{source_label}
                    RETURN n AS node
                }}
                WITH DISTINCT node
                WITH node, count {{ (node)-[:{rel_type}]->(:{target_label}) }} AS cnt
                WHERE cnt > 1
                WITH collect(node.id) AS ids
                WHERE size(ids) > 0
                CALL apoc.util.validate(
                    true,
                    '{cardinality_name}: {source_label} must have at most one {rel_type}. IDs: %s',
                    [apoc.text.join(ids, ', ')]
                )
                RETURN null
            """,
                "audit": f"""
                MATCH (n:{source_label})
                WITH n, count {{ (n)-[:{rel_type}]->(:{target_label}) }} AS cnt
                WHERE cnt > 1
                RETURN n.id AS violating_id
            """,
            }
        )

    return triggers


def _rel_target_type_trigger(
    name: str,
    description: str,
    rel_type: str,
    target_label: str,
) -> dict:
    """Generate a trigger + audit pair that enforces a relationship's target node type."""
    return {
        "name": name,
        "description": description,
        "phase": "before",
        "query": f"""
            UNWIND $createdRelationships AS rel
            WITH rel
            WHERE type(rel) = '{rel_type}'
            WITH rel, startNode(rel) AS source, endNode(rel) AS target
            WHERE NOT target:{target_label}
            WITH collect(source.id) AS ids
            WHERE size(ids) > 0
            CALL apoc.util.validate(
                true,
                '{name}: {rel_type} must point to {target_label}. IDs: %s',
                [apoc.text.join(ids, ', ')]
            )
            RETURN null
        """,
        "audit": f"""
            MATCH (n)-[:{rel_type}]->(target)
            WHERE NOT target:{target_label}
            RETURN n.id AS violating_id
        """,
    }


TRIGGERS: list[dict] = []

# =========================================================================
# Work
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_work_has_category",
        "Every Work must have HAS_CATEGORY->Category",
        "Work",
        "HAS_CATEGORY",
        "Category",
    )
)

# =========================================================================
# Tag
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_tag_belongs_to",
        "Every Tag must have BELONGS_TO->Application",
        "Tag",
        "BELONGS_TO",
        "Application",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_tag_has_title",
        "Every Tag must have HAS_TITLE->Nomen",
        "Tag",
        "HAS_TITLE",
        "Nomen",
    )
)

# =========================================================================
# Category
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_category_belongs_to",
        "Every Category must have BELONGS_TO->Application",
        "Category",
        "BELONGS_TO",
        "Application",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_category_has_title",
        "Every Category must have HAS_TITLE->Nomen",
        "Category",
        "HAS_TITLE",
        "Nomen",
    )
)

# =========================================================================
# Text
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_text_has_text_of",
        "Every Text must have TEXT_OF->Work",
        "Text",
        "TEXT_OF",
        "Work",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_text_has_language",
        "Every Text must have HAS_LANGUAGE->Language",
        "Text",
        "HAS_LANGUAGE",
        "Language",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_text_has_title",
        "Every Text must have HAS_TITLE->Nomen",
        "Text",
        "HAS_TITLE",
        "Nomen",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_text_has_license",
        "Every Text must have HAS_LICENSE->LicenseType",
        "Text",
        "HAS_LICENSE",
        "LicenseType",
    )
)

# Text — target type enforcement for optional typed rels
TRIGGERS.append(
    _rel_target_type_trigger(
        "enforce_translation_of_target",
        "TRANSLATION_OF must point to a Text node",
        "TRANSLATION_OF",
        "Text",
    )
)
TRIGGERS.append(
    _rel_target_type_trigger(
        "enforce_commentary_of_target",
        "COMMENTARY_OF must point to a Text node",
        "COMMENTARY_OF",
        "Text",
    )
)

# =========================================================================
# Edition
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_edition_has_type",
        "Every Edition must have HAS_TYPE->EditionType",
        "Edition",
        "HAS_TYPE",
        "EditionType",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_edition_edition_of",
        "Every Edition must have EDITION_OF->Text",
        "Edition",
        "EDITION_OF",
        "Text",
    )
)

# --- Diplomatic Edition must have exactly one Pagination ------------------
TRIGGERS.append(
    {
        "name": "enforce_diplomatic_edition_has_pagination",
        "description": "A diplomatic Edition must have exactly one Pagination",
        "phase": "before",
        "query": """
        CALL () {
            UNWIND $createdNodes AS n
            WITH n WHERE n:Edition
            RETURN n AS node
          UNION ALL
            UNWIND $deletedRelationships AS rel
            WITH rel WHERE type(rel) = 'PAGINATION_OF'
            WITH endNode(rel) AS n
            WHERE n:Edition
            RETURN n AS node
        }
        WITH DISTINCT node
        WHERE (node)-[:HAS_TYPE]->(:EditionType {name: 'diplomatic'})
        WITH node, count { (node)<-[:PAGINATION_OF]-(:Pagination) } AS cnt
        WHERE cnt <> 1
        WITH collect(node.id) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_diplomatic_edition_has_pagination: must have exactly one Pagination. IDs: %s',
            [apoc.text.join(ids, ', ')]
        )
        RETURN null
    """,
        "audit": """
        MATCH (e:Edition)-[:HAS_TYPE]->(:EditionType {name: 'diplomatic'})
        WITH e, count { (e)<-[:PAGINATION_OF]-(:Pagination) } AS cnt
        WHERE cnt <> 1
        RETURN e.id AS violating_id
    """,
    }
)

# =========================================================================
# Segmentation
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_segmentation_segmentation_of",
        "Every Segmentation must have SEGMENTATION_OF->Edition",
        "Segmentation",
        "SEGMENTATION_OF",
        "Edition",
    )
)

# =========================================================================
# Segment
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_segment_segment_of",
        "Every Segment must have SEGMENT_OF->Segmentation",
        "Segment",
        "SEGMENT_OF",
        "Segmentation",
    )
)

# =========================================================================
# Span — multi-target: must point to one of several labels
# =========================================================================
TRIGGERS.append(
    {
        "name": "enforce_span_span_of",
        "description": (
            "Every Span must have SPAN_OF to one of: Segment, BibliographicMetadata, Note, Attribute, Page"
        ),
        "phase": "before",
        "query": """
        CALL () {
            UNWIND $createdNodes AS n
            WITH n WHERE n:Span
            RETURN n AS node
          UNION ALL
            UNWIND $deletedRelationships AS rel
            WITH rel WHERE type(rel) = 'SPAN_OF'
            WITH startNode(rel) AS n
            WHERE n:Span
            RETURN n AS node
        }
        WITH DISTINCT node
        WHERE NOT (node)-[:SPAN_OF]->(:Segment|BibliographicMetadata|Note|Attribute|Page)
        WITH collect(coalesce(toString(node.start), 'unknown')) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_span_span_of: Span must have SPAN_OF. Count: %s',
            [toString(size(ids))]
        )
        RETURN null
    """,
        "audit": """
        MATCH (s:Span)
        WHERE NOT (s)-[:SPAN_OF]->(:Segment|BibliographicMetadata|Note|Attribute|Page)
        RETURN toString(s.start) + '-' + toString(s.end) AS violating_id
    """,
    }
)
TRIGGERS.append(
    {
        "name": "enforce_span_span_of_max_one",
        "description": "Span must have at most one SPAN_OF relationship",
        "phase": "before",
        "query": """
        CALL () {
            UNWIND $createdNodes AS n
            WITH n WHERE n:Span
            RETURN n AS node
          UNION ALL
            UNWIND $createdRelationships AS rel
            WITH rel WHERE type(rel) = 'SPAN_OF'
            WITH startNode(rel) AS n
            WHERE n:Span
            RETURN n AS node
        }
        WITH DISTINCT node
        WITH node, count { (node)-[:SPAN_OF]->() } AS cnt
        WHERE cnt > 1
        WITH collect(coalesce(toString(node.start) + '-' + toString(node.end), 'unknown')) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_span_span_of_max_one: Span must have at most one SPAN_OF. Count: %s',
            [toString(size(ids))]
        )
        RETURN null
    """,
        "audit": """
        MATCH (s:Span)
        WITH s, count { (s)-[:SPAN_OF]->() } AS cnt
        WHERE cnt > 1
        RETURN toString(s.start) + '-' + toString(s.end) AS violating_id
    """,
    }
)

# =========================================================================
# BibliographicMetadata
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_bibmeta_bibliography_of",
        "Every BibliographicMetadata must have BIBLIOGRAPHY_OF->Edition",
        "BibliographicMetadata",
        "BIBLIOGRAPHY_OF",
        "Edition",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_bibmeta_has_type",
        "Every BibliographicMetadata must have HAS_TYPE->BibliographyType",
        "BibliographicMetadata",
        "HAS_TYPE",
        "BibliographyType",
    )
)

# =========================================================================
# Note
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_note_note_of",
        "Every Note must have NOTE_OF->Edition",
        "Note",
        "NOTE_OF",
        "Edition",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_note_has_type",
        "Every Note must have HAS_TYPE->NoteType",
        "Note",
        "HAS_TYPE",
        "NoteType",
    )
)

# =========================================================================
# Attribute
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_attribute_attribute_of",
        "Every Attribute must have ATTRIBUTE_OF->Edition",
        "Attribute",
        "ATTRIBUTE_OF",
        "Edition",
    )
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_attribute_has_type",
        "Every Attribute must have HAS_TYPE->AttributeType",
        "Attribute",
        "HAS_TYPE",
        "AttributeType",
    )
)

# =========================================================================
# Pagination
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_pagination_pagination_of",
        "Every Pagination must have PAGINATION_OF->Edition",
        "Pagination",
        "PAGINATION_OF",
        "Edition",
    )
)

# =========================================================================
# Volume
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_volume_volume_of",
        "Every Volume must have VOLUME_OF->Pagination",
        "Volume",
        "VOLUME_OF",
        "Pagination",
    )
)

# =========================================================================
# Page
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_page_page_of",
        "Every Page must have PAGE_OF->Volume",
        "Page",
        "PAGE_OF",
        "Volume",
    )
)

# =========================================================================
# Contribution — multi-target BY, plus required WITH_ROLE
# =========================================================================
TRIGGERS.append(
    {
        "name": "enforce_contribution_by",
        "description": "Every Contribution must have BY->Person or BY->AI",
        "phase": "before",
        "query": """
        CALL () {
            UNWIND $createdNodes AS n
            WITH n WHERE n:Contribution
            RETURN n AS node
          UNION ALL
            UNWIND $deletedRelationships AS rel
            WITH rel WHERE type(rel) = 'BY'
            WITH startNode(rel) AS n
            WHERE n:Contribution
            RETURN n AS node
        }
        WITH DISTINCT node
        WHERE NOT (node)-[:BY]->(:Person|AI)
        WITH collect(elementId(node)) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_contribution_by: Contribution must have BY->Person|AI. Count: %s',
            [toString(size(ids))]
        )
        RETURN null
    """,
        "audit": """
        MATCH (c:Contribution)
        WHERE NOT (c)-[:BY]->(:Person|AI)
        RETURN elementId(c) AS violating_id
    """,
    }
)
TRIGGERS.append(
    {
        "name": "enforce_contribution_by_max_one",
        "description": "Contribution must have at most one BY relationship",
        "phase": "before",
        "query": """
        CALL () {
            UNWIND $createdNodes AS n
            WITH n WHERE n:Contribution
            RETURN n AS node
          UNION ALL
            UNWIND $createdRelationships AS rel
            WITH rel WHERE type(rel) = 'BY'
            WITH startNode(rel) AS n
            WHERE n:Contribution
            RETURN n AS node
        }
        WITH DISTINCT node
        WITH node, count { (node)-[:BY]->() } AS cnt
        WHERE cnt > 1
        WITH collect(elementId(node)) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_contribution_by_max_one: Contribution must have at most one BY. Count: %s',
            [toString(size(ids))]
        )
        RETURN null
    """,
        "audit": """
        MATCH (c:Contribution)
        WITH c, count { (c)-[:BY]->() } AS cnt
        WHERE cnt > 1
        RETURN elementId(c) AS violating_id
    """,
    }
)
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_contribution_with_role",
        "Every Contribution must have WITH_ROLE->RoleType",
        "Contribution",
        "WITH_ROLE",
        "RoleType",
    )
)

# =========================================================================
# Person
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_person_has_name",
        "Every Person must have HAS_NAME->Nomen",
        "Person",
        "HAS_NAME",
        "Nomen",
    )
)

# =========================================================================
# Nomen — cardinality: many, so only existence check (no max-one)
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_nomen_has_localization",
        "Every Nomen must have at least one HAS_LOCALIZATION->LocalizedText",
        "Nomen",
        "HAS_LOCALIZATION",
        "LocalizedText",
        cardinality="many",
    )
)

# =========================================================================
# LocalizedText
# =========================================================================
TRIGGERS.extend(
    _required_rel_trigger(
        "enforce_localizedtext_has_language",
        "Every LocalizedText must have HAS_LANGUAGE->Language",
        "LocalizedText",
        "HAS_LANGUAGE",
        "Language",
    )
)

# =========================================================================
# Semantic constraints — cross-cutting rules beyond simple existence/cardinality
# =========================================================================

# --- Text: TRANSLATION_OF and COMMENTARY_OF are mutually exclusive ----------
TRIGGERS.append(
    {
        "name": "enforce_text_translation_commentary_exclusive",
        "description": "A Text must not have both TRANSLATION_OF and COMMENTARY_OF",
        "phase": "before",
        "query": """
        UNWIND $createdNodes AS node
        WITH node
        WHERE node:Text
        WITH node
        WHERE (node)-[:TRANSLATION_OF]->() AND (node)-[:COMMENTARY_OF]->()
        WITH collect(node.id) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_text_translation_commentary_exclusive: Text has both TRANSLATION_OF and COMMENTARY_OF. IDs: %s',
            [apoc.text.join(ids, ', ')]
        )
        RETURN null
    """,
        "audit": """
        MATCH (t:Text)
        WHERE (t)-[:TRANSLATION_OF]->() AND (t)-[:COMMENTARY_OF]->()
        RETURN t.id AS violating_id
    """,
    }
)

# --- Self-referential prevention -------------------------------------------
# A relationship must not point back to the same node it originates from.


def _no_self_ref_trigger(
    name: str,
    description: str,
    rel_type: str,
) -> dict:
    """Generate a trigger + audit pair preventing a relationship from pointing to itself."""
    return {
        "name": name,
        "description": description,
        "phase": "before",
        "query": f"""
            UNWIND $createdRelationships AS rel
            WITH rel
            WHERE type(rel) = '{rel_type}'
            WITH rel, startNode(rel) AS source, endNode(rel) AS target
            WHERE source = target
            WITH collect(source.id) AS ids
            WHERE size(ids) > 0
            CALL apoc.util.validate(
                true,
                '{name}: {rel_type} must not be self-referential. IDs: %s',
                [apoc.text.join(ids, ', ')]
            )
            RETURN null
        """,
        "audit": f"""
            MATCH (n)-[:{rel_type}]->(n)
            RETURN n.id AS violating_id
        """,
    }


TRIGGERS.append(
    _no_self_ref_trigger(
        "enforce_no_self_translation_of",
        "TRANSLATION_OF must not point to self",
        "TRANSLATION_OF",
    )
)
TRIGGERS.append(
    _no_self_ref_trigger(
        "enforce_no_self_commentary_of",
        "COMMENTARY_OF must not point to self",
        "COMMENTARY_OF",
    )
)
TRIGGERS.append(
    _no_self_ref_trigger(
        "enforce_no_self_alternative_of",
        "ALTERNATIVE_OF must not point to self",
        "ALTERNATIVE_OF",
    )
)
TRIGGERS.append(
    _no_self_ref_trigger(
        "enforce_no_self_child_of",
        "CHILD_OF must not point to self",
        "CHILD_OF",
    )
)
TRIGGERS.append(
    _no_self_ref_trigger(
        "enforce_no_self_aligned_to",
        "ALIGNED_TO must not point to self",
        "ALIGNED_TO",
    )
)

# --- Display segmentation segments must not have ALIGNED_TO ----------------
TRIGGERS.append(
    {
        "name": "enforce_display_no_aligned_to",
        "description": "Segments of a Display segmentation must not have ALIGNED_TO relationships",
        "phase": "before",
        "query": """
        UNWIND $createdRelationships AS rel
        WITH rel
        WHERE type(rel) = 'ALIGNED_TO'
        WITH startNode(rel) AS source, endNode(rel) AS target
        WITH source, target
        WHERE (source)-[:SEGMENT_OF]->(:Segmentation:Display)
           OR (target)-[:SEGMENT_OF]->(:Segmentation:Display)
        WITH collect(DISTINCT coalesce(source.id, 'unknown')) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_display_no_aligned_to: ALIGNED_TO must not involve segments of a Display segmentation. IDs: %s',
            [apoc.text.join(ids, ', ')]
        )
        RETURN null
    """,
        "audit": """
        MATCH (seg:Segment)-[:ALIGNED_TO]-()
        WHERE (seg)-[:SEGMENT_OF]->(:Segmentation:Display)
        RETURN seg.id AS violating_id
    """,
    }
)

# --- Edition: at most one Display segmentation -----------------------------
TRIGGERS.append(
    {
        "name": "enforce_edition_single_display_segmentation",
        "description": "Each Edition must have at most one Display segmentation",
        "phase": "before",
        "query": """
        CALL () {
            UNWIND $createdNodes AS node
            WITH node
            WHERE node:Segmentation:Display
            MATCH (node)-[:SEGMENTATION_OF]->(edition:Edition)
            RETURN edition
          UNION
            UNWIND $createdRelationships AS rel
            WITH rel
            WHERE type(rel) = 'SEGMENTATION_OF'
            WITH startNode(rel) AS segmentation, endNode(rel) AS edition
            WHERE segmentation:Segmentation:Display AND edition:Edition
            RETURN edition
        }
        WITH DISTINCT edition
        MATCH (edition)<-[:SEGMENTATION_OF]-(segmentation:Segmentation:Display)
        WITH edition, collect(segmentation.id) AS segmentation_ids
        WHERE size(segmentation_ids) > 1
        CALL apoc.util.validate(
            true,
            'enforce_edition_single_display_segmentation: Edition has multiple Display segmentations. '
            + 'Edition %s has: %s',
            [edition.id, apoc.text.join(segmentation_ids, ', ')]
        )
        RETURN null
    """,
        "audit": """
        MATCH (edition:Edition)<-[:SEGMENTATION_OF]-(segmentation:Segmentation:Display)
        WITH edition, collect(segmentation.id) AS segmentation_ids
        WHERE size(segmentation_ids) > 1
        RETURN edition.id + ': ' + apoc.text.join(segmentation_ids, ', ') AS violating_id
    """,
    }
)

# --- Span: start must be strictly less than end ----------------------------
TRIGGERS.append(
    {
        "name": "enforce_span_start_lt_end",
        "description": "Span.start must be strictly less than Span.end",
        "phase": "before",
        "query": """
        UNWIND $createdNodes AS node
        WITH node
        WHERE node:Span
        WITH node
        WHERE node.start >= node.end
        WITH collect(toString(node.start) + '-' + toString(node.end)) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_span_start_lt_end: Span.start must be < Span.end. Spans: %s',
            [apoc.text.join(ids, ', ')]
        )
        RETURN null
    """,
        "audit": """
        MATCH (s:Span)
        WHERE s.start >= s.end
        RETURN toString(s.start) + '-' + toString(s.end) AS violating_id
    """,
    }
)

# --- Nomen: ALTERNATIVE_OF must not form chains ----------------------------
# If Nomen A -[:ALTERNATIVE_OF]-> Nomen B, then B must NOT itself have
# an outgoing ALTERNATIVE_OF. Only one level of indirection is allowed.
TRIGGERS.append(
    {
        "name": "enforce_nomen_no_alternative_chain",
        "description": "A Nomen that is ALTERNATIVE_OF another must not itself be the target of ALTERNATIVE_OF",
        "phase": "before",
        "query": """
        UNWIND $createdRelationships AS rel
        WITH rel
        WHERE type(rel) = 'ALTERNATIVE_OF'
        WITH startNode(rel) AS child, endNode(rel) AS parent
        WHERE (child)<-[:ALTERNATIVE_OF]-() OR (parent)-[:ALTERNATIVE_OF]->()
        WITH collect(child.id) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_nomen_no_alternative_chain: ALTERNATIVE_OF must not form chains. IDs: %s',
            [apoc.text.join(ids, ', ')]
        )
        RETURN null
    """,
        "audit": """
        MATCH (a:Nomen)-[:ALTERNATIVE_OF]->(b:Nomen)-[:ALTERNATIVE_OF]->(c:Nomen)
        RETURN b.id AS violating_id
    """,
    }
)


# =========================================================================
# Validations extracted from DatabaseValidator
# =========================================================================

# --- Work: at most one original Text --------------------------------------
# A Work can only have one Text with TEXT_OF {original: true}
TRIGGERS.append(
    {
        "name": "enforce_work_one_original_text",
        "description": "A Work can have at most one original Text (TEXT_OF with original:true)",
        "phase": "before",
        "query": """
        UNWIND $createdRelationships AS rel
        WITH rel
        WHERE type(rel) = 'TEXT_OF' AND rel.original = true
        WITH endNode(rel) AS work
        WHERE work:Work
        WITH work, count { (work)<-[:TEXT_OF {original: true}]-(:Text) } AS cnt
        WHERE cnt > 1
        WITH collect(work.id) AS ids
        WHERE size(ids) > 0
        CALL apoc.util.validate(
            true,
            'enforce_work_one_original_text: Work already has an original text. IDs: %s',
            [apoc.text.join(ids, ', ')]
        )
        RETURN null
        """,
        "audit": """
        MATCH (w:Work)<-[:TEXT_OF {original: true}]-(:Text)
        WITH w, count(*) AS cnt
        WHERE cnt > 1
        RETURN w.id AS violating_id
        """,
    }
)

# --- Text: title + language must be unique --------------------------------
TRIGGERS.append(
    {
        "name": "enforce_text_title_unique",
        "description": "Text title + language combination must be unique",
        "phase": "before",
        "query": """
        UNWIND $createdNodes AS node
        WITH node
        WHERE node:LocalizedText
        MATCH (node)<-[:HAS_LOCALIZATION]-(n:Nomen)<-[:HAS_TITLE]-(text_node:Text)-[:HAS_LANGUAGE]->(lang:Language)
        MATCH (other:LocalizedText {text: node.text})<-[:HAS_LOCALIZATION]-(:Nomen)<-[:HAS_TITLE]-(other_text:Text)
              -[:HAS_LANGUAGE]->(:Language {code: lang.code})
        WITH node.text AS text, lang.code AS code, count(DISTINCT other_text) AS text_count
        WHERE text_count > 1
        WITH collect(DISTINCT text + ':' + code) AS combos
        WHERE size(combos) > 0
        CALL apoc.util.validate(
            true,
            'enforce_text_title_unique: Duplicate title+language exists. Combos: %s',
            [apoc.text.join(combos, ', ')]
        )
        RETURN null
        """,
        "audit": """
        MATCH (t:Text)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
              -[:HAS_LANGUAGE]->(l:Language)
        WITH lt.text AS text, l.code AS code, count(*) AS cnt
        WHERE cnt > 1
        RETURN text + ':' + code AS violating_id
        """,
    }
)


async def install_triggers(driver: AsyncDriver) -> None:
    """Install all structural constraint triggers. Idempotent — safe to call on every startup."""

    async def write(tx: AsyncManagedTransaction) -> None:
        for trigger in TRIGGERS:
            await tx.run(
                "CALL apoc.trigger.install($database, $name, $statement, {phase: $phase})",
                database=DATABASE_NAME,
                name=trigger["name"],
                statement=trigger["query"],
                phase=trigger["phase"],
            )
            logger.info("Installed trigger: %s — %s", trigger["name"], trigger["description"])

    async with driver.session(database=DATABASE_NAME) as session:
        await session.execute_write(write)

    logger.info("All %d triggers installed.", len(TRIGGERS))

    async with driver.session(database=DATABASE_NAME) as session:
        await session.execute_write(write)

    logger.info("All %d triggers removed.", len(TRIGGERS))


async def audit_triggers(driver: AsyncDriver) -> dict[str, list[str]]:
    """Run all audit queries against the full database. Returns a dict of trigger name -> list of violating IDs.

    An empty dict means no violations found.
    """

    async def read(tx: AsyncManagedTransaction) -> dict[str, list[str]]:
        found: dict[str, list[str]] = {}
        for trigger in TRIGGERS:
            if "audit" not in trigger:
                continue

            result = await tx.run(trigger["audit"])
            records = await result.data()
            violating_ids = [r["violating_id"] for r in records if r["violating_id"]]
            if violating_ids:
                found[trigger["name"]] = violating_ids
                preview = ", ".join(violating_ids[:5])
                suffix = "…" if len(violating_ids) > 5 else ""
                logger.warning(
                    "Audit violation [%s]: %d nodes — %s%s",
                    trigger["name"],
                    len(violating_ids),
                    preview,
                    suffix,
                )
        return found

    async with driver.session(database=DATABASE_NAME) as session:
        violations = await session.execute_read(read)

    if not violations:
        logger.info("Audit passed: no violations found across %d constraints.", len(TRIGGERS))
    else:
        logger.warning("Audit found violations in %d constraint(s).", len(violations))

    return violations


async def list_triggers(driver: AsyncDriver) -> list[dict]:
    """List all currently installed triggers. Returns raw APOC trigger info."""

    async def read(tx: AsyncManagedTransaction) -> list[dict]:
        result = await tx.run("CALL apoc.trigger.show($database)", database=DATABASE_NAME)
        return await result.data()

    async with driver.session(database=DATABASE_NAME) as session:
        return await session.execute_read(read)
