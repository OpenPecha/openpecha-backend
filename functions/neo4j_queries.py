from typing import LiteralString, cast


class QueryDict(dict[str, str]):
    """Dictionary that casts values to LiteralString on access for Neo4j driver compatibility."""

    def __getitem__(self, key: str) -> LiteralString:
        return cast("LiteralString", super().__getitem__(key))


def _primary_nomen(label: str, relationship: str) -> str:
    return f"""
        ({label})-[:{relationship}]->({label}_n:Nomen)-[:HAS_LOCALIZATION]->
        ({label}_lt:LocalizedText)-[:HAS_LANGUAGE]->({label}_l:Language) | {{
            language: {label}_l.code,
            text: {label}_lt.text
        }}
    """


def _alternative_nomen(label: str, relationship: str) -> str:
    return f"""
        ({label})-[:{relationship}]->(:Nomen)<-[:ALTERNATIVE_OF]-({label}_an:Nomen) | [
            ({label}_an)-[:HAS_LOCALIZATION]->({label}_at:LocalizedText)-[:HAS_LANGUAGE]->({label}_al:Language) | {{
                language: {label}_al.code,
                text: {label}_at.text
            }}
        ]
    """


def _person_fragment(label: str) -> str:
    return f"""
{{
    id: {label}.id,
    bdrc: {label}.bdrc,
    wiki: {label}.wiki,
    name: [{_primary_nomen(label, "HAS_NAME")}],
    alt_names: [{_alternative_nomen(label, "HAS_NAME")}]
}}
"""


class Queries:
    def __init__(self) -> None:
        pass

    persons = QueryDict(
        {
            "fetch_by_id": f"""
    MATCH (person:Person {{id: $id}})
    RETURN {_person_fragment("person")} AS person
    """,
            "fetch_all": f"""
    MATCH (person:Person)
    RETURN {_person_fragment("person")} AS person
    SKIP $offset LIMIT $limit
    """,
            "create": """
    MATCH (n:Nomen {id: $primary_nomen_id})
    CREATE (p:Person {id: $id, bdrc: $bdrc, wiki: $wiki})
    CREATE (p)-[:HAS_NAME]->(n)
    RETURN p.id as person_id
    """,
        }
    )

    nomens = QueryDict(
        {
            "create": """
    OPTIONAL MATCH (primary:Nomen {id: $primary_nomen_id})
    CREATE (n:Nomen {id: $nomen_id})
    WITH n, primary
    CALL (*) {
        WHEN primary IS NOT NULL THEN { CREATE (n)-[:ALTERNATIVE_OF]->(primary) }
    }
    FOREACH (lt IN $localized_texts |
        MERGE (l:Language {code: lt.base_lang_code})
        CREATE (n)-[:HAS_LOCALIZATION]->(locText:LocalizedText {text: lt.text})
            -[:HAS_LANGUAGE {bcp47: lt.bcp47_tag}]->(l)
    )
    RETURN n.id as nomen_id
    """,
        }
    )

    sections = QueryDict(
        {
            "create_batch": """
    MATCH (a:Annotation {id: $annotation_id})
    UNWIND $sections AS sec
    CREATE (s:Section {
        id: sec.id,
        title: sec.title
    })
    CREATE (s)-[:SECTION_OF]->(a)
    WITH s, sec.segments AS segment_ids
    UNWIND segment_ids AS segment_id
    MATCH (seg:Segment {id: segment_id})
    CREATE (seg)-[:PART_OF]->(s)
    """,
            "delete_sections": """
    MATCH (a:Annotation {id: $annotation_id})
    OPTIONAL MATCH (s:Section)-[r1:SECTION_OF]->(a)
    OPTIONAL MATCH (seg:Segment)-[r2:PART_OF]->(s)
    DELETE r1, r2
    """,
        }
    )

    segments = QueryDict(
        {
            "delete_all_segments_by_annotation_id": """
    MATCH (a:Annotation {id: $annotation_id})<-[:SEGMENTATION_OF]-(s:Segment)
    OPTIONAL MATCH (s)-[:HAS_REFERENCE]->(ref:Reference)
    DELETE ref
    DETACH DELETE s
    """,
            "delete_alignment_segments": """
    MATCH (source_ann:Annotation {id: $source_annotation_id})<-[:SEGMENTATION_OF]-(source_seg:Segment)
    MATCH (target_ann:Annotation {id: $target_annotation_id})<-[:SEGMENTATION_OF]-(target_seg:Segment)
    OPTIONAL MATCH (source_seg)-[aligned:ALIGNED_TO]-(target_seg)
    DELETE aligned
    DETACH DELETE source_seg, target_seg
    """,
            "create_batch": """
    MATCH (a:Annotation {id: $annotation_id})
    UNWIND $segments AS seg
    CREATE (s:Segment {
        id: seg.id,
        span_start: seg.span.start,
        span_end: seg.span.end
    })
    CREATE (s)-[:SEGMENTATION_OF]->(a)
    """,
            "create_alignments_batch": """
    UNWIND $alignments AS alignment
    MATCH (source:Segment {id: alignment.source_id})
    MATCH (target:Segment {id: alignment.target_id})
    CREATE (source)-[:ALIGNED_TO]->(target)
    """,
            "find_by_span": """
    MATCH (m:Manifestation {id: $manifestation_id})
        <-[:ANNOTATION_OF]-(:Annotation)
        <-[:SEGMENTATION_OF]-(seg:Segment)
    WHERE seg.span_start <= $span_end AND seg.span_end >= $span_start
    RETURN seg.id as segment_id,
        seg.span_start as span_start,
        seg.span_end as span_end
    """,
            "find_aligned_segments_outgoing": """
    MATCH (source_seg:Segment {id: $segment_id})-[:ALIGNED_TO]->(target_seg:Segment)
        -[:SEGMENTATION_OF]->(:Annotation)
        -[:ANNOTATION_OF]->(m:Manifestation)
        -[:MANIFESTATION_OF]->(e:Expression)
    WITH m.id as manifestation_id, e.id as expression_id,
        collect({
            segment_id: target_seg.id,
            span_start: target_seg.span_start,
            span_end: target_seg.span_end
        }) as segments
    RETURN manifestation_id, expression_id, segments
    """,
            "find_aligned_segments_incoming": """
    MATCH (source_seg:Segment {id: $segment_id})<-[:ALIGNED_TO]-(source_of_seg:Segment)
        -[:SEGMENTATION_OF]->(:Annotation)
        -[:ANNOTATION_OF]->(m:Manifestation)
        -[:MANIFESTATION_OF]->(e:Expression)
    WITH m.id as manifestation_id, e.id as expression_id,
        collect({
            segment_id: source_of_seg.id,
            span_start: source_of_seg.span_start,
            span_end: source_of_seg.span_end
        }) as segments
    RETURN manifestation_id, expression_id, segments
    """,
            "get_aligned_segments": """
    MATCH (a1:Annotation {id: $alignment_1_id})<-[:SEGMENTATION_OF]-(s1:Segment)
    WHERE s1.span_start < $span_end AND s1.span_end > $span_start
    MATCH (s1)-[:ALIGNED_TO]-(s2:Segment)
    RETURN DISTINCT s2.id as segment_id,
        s2.span_start as span_start,
        s2.span_end as span_end
    ORDER BY s2.span_start
    """,
            "get_by_id": """
    MATCH (seg:Segment {id: $segment_id})
        -[:SEGMENTATION_OF]->(:Annotation)
        -[:ANNOTATION_OF]->(m:Manifestation)
        -[:MANIFESTATION_OF]->(e:Expression)
    RETURN seg.id as segment_id,
        seg.span_start as span_start,
        seg.span_end as span_end,
        m.id as manifestation_id,
        e.id as expression_id
    """,
            "get_batch_by_ids": """
    UNWIND $segment_ids AS segment_id
    MATCH (seg:Segment {id: segment_id})
    RETURN seg.id as segment_id,
        seg.span_start as span_start,
        seg.span_end as span_end
    ORDER BY seg.id
    """,
            "find_related_alignment_only": """
    MATCH (source_manif:Manifestation {id: $manifestation_id})
        <-[:ANNOTATION_OF]-(align_annot:Annotation)
        -[:HAS_TYPE]->(at:AnnotationType {name: 'alignment'})
    MATCH (align_annot)<-[:SEGMENTATION_OF]-(source_seg:Segment)
    WHERE source_seg.span_start < $span_end AND source_seg.span_end > $span_start

    // Follow bidirectional ALIGNED_TO relationships
    MATCH (source_seg)-[:ALIGNED_TO]-(target_seg:Segment)
    MATCH (target_seg)-[:SEGMENTATION_OF]->(target_align_annot:Annotation)
        -[:HAS_TYPE]->(tat:AnnotationType {name: 'alignment'})
    MATCH (target_align_annot)-[:ANNOTATION_OF]->(target_manif:Manifestation)
    MATCH (target_manif)-[:MANIFESTATION_OF]->(target_expr:Expression)

    WITH target_manif, target_expr, COLLECT(DISTINCT target_seg) as target_segments

    RETURN
        target_manif.id as manifestation_id,
        target_expr.id as expression_id,
        [seg IN target_segments | {
            id: seg.id,
            span_start: seg.span_start,
            span_end: seg.span_end
        }] as segments
    """,
            "find_related_with_transfer": """
    // Step 1: Find overlapping segments in source segmentation annotation
    MATCH (source_manif:Manifestation {id: $manifestation_id})
        <-[:ANNOTATION_OF]-(source_seg_annot:Annotation)
        -[:HAS_TYPE]->(sat:AnnotationType {name: 'segmentation'})
    MATCH (source_seg_annot)<-[:SEGMENTATION_OF]-(source_seg_seg:Segment)
    WHERE source_seg_seg.span_start < $span_end AND source_seg_seg.span_end > $span_start

    WITH source_manif,
        MIN(source_seg_seg.span_start) as expanded_start,
        MAX(source_seg_seg.span_end) as expanded_end

    // Step 2: Find ALL alignment annotations in source manifestation
    MATCH (source_manif)<-[:ANNOTATION_OF]-(source_align_annot:Annotation)
        -[:HAS_TYPE]->(aat:AnnotationType {name: 'alignment'})
    MATCH (source_align_annot)<-[:SEGMENTATION_OF]-(source_align_seg:Segment)
    WHERE source_align_seg.span_start < expanded_end AND source_align_seg.span_end > expanded_start

    // Step 3: Follow ALIGNED_TO to target alignment segments
    MATCH (source_align_seg)-[:ALIGNED_TO]-(target_align_seg:Segment)
    MATCH (target_align_seg)-[:SEGMENTATION_OF]->(target_align_annot:Annotation)
        -[:HAS_TYPE]->(taat:AnnotationType {name: 'alignment'})
    MATCH (target_align_annot)-[:ANNOTATION_OF]->(target_manif:Manifestation)
    MATCH (target_manif)-[:MANIFESTATION_OF]->(target_expr:Expression)

    // Step 4: Find overlapping segments in target segmentation annotation
    MATCH (target_manif)<-[:ANNOTATION_OF]-(target_seg_annot:Annotation)
        -[:HAS_TYPE]->(tsat:AnnotationType {name: 'segmentation'})
    MATCH (target_seg_annot)<-[:SEGMENTATION_OF]-(target_seg_seg:Segment)
    WHERE target_seg_seg.span_start < target_align_seg.span_end
        AND target_seg_seg.span_end > target_align_seg.span_start

    // Step 5: Collect and group by target manifestation
    WITH target_manif, target_expr, COLLECT(DISTINCT target_seg_seg) as target_segments

    RETURN
        target_manif.id as manifestation_id,
        target_expr.id as expression_id,
        [seg IN target_segments | {
            id: seg.id,
            span_start: seg.span_start,
            span_end: seg.span_end
        }] as segments
    """,
            "get_related_segments": """
    MATCH (a1:Annotation {id: $alignment_1_id})<-[:SEGMENTATION_OF]-(s1:Segment)
    WHERE s1.span_start < $span_end AND s1.span_end > $span_start
    MATCH (s1)-[:ALIGNED_TO]-(s2:Segment)
    RETURN DISTINCT s2.id as segment_id,
        s2.span_start as span_start,
        s2.span_end as span_end
    ORDER BY s2.span_start
    """,
            "get_overlapping_segments": """
    MATCH (m:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(ann:Annotation)
        -[:HAS_TYPE]->(:AnnotationType {name: 'segmentation'})
    MATCH (ann)<-[:SEGMENTATION_OF]-(s:Segment)
    MATCH (m)-[:MANIFESTATION_OF]->(e:Expression)
    WHERE s.span_start < $span_end AND s.span_end > $span_start
    RETURN s.id as segment_id,
        s.span_start as span_start,
        s.span_end as span_end,
        e.id as expression_id
    ORDER BY s.span_start
    """,
            "get_overlapping_segments_batch": """
    UNWIND $segment_ids AS input_segment_id
    MATCH (input_seg:Segment {id: input_segment_id})
        -[:SEGMENTATION_OF]->(input_ann:Annotation)
        -[:ANNOTATION_OF]->(m:Manifestation)
    MATCH (seg_ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'segmentation'})
    MATCH (seg_ann)-[:ANNOTATION_OF]->(m)
    MATCH (seg:Segment)-[:SEGMENTATION_OF]->(seg_ann)
    WHERE seg.span_start < input_seg.span_end AND seg.span_end > input_seg.span_start
    RETURN input_segment_id,
        collect(seg.id) as overlapping_segments
    """,
            "find_related_by_segment_id": """
    MATCH (source_seg:Segment {id: $segment_id})
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(source_manif:Manifestation)

    MATCH (source_seg)-[:ALIGNED_TO*1..10]-(related_seg:Segment)
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(related_manif:Manifestation)
        -[:MANIFESTATION_OF]->(related_expr:Expression)
    WHERE related_manif <> source_manif

    MATCH (related_span:Span)-[:SPAN_OF]->(related_seg)

    RETURN related_manif.id as manifestation_id, related_expr.id as expression_id,
        COLLECT(DISTINCT {
            id: related_seg.id,
            span_start: related_span.start,
            span_end: related_span.end
        }) as segments
    """,
            "find_segments_by_span": """
    MATCH (manif:Manifestation {id: $manifestation_id})
        <-[:SEGMENTATION_OF]-(:Segmentation)
        <-[:SEGMENT_OF]-(seg:Segment)
        <-[:SPAN_OF]-(span:Span)
    WHERE span.start < $span_end AND span.end > $span_start
    RETURN DISTINCT seg.id as segment_id
    """,
            "fetch_by_id": """
    MATCH (seg:Segment {id: $segment_id})
        -[:SEGMENT_OF]->(:Segmentation)
        -[:SEGMENTATION_OF]->(manif:Manifestation)
        -[:MANIFESTATION_OF]->(expr:Expression)
    MATCH (span:Span)-[:SPAN_OF]->(seg)
    RETURN seg.id as segment_id,
        manif.id as manifestation_id,
        expr.id as expression_id,
        span.start as span_start,
        span.end as span_end
    """,
        }
    )

    references = QueryDict(
        {
            "create": """
    CREATE (r:Reference {
        id: $reference_id,
        name: $name
    })
    RETURN r.id as reference_id
    """,
            "create_batch": """
    UNWIND $references AS ref
    MERGE (r:Reference {id: ref.id})
    SET r.name = ref.name,
        r.description = ref.description
    RETURN r.id as reference_id
    """,
            "link_to_segments": """
    UNWIND $segment_references AS sr
    MATCH (s:Segment {id: sr.segment_id})
    MATCH (r:Reference {id: sr.reference_id})
    CREATE (s)-[:HAS_REFERENCE]->(r)
    """,
        }
    )

    ai = QueryDict(
        {
            "find_or_create": """
        MERGE (ai:AI {id: $ai_id})
        RETURN elementId(ai) AS ai_element_id
    """
        }
    )

    categories = QueryDict(
        {
            "create": """
    CREATE (c:Category {id: $category_id, application: $application})
    CREATE (n:Nomen)
    CREATE (c)-[:HAS_TITLE]->(n)
    WITH c, n
    FOREACH (lt IN $localized_texts |
        MERGE (l:Language {code: lt.language})
        CREATE (n)-[:HAS_LOCALIZATION]->(locText:LocalizedText {text: lt.text})-[:HAS_LANGUAGE]->(l)
    )
    WITH c
    CALL (*) {
        WITH c
        OPTIONAL MATCH (parent:Category {id: $parent_id})
        FOREACH (_ IN CASE WHEN parent IS NOT NULL THEN [1] ELSE [] END |
            CREATE (c)-[:HAS_PARENT]->(parent)
        )
    }
    RETURN c.id AS category_id
    """,
            "get_categories": """
    MATCH (c:Category {application: $application})
    WHERE
        CASE
            WHEN $parent_id IS NULL THEN NOT (c)-[:HAS_PARENT]->(:Category)
            ELSE EXISTS((c)-[:HAS_PARENT]->(:Category {id: $parent_id}))
        END
    OPTIONAL MATCH (c)-[:HAS_PARENT]->(parent:Category)
    OPTIONAL MATCH (c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
                -[:HAS_LANGUAGE]->(l:Language {code: $language})
    OPTIONAL MATCH (child:Category)-[:HAS_PARENT]->(c)
    WITH c, parent, lt, COUNT(DISTINCT child) AS child_count
    RETURN c.id AS id, parent.id AS parent, lt.text AS title, child_count > 0 AS has_child
    """,
            "find_existing_category": """
    MATCH (c:Category {application: $application})
    WHERE
        CASE
            WHEN $parent_id IS NULL THEN NOT (c)-[:HAS_PARENT]->(:Category)
            ELSE EXISTS((c)-[:HAS_PARENT]->(:Category {id: $parent_id}))
        END
    MATCH (c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[:HAS_LANGUAGE]->(l:Language)
    WHERE l.code = $language AND toLower(lt.text) = toLower($title_text)
    RETURN c.id AS category_id
    LIMIT 1
    """,
        }
    )

    works = QueryDict(
        {
            "link_to_category": """
    MATCH (w:Work {id: $work_id})
    MATCH (c:Category {id: $category_id})
    CREATE (w)-[:BELONGS_TO]->(c)
    """,
        }
    )

    enum = QueryDict(
        {
            "create_language": """
    CREATE (l:Language {code: toLower($code), name: toLower($name)})
    RETURN l.id as language_id
    """,
            "list_languages": """
    MATCH (l:Language)
    RETURN l.code AS code, l.name AS name
    ORDER BY name ASC
    """,
            "create_bibliography": """
    CREATE (bt:BibliographyType {name: toLower($name)})
    RETURN bt.id as bibliography_type_id
    """,
            "list_bibliography": """
    MATCH (bt:BibliographyType)
    RETURN bt.name AS name
    ORDER BY name ASC
    """,
            "create_manifestation": """
    CREATE (mt:ManifestationType {name: toLower($name)})
    RETURN mt.id as manifestation_type_id
    """,
            "list_manifestation": """
    MATCH (mt:ManifestationType)
    RETURN mt.name AS name
    ORDER BY name ASC
    """,
            "create_role": """
    CREATE (rt:RoleType {name: toLower($name), description: $description})
    RETURN rt.id as role_type_id
    """,
            "list_role": """
    MATCH (rt:RoleType)
    RETURN rt.name AS name, rt.description AS description
    ORDER BY name ASC
    """,
            "create_annotation": """
    CREATE (at:AnnotationType {name: toLower($name)})
    RETURN at.id as annotation_type_id
    """,
            "list_annotation": """
    MATCH (at:AnnotationType)
    RETURN at.name AS name
    ORDER BY name ASC
    """,
        }
    )
