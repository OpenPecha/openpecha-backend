class Queries:
    @staticmethod
    def primary_nomen(label, relationship):
        return f"""
        ({label})-[:{relationship}]->({label}_n:Nomen)-[:HAS_LOCALIZATION]->
        ({label}_lt:LocalizedText)-[:HAS_LANGUAGE]->({label}_l:Language) | {{
            language: {label}_l.code,
            text: {label}_lt.text
        }}
    """

    @staticmethod
    def alternative_nomen(label, relationship):
        return f"""
        ({label})-[:{relationship}]->(:Nomen)<-[:ALTERNATIVE_OF]-({label}_an:Nomen) | [
            ({label}_an)-[:HAS_LOCALIZATION]->({label}_at:LocalizedText)-[:HAS_LANGUAGE]->({label}_al:Language) | {{
                language: {label}_al.code,
                text: {label}_at.text
            }}
        ]
    """

    @staticmethod
    def manifestation_alignment_sources(label):
        return f"""
        [({label})<-[:ANNOTATION_OF]-(target_ann:Annotation)
         <-[:ALIGNED_TO]-(source_ann:Annotation)
         -[:ANNOTATION_OF]->(source_m:Manifestation) | source_m.id]
    """

    @staticmethod
    def manifestation_alignment_targets(label):
        return f"""
        [({label})<-[:ANNOTATION_OF]-(this_ann:Annotation)
         -[:ALIGNED_TO]->(target_ann:Annotation)
         -[:ANNOTATION_OF]->(target_m:Manifestation) | target_m.id]
    """

    @staticmethod
    def person_fragment(label):
        return f"""
{{
    id: {label}.id,
    bdrc: {label}.bdrc,
    wiki: {label}.wiki,
    name: [{Queries.primary_nomen(label, "HAS_NAME")}],
    alt_names: [{Queries.alternative_nomen(label, "HAS_NAME")}]
}}
"""

    @staticmethod
    def expression_compact_fragment(label):
        return f"""
{{
    id: {label}.id,
    title: [{Queries.primary_nomen(label, "HAS_TITLE")}],
    language: [({label})-[:HAS_LANGUAGE]->({label}_l:Language) | {label}_l.code][0],
    type: {Queries.infer_expression_type(label)},
}}
"""

    @staticmethod
    def manifestation_fragment(label):
        return f"""
{{
    id: {label}.id,
    bdrc: {label}.bdrc,
    wiki: {label}.wiki,
    type: [({label})-[:HAS_TYPE]->(mf_mt:ManifestationType) | mf_mt.name][0],
    annotations: [
        ({label})<-[:ANNOTATION_OF]-(mf_ann:Annotation) | {{
            id: mf_ann.id,
            type: [(mf_ann)-[:HAS_TYPE]->(mf_at:AnnotationType) | mf_at.name][0],
            aligned_to: [(mf_ann)-[:ALIGNED_TO]->(mf_target:Annotation) | mf_target.id][0]
        }}
    ],
    colophon: {label}.colophon,
    copyright: [({label})-[:HAS_COPYRIGHT]->(mf_cs:CopyrightStatus) | mf_cs.name][0],
    incipit_title: [{Queries.primary_nomen(label, 'HAS_INCIPIT_TITLE')}],
    alt_incipit_titles: [{Queries.alternative_nomen(label, 'HAS_INCIPIT_TITLE')}],
    alignment_sources: {Queries.manifestation_alignment_sources(label)},
    alignment_targets: {Queries.manifestation_alignment_targets(label)}
}}
"""

    @staticmethod
    def infer_expression_type(label):
        """Fragment to infer expression type from relationships instead of HAS_TYPE"""
        return f"""
CASE
    WHEN ({label})-[:EXPRESSION_OF {{original: false}}]->(:Work) THEN 'translation'
    WHEN ({label})-[:COMMENTARY_OF]->(:Expression) THEN 'commentary'
    WHEN ({label})-[:EXPRESSION_OF {{original: true}}]->(:Work) THEN 'root'
    ELSE null
END
"""

    @staticmethod
    def expression_fragment(label):
        return f"""
{{
    id: {label}.id,
    bdrc: {label}.bdrc,
    wiki: {label}.wiki,
    type: {Queries.infer_expression_type(label)},
    target: COALESCE(
        [({label})-[:TRANSLATION_OF]->(ef_target:Expression) | ef_target.id][0],
        [({label})-[:COMMENTARY_OF]->(ef_target:Expression) | ef_target.id][0]
    ),
    contributors: (
        [({label})-[:HAS_CONTRIBUTION]->(ef_contrib:Contribution)-[:BY]->(ef_person:Person) | {{
            person_id: ef_person.id,
            person_bdrc_id: ef_person.bdrc,
            role: [(ef_contrib)-[:WITH_ROLE]->(ef_role:RoleType) | ef_role.name][0]
        }}]
        +
        [({label})-[:HAS_CONTRIBUTION]->(ef_contrib:Contribution)-[:BY]->(ef_ai:AI) | {{
            ai_id: ef_ai.id,
            role: [(ef_contrib)-[:WITH_ROLE]->(ef_role:RoleType) | ef_role.name][0]
        }}]
    ),
    date: {label}.date,
    title: [{Queries.primary_nomen(label, 'HAS_TITLE')}],
    alt_titles: [{Queries.alternative_nomen(label, 'HAS_TITLE')}],
    language: [({label})-[:HAS_LANGUAGE]->(ef_lang:Language) | ef_lang.code][0],
    category_id: [({label})-[:EXPRESSION_OF]->(ef_work:Work)-[:BELONGS_TO]->(ef_cat:Category) | ef_cat.id][0]
}}
"""

    @staticmethod
    def create_expression_base(label):
        return f"CREATE ({label}:Expression {{id: $expression_id, bdrc: $bdrc, wiki: $wiki, date: $date}})"


Queries.expressions = {
    "fetch_by_id": f"""
    MATCH (e:Expression {{id: $id}})

    RETURN {Queries.expression_fragment('e')} AS expression
""",
    "fetch_by_bdrc": f"""
    MATCH (e:Expression {{bdrc: $bdrc_id}})

    RETURN {Queries.expression_fragment('e')} AS expression
""",
    "fetch_all": f"""
    MATCH (e:Expression)
    WITH e
    WHERE ($type IS NULL OR {Queries.infer_expression_type('e')} = $type)
    AND ($language IS NULL OR [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0] = $language)

    OFFSET $offset
    LIMIT $limit

    RETURN {Queries.expression_fragment('e')} AS expression
""",
    "fetch_related": f"""
    MATCH (e:Expression {{id: $id}})

    RETURN [
        sibling IN [(e)-[:EXPRESSION_OF]->(work:Work)<-[:EXPRESSION_OF]-
        (s:Expression) WHERE s.id <> $id | s] |
            {Queries.expression_compact_fragment('sibling')}
    ] AS related_expressions
""",
    "create_standalone": f"""
CREATE (w:Work {{id: $work_id}})
{Queries.create_expression_base('e')}
WITH w, e
MATCH (n:Nomen) WHERE elementId(n) = $title_nomen_element_id
MATCH (l:Language {{code: $language_code}})
CREATE (e)-[:EXPRESSION_OF {{original: $original}}]->(w),
       (e)-[:HAS_LANGUAGE {{bcp47: $bcp47_tag}}]->(l),
       (e)-[:HAS_TITLE]->(n)
RETURN e.id as expression_id
""",
    "create_contribution": """
MATCH (e:Expression {id: $expression_id})
MATCH (p:Person) WHERE (($person_id IS NOT NULL AND p.id = $person_id)
                        OR ($person_bdrc_id IS NOT NULL AND p.bdrc = $person_bdrc_id))
MATCH (rt:RoleType {name: $role_name})
CREATE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(p),
       (c)-[:WITH_ROLE]->(rt)
RETURN elementId(c) as contribution_element_id
""",
    "create_ai_contribution": """
MATCH (e:Expression {id: $expression_id})
MATCH (ai: AI) WHERE elementId(ai) = $ai_element_id
MATCH (rt:RoleType {name: $role_name})
CREATE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(ai),
       (c)-[:WITH_ROLE]->(rt)
RETURN elementId(c) as contribution_element_id
""",
    "create_translation": f"""
MATCH (target:Expression {{id: $target_id}})-[:EXPRESSION_OF]->(w:Work)
{Queries.create_expression_base('e')}
WITH target, w, e
MATCH (n:Nomen) WHERE elementId(n) = $title_nomen_element_id
MATCH (l:Language {{code: $language_code}})
CREATE (e)-[:EXPRESSION_OF {{original: false}}]->(w),
       (e)-[:TRANSLATION_OF]->(target),
       (e)-[:HAS_LANGUAGE {{bcp47: $bcp47_tag}}]->(l),
       (e)-[:HAS_TITLE]->(n)
RETURN e.id as expression_id
""",
    "create_commentary": f"""
MATCH (target:Expression {{id: $target_id}})
CREATE (commentary_work:Work {{id: $work_id}})
{Queries.create_expression_base('e')}
WITH target, commentary_work, e
MATCH (n:Nomen) WHERE elementId(n) = $title_nomen_element_id
MATCH (l:Language {{code: $language_code}})
CREATE (e)-[:COMMENTARY_OF]->(target),
       (e)-[:EXPRESSION_OF {{original: true}}]->(commentary_work),
       (e)-[:HAS_LANGUAGE {{bcp47: $bcp47_tag}}]->(l),
       (e)-[:HAS_TITLE]->(n)
RETURN e.id as expression_id
""",
}

Queries.persons = {
    "fetch_by_id": f"""
MATCH (person:Person {{id: $id}})
RETURN {Queries.person_fragment('person')} AS person
""",
    "fetch_all": f"""
MATCH (person:Person)
RETURN {Queries.person_fragment('person')} AS person
SKIP $offset LIMIT $limit
""",
    "create": """
MATCH (n:Nomen) WHERE elementId(n) = $primary_name_element_id
CREATE (p:Person {id: $id, bdrc: $bdrc, wiki: $wiki})
CREATE (p)-[:HAS_NAME]->(n)
RETURN p.id as person_id
""",
}

Queries.nomens = {
    "create": """
OPTIONAL MATCH (primary:Nomen)
WHERE elementId(primary) = $primary_name_element_id
CREATE (n:Nomen)
WITH n, primary
CALL (*) {
    WHEN primary IS NOT NULL THEN { CREATE (n)-[:ALTERNATIVE_OF]->(primary) }
}
FOREACH (lt IN $localized_texts |
    MERGE (l:Language {code: lt.base_lang_code})
    CREATE (n)-[:HAS_LOCALIZATION]->(locText:LocalizedText {text: lt.text})-[:HAS_LANGUAGE {bcp47: lt.bcp47_tag}]->(l)
)
RETURN elementId(n) as element_id
""",
}

Queries.manifestations = {
    "fetch": f"""
    MATCH (m:Manifestation)
    WHERE ($manifestation_id IS NOT NULL AND m.id = $manifestation_id) OR
          ($expression_id IS NOT NULL AND (m)-[:MANIFESTATION_OF]->(:Expression {{id: $expression_id}}))
    MATCH (m)-[:MANIFESTATION_OF]->(e:Expression)
    WHERE $manifestation_type IS NULL OR [(m)-[:HAS_TYPE]->(mt:ManifestationType) | mt.name][0] = $manifestation_type

    RETURN {Queries.manifestation_fragment('m')} AS manifestation, e.id AS expression_id
""",
    "fetch_by_annotation_id": f"""
    MATCH (a:Annotation {{id: $annotation_id}})-[:ANNOTATION_OF]->(m:Manifestation)
    RETURN m.id AS manifestation_id
""",
    "fetch_by_annotation": f"""
    MATCH (a:Annotation {{id: $annotation_id}})-[:ANNOTATION_OF]->(m:Manifestation)
    MATCH (m)-[:MANIFESTATION_OF]->(e:Expression)

    RETURN {Queries.manifestation_fragment('m')} AS manifestation, e.id AS expression_id
""",
    "create": """
MATCH (e:Expression {id: $expression_id})
OPTIONAL MATCH (it:Nomen)
  WHERE elementId(it) = $incipit_element_id
MERGE (mt:ManifestationType {name: $type})
MERGE (cs:CopyrightStatus {name: $copyright})
CREATE (m:Manifestation {
  id: $manifestation_id,
  bdrc: $bdrc,
  wiki: $wiki,
  colophon: $colophon
})
WITH m, e, mt, cs, it

CREATE (m)-[:MANIFESTATION_OF]->(e),
       (m)-[:HAS_TYPE]->(mt),
       (m)-[:HAS_COPYRIGHT]->(cs)
CALL (*) {
  WHEN it IS NOT NULL THEN { CREATE (m)-[:HAS_INCIPIT_TITLE]->(it) }
}
RETURN m.id AS manifestation_id
""",
    "find_related_instances": f"""
    // Find all alignment annotations on the given manifestation
    MATCH (m:Manifestation {{id: $manifestation_id}})
          <-[:ANNOTATION_OF]-(ann:Annotation)
          -[:HAS_TYPE]->(at:AnnotationType {{name: 'alignment'}})
    
    // Case 1: This annotation has aligned_to (manifestation is translation/commentary)
    // Follow the aligned_to relationship to find the target manifestation
    OPTIONAL MATCH (ann)-[:ALIGNED_TO]->(target_ann:Annotation)
                  -[:ANNOTATION_OF]->(related_m1:Manifestation)
                  -[:MANIFESTATION_OF]->(related_e1:Expression)
    
    // Case 2: Other annotations point to this one (manifestation is root)
    // Find source annotations that have aligned_to pointing to this annotation
    OPTIONAL MATCH (source_ann:Annotation)-[:ALIGNED_TO]->(ann)
    OPTIONAL MATCH (source_ann)-[:ANNOTATION_OF]->(related_m2:Manifestation)
                              -[:MANIFESTATION_OF]->(related_e2:Expression)
    
    // Combine both cases
    WITH COALESCE(related_m1, related_m2) as related_m,
         COALESCE(related_e1, related_e2) as related_e,
         related_m1, related_m2,
         ann, source_ann
    WHERE related_m IS NOT NULL
    
    RETURN DISTINCT {{
        manifestation: {Queries.manifestation_fragment('related_m')},
        expression: {Queries.expression_fragment('related_e')},
        alignment_annotation_id: CASE 
            WHEN related_m1 IS NOT NULL THEN ann.id 
            ELSE source_ann.id 
        END
    }} as related_instance
""",
    "find_expression_related_instances": f"""
    // First, find the expression for the given manifestation
    MATCH (m:Manifestation {{id: $manifestation_id}})-[:MANIFESTATION_OF]->(e:Expression)
    
    // Find any expression-level relationships (both to and from)
    MATCH (e)-[:TRANSLATION_OF|:COMMENTARY_OF]-(related_e:Expression)
    MATCH (related_e)<-[:MANIFESTATION_OF]-(related_m:Manifestation)
    
    // Exclude the original manifestation
    WHERE related_m.id <> $manifestation_id
    
    RETURN DISTINCT {{
        manifestation: {Queries.manifestation_fragment('related_m')},
        expression: {Queries.expression_fragment('related_e')},
        alignment_annotation_id: null
    }} as related_instance
""",
}

Queries.annotations = {
    "delete": """
MATCH (a:Annotation {id: $annotation_id})
DETACH DELETE a
""",
    "create": """
MATCH (m:Manifestation {id: $manifestation_id})
MERGE (at:AnnotationType {name: $type})
WITH m, at
OPTIONAL MATCH (target:Annotation {id: $aligned_to_id})

CREATE (a:Annotation {id: $annotation_id})-[:HAS_TYPE]->(at),
       (a)-[:ANNOTATION_OF]->(m)

CALL (*) {
    WHEN target IS NOT NULL THEN { CREATE (a)-[:ALIGNED_TO]->(target) }
}

RETURN a.id AS annotation_id
""",
    "get_annotation_type": """
MATCH (a:Annotation {id: $annotation_id})-[:HAS_TYPE]->(at:AnnotationType)
RETURN at.name as annotation_type
""",
    "get_aligned_annotation": """
MATCH (a:Annotation {id: $annotation_id})-[:ALIGNED_TO]->(target_ann:Annotation)
RETURN target_ann.id as aligned_to_id
""",
    "get_alignment_pair": """
MATCH (a:Annotation {id: $annotation_id})
OPTIONAL MATCH (a)-[:ALIGNED_TO]->(target:Annotation)
OPTIONAL MATCH (source:Annotation)-[:ALIGNED_TO]->(a)
WITH a, 
     CASE WHEN target IS NOT NULL THEN a.id ELSE source.id END as source_id,
     CASE WHEN target IS NOT NULL THEN target.id ELSE a.id END as target_id
RETURN source_id, target_id
""",
    "get_alignment_pairs_by_manifestation": """
MATCH (m:Manifestation {id: $manifestation_id})
MATCH (m)-[:ANNOTATION_OF]->(a:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'alignment'})
WITH a, m.id as manifestation_id

RETURN manifestation_id, a.id as alignment_1_id, a.id as alignment_2_id
""",
    "delete_alignment_annotations": """
MATCH (source:Annotation {id: $source_annotation_id})
MATCH (target:Annotation {id: $target_annotation_id})
OPTIONAL MATCH (source)-[aligned:ALIGNED_TO]-(target)
DELETE aligned
DETACH DELETE source, target
""",
    "get_segments": """
MATCH (a:Annotation {id: $annotation_id})
<-[:SEGMENTATION_OF]-(s:Segment)
OPTIONAL MATCH (s)-[:HAS_REFERENCE]->(r:Reference)
OPTIONAL MATCH (s)-[:HAS_TYPE]->(bt:BibliographyType)
RETURN s.id as id,
       s.span_start as start,
       s.span_end as end,
       r.name as reference,
       bt.name as bibliography_type
ORDER BY s.span_start
""",
    "get_sections": """
MATCH (a:Annotation {id: $annotation_id})
<-[:SECTION_OF]-(s:Section)
OPTIONAL MATCH (seg:Segment)-[:PART_OF]->(s)
WITH s, collect(seg.id) as segment_ids
RETURN s.id as id, s.title as title, segment_ids as segments
ORDER BY s.title
""",
    "get_alignment_indices": """
// First, get all target segments ordered by span to establish indices
MATCH (target_ann:Annotation {id: $target_annotation_id})<-[:SEGMENTATION_OF]-(all_targets:Segment)
WITH all_targets
ORDER BY all_targets.span_start
WITH collect(all_targets) as ordered_all_targets

// Create a map of segment_id -> index
UNWIND range(0, size(ordered_all_targets)-1) as idx
WITH ordered_all_targets[idx].id as seg_id, idx as segment_index

// Now find which target segments this source aligns to
MATCH (source:Segment {id: $source_segment_id})-[:ALIGNED_TO]->(aligned:Segment)
WHERE aligned.id = seg_id

RETURN segment_index as index
ORDER BY segment_index
""",
    "get_alignment_annotations_of_an_expression": """
MATCH (e:Expression {id: $expression_id})
<-[:MANIFESTATION_OF]-(m:Manifestation)-[:HAS_TYPE]->(:ManifestationType {name: 'critical'})
<-[:ANNOTATION_OF]-(ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'alignment'})

OPTIONAL MATCH (ann)-[:ALIGNED_TO]->(outgoing:Annotation)
OPTIONAL MATCH (incoming:Annotation)-[:ALIGNED_TO]->(ann)

WITH ann,
     collect(DISTINCT outgoing.id) AS outgoing_ids,
     collect(DISTINCT incoming.id) AS incoming_ids
WITH ann, outgoing_ids + incoming_ids AS aligned_ids
UNWIND aligned_ids AS aligned_id

WITH ann, aligned_id
WHERE aligned_id IS NOT NULL

RETURN DISTINCT ann.id AS annotation_id,
       aligned_id AS aligned_annotation_id
""",
}

Queries.sections = {
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

Queries.segments = {
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
WITH m.id as manifestation_id,
     collect({
         segment_id: target_seg.id,
         span_start: target_seg.span_start,
         span_end: target_seg.span_end
     }) as segments
RETURN manifestation_id, segments
""",
    "find_aligned_segments_incoming": """
MATCH (source_seg:Segment {id: $segment_id})<-[:ALIGNED_TO]-(source_of_seg:Segment)
      -[:SEGMENTATION_OF]->(:Annotation)
      -[:ANNOTATION_OF]->(m:Manifestation)
WITH m.id as manifestation_id,
     collect({
         segment_id: source_of_seg.id,
         span_start: source_of_seg.span_start,
         span_end: source_of_seg.span_end
     }) as segments
RETURN manifestation_id, segments
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
    "get_overlapping_segments": """
MATCH (m:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'segmentation'})
MATCH (ann)<-[:SEGMENTATION_OF]-(s:Segment)
WHERE s.span_start < $span_end AND s.span_end > $span_start
RETURN s.id as segment_id,
       s.span_start as span_start,
       s.span_end as span_end
ORDER BY s.span_start
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
    "find_related_alignment_only": """
MATCH (source_manif:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(align_annot:Annotation)-[:HAS_TYPE]->(at:AnnotationType {name: 'alignment'})
MATCH (align_annot)<-[:SEGMENTATION_OF]-(source_seg:Segment)
WHERE source_seg.span_start < $span_end AND source_seg.span_end > $span_start

// Follow bidirectional ALIGNED_TO relationships
MATCH (source_seg)-[:ALIGNED_TO]-(target_seg:Segment)
MATCH (target_seg)-[:SEGMENTATION_OF]->(target_align_annot:Annotation)-[:HAS_TYPE]->(tat:AnnotationType {name: 'alignment'})
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
MATCH (source_manif:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(source_seg_annot:Annotation)-[:HAS_TYPE]->(sat:AnnotationType {name: 'segmentation'})
MATCH (source_seg_annot)<-[:SEGMENTATION_OF]-(source_seg_seg:Segment)
WHERE source_seg_seg.span_start < $span_end AND source_seg_seg.span_end > $span_start

WITH source_manif, 
     MIN(source_seg_seg.span_start) as expanded_start,
     MAX(source_seg_seg.span_end) as expanded_end

// Step 2: Find ALL alignment annotations in source manifestation
MATCH (source_manif)<-[:ANNOTATION_OF]-(source_align_annot:Annotation)-[:HAS_TYPE]->(aat:AnnotationType {name: 'alignment'})
MATCH (source_align_annot)<-[:SEGMENTATION_OF]-(source_align_seg:Segment)
WHERE source_align_seg.span_start < expanded_end AND source_align_seg.span_end > expanded_start

// Step 3: Follow ALIGNED_TO to target alignment segments
MATCH (source_align_seg)-[:ALIGNED_TO]-(target_align_seg:Segment)
MATCH (target_align_seg)-[:SEGMENTATION_OF]->(target_align_annot:Annotation)-[:HAS_TYPE]->(taat:AnnotationType {name: 'alignment'})
MATCH (target_align_annot)-[:ANNOTATION_OF]->(target_manif:Manifestation)
MATCH (target_manif)-[:MANIFESTATION_OF]->(target_expr:Expression)

// Step 4: Find overlapping segments in target segmentation annotation
MATCH (target_manif)<-[:ANNOTATION_OF]-(target_seg_annot:Annotation)-[:HAS_TYPE]->(tsat:AnnotationType {name: 'segmentation'})
MATCH (target_seg_annot)<-[:SEGMENTATION_OF]-(target_seg_seg:Segment)
WHERE target_seg_seg.span_start < target_align_seg.span_end AND target_seg_seg.span_end > target_align_seg.span_start

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
    "find_overlapping_alignment_segments": """
MATCH (m:Manifestation {id: $manifestation_id})
      <-[:ANNOTATION_OF]-(ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'alignment'})
MATCH (ann)<-[:SEGMENTATION_OF]-(seg:Segment)
WHERE seg.span_start < $span_end AND seg.span_end > $span_start
RETURN seg.id as segment_id, 
       seg.span_start as span_start, 
       seg.span_end as span_end
""",
    "find_directly_aligned_segments_batch": """
UNWIND $segment_ids AS seg_id
MATCH (source:Segment {id: seg_id})-[:ALIGNED_TO]-(target:Segment)
MATCH (target)-[:SEGMENTATION_OF]->(:Annotation)-[:ANNOTATION_OF]->(m:Manifestation)
MATCH (m)-[:MANIFESTATION_OF]->(e:Expression)
RETURN source.id as source_segment_id,
       target.id as target_segment_id,
       target.span_start as target_span_start,
       target.span_end as target_span_end,
       m.id as target_manifestation_id,
       e.id as target_expression_id
""",
    "find_segmentation_segments_for_spans": """
MATCH (m:Manifestation {id: $manifestation_id})
      <-[:ANNOTATION_OF]-(ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'segmentation'})
MATCH (ann)<-[:SEGMENTATION_OF]-(seg:Segment)
WHERE ANY(span IN $spans WHERE seg.span_start < span.end AND seg.span_end > span.start)
RETURN seg.id as segment_id,
       seg.span_start as span_start,
       seg.span_end as span_end
""",
    "find_related_expressions_for_manifestations": """
// Given a list of manifestation IDs, find all related expressions
UNWIND $manifestation_ids AS manif_id
MATCH (m:Manifestation {id: manif_id})-[:MANIFESTATION_OF]->(e:Expression)

// Find related expressions via TRANSLATION_OF and COMMENTARY_OF
OPTIONAL MATCH (e)-[:TRANSLATION_OF]->(translation_target:Expression)
OPTIONAL MATCH (translation_target)<-[:TRANSLATION_OF]-(sibling_translation:Expression)
WHERE sibling_translation.id <> e.id

OPTIONAL MATCH (translation_target)<-[:COMMENTARY_OF]-(commentary_of_target:Expression)
OPTIONAL MATCH (commentary_of_target)<-[:TRANSLATION_OF]-(commentary_translation:Expression)

OPTIONAL MATCH (e)-[:COMMENTARY_OF]->(commentary_target:Expression)
OPTIONAL MATCH (commentary_target)<-[:COMMENTARY_OF]-(sibling_commentary:Expression)
WHERE sibling_commentary.id <> e.id

OPTIONAL MATCH (commentary_target)<-[:TRANSLATION_OF]-(translation_of_target:Expression)

OPTIONAL MATCH (e)<-[:TRANSLATION_OF]-(translation_of_e:Expression)
OPTIONAL MATCH (e)<-[:COMMENTARY_OF]-(commentary_of_e:Expression)

WITH m, e,
     collect(DISTINCT sibling_translation) + 
     collect(DISTINCT commentary_of_target) +
     collect(DISTINCT commentary_translation) +
     collect(DISTINCT sibling_commentary) +
     collect(DISTINCT translation_of_target) +
     collect(DISTINCT translation_of_e) +
     collect(DISTINCT commentary_of_e) as related_expressions

UNWIND related_expressions AS related_expr
WITH m, e, related_expr
WHERE related_expr IS NOT NULL

MATCH (related_expr)<-[:MANIFESTATION_OF]-(related_manif:Manifestation)

RETURN m.id as source_manifestation_id,
       e.id as source_expression_id,
       related_expr.id as related_expression_id,
       related_manif.id as related_manifestation_id,
       CASE
           WHEN (related_expr)-[:TRANSLATION_OF]->(e) THEN 'translation_of_source'
           WHEN (related_expr)-[:COMMENTARY_OF]->(e) THEN 'commentary_of_source'
           WHEN (e)-[:TRANSLATION_OF]->()<-[:TRANSLATION_OF]-(related_expr) THEN 'sibling_translation'
           WHEN (e)-[:TRANSLATION_OF]->()<-[:COMMENTARY_OF]-(related_expr) THEN 'commentary_of_common_target'
           WHEN (e)-[:COMMENTARY_OF]->()<-[:COMMENTARY_OF]-(related_expr) THEN 'sibling_commentary'
           WHEN (e)-[:COMMENTARY_OF]->()<-[:TRANSLATION_OF]-(related_expr) THEN 'translation_of_common_target'
           WHEN (e)-[:TRANSLATION_OF]->()<-[:COMMENTARY_OF]-()<-[:TRANSLATION_OF]-(related_expr) THEN 'translation_of_commentary_of_target'
           ELSE 'other'
       END as relationship_type
""",
"get_alignment_segments": """
MATCH (a:Annotation {id: $annotation_id})<-[:SEGMENTATION_OF]-(s:Segment)
MATCH (s)-[:ALIGNED_TO]->(a2:Annotation {id: $alignment_id})<-[:SEGMENTATION_OF]-(s2:Segment)
WHERE s2.span_start < $span_end AND s2.span_end > $span_start
RETURN s2.id as segment_id,
       s2.span_start as span_start,
       s2.span_end as span_end
""",
}

Queries.references = {
    "create": """
CREATE (r:Reference {
    id: $reference_id,
    name: $name,
    description: $description
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
"""
}

Queries.bibliography_types = {
    "link_to_segments": """
// Link segments to existing bibliography types only (no new types created)
UNWIND $segment_and_type_names AS sbt
MATCH (s:Segment {id: sbt.segment_id})
MATCH (bt:BibliographyType {name: sbt.type_name})
CREATE (s)-[:HAS_TYPE]->(bt)
"""
}

Queries.ai = {
    "find_or_create": """
    MERGE (ai:AI {id: $ai_id})
    RETURN elementId(ai) AS ai_element_id
"""
}

Queries.categories = {
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
OPTIONAL MATCH (c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[:HAS_LANGUAGE]->(l:Language {code: $language})
OPTIONAL MATCH (child:Category)-[:HAS_PARENT]->(c)
WITH c, parent, lt, COUNT(DISTINCT child) AS child_count
RETURN c.id AS id, parent.id AS parent, lt.text AS title, child_count > 0 AS has_child
""",
}

Queries.works = {
    "link_to_category": """
MATCH (w:Work {id: $work_id})
MATCH (c:Category {id: $category_id})
CREATE (w)-[:BELONGS_TO]->(c)
""",
}