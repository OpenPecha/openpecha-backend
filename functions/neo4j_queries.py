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
    language: [({label})-[:HAS_LANGUAGE]->(ef_lang:Language) | ef_lang.code][0]
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

    RETURN {Queries.manifestation_fragment('m')} AS manifestation, e.id AS expression_id
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
}

Queries.annotations = {
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
    "get_segments": """
MATCH (a:Annotation {id: $annotation_id})
<-[:SEGMENTATION_OF]-(s:Segment)
OPTIONAL MATCH (s)-[:HAS_REFERENCE]->(r:Reference)
RETURN s.id as id,
       s.span_start as start,
       s.span_end as end,
       r.name as reference
ORDER BY s.span_start
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
"""
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
"""
}

Queries.segments = {
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

Queries.ai = {
    "find_or_create": """
    MERGE (ai:AI {id: $ai_id})
    RETURN elementId(ai) AS ai_element_id
"""
}
