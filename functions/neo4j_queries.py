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
    type: {Queries.get_expression_type(label)},
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
    source: [({label})-[:HAS_SOURCE]->(mf_s:Source) | mf_s.name][0],
    incipit_title: [{Queries.primary_nomen(label, 'HAS_INCIPIT_TITLE')}],
    alt_incipit_titles: [{Queries.alternative_nomen(label, 'HAS_INCIPIT_TITLE')}],
    alignment_sources: {Queries.manifestation_alignment_sources(label)},
    alignment_targets: {Queries.manifestation_alignment_targets(label)}
}}
"""

    @staticmethod
    def get_expression_type(label):
        """Fragment to infer expression type from relationships instead of HAS_TYPE"""
        return f"""
CASE
    WHEN ({label})-[:COMMENTARY_OF]->(:Expression) THEN 'commentary'
    WHEN ({label})-[:TRANSLATION_OF]->(:Expression) THEN 'translation'
    WHEN ({label})<-[:TRANSLATION_OF]-(:Expression) THEN 'translation_source'
    WHEN ({label})<-[:COMMENTARY_OF]-(:Expression) THEN 'root'
    ELSE 'none'
END
"""

    @staticmethod
    def expression_fragment(label):
        return f"""
{{
    id: {label}.id,
    bdrc: {label}.bdrc,
    wiki: {label}.wiki,
    type: {Queries.get_expression_type(label)},
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
    category_id: [({label})-[:EXPRESSION_OF]->(ef_work:Work)-[:BELONGS_TO]->(ef_cat:Category) | ef_cat.id][0],
    copyright: [({label})-[:HAS_COPYRIGHT]->(ef_copyright:Copyright) | ef_copyright.name][0],
    license: [({label})-[:HAS_LICENSE]->(ef_license:License) | ef_license.name][0]
}}
"""

    @staticmethod
    def create_expression_base(label):
        return f"CREATE ({label}:Expression {{id: $expression_id, bdrc: $bdrc, wiki: $wiki, date: $date}})"

    @staticmethod
    def create_copyright_and_license(expression_label):
        """
        Matches existing Copyright node by status and links it to the expression.
        Also matches existing License node by name and links it.
        
        Returns Cypher fragment that:
        1. Matches Copyright node by status value
        2. Links Expression to Copyright
        3. Matches License node by name (creates if not exists)
        4. Links Expression to License
        """
        return f"""
MATCH (copyright:Copyright {{status: $copyright}})
CREATE ({expression_label})-[:HAS_COPYRIGHT]->(copyright)
MERGE (license:License {{name: $license}})
CREATE ({expression_label})-[:HAS_LICENSE]->(license)
"""


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
    WHERE ($type IS NULL OR {Queries.get_expression_type('e')} = $type)
    AND ($language IS NULL OR [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0] = $language)

    OFFSET $offset
    LIMIT $limit

    RETURN {Queries.expression_fragment('e')} AS expression
""",
    "fetch_all_relations": """
    MATCH (e:Expression)
    RETURN e.id AS id,
      [ (e)-[r:TRANSLATION_OF|COMMENTARY_OF]-(other:Expression)
        | {
            type: type(r),
            direction: CASE WHEN startNode(r) = e THEN 'out' ELSE 'in' END,
            otherId: other.id
          }
      ] AS relations
    ORDER BY id
""",
    "get_expressions_metadata_by_ids": f"""
MATCH (e:Expression)
WHERE e.id IN $expression_ids
RETURN e.id as expression_id, {Queries.expression_fragment('e')} as metadata
""",
    "fetch_by_category": f"""
    MATCH (c:Category {{id: $category_id}})
    MATCH (e:Expression)-[:EXPRESSION_OF]->(:Work)-[:BELONGS_TO]->(c)
    WITH e
    WHERE {Queries.get_expression_type('e')} <> 'commentary'
      AND ($language IS NULL OR [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0] = $language)
      AND (
        $instance_type IS NULL OR EXISTS {{
          MATCH (e)<-[:MANIFESTATION_OF]-(m:Manifestation)-[:HAS_TYPE]->(mt:ManifestationType)
          WHERE mt.name = $instance_type
          RETURN 1
        }}
      )
    OPTIONAL MATCH (e)<-[:MANIFESTATION_OF]-(m:Manifestation)-[:HAS_TYPE]->(mt:ManifestationType)
    WHERE $instance_type IS NULL OR mt.name = $instance_type
    WITH e, collect(m) as ms

    OFFSET $offset
    LIMIT $limit

    RETURN {{
      text_metadata: {Queries.expression_fragment('e')},
      instance_metadata: [m IN ms | {Queries.manifestation_fragment('m')}]
    }} AS item
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
{Queries.create_copyright_and_license('e')}
RETURN e.id as expression_id
""",
        "_old_create_contribution": """
MATCH (e:Expression {id: $expression_id})
MATCH (p:Person) WHERE (($person_id IS NOT NULL AND p.id = $person_id)
                        OR ($person_bdrc_id IS NOT NULL AND p.bdrc = $person_bdrc_id))
MATCH (rt:RoleType {name: $role_name})
CREATE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(p),
       (c)-[:WITH_ROLE]->(rt)
RETURN elementId(c) as contribution_element_id
""",
    "create_contribution": """
MATCH (e:Expression {id: $expression_id})
MATCH (p:Person) WHERE (($person_id IS NOT NULL AND p.id = $person_id)
                        OR ($person_bdrc_id IS NOT NULL AND p.bdrc = $person_bdrc_id))
MATCH (rt:RoleType {name: $role_name})
MERGE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(p)
MERGE (c)-[:WITH_ROLE]->(rt)
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
{Queries.create_copyright_and_license('e')}
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
{Queries.create_copyright_and_license('e')}
RETURN e.id as expression_id
""",
    "get_texts_group": f"""
MATCH (e1:Expression {{id: $expression_id}})
MATCH (e1)-[:EXPRESSION_OF]->(w:Work)
MATCH (w)<-[:EXPRESSION_OF]-(e:Expression)
WHERE e.id <> e1.id
RETURN {Queries.expression_fragment('e')} AS expression
""",
    "title_search": f"""
MATCH (lt:LocalizedText)<-[:HAS_LOCALIZATION]-(n:Nomen)
MATCH (e:Expression)-[:HAS_TITLE]->(titleNomen:Nomen)
WHERE lt.text CONTAINS $title
  AND (n = titleNomen OR (n)-[:ALTERNATIVE_OF]->(titleNomen))
MATCH (e)<-[:MANIFESTATION_OF]-(m:Manifestation)-[:HAS_TYPE]->(mt: ManifestationType {{name: 'critical'}})
RETURN DISTINCT e.id as expression_id, lt.text as title, m.id as manifestation_id
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
MERGE (S:Source {name: $source})
CREATE (m:Manifestation {
  id: $manifestation_id,
  bdrc: $bdrc,
  wiki: $wiki,
  colophon: $colophon
})
WITH m, e, mt, S, it

CREATE (m)-[:MANIFESTATION_OF]->(e),
       (m)-[:HAS_TYPE]->(mt),
       (m)-[:HAS_SOURCE]->(S)
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
    "get_expression_ids_by_manifestation_ids": """
MATCH (m:Manifestation)-[:MANIFESTATION_OF]->(e:Expression)
WHERE m.id IN $manifestation_ids
RETURN m.id as manifestation_id, e.id as expression_id
""",
    "get_manifestations_metadata_by_ids": f"""
MATCH (m:Manifestation)
WHERE m.id IN $manifestation_ids
RETURN m.id as manifestation_id, {Queries.manifestation_fragment('m')} as metadata
""",
    "fetch_expression_id_by_manifestation_id": """
MATCH (m:Manifestation {id: $manifestation_id})-[:MANIFESTATION_OF]->(e:Expression)
RETURN e.id as expression_id
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
OPTIONAL MATCH (s)-[:ALIGNED_TO]->(aligned_seg:Segment)
WITH s, r, bt, collect(aligned_seg.id) as aligned_segments
RETURN s.id as id,
       s.span_start as start,
       s.span_end as end,
       r.name as reference,
       bt.name as bibliography_type,
       aligned_segments
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
    "get_durchen_annotation": """
MATCH (a:Annotation {id: $annotation_id})<-[:SEGMENTATION_OF]-(s:Segment)-[:HAS_DURCHEN_NOTE]->(n:DurchenNote)
RETURN DISTINCT
    s.id as id,
    s.span_start as span_start,
    s.span_end as span_end,
    n.note as note
""",
    "get_alignment_pairs_by_manifestation": """
MATCH (m:Manifestation {id: $manifestation_id})
MATCH (m)<-[:ANNOTATION_OF]-(a1:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'alignment'})
MATCH (a1)-[:ALIGNED_TO]-(a2:Annotation)
WITH a1, a2, m.id as manifestation_id

RETURN manifestation_id, a1.id as alignment_1_id, a2.id as alignment_2_id
""",
    "get_segmentation_annotation_by_manifestation": """
MATCH (m:Manifestation {id: $manifestation_id})
MATCH (m)<-[:ANNOTATION_OF]-(a:Annotation)-[:HAS_TYPE]->(at:AnnotationType)
WHERE at.name IN ['segmentation', 'pagination']
WITH a
LIMIT 1
OPTIONAL MATCH (a)<-[:SEGMENTATION_OF]-(s:Segment)
WITH collect(DISTINCT {
    id: s.id,
    span_start: s.span_start,
    span_end: s.span_end
}) as segments
RETURN segments
""",
    "check_annotation_type_exists": """
MATCH (m:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(a:Annotation)-[:HAS_TYPE]->(at:AnnotationType {name: $annotation_type})
RETURN count(a) > 0 as exists
""",
    "check_alignment_relationship_exists": """
MATCH (source_m:Manifestation {id: $source_manifestation_id})<-[:ANNOTATION_OF]-(source_ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'alignment'})
OPTIONAL MATCH (source_ann)-[:ALIGNED_TO]->(target_ann:Annotation)-[:ANNOTATION_OF]->(target_m:Manifestation {id: $target_manifestation_id})
OPTIONAL MATCH (target_m)<-[:ANNOTATION_OF]-(target_ann2:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'alignment'})<-[:ALIGNED_TO]-(source_ann2:Annotation)-[:ANNOTATION_OF]->(source_m)
WITH source_ann, target_ann, source_ann2, target_ann2
RETURN (target_ann IS NOT NULL OR source_ann2 IS NOT NULL) as exists
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
      -[:SEGMENTATION_OF]->(:Annotation)
      -[:ANNOTATION_OF]->(m:Manifestation)
      -[:MANIFESTATION_OF]->(e:Expression)
RETURN seg.id as segment_id,
       seg.span_start as span_start,
       seg.span_end as span_end,
       m.id as manifestation_id,
       e.id as expression_id
ORDER BY seg.id
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
MATCH (m:Manifestation {id: $manifestation_id})<-[:ANNOTATION_OF]-(ann:Annotation)-[:HAS_TYPE]->(:AnnotationType {name: 'segmentation'})
MATCH (ann)<-[:SEGMENTATION_OF]-(s:Segment)
WHERE s.span_start < $span_end AND s.span_end > $span_start
RETURN s.id as segment_id,
       s.span_start as span_start,
       s.span_end as span_end
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
WHERE seg.span_start < input_seg.span_end 
  AND seg.span_end > input_seg.span_start
RETURN input_segment_id, 
       collect(seg.id) as overlapping_segments
"""
}

Queries.references = {
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
"""
}

Queries.durchen_notes = {
    "create": """
UNWIND $segments AS seg
MATCH (s:Segment {id: seg.id})
CREATE (n:DurchenNote {note: seg.note})
CREATE (s)-[:HAS_DURCHEN_NOTE]->(n)
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

Queries.enum = {
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