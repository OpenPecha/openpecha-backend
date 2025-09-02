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
    def infer_expression_type(label):
        """Fragment to infer expression type from relationships instead of HAS_TYPE"""
        return f"""
CASE
    WHEN ({label})-[:EXPRESSION_OF {{original: false}}]->(:Work) THEN 'translation'
    WHEN ({label})-[:EXPRESSION_OF]->(:Work)-[:COMMENTARY_OF]->(:Work) THEN 'commentary'
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
    parent: COALESCE(
        [({label})-[:TRANSLATION_OF]->(parent:Expression) | parent.id][0],
        [({label})-[:EXPRESSION_OF]->(w:Work)-[:COMMENTARY_OF]->(:Work)
        <-[:EXPRESSION_OF]-(parent:Expression) | parent.id][0]
    ),
    contributors: (
        [({label})-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(person:Person) | {{
            person_id: person.id,
            person_bdrc_id: person.bdrc,
            role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
        }}]
        +
        [({label})-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(ai:AI) | {{
            ai_id: ai.id,
            role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
        }}]
    ),
    date: {label}.date,
    title: [{Queries.primary_nomen(label, 'HAS_TITLE')}],
    alt_titles: [{Queries.alternative_nomen(label, 'HAS_TITLE')}],
    language: [({label})-[:HAS_LANGUAGE]->(l:Language) | l.code][0]
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
MERGE (l:Language {{code: $language_code}})
CREATE (e)-[:EXPRESSION_OF {{original: $original}}]->(w),
       (e)-[:HAS_LANGUAGE {{tags: $bcp47_tag}}]->(l),
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
MATCH (parent:Expression {{id: $parent_id}})-[:EXPRESSION_OF]->(w:Work)
{Queries.create_expression_base('e')}
WITH parent, w, e
MATCH (n:Nomen) WHERE elementId(n) = $title_nomen_element_id
MERGE (l:Language {{code: $language_code}})
CREATE (e)-[:EXPRESSION_OF {{original: false}}]->(w),
       (e)-[:TRANSLATION_OF]->(parent),
       (e)-[:HAS_LANGUAGE {{tags: $bcp47_tag}}]->(l),
       (e)-[:HAS_TITLE]->(n)
RETURN e.id as expression_id
""",
    "create_commentary": f"""
MATCH (parent:Expression {{id: $parent_id}})-[:EXPRESSION_OF]->(parent_work:Work)
CREATE (commentary_work:Work {{id: $work_id}})
{Queries.create_expression_base('e')}
WITH parent, parent_work, commentary_work, e
MATCH (n:Nomen) WHERE elementId(n) = $title_nomen_element_id
MERGE (l:Language {{code: $language_code}})
CREATE (commentary_work)-[:COMMENTARY_OF]->(parent_work),
       (e)-[:EXPRESSION_OF {{original: true}}]->(commentary_work),
       (e)-[:HAS_LANGUAGE {{tags: $bcp47_tag}}]->(l),
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
FOREACH (_ IN CASE WHEN primary IS NOT NULL THEN [1] ELSE [] END |
    CREATE (n)-[:ALTERNATIVE_OF]->(primary)
)
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
    OPTIONAL MATCH (m)-[:MANIFESTATION_OF]->(e:Expression)

    RETURN {{
        id: m.id,
        bdrc: m.bdrc,
        wiki: m.wiki,
        type: [(m)-[:HAS_TYPE]->(mt:ManifestationType) | mt.name][0],
        annotations: [
            (m)<-[:ANNOTATION_OF]-(a:Annotation) | {{
                id: a.id,
                type: [(a)-[:HAS_TYPE]->(at:AnnotationType) | at.name][0],
                aligned_to: [(a)-[:ALIGNED_TO]->(target:Annotation) | target.id][0]
            }}
        ],
        colophon: m.colophon,
        copyright: [(m)-[:HAS_COPYRIGHT]->(cs:CopyrightStatus) | cs.name][0],
        incipit_title: [{Queries.primary_nomen('m', 'HAS_TITLE')}],
        alt_incipit_titles: [{Queries.alternative_nomen('m', 'HAS_TITLE')}]
    }} AS manifestation, e.id AS expression_id
""",
    "create": """
MATCH (e:Expression {id: $expression_id})
MERGE (mt:ManifestationType {name: $type})
MERGE (cs:CopyrightStatus {name: $copyright})
CREATE (m:Manifestation {
  id: $manifestation_id,
  bdrc: $bdrc,
  wiki: $wiki,
  colophon: $colophon
})
WITH m, e, mt, cs

OPTIONAL MATCH (n:Nomen)
  WHERE elementId(n) = $title_nomen_element_id
CREATE (m)-[:MANIFESTATION_OF]->(e),
       (m)-[:HAS_TYPE]->(mt),
       (m)-[:HAS_COPYRIGHT]->(cs)
FOREACH (_ IN CASE WHEN n IS NULL THEN [] ELSE [1] END |
  CREATE (m)-[:HAS_TITLE]->(n)
)
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

FOREACH (_ IN CASE WHEN target IS NULL THEN [] ELSE [1] END |
  CREATE (a)-[:ALIGNED_TO]->(target)
)

RETURN a.id AS annotation_id
"""
}

Queries.ai = {
    "find_or_create": """
    MERGE (ai:AI {id: $ai_id})
    RETURN elementId(ai) AS ai_element_id
"""
}
