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
    def expression_fragment(label):
        return f"""
{{
    id: {label}.id,
    title: [{Queries.primary_nomen(label, "HAS_TITLE")}],
    language: [({label})-[:HAS_LANGUAGE]->({label}_l:Language) | {label}_l.code][0],
    type: [({label})-[:HAS_TYPE]->({label}_t:TextType) | {label}_t.name][0]
}}
"""


Queries.expressions = {
    "fetch_by_id": f"""
    MATCH (e:Expression {{id: $id}})

    RETURN {{
        id: e.id,
        bdrc: e.bdrc,
        wiki: e.wiki,
        type: [(e)-[:HAS_TYPE]->(tt:TextType) | tt.name][0],
        contributors: [
            (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(person:Person) | {{
                person_id: person.id,
                person_bdrc_id: person.bdrc,
                role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
            }}
        ],
        related: [
            sibling IN [(e)-[:EXPRESSION_OF]->(work:Work)<-[:EXPRESSION_OF]-(s:Expression) | s] |
                {Queries.expression_fragment('sibling')}
        ],
        date: e.date,
        title: [{Queries.primary_nomen('e', 'HAS_TITLE')}],
        alt_titles: [{Queries.alternative_nomen('e', 'HAS_TITLE')}],
        language: [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0]
    }} AS expression
""",
    "fetch_all": f"""
    MATCH (e:Expression)
    WITH e
    WHERE ($type IS NULL OR [(e)-[:HAS_TYPE]->(tt:TextType) | tt.name][0] = $type)
    AND ($language IS NULL OR [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0] = $language)

    SKIP $offset
    LIMIT $limit

    RETURN {{
        id: e.id,
        bdrc: e.bdrc,
        wiki: e.wiki,
        type: [(e)-[:HAS_TYPE]->(tt:TextType) | tt.name][0],
        contributors: [
            (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(person:Person) | {{
                person_id: person.id,
                person_bdrc_id: person.bdrc,
                role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
            }}
        ],
        date: e.date,
        title: [{Queries.primary_nomen('e', 'HAS_TITLE')}],
        alt_titles: [{Queries.alternative_nomen('e', 'HAS_TITLE')}],
        language: [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0]
    }} AS expression
""",
    "fetch_related": f"""
    MATCH (e:Expression {{id: $id}})

    RETURN [
        sibling IN [(e)-[:EXPRESSION_OF]->(work:Work)<-[:EXPRESSION_OF]-
        (s:Expression) WHERE s.id <> $id | s] |
            {Queries.expression_fragment('sibling')}
    ] AS related_expressions
""",
    "create": """
CREATE (w:Work {id: $work_id})
CREATE (e:Expression {id: $expression_id, bdrc: $bdrc, wiki: $wiki, date: $date})
WITH w, e
MATCH (tt:TextType {name: $type_name})
WITH w, e, tt
MERGE (l:Language {code: $language_code})
WITH w, e, tt, l
MATCH (n:Nomen) WHERE elementId(n) = $title_nomen_element_id
CREATE (e)-[:EXPRESSION_OF {original: true}]->(w),
       (e)-[:HAS_TYPE]->(tt),
       (e)-[:HAS_LANGUAGE {tags: $bcp47_tag}]->(l),
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
RETURN p.id as person_id
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
CREATE (p:Person {id: $id, bdrc: $bdrc, wiki: $wiki})
WITH p
MATCH (n:Nomen) WHERE elementId(n) = $primary_name_element_id
CREATE (p)-[:HAS_NAME]->(n)
RETURN p.id as person_id
""",
}

Queries.nomens = {
    "create": """
OPTIONAL MATCH (primary:Nomen)
WHERE $primary_name_element_id IS NOT NULL AND elementId(primary) = $primary_name_element_id
WITH primary
CREATE (n:Nomen)
FOREACH (_ IN CASE WHEN primary IS NOT NULL THEN [1] ELSE [] END |
    CREATE (n)-[:ALTERNATIVE_OF]->(primary)
)
WITH n
FOREACH (lt IN $localized_texts |
    MERGE (l:Language {code: lt.base_lang_code})
    CREATE (n)-[:HAS_LOCALIZATION]->(locText:LocalizedText {text: lt.text})-[:HAS_LANGUAGE {bcp47: lt.bcp47_tag}]->(l)
)
RETURN elementId(n) as element_id
""",
}

Queries.manifestations = {
    "fetch_by_id": f"""
    MATCH (m:Manifestation {{id: $id}})

    RETURN {{
        id: m.id,
        bdrc: m.bdrc,
        wiki: m.wiki,
        type: [(m)-[:HAS_TYPE]->(mt:ManifestationType) | mt.name][0],
        manifestation_of: [(m)-[:MANIFESTATION_OF]->(e:Expression) | e.id][0],
        annotations: [
            (m)-[:HAS_ANNOTATION]->(a:Annotation) | {{
                id: a.id,
                type: [(a)-[:HAS_TYPE]->(at:AnnotationType) | at.name][0],
                name: a.name,
                aligned_to: [(a)-[:ALIGNED_TO]->(target:Annotation) | target.id][0]
            }}
        ],
        colophon: m.colophon,
        copyright: [(m)-[:HAS_COPYRIGHT]->(cs:CopyrightStatus) | cs.status][0],
        incipit_title: [{Queries.primary_nomen('m', 'HAS_TITLE')}],
        alt_incipit_titles: [{Queries.alternative_nomen('m', 'HAS_TITLE')}]
    }} AS manifestation
"""
}
