class Queries:
    """Neo4j queries organized by domain with private helper methods."""

    # Private helper methods for query fragments
    @staticmethod
    def person_primary_name(label):
        return f"""
({label})-[:HAS_NAME]->({label}_n:Nomen)-[:HAS_LOCALIZATION]->
({label}_lt:LocalizedText)-[:HAS_LANGUAGE]->({label}_l:Language)
WHERE exists((:Nomen)-[:ALTERNATIVE_OF]->({label}_n)) | {{
    language: {label}_l.code,
    text: {label}_lt.text
}}
"""

    @staticmethod
    def person_alternative_names(label):
        return f"""
({label})-[:HAS_NAME]->(:Nomen)<-[:ALTERNATIVE_OF]-({label}_an:Nomen) | [
    ({label}_an)-[:HAS_LOCALIZATION]->({label}_at:LocalizedText)-[:HAS_LANGUAGE]->({label}_al:Language) | {{
        language: {label}_al.code,
        text: {label}_at.text
    }}
]
"""

    @staticmethod
    def title_primary(label):
        return f"""
({label})-[:HAS_TITLE]->({label}_n:Nomen)-[:HAS_LOCALIZATION]->({label}_lt:LocalizedText)-[:HAS_LANGUAGE]->({label}_l:Language)
WHERE exists((:Nomen)-[:ALTERNATIVE_OF]->({label}_n)) | {{
    language: {label}_l.code,
    text: {label}_lt.text
}}
"""

    @staticmethod
    def title_alternative(label):
        return f"""
({label})-[:HAS_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-({label}_an:Nomen) | [
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
    name: [{Queries.person_primary_name(label)}],
    alt_names: [{Queries.person_alternative_names(label)}]
}}
"""

    @staticmethod
    def expression_fragment(label):
        return f"""
{{
    id: {label}.id,
    title: [{Queries.title_primary(label)}],
    language: [({label})-[:HAS_LANGUAGE]->({label}_l:Language) | {label}_l.code][0],
    type: [({label})-[:HAS_TYPE]->({label}_t:ExpressionType) | {label}_t.name][0]
}}
"""


Queries.expressions = {
    "fetch_by_id": f"""
    MATCH (e:Expression)
    WHERE e.id = $id

    RETURN {{
        id: e.id,
        bdrc: e.bdrc,
        wiki: e.wiki,
        type: [(e)-[:HAS_TYPE]->(et:ExpressionType) | et.name][0],
        contributors: [
            (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(person:Person) | {{
                person: {Queries.person_fragment('person')},
                role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
            }}
        ],
        related: [
            sibling IN [(e)-[:EXPRESSION_OF]->(work:Work)<-[:EXPRESSION_OF]-(s:Expression) | s] |
                {Queries.expression_fragment('sibling')}
        ],
        date: e.date,
        title: [{Queries.title_primary('e')}],
        alt_titles: [{Queries.title_alternative('e')}],
        language: [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0]
    }} AS expression
""",
    "fetch_all": f"""
    MATCH (e:Expression)
    WITH e
    WHERE ($type IS NULL OR [(e)-[:HAS_TYPE]->(et:ExpressionType) | et.name][0] = $type)
    AND ($language IS NULL OR [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0] = $language)

    SKIP $offset
    LIMIT $limit

    RETURN {{
        id: e.id,
        bdrc: e.bdrc,
        wiki: e.wiki,
        type: [(e)-[:HAS_TYPE]->(et:ExpressionType) | et.name][0],
        contributors: [
            (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(person:Person) | {{
                person: {Queries.person_fragment('person')},
                role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
            }}
        ],
        date: e.date,
        title: [{Queries.title_primary('e')}],
        alt_titles: [{Queries.title_alternative('e')}],
        language: [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0]
    }} AS expression
""",
    "fetch_related": f"""
    MATCH (e:Expression)
    WHERE e.id = $id

    RETURN [
        sibling IN [(e)-[:EXPRESSION_OF]->(work:Work)<-[:EXPRESSION_OF]-
        (s:Expression) WHERE s.id <> $id | s] |
            {Queries.expression_fragment('sibling')}
    ] AS related_expressions
""",
}

Queries.persons = {
    "fetch_by_id": f"""
MATCH (person:Person)
WHERE person.id = $id
RETURN {Queries.person_fragment('person')} AS person
""",
    "fetch_all": f"""
MATCH (person:Person)
RETURN {Queries.person_fragment('person')} AS person
""",
    "create": """
CREATE (p:Person {id: $id, bdrc: $bdrc, wiki: $wiki})
RETURN p.id as person_id
""",
}

Queries.nomens = {
    "create": """
CREATE (n:Nomen {id: $id})
RETURN n.id as nomen_id
""",
    "link_to_person": """
MATCH (p:Person), (n:Nomen)
WHERE p.id = $person_id AND n.id = $primary_name_id
CREATE (p)-[:HAS_NAME]->(n)
""",
    "link_alternative": """
MATCH (primary:Nomen), (alt:Nomen)
WHERE primary.id = $primary_name_id AND alt.id = $alt_name_id
CREATE (alt)-[:ALTERNATIVE_OF]->(primary)
""",
    "create_localized_text": """
MATCH (n:Nomen), (l:Language)
WHERE n.id = $id AND l.code = $base_lang_code
CREATE (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText {id: $id, text: $text})-[:HAS_LANGUAGE {bcp47: $bcp47_tag}]->(l)
""",
}

Queries.languages = {
    "create_or_find": """
MERGE (l:Language {code: $lang_code})
"""
}

Queries.manifestations = {
    "fetch_by_id": f"""
    MATCH (m:Manifestation)
    WHERE m.id = $id

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
        incipit_title: [{Queries.title_primary('m')}],
        alt_incipit_titles: [{Queries.title_alternative('m')}]
    }} AS manifestation
"""
}
