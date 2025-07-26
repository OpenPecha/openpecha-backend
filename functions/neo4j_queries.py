def person_primary_name(label):
    return f"""
({label})-[:HAS_NAME]->({label}_n:Nomen)-[:HAS_LOCALIZATION]->({label}_lt:LocalizedText)-[:HAS_LANGUAGE]->({label}_l:Language)
WHERE exists((:Nomen)-[:ALTERNATIVE_OF]->({label}_n)) | {{
    language: {label}_l.code,
    text: {label}_lt.text
}}
"""


def person_alternative_names(label):
    return f"""
({label})-[:HAS_NAME]->(:Nomen)<-[:ALTERNATIVE_OF]-({label}_an:Nomen) | [
    ({label}_an)-[:HAS_LOCALIZATION]->({label}_at:LocalizedText)-[:HAS_LANGUAGE]->({label}_al:Language) | {{
        language: {label}_al.code,
        text: {label}_at.text
    }}
]
"""


def title_primary(label):
    return f"""
({label})-[:HAS_TITLE]->({label}_n:Nomen)-[:HAS_LOCALIZATION]->({label}_lt:LocalizedText)-[:HAS_LANGUAGE]->({label}_l:Language)
WHERE exists((:Nomen)-[:ALTERNATIVE_OF]->({label}_n)) | {{
    language: {label}_l.code,
    text: {label}_lt.text
}}
"""


def title_alternative(label):
    return f"""
({label})-[:HAS_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-({label}_an:Nomen) | [
    ({label}_an)-[:HAS_LOCALIZATION]->({label}_at:LocalizedText)-[:HAS_LANGUAGE]->({label}_al:Language) | {{
        language: {label}_al.code,
        text: {label}_at.text
    }}
]
"""


def person_fragment(label):
    return f"""
{{
    id: {label}.id,
    name: [{person_primary_name(label)}],
    alt_names: [{person_alternative_names(label)}]
}}
"""


def expression_fragment(label):
    return f"""
{{
    id: elementId({label}),
    title: [{title_primary(label)}],
    language: [({label})-[:HAS_LANGUAGE]->({label}_l:Language) | {label}_l.code][0],
    type: [({label})-[:HAS_TYPE]->({label}_t:ExpressionType) | {label}_t.name][0]
}}
"""


FETCH_EXPRESSION_QUERY = f"""
    MATCH (e:Expression)
    WHERE elementId(e) = $expressionElementId

    RETURN {{
        id: elementId(e),
        bdrc: e.bdrc,
        wiki: e.wiki,
        type: [(e)-[:HAS_TYPE]->(et:ExpressionType) | et.name][0],
        contributors: [
            (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(person:Person) | {{
                person: {person_fragment('person')},
                role: [(c)-[:WITH_ROLE]->(rt:RoleType) | rt.name][0]
            }}
        ],
        related: [
            sibling IN [(e)-[:EXPRESSION_OF]->(work:Work)<-[:EXPRESSION_OF]-(s:Expression) | s] |
                {expression_fragment('sibling')}
        ],
        date: e.date,
        title: [{title_primary('e')}],
        alt_titles: [{title_alternative('e')}],
        language: [(e)-[:HAS_LANGUAGE]->(l:Language) | l.code][0]
    }} AS expression
"""

FETCH_PERSON_QUERY = f"""
    MATCH (person:Person)
    WHERE person.id = $personId
    RETURN {person_fragment('person')} AS person
"""

FETCH_ALL_PERSONS_QUERY = f"""
    MATCH (person:Person)
    RETURN {person_fragment('person')} AS person
"""

# Person creation queries
CREATE_PERSON_QUERY = """
CREATE (p:Person {id: $person_id})
RETURN p.id as person_id
"""

CREATE_NOMEN_QUERY = """
CREATE (n:Nomen {id: $nomen_id})
RETURN n.id as nomen_id
"""

LINK_PERSON_TO_NOMEN_QUERY = """
MATCH (p:Person), (n:Nomen)
WHERE p.id = $person_id AND n.id = $primary_name_id
CREATE (p)-[:HAS_NAME]->(n)
"""

LINK_ALTERNATIVE_NOMEN_QUERY = """
MATCH (primary:Nomen), (alt:Nomen)
WHERE primary.id = $primary_name_id AND alt.id = $alt_name_id
CREATE (alt)-[:ALTERNATIVE_OF]->(primary)
"""

CREATE_OR_FIND_LANGUAGE_QUERY = """
MERGE (l:Language {code: $lang_code})
"""

CREATE_LOCALIZED_TEXT_QUERY = """
MATCH (n:Nomen), (l:Language)
WHERE n.id = $nomen_id AND l.code = $base_lang_code
CREATE (n)-[:HAS_LOCALIZED_TEXT]->(lt:LocalizedText {text: $text})-[:HAS_LANGUAGE {bcp47: $bcp47_tag}]->(l)
"""

FETCH_ALL_EXPRESSIONS_QUERY = f"""
    MATCH (e:Expression)
    RETURN {expression_fragment('expression')} AS expression
"""

FETCH_MANIFESTATION_QUERY = f"""
    MATCH (m:Manifestation)
    WHERE elementId(m) = $manifestationElementId

    RETURN {{
        id: elementId(m),
        bdrc: m.bdrc,
        wiki: m.wiki,
        type: [(m)-[:HAS_TYPE]->(mt:ManifestationType) | mt.name][0],
        manifestation_of: [(m)-[:MANIFESTATION_OF]->(e:Expression) | elementId(e)][0],
        annotations: [
            (m)-[:HAS_ANNOTATION]->(a:Annotation) | {{
                id: elementId(a),
                type: [(a)-[:HAS_TYPE]->(at:AnnotationType) | at.name][0],
                name: a.name,
                aligned_to: [(a)-[:ALIGNED_TO]->(target:Annotation) | elementId(target)][0]
            }}
        ],
        colophon: m.colophon,
        copyright: [(m)-[:HAS_COPYRIGHT]->(cs:CopyrightStatus) | cs.status][0],
        incipit_title: [{title_primary('m')}],
        alt_incipit_titles: [{title_alternative('m')}]
    }} AS manifestation
"""


# INSERT_EXPRESSION_QUERY


# set alternative title
# set title
# set alternative name
# set name
# set language
# add / remove contributor
# set type

# create expression
# create manifestation
# create work
# create person
# create language

# delete expression
# delete manifestation
# delete work
# delete person
# delete language
