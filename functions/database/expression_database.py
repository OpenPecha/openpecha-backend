from __future__ import annotations

from typing import TYPE_CHECKING, Any

from exceptions import DataNotFoundError, DataValidationError
from identifier import generate_id
from models import (
    AIContributionModel,
    ContributionBase,
    ContributionInput,
    ExpressionInput,
    ExpressionOutput,
    LicenseType,
)
from request_models import ExpressionFilter

from .data_adapter import DataAdapter
from .database_validator import DatabaseValidator
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from neo4j import ManagedTransaction, Record, Session

    from .database import Database


class ExpressionDatabase:
    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    _EXPRESSION_RETURN = """
    {
        id: e.id,
        bdrc: e.bdrc,
        wiki: e.wiki,
        commentary_of: [(e)-[:COMMENTARY_OF]->(c_target:Expression) | c_target.id][0],
        translation_of: [(e)-[:TRANSLATION_OF]->(t_target:Expression) | t_target.id][0],
        commentaries: [(e)<-[:COMMENTARY_OF]-(c_child:Expression) | c_child.id],
        translations: [(e)<-[:TRANSLATION_OF]-(t_child:Expression) | t_child.id],
        contributors: (
            [(e)-[:HAS_CONTRIBUTION]->(contrib:Contribution)-[:BY]->(person:Person) | {
                person_id: person.id,
                person_bdrc_id: person.bdrc,
                role: [(contrib)-[:WITH_ROLE]->(role:RoleType) | role.name][0],
                person_name: [(person)-[:HAS_NAME]->(n:Nomen)-[:HAS_LOCALIZATION]->
                    (lt:LocalizedText)-[r:HAS_LANGUAGE]->(lang:Language)
                    WHERE NOT EXISTS { (n)-[:ALTERNATIVE_OF]->(:Nomen) } |
                    {language: coalesce(r.bcp47, lang.code), text: lt.text}]
            }]
            +
            [(e)-[:HAS_CONTRIBUTION]->(contrib:Contribution)-[:BY]->(ai:AI) | {
                ai_id: ai.id,
                role: [(contrib)-[:WITH_ROLE]->(role:RoleType) | role.name][0]
            }]
        ),
        date: e.date,
        title: [(e)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[r:HAS_LANGUAGE]->(lang:Language)
            WHERE NOT EXISTS { (n)-[:ALTERNATIVE_OF]->(:Nomen) } |
            {language: coalesce(r.bcp47, lang.code), text: lt.text}],
        alt_titles: [(e)-[:HAS_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-(an:Nomen) |
            [(an)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[r:HAS_LANGUAGE]->(lang:Language) |
                {language: coalesce(r.bcp47, lang.code), text: lt.text}]],
        language: [(e)-[r:HAS_LANGUAGE]->(lang:Language) | coalesce(r.bcp47, lang.code)][0],
        category_id: [(e)-[:EXPRESSION_OF]->(work:Work)-[:HAS_CATEGORY]->(cat:Category) | cat.id][0],
        license: [(e)-[:HAS_LICENSE]->(license:LicenseType) | license.name][0],
        editions: [(e)<-[:MANIFESTATION_OF]-(m:Manifestation) | m.id]
    } AS expression
    """

    GET_QUERY = f"""
    MATCH (e:Expression)
    WHERE ($id IS NOT NULL AND e.id = $id)
       OR ($bdrc_id IS NOT NULL AND e.bdrc = $bdrc_id)
    RETURN {_EXPRESSION_RETURN}
    """

    GET_ALL_QUERY = f"""
    MATCH (e:Expression)
    WHERE ($language IS NULL OR EXISTS {{ (e)-[:HAS_LANGUAGE]->(:Language {{code: $language}}) }})
    AND ($title IS NULL OR EXISTS {{
        MATCH (e)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
        WHERE toLower(lt.text) CONTAINS toLower($title)
    }} OR EXISTS {{
        MATCH (e)-[:HAS_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-(an:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
        WHERE toLower(lt.text) CONTAINS toLower($title)
    }})
    AND ($category_id IS NULL OR EXISTS {{
        (e)-[:EXPRESSION_OF]->(:Work)-[:HAS_CATEGORY]->(:Category {{id: $category_id}})
    }})
    WITH e
    ORDER BY e.id
    SKIP $offset
    LIMIT $limit
    RETURN {_EXPRESSION_RETURN}
    """

    UPDATE_TITLE_QUERY = """
    MATCH (e:Expression {id: $expression_id})-[:HAS_TITLE]->(primary_nomen:Nomen)
    MATCH (l:Language {code: $title.lang_code})
    OPTIONAL MATCH (primary_nomen)-[:HAS_LOCALIZATION]->(existing_lt:LocalizedText)-[:HAS_LANGUAGE]->(l)
    WITH e, primary_nomen, l, existing_lt
    CALL (*) {
        WHEN existing_lt IS NOT NULL THEN { SET existing_lt.text = $title.text }
        ELSE { CREATE (primary_nomen)-[:HAS_LOCALIZATION]->(:LocalizedText {text: $title.text})-[:HAS_LANGUAGE]->(l) }
    }
    RETURN e.id as expression_id
    """

    UPDATE_LICENSE_QUERY = """
    MATCH (e:Expression {id: $expression_id})
    OPTIONAL MATCH (e)-[lc_rel:HAS_LICENSE]->(license:LicenseType)
    WITH e, lc_rel
    DELETE lc_rel
    MATCH (license:LicenseType {name: $license})
    MERGE (e)-[:HAS_LICENSE]->(license)
    RETURN e.id as expression_id
    """

    _CREATE_EXPRESSION_LINKS = """
    MATCH (n:Nomen {id: $title_nomen_id}), (l:Language {code: $language_code})
    MATCH (license:LicenseType {name: $license})
    MERGE (e)-[:HAS_LANGUAGE {bcp47: $bcp47_tag}]->(l)
    MERGE (e)-[:HAS_TITLE]->(n)
    MERGE (e)-[:HAS_LICENSE]->(license)
    RETURN e.id as expression_id
    """

    CREATE_STANDALONE_QUERY = f"""
    CREATE (w:Work {{id: $work_id}})
    CREATE (e:Expression {{id: $expression_id, bdrc: $bdrc, wiki: $wiki, date: $date}})
    MERGE (e)-[:EXPRESSION_OF {{original: $original}}]->(w)
    {_CREATE_EXPRESSION_LINKS}
    """

    CREATE_TRANSLATION_QUERY = f"""
    MATCH (target:Expression {{id: $target_id}})-[:EXPRESSION_OF]->(w:Work)
    CREATE (e:Expression {{id: $expression_id, bdrc: $bdrc, wiki: $wiki, date: $date}})
    MERGE (e)-[:EXPRESSION_OF {{original: false}}]->(w)
    MERGE (e)-[:TRANSLATION_OF]->(target)
    {_CREATE_EXPRESSION_LINKS}
    """

    CREATE_COMMENTARY_QUERY = f"""
    MATCH (target:Expression {{id: $target_id}})
    CREATE (w:Work {{id: $work_id}})
    CREATE (e:Expression {{id: $expression_id, bdrc: $bdrc, wiki: $wiki, date: $date}})
    MERGE (e)-[:COMMENTARY_OF]->(target)
    MERGE (e)-[:EXPRESSION_OF {{original: true}}]->(w)
    {_CREATE_EXPRESSION_LINKS}
    """

    LINK_WORK_TO_CATEGORY_QUERY = """
    MATCH (w:Work {id: $work_id})
    MATCH (c:Category {id: $category_id})
    CREATE (w)-[:HAS_CATEGORY]->(c)
    """

    CREATE_CONTRIBUTION_QUERY = """
    MATCH (e:Expression {id: $expression_id})
    MATCH (p:Person) WHERE (($person_id IS NOT NULL AND p.id = $person_id)
                            OR ($person_bdrc_id IS NOT NULL AND p.bdrc = $person_bdrc_id))
    MATCH (rt:RoleType {name: $role_name})
    MERGE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(p)
    MERGE (c)-[:WITH_ROLE]->(rt)
    RETURN elementId(c) as contribution_element_id
    """

    CREATE_AI_CONTRIBUTION_QUERY = """
    MATCH (e:Expression {id: $expression_id})
    MATCH (rt:RoleType {name: $role_name})
    MERGE (ai:AI {id: $ai_id})
    CREATE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(ai),
        (c)-[:WITH_ROLE]->(rt)
    RETURN elementId(c) as contribution_element_id
    """

    @staticmethod
    def _parse_record(record: dict | Record) -> ExpressionOutput:
        data = record.get("expression", record) if isinstance(record, dict) else record.data()["expression"]
        return DataAdapter.expression(data)

    def get(self, expression_id: str) -> ExpressionOutput:
        with self.session as session:
            result = session.run(ExpressionDatabase.GET_QUERY, id=expression_id, bdrc_id=None).single()
            if result is None:
                raise DataNotFoundError(f"Expression with ID '{expression_id}' not found")
            return self._parse_record(result.data())

    def get_by_bdrc(self, bdrc_id: str) -> ExpressionOutput:
        with self.session as session:
            result = session.run(ExpressionDatabase.GET_QUERY, id=None, bdrc_id=bdrc_id).single()
            if result is None:
                raise DataNotFoundError(f"Expression with BDRC ID '{bdrc_id}' not found")
            return self._parse_record(result.data())

    def get_all(self, offset: int, limit: int, filters: ExpressionFilter | None = None) -> list[ExpressionOutput]:
        filters = filters or ExpressionFilter()

        def _get_all(tx: ManagedTransaction) -> list[ExpressionOutput]:
            if filters.language:
                DatabaseValidator.validate_language_code_exists(tx, filters.language)
            result = tx.run(
                ExpressionDatabase.GET_ALL_QUERY,
                offset=offset,
                limit=limit,
                language=filters.language,
                title=filters.title,
                category_id=filters.category_id,
            )
            return [self._parse_record(r.data()) for r in result]

        with self.session as session:
            return session.execute_read(_get_all)

    def update_title(self, expression_id: str, title: dict[str, str]) -> None:
        with self.session as session:
            result = session.execute_write(
                lambda tx: tx.run(
                    ExpressionDatabase.UPDATE_TITLE_QUERY, expression_id=expression_id, title=title
                ).single()
            )
            if result is None:
                raise DataNotFoundError(f"Expression with ID '{expression_id}' not found")

    def update_license(self, expression_id: str, license_type: LicenseType) -> None:
        with self.session as session:
            result = session.execute_write(
                lambda tx: tx.run(
                    ExpressionDatabase.UPDATE_LICENSE_QUERY,
                    expression_id=expression_id,
                    license=license_type.value,
                ).single()
            )
            if result is None:
                raise DataNotFoundError(f"Expression with ID '{expression_id}' not found")

    def create(self, expression: ExpressionInput) -> str:
        with self.session as session:
            return session.execute_write(lambda tx: ExpressionDatabase.create_with_transaction(tx, expression))

    def validate_create(self, expression: ExpressionInput) -> None:
        with self.session as session:
            session.execute_read(lambda tx: ExpressionDatabase._validate_create(tx, expression))

    @staticmethod
    def _validate_create(tx: ManagedTransaction, expression: ExpressionInput) -> None:
        DatabaseValidator.validate_expression_title_unique(tx, expression.title.root)
        if not expression.contributions:
            return
        person_ids = [c.person_id for c in expression.contributions if isinstance(c, ContributionInput) and c.person_id]
        person_bdrc_ids = [
            c.person_bdrc_id for c in expression.contributions if isinstance(c, ContributionInput) and c.person_bdrc_id
        ]
        DatabaseValidator.validate_person_references(tx, person_ids)
        DatabaseValidator.validate_person_bdrc_references(tx, person_bdrc_ids)

    @staticmethod
    def create_with_transaction(
        tx: ManagedTransaction, expression: ExpressionInput, expression_id: str | None = None
    ) -> str:
        expression_id = expression_id or generate_id()
        ExpressionDatabase._validate_translation_language(tx, expression)

        work_id = generate_id()
        DatabaseValidator.validate_expression_creation(tx, expression, work_id)
        base_lang_code = expression.language.split("-")[0].lower()
        DatabaseValidator.validate_language_code_exists(tx, base_lang_code)
        DatabaseValidator.validate_category_exists(tx, expression.category_id)

        alt_titles = [dict(t.root) for t in expression.alt_titles] if expression.alt_titles else []
        title_nomen_id = NomenDatabase.create_with_transaction(tx, dict(expression.title.root), alt_titles)

        params: dict[str, Any] = {
            "expression_id": expression_id,
            "bdrc": expression.bdrc,
            "wiki": expression.wiki,
            "date": expression.date,
            "language_code": base_lang_code,
            "bcp47_tag": expression.language,
            "title_nomen_id": title_nomen_id,
            "target_id": expression.translation_of or expression.commentary_of,
            "license": expression.license.value,
        }

        if expression.commentary_of:
            tx.run(ExpressionDatabase.CREATE_COMMENTARY_QUERY, work_id=work_id, **params)
        elif expression.translation_of:
            tx.run(ExpressionDatabase.CREATE_TRANSLATION_QUERY, **params)
        else:
            tx.run(ExpressionDatabase.CREATE_STANDALONE_QUERY, work_id=work_id, original=True, **params)

        if expression.category_id:
            tx.run(ExpressionDatabase.LINK_WORK_TO_CATEGORY_QUERY, work_id=work_id, category_id=expression.category_id)

        for contribution in expression.contributions or []:
            ExpressionDatabase._create_contribution(tx, expression_id, contribution)

        return expression_id

    @staticmethod
    def _validate_translation_language(tx: ManagedTransaction, expression: ExpressionInput) -> None:
        if not expression.translation_of:
            return
        result = tx.run(ExpressionDatabase.GET_QUERY, id=expression.translation_of, bdrc_id=None).single()
        if not result:
            raise DataNotFoundError(f"Target expression '{expression.translation_of}' not found for translation")
        target_language = result.data()["expression"]["language"]
        if target_language == expression.language:
            raise DataValidationError("Translation must have a different language than the target expression")

    @staticmethod
    def _create_contribution(
        tx: ManagedTransaction, expression_id: str, contribution: ContributionBase | AIContributionModel
    ) -> None:
        if isinstance(contribution, ContributionBase):
            result = tx.run(
                ExpressionDatabase.CREATE_CONTRIBUTION_QUERY,
                expression_id=expression_id,
                person_id=contribution.person_id,
                person_bdrc_id=contribution.person_bdrc_id,
                role_name=contribution.role.value,
            ).single()
            if not result:
                raise DataNotFoundError(
                    f"Person or Role not found. Person: id={contribution.person_id}, "
                    f"bdrc_id={contribution.person_bdrc_id}; Role: {contribution.role.value}"
                )
        elif isinstance(contribution, AIContributionModel):
            result = tx.run(
                ExpressionDatabase.CREATE_AI_CONTRIBUTION_QUERY,
                expression_id=expression_id,
                ai_id=contribution.ai_id,
                role_name=contribution.role.value,
            ).single()
            if not result:
                raise DataNotFoundError(
                    f"AI contribution creation failed. AI: {contribution.ai_id}; Role: {contribution.role.value}"
                )
