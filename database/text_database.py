from typing import TYPE_CHECKING, Any, LiteralString

from neo4j.exceptions import ConstraintError

from exceptions import DataConflictError, DataNotFoundError, DataValidationError
from identifier import generate_id
from models.contribution import AIContribution, PersonContributionBase
from models.requests import TextFilter
from models.text import TextInput, TextOutput, TextPatch

from .database_validator import DatabaseValidator
from .nomen_database import NomenDatabase
from .search_text import build_substring_search_value
from .tag_database import TagDatabase

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from .database import Database


class TextDatabase:
    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    _TEXT_RETURN: LiteralString = """
    {
        id: e.id,
        bdrc: e.bdrc,
        wiki: e.wiki,
        commentary_of: [(e)-[:COMMENTARY_OF]->(c_target:Text) | c_target.id][0],
        translation_of: [(e)-[:TRANSLATION_OF]->(t_target:Text) | t_target.id][0],
        commentaries: [(e)<-[:COMMENTARY_OF]-(c_child:Text) | c_child.id],
        translations: [(e)<-[:TRANSLATION_OF]-(t_child:Text) | t_child.id],
        contributions: (
            [(e)-[:HAS_CONTRIBUTION]->(contrib:Contribution)-[:BY]->(person:Person) | {
                type: "person",
                id: person.id,
                bdrc_id: person.bdrc,
                role: [(contrib)-[:WITH_ROLE]->(role:RoleType) | role.name][0],
                name: CASE WHEN EXISTS {
                    (person)-[:HAS_NAME]->(person_name:Nomen)-[:HAS_LOCALIZATION]->(:LocalizedText)
                    WHERE NOT EXISTS { (person_name)-[:ALTERNATIVE_OF]->(:Nomen) }
                } THEN apoc.map.fromPairs([(person)-[:HAS_NAME]->(n:Nomen)-[:HAS_LOCALIZATION]->
                    (lt:LocalizedText)-[r:HAS_LANGUAGE]->(lang:Language)
                    WHERE NOT EXISTS { (n)-[:ALTERNATIVE_OF]->(:Nomen) } |
                    [coalesce(r.bcp47, lang.code), lt.text]]) ELSE null END
            }]
            +
            [(e)-[:HAS_CONTRIBUTION]->(contrib:Contribution)-[:BY]->(ai:AI) | {
                type: "ai",
                id: ai.id,
                role: [(contrib)-[:WITH_ROLE]->(role:RoleType) | role.name][0]
            }]
        ),
        date: e.date,
        title: apoc.map.fromPairs([(e)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->
            (lt:LocalizedText)-[r:HAS_LANGUAGE]->(lang:Language)
            WHERE NOT EXISTS { (n)-[:ALTERNATIVE_OF]->(:Nomen) } |
            [coalesce(r.bcp47, lang.code), lt.text]]),
        alt_titles: [(e)-[:HAS_TITLE]->(:Nomen)<-[:ALTERNATIVE_OF]-(an:Nomen) |
            apoc.map.fromPairs([(an)-[:HAS_LOCALIZATION]->(lt:LocalizedText)-[r:HAS_LANGUAGE]->(lang:Language) |
                [coalesce(r.bcp47, lang.code), lt.text]])],
        language: [(e)-[r:HAS_LANGUAGE]->(lang:Language) | coalesce(r.bcp47, lang.code)][0],
        category_id: [(e)-[:TEXT_OF]->(work:Work)-[:HAS_CATEGORY]->(cat:Category) | cat.id][0],
        license: coalesce([(e)-[:HAS_LICENSE]->(license:LicenseType) | license.name][0], "public"),
        editions: [(e)<-[:EDITION_OF]-(m:Edition) | m.id],
        tag_ids: [(e)-[:TEXT_OF]->(w:Work)-[:HAS_TAG]->(t:Tag)
            WHERE ($application IS NULL OR (t)-[:BELONGS_TO]->(:Application {id: $application})) | t.id]
    } AS text
    """

    GET_QUERY: LiteralString = f"""
    MATCH (e:Text {{id: $id}})
    RETURN {_TEXT_RETURN}
    """

    GET_ALL_QUERY: LiteralString = f"""
    CALL {{
        WITH $title_search AS title_search
        WITH title_search WHERE title_search IS NULL
        MATCH (e:Text)
        RETURN e
        UNION
        WITH $title_search AS title_search
        WITH title_search WHERE title_search IS NOT NULL
        MATCH (lt:LocalizedText)
        WHERE lt.search_text CONTAINS title_search
        MATCH (lt)<-[:HAS_LOCALIZATION]-(n:Nomen)
        CALL (n) {{
            MATCH (n)<-[:HAS_TITLE]-(e:Text)
            RETURN e
          UNION
            MATCH (n)-[:ALTERNATIVE_OF]->(:Nomen)<-[:HAS_TITLE]-(e:Text)
            RETURN e
        }}
        RETURN DISTINCT e
    }}
    WITH e
    WHERE ($language IS NULL OR (e)-[:HAS_LANGUAGE]->(:Language {{code: $language}}))
    AND ($category_id IS NULL OR (e)-[:TEXT_OF]->(:Work)-[:HAS_CATEGORY]->(:Category {{id: $category_id}}))
    AND ($author_id IS NULL OR EXISTS {{
        (e)-[:HAS_CONTRIBUTION]->(:Contribution)-[:BY]->(p:Person {{id: $author_id}})
    }})
    AND ($tag_id IS NULL OR (e)-[:TEXT_OF]->(:Work)-[:HAS_TAG]->(:Tag {{id: $tag_id}}))
    AND ($bdrc IS NULL OR e.bdrc = $bdrc)
    AND ($wiki IS NULL OR e.wiki = $wiki)
    WITH e
    ORDER BY e.id
    SKIP $offset
    LIMIT $limit
    RETURN {_TEXT_RETURN}
    """

    UPDATE_LICENSE_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})
    OPTIONAL MATCH (e)-[r:HAS_LICENSE]->()
    DELETE r
    WITH e
    MATCH (license:LicenseType {name: $license})
    MERGE (e)-[:HAS_LICENSE]->(license)
    RETURN e.id as text_id
    """

    UPDATE_LANGUAGE_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})
    OPTIONAL MATCH (e)-[r:HAS_LANGUAGE]->()
    DELETE r
    WITH e
    MATCH (lang:Language {code: $language_code})
    MERGE (e)-[:HAS_LANGUAGE {bcp47: $bcp47_tag}]->(lang)
    RETURN e.id as text_id
    """

    UPDATE_CATEGORY_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})-[:TEXT_OF]->(w:Work)
    OPTIONAL MATCH (w)-[r:HAS_CATEGORY]->()
    DELETE r
    WITH w
    MATCH (cat:Category {id: $category_id})
    MERGE (w)-[:HAS_CATEGORY]->(cat)
    RETURN w.id as work_id
    """

    UPDATE_PROPERTIES_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})
    SET e.bdrc = $bdrc, e.wiki = $wiki, e.date = $date
    RETURN e.id as text_id
    """

    DELETE_TITLE_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})-[:HAS_TITLE]->(n:Nomen)
    OPTIONAL MATCH (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
    OPTIONAL MATCH (n)<-[:ALTERNATIVE_OF]-(alt:Nomen)-[:HAS_LOCALIZATION]->(alt_lt:LocalizedText)
    DETACH DELETE n, lt, alt, alt_lt
    FINISH
    """

    LINK_TITLE_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})
    MATCH (n:Nomen {id: $nomen_id})
    CREATE (e)-[:HAS_TITLE]->(n)
    FINISH
    """

    _CREATE_TEXT_LINKS: LiteralString = """
    MATCH (n:Nomen {id: $title_nomen_id}), (l:Language {code: $language_code})
    MATCH (license:LicenseType {name: $license})
    MERGE (e)-[:HAS_LANGUAGE {bcp47: $bcp47_tag}]->(l)
    MERGE (e)-[:HAS_TITLE]->(n)
    MERGE (e)-[:HAS_LICENSE]->(license)
    RETURN e.id as text_id
    """

    CREATE_STANDALONE_QUERY: LiteralString = f"""
    CREATE (w:Work {{id: $work_id}})
    CREATE (e:Text {{id: $text_id, bdrc: $bdrc, wiki: $wiki, date: $date}})
    MERGE (e)-[:TEXT_OF {{original: $original}}]->(w)
    {_CREATE_TEXT_LINKS}
    """

    CREATE_TRANSLATION_QUERY: LiteralString = f"""
    MATCH (target:Text {{id: $target_id}})-[:TEXT_OF]->(w:Work)
    CREATE (e:Text {{id: $text_id, bdrc: $bdrc, wiki: $wiki, date: $date}})
    MERGE (e)-[:TEXT_OF {{original: false}}]->(w)
    MERGE (e)-[:TRANSLATION_OF]->(target)
    {_CREATE_TEXT_LINKS}
    """

    CREATE_COMMENTARY_QUERY: LiteralString = f"""
    MATCH (target:Text {{id: $target_id}})
    CREATE (w:Work {{id: $work_id}})
    CREATE (e:Text {{id: $text_id, bdrc: $bdrc, wiki: $wiki, date: $date}})
    MERGE (e)-[:COMMENTARY_OF]->(target)
    MERGE (e)-[:TEXT_OF {{original: true}}]->(w)
    {_CREATE_TEXT_LINKS}
    """

    UPDATE_TAGS_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})-[:TEXT_OF]->(w:Work)
    OPTIONAL MATCH (w)-[r:HAS_TAG]->(:Tag)
    DELETE r
    WITH w
    UNWIND $tag_ids AS tag_id
    MATCH (t:Tag {id: tag_id})
    MERGE (w)-[:HAS_TAG]->(t)
    RETURN w.id AS work_id
    """

    GET_WORK_ID_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})-[:TEXT_OF]->(w:Work)
    RETURN w.id AS work_id
    """

    LINK_WORK_TO_CATEGORY_QUERY: LiteralString = """
    MATCH (w:Work {id: $work_id})
    MATCH (c:Category {id: $category_id})
    CREATE (w)-[:HAS_CATEGORY]->(c)
    FINISH
    """

    CREATE_CONTRIBUTION_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})
    MATCH (p:Person) WHERE (($person_id IS NOT NULL AND p.id = $person_id)
                            OR ($person_bdrc_id IS NOT NULL AND p.bdrc = $person_bdrc_id))
    MATCH (rt:RoleType {name: $role_name})
    CREATE (c:Contribution)
    CREATE (e)-[:HAS_CONTRIBUTION]->(c),
           (c)-[:BY]->(p),
           (c)-[:WITH_ROLE]->(rt)
    RETURN elementId(c) as contribution_element_id
    """

    CREATE_AI_CONTRIBUTION_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})
    MATCH (rt:RoleType {name: $role_name})
    MERGE (ai:AI {id: $ai_id})
    CREATE (e)-[:HAS_CONTRIBUTION]->(c:Contribution)-[:BY]->(ai),
        (c)-[:WITH_ROLE]->(rt)
    RETURN elementId(c) as contribution_element_id
    """

    DELETE_CHECK_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})-[:TEXT_OF]->(w:Work)
    RETURN w.id AS work_id,
           count { (e)<-[:EDITION_OF]-(:Edition) } AS edition_count,
           count { (e)<-[:TRANSLATION_OF]-(:Text) } AS translation_count,
           count { (e)<-[:COMMENTARY_OF]-(:Text) } AS commentary_count
    """

    DELETE_QUERY: LiteralString = """
    MATCH (e:Text {id: $text_id})-[:TEXT_OF]->(w:Work)
    WITH e, w, count { (:Text)-[:TEXT_OF]->(w) } AS work_text_count
    CALL (e) {
        OPTIONAL MATCH (e)-[:HAS_TITLE]->(n:Nomen)
        OPTIONAL MATCH (n)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
        OPTIONAL MATCH (n)<-[:ALTERNATIVE_OF]-(alt:Nomen)-[:HAS_LOCALIZATION]->(alt_lt:LocalizedText)
        DETACH DELETE n, lt, alt, alt_lt
        RETURN count(*) AS title_delete_count
    }
    CALL (e) {
        OPTIONAL MATCH (e)-[:HAS_CONTRIBUTION]->(c:Contribution)
        DETACH DELETE c
        RETURN count(*) AS contribution_delete_count
    }
    WITH e, w, work_text_count
    DETACH DELETE e
    WITH w, work_text_count
    FOREACH (_ IN CASE WHEN work_text_count = 1 THEN [1] ELSE [] END | DETACH DELETE w)
    RETURN work_text_count = 1 AS work_deleted
    """

    async def get(self, text_id: str, application: str | None = None) -> TextOutput:
        async def read(tx: AsyncManagedTransaction) -> TextOutput:
            result = await tx.run(TextDatabase.GET_QUERY, id=text_id, application=application)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Text with ID '{text_id}' not found")
            return TextOutput.model_validate(record["text"])

        async with self.session as session:
            return await session.execute_read(read)

    async def get_work_id(self, text_id: str) -> str:
        async def read(tx: AsyncManagedTransaction) -> str:
            result = await tx.run(TextDatabase.GET_WORK_ID_QUERY, text_id=text_id)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Text with ID '{text_id}' not found or has no associated Work")
            return record["work_id"]

        async with self.session as session:
            return await session.execute_read(read)

    async def get_all(
        self, offset: int, limit: int, filters: TextFilter | None = None, application: str | None = None
    ) -> list[TextOutput]:
        filters = filters or TextFilter()

        async def read(tx: AsyncManagedTransaction) -> list[TextOutput]:
            if filters.language:
                await DatabaseValidator.validate_language_code_exists(tx, filters.language)
            result = await tx.run(
                TextDatabase.GET_ALL_QUERY,
                offset=offset,
                limit=limit,
                language=filters.language,
                title_search=build_substring_search_value(filters.title),
                category_id=filters.category_id,
                author_id=filters.author_id,
                tag_id=filters.tag_id,
                bdrc=filters.bdrc,
                wiki=filters.wiki,
                application=application,
            )
            return [TextOutput.model_validate(record["text"]) for record in await result.data()]

        async with self.session as session:
            return await session.execute_read(read)

    async def create(self, text: TextInput) -> str:
        try:
            async with self.session as session:
                return await session.execute_write(lambda tx: TextDatabase.create_with_transaction(tx, text))
        except ConstraintError as e:
            raise DataConflictError(str(e)) from e

    async def delete(self, text_id: str) -> None:
        async def write(tx: AsyncManagedTransaction) -> None:
            result = await tx.run(TextDatabase.DELETE_CHECK_QUERY, text_id=text_id)
            record = await result.single()
            if record is None:
                raise DataNotFoundError(f"Text with ID '{text_id}' not found")

            blockers = []
            if record["edition_count"]:
                blockers.append(f"{record['edition_count']} edition(s)")
            if record["translation_count"]:
                blockers.append(f"{record['translation_count']} translation(s)")
            if record["commentary_count"]:
                blockers.append(f"{record['commentary_count']} commentary/commentaries")
            if blockers:
                raise DataConflictError(f"Text '{text_id}' cannot be deleted because it has {', '.join(blockers)}")

            await tx.run(TextDatabase.DELETE_QUERY, text_id=text_id)

        async with self.session as session:
            await session.execute_write(write)

    @staticmethod
    async def create_with_transaction(tx: AsyncManagedTransaction, text: TextInput, text_id: str | None = None) -> str:
        text_id = text_id or generate_id()
        await TextDatabase._validate_translation_language(tx, text)

        work_id = generate_id()
        await DatabaseValidator.validate_text_creation(tx, text, work_id)
        base_lang_code = text.language.split("-")[0].lower()
        await DatabaseValidator.validate_language_code_exists(tx, base_lang_code)
        await DatabaseValidator.validate_category_exists(tx, text.category_id)

        alt_titles = [dict(t.root) for t in text.alt_titles] if text.alt_titles else []
        title_nomen_id = await NomenDatabase.create_with_transaction(tx, dict(text.title.root), alt_titles)

        params: dict[str, Any] = {
            "text_id": text_id,
            "bdrc": text.bdrc,
            "wiki": text.wiki,
            "date": text.date,
            "language_code": base_lang_code,
            "bcp47_tag": text.language,
            "title_nomen_id": title_nomen_id,
            "target_id": text.translation_of or text.commentary_of,
            "license": text.license.value,
        }

        if text.commentary_of:
            await tx.run(TextDatabase.CREATE_COMMENTARY_QUERY, work_id=work_id, **params)
        elif text.translation_of:
            await tx.run(TextDatabase.CREATE_TRANSLATION_QUERY, **params)
        else:
            await tx.run(TextDatabase.CREATE_STANDALONE_QUERY, work_id=work_id, original=True, **params)

        if text.category_id:
            await tx.run(TextDatabase.LINK_WORK_TO_CATEGORY_QUERY, work_id=work_id, category_id=text.category_id)

        for contribution in text.contributions or []:
            await TextDatabase._create_contribution(tx, text_id, contribution)

        if text.tag_ids:
            await DatabaseValidator.validate_tags_exist(tx, list(text.tag_ids))
            for tag_id in text.tag_ids:
                await TagDatabase.tag_work_with_transaction(tx, work_id, tag_id)

        return text_id

    @staticmethod
    async def _validate_translation_language(tx: AsyncManagedTransaction, text: TextInput) -> None:
        if not text.translation_of:
            return
        result = await tx.run(TextDatabase.GET_QUERY, id=text.translation_of, bdrc_id=None, application=None)
        record = await result.single()
        if not record:
            raise DataNotFoundError(f"Target text '{text.translation_of}' not found for translation")
        target_language = record.data()["text"]["language"]
        if target_language == text.language:
            raise DataValidationError("Translation must have a different language than the target text")

    @staticmethod
    async def _create_contribution(
        tx: AsyncManagedTransaction, text_id: str, contribution: PersonContributionBase | AIContribution
    ) -> None:
        if isinstance(contribution, PersonContributionBase):
            result = await tx.run(
                TextDatabase.CREATE_CONTRIBUTION_QUERY,
                text_id=text_id,
                person_id=contribution.id,
                person_bdrc_id=contribution.bdrc_id,
                role_name=contribution.role.value,
            )
            record = await result.single()
            if not record:
                raise DataNotFoundError(
                    f"Person or Role not found. Person: id={contribution.id}, "
                    f"bdrc_id={contribution.bdrc_id}; Role: {contribution.role.value}"
                )
        elif isinstance(contribution, AIContribution):
            result = await tx.run(
                TextDatabase.CREATE_AI_CONTRIBUTION_QUERY,
                text_id=text_id,
                ai_id=contribution.id,
                role_name=contribution.role.value,
            )
            record = await result.single()
            if not record:
                raise DataNotFoundError(
                    f"AI contribution creation failed. AI: {contribution.id}; Role: {contribution.role.value}"
                )

    async def update(self, text_id: str, patch: TextPatch, application: str | None = None) -> TextOutput:
        existing = await self.get(text_id)

        merged_bdrc = patch.bdrc if patch.bdrc is not None else existing.bdrc
        merged_wiki = patch.wiki if patch.wiki is not None else existing.wiki
        merged_date = patch.date if patch.date is not None else existing.date
        merged_title: dict[str, str] = dict(patch.title.root) if patch.title is not None else dict(existing.title.root)
        merged_alt_titles: list[dict[str, str]] | None = (
            [dict(alt.root) for alt in patch.alt_titles]
            if patch.alt_titles is not None
            else ([dict(alt.root) for alt in existing.alt_titles] if existing.alt_titles else None)
        )
        merged_language = patch.language if patch.language is not None else existing.language
        merged_category_id = patch.category_id if patch.category_id is not None else existing.category_id
        merged_license = patch.license if patch.license is not None else existing.license

        TextOutput.model_validate(
            {
                "id": existing.id,
                "bdrc": merged_bdrc,
                "wiki": merged_wiki,
                "date": merged_date,
                "title": merged_title,
                "alt_titles": merged_alt_titles,
                "language": merged_language,
                "category_id": merged_category_id,
                "license": merged_license,
                "contributions": [c.model_dump() for c in existing.contributions],
            }
        )

        async def update_transaction(tx: AsyncManagedTransaction) -> None:
            await tx.run(
                TextDatabase.UPDATE_PROPERTIES_QUERY,
                text_id=text_id,
                bdrc=merged_bdrc,
                wiki=merged_wiki,
                date=merged_date,
            )

            if patch.title is not None or patch.alt_titles is not None:
                await tx.run(TextDatabase.DELETE_TITLE_QUERY, text_id=text_id)
                title_nomen_id = await NomenDatabase.create_with_transaction(tx, merged_title, merged_alt_titles)
                await tx.run(TextDatabase.LINK_TITLE_QUERY, text_id=text_id, nomen_id=title_nomen_id)

            if patch.license is not None:
                await tx.run(TextDatabase.UPDATE_LICENSE_QUERY, text_id=text_id, license=patch.license.value)

            if patch.language is not None:
                base_lang_code = patch.language.split("-")[0].lower()
                await tx.run(
                    TextDatabase.UPDATE_LANGUAGE_QUERY,
                    text_id=text_id,
                    language_code=base_lang_code,
                    bcp47_tag=patch.language,
                )

            if patch.category_id is not None:
                await tx.run(TextDatabase.UPDATE_CATEGORY_QUERY, text_id=text_id, category_id=patch.category_id)

            if patch.tag_ids is not None:
                await DatabaseValidator.validate_tags_exist(tx, list(patch.tag_ids))
                await tx.run(
                    TextDatabase.UPDATE_TAGS_QUERY,
                    text_id=text_id,
                    tag_ids=list(patch.tag_ids),
                )

        async with self.session as session:
            try:
                await session.execute_write(update_transaction)
            except ConstraintError as e:
                raise DataConflictError(str(e)) from e
            return await self.get(text_id, application=application)
