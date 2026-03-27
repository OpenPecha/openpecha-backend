from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataValidationError
from identifier import generate_id

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from neo4j import AsyncManagedTransaction, AsyncSession

    from models.category import CategoryDetailOutput, CategoryInput, CategoryOutput

    from .database import Database


class CategoryDatabase:
    GET_ALL_QUERY = """
    MATCH (c:Category)-[:BELONGS_TO]->(app:Application {id: $application})
    WHERE ($parent_id IS NULL AND NOT EXISTS { (c)-[:HAS_PARENT]->(:Category) })
       OR (c)-[:HAS_PARENT]->(:Category {id: $parent_id})
    RETURN {
        id: c.id,
        title: [(c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(l:Language) | {language: l.code, text: lt.text}],
        description: [(c)-[:HAS_DESCRIPTION]->(dn:Nomen)-[:HAS_LOCALIZATION]->(dlt:LocalizedText)
            -[:HAS_LANGUAGE]->(dl:Language) | {language: dl.code, text: dlt.text}],
        parent_id: [(c)-[:HAS_PARENT]->(parent:Category) | parent.id][0],
        children: [(child:Category)-[:HAS_PARENT]->(c) | child.id]
    } AS category
    """

    CREATE_QUERY = """
        MATCH (n:Nomen {id: $nomen_id})
        MATCH (app:Application {id: $application})
        CREATE (c:Category {id: $category_id})
        CREATE (c)-[:HAS_TITLE]->(n)
        CREATE (c)-[:BELONGS_TO]->(app)
        WITH c
        OPTIONAL MATCH (parent:Category {id: $parent_id})
        OPTIONAL MATCH (desc_nomen:Nomen {id: $description_nomen_id})
        WITH c, parent, desc_nomen
        CALL (*) { WHEN parent IS NOT NULL THEN { CREATE (c)-[:HAS_PARENT]->(parent) } }
        WITH c, desc_nomen
        CALL (*) { WHEN desc_nomen IS NOT NULL THEN { CREATE (c)-[:HAS_DESCRIPTION]->(desc_nomen) } }
        RETURN c.id AS category_id
    """

    GET_BY_ID_QUERY = """
    MATCH (c:Category {id: $category_id})
    RETURN {
        id: c.id,
        title: [(c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(l:Language) | {language: l.code, text: lt.text}],
        description: [(c)-[:HAS_DESCRIPTION]->(dn:Nomen)-[:HAS_LOCALIZATION]->(dlt:LocalizedText)
            -[:HAS_LANGUAGE]->(dl:Language) | {language: dl.code, text: dlt.text}],
        parent_id: [(c)-[:HAS_PARENT]->(parent:Category) | parent.id][0],
        children: [
            (child:Category)-[:HAS_PARENT]->(c) | {
                id: child.id,
                title: [(child)-[:HAS_TITLE]->(cn:Nomen)-[:HAS_LOCALIZATION]->(clt:LocalizedText)
                    -[:HAS_LANGUAGE]->(cl:Language) | {language: cl.code, text: clt.text}],
                description: [(child)-[:HAS_DESCRIPTION]->(cdn:Nomen)-[:HAS_LOCALIZATION]->(cdlt:LocalizedText)
                    -[:HAS_LANGUAGE]->(cdl:Language) | {language: cdl.code, text: cdlt.text}],
                parent_id: c.id,
                children: [(grandchild:Category)-[:HAS_PARENT]->(child) | grandchild.id]
            }
        ]
    } AS category
    """

    FIND_EXISTING_QUERY = """
        MATCH (c:Category)-[:BELONGS_TO]->(app:Application {id: $application})
        WHERE ($parent_id IS NULL AND NOT EXISTS { (c)-[:HAS_PARENT]->(:Category) })
        OR (c)-[:HAS_PARENT]->(:Category {id: $parent_id})
        MATCH (c)-[:HAS_TITLE]->(:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(:Language {code: $language})
        WHERE toLower(lt.text) = toLower($title_text)
        RETURN c.id AS category_id
        LIMIT 1
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> AsyncSession:
        return self._db.get_session()

    async def get_all(self, application: str, parent_id: str | None = None) -> list[CategoryOutput]:
        async with self.session as session:
            result = await session.run(
                CategoryDatabase.GET_ALL_QUERY,
                application=application,
                parent_id=parent_id,
            )
            records = await result.data()
            return [DataAdapter.category(record["category"]) for record in records]

    async def get_by_id(self, category_id: str) -> CategoryDetailOutput | None:
        async with self.session as session:
            result = await session.run(CategoryDatabase.GET_BY_ID_QUERY, category_id=category_id)
            record = await result.single()
            if record is None:
                return None
            return DataAdapter.category_detail(record["category"])

    async def create(self, category: CategoryInput, application: str) -> str:
        async def create_transaction(tx: AsyncManagedTransaction) -> str:
            await self._validate_not_exists_tx(tx, application, category.title.root, category.parent_id)

            category_id = generate_id()
            nomen_id = await NomenDatabase.create_with_transaction(tx, category.title.root, None)
            description_nomen_id = None
            if category.description is not None:
                description_nomen_id = await NomenDatabase.create_with_transaction(tx, category.description.root, None)

            result = await tx.run(
                CategoryDatabase.CREATE_QUERY,
                category_id=category_id,
                application=application,
                nomen_id=nomen_id,
                parent_id=category.parent_id,
                description_nomen_id=description_nomen_id,
            )
            record = await result.single(strict=True)
            return record["category_id"]

        async with self.session as session:
            return str(await session.execute_write(create_transaction))

    async def _validate_not_exists_tx(
        self, tx: AsyncManagedTransaction, application: str, title: dict[str, str], parent_id: str | None
    ) -> None:
        for language, title_text in title.items():
            result = await tx.run(
                CategoryDatabase.FIND_EXISTING_QUERY,
                application=application,
                parent_id=parent_id,
                language=language,
                title_text=title_text,
            )
            record = await result.single()
            if record:
                raise DataValidationError(
                    f"Category with title '{title_text}' in language '{language}' "
                    f"already exists for application '{application}'"
                )
