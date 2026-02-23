from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataNotFoundError, DataValidationError
from identifier import generate_id

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from models import CategoryInput, CategoryOutput, CategoryPatch
    from neo4j import ManagedTransaction, Session

    from .database import Database

_CATEGORY_RETURN = """
{
    id: c.id,
    title: [(c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
        -[:HAS_LANGUAGE]->(l:Language) | {language: l.code, text: lt.text}],
    description: [(c)-[:HAS_DESCRIPTION]->(dn:Nomen)-[:HAS_LOCALIZATION]->(dlt:LocalizedText)
        -[:HAS_LANGUAGE]->(dl:Language) | {language: dl.code, text: dlt.text}],
    parent_id: [(c)-[:HAS_PARENT]->(parent:Category) | parent.id][0],
    children: [(child:Category)-[:HAS_PARENT]->(c) | child.id]
}
"""


class CategoryDatabase:
    GET_ALL_QUERY = f"""
    MATCH (c:Category)-[:BELONGS_TO]->(app:Application {{id: $application}})
    WHERE ($parent_id IS NULL AND NOT EXISTS {{ (c)-[:HAS_PARENT]->(:Category) }})
       OR (c)-[:HAS_PARENT]->(:Category {{id: $parent_id}})
    RETURN {_CATEGORY_RETURN} AS category
    """

    GET_BY_ID_QUERY = f"""
    MATCH (c:Category {{id: $category_id}})-[:BELONGS_TO]->(app:Application {{id: $application}})
    RETURN {_CATEGORY_RETURN} AS category
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

    FIND_EXISTING_QUERY = """
        MATCH (c:Category)-[:BELONGS_TO]->(app:Application {id: $application})
        WHERE (($parent_id IS NULL AND NOT EXISTS { (c)-[:HAS_PARENT]->(:Category) })
        OR (c)-[:HAS_PARENT]->(:Category {id: $parent_id}))
        AND ($exclude_category_id IS NULL OR c.id <> $exclude_category_id)
        MATCH (c)-[:HAS_TITLE]->(:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(:Language {code: $language})
        WHERE toLower(lt.text) = toLower($title_text)
        RETURN c.id AS category_id
        LIMIT 1
    """

    DELETE_TITLE_QUERY = """
        MATCH (c:Category {id: $category_id})-[:HAS_TITLE]->(n:Nomen)
        DETACH DELETE n
    """

    LINK_TITLE_QUERY = """
        MATCH (c:Category {id: $category_id})
        MATCH (n:Nomen {id: $nomen_id})
        CREATE (c)-[:HAS_TITLE]->(n)
    """

    DELETE_DESCRIPTION_QUERY = """
        MATCH (c:Category {id: $category_id})-[:HAS_DESCRIPTION]->(n:Nomen)
        DETACH DELETE n
    """

    LINK_DESCRIPTION_QUERY = """
        MATCH (c:Category {id: $category_id})
        MATCH (n:Nomen {id: $nomen_id})
        CREATE (c)-[:HAS_DESCRIPTION]->(n)
    """

    REMOVE_PARENT_QUERY = """
        MATCH (c:Category {id: $category_id})-[r:HAS_PARENT]->()
        DELETE r
    """

    SET_PARENT_QUERY = """
        MATCH (c:Category {id: $category_id})
        MATCH (parent:Category {id: $parent_id})-[:BELONGS_TO]->(app:Application {id: $application})
        CREATE (c)-[:HAS_PARENT]->(parent)
        RETURN parent.id AS parent_id
    """

    def __init__(self, db: Database) -> None:
        self._db = db

    @property
    def session(self) -> Session:
        return self._db.get_session()

    def get_all(self, application: str, parent_id: str | None = None) -> list[CategoryOutput]:
        with self.session as session:
            result = session.run(
                CategoryDatabase.GET_ALL_QUERY,
                application=application,
                parent_id=parent_id,
            )
            return [DataAdapter.category(record.data()["category"]) for record in result]

    def get(self, category_id: str, application: str) -> CategoryOutput:
        with self.session as session:
            result = session.run(
                CategoryDatabase.GET_BY_ID_QUERY,
                category_id=category_id,
                application=application,
            )
            record = result.single()
            if not record:
                raise DataNotFoundError(
                    f"Category with ID '{category_id}' not found for application '{application}'"
                )
            return DataAdapter.category(record.data()["category"])

    def create(self, category: CategoryInput, application: str) -> str:
        def create_transaction(tx: ManagedTransaction) -> str:
            self._validate_not_exists_tx(
                tx, application, category.title.root, category.parent_id, exclude_category_id=None
            )

            category_id = generate_id()
            nomen_id = NomenDatabase.create_with_transaction(tx, category.title.root, None)
            description_nomen_id = None
            if category.description is not None:
                description_nomen_id = NomenDatabase.create_with_transaction(
                    tx, category.description.root, None
                )

            result = tx.run(
                CategoryDatabase.CREATE_QUERY,
                category_id=category_id,
                application=application,
                nomen_id=nomen_id,
                parent_id=category.parent_id,
                description_nomen_id=description_nomen_id,
            )
            record = result.single(strict=True)
            return record["category_id"]

        with self.session as session:
            return str(session.execute_write(create_transaction))

    def _validate_not_exists_tx(
        self,
        tx: ManagedTransaction,
        application: str,
        title: dict[str, str],
        parent_id: str | None,
        *,
        exclude_category_id: str | None = None,
    ) -> None:
        for language, title_text in title.items():
            result = tx.run(
                CategoryDatabase.FIND_EXISTING_QUERY,
                application=application,
                parent_id=parent_id,
                language=language,
                title_text=title_text,
                exclude_category_id=exclude_category_id,
            )
            record = result.single()
            if record:
                raise DataValidationError(
                    f"Category with title '{title_text}' in language '{language}' "
                    f"already exists for application '{application}'"
                )

    def update(
        self, category_id: str, patch: CategoryPatch, application: str
    ) -> CategoryOutput:
        existing = self.get(category_id, application)
        fields_set = patch.model_fields_set

        def update_transaction(tx: ManagedTransaction) -> None:
            if "title" in fields_set and patch.title is not None:
                self._validate_not_exists_tx(
                    tx,
                    application,
                    patch.title.root,
                    patch.parent_id if "parent_id" in fields_set else existing.parent_id,
                    exclude_category_id=category_id,
                )
                tx.run(CategoryDatabase.DELETE_TITLE_QUERY, category_id=category_id)
                nomen_id = NomenDatabase.create_with_transaction(
                    tx, patch.title.root, None
                )
                tx.run(
                    CategoryDatabase.LINK_TITLE_QUERY,
                    category_id=category_id,
                    nomen_id=nomen_id,
                )
            elif "title" in fields_set and patch.title is None:
                raise DataValidationError("Title cannot be null")

            if "description" in fields_set:
                tx.run(CategoryDatabase.DELETE_DESCRIPTION_QUERY, category_id=category_id)
                if patch.description is not None:
                    desc_nomen_id = NomenDatabase.create_with_transaction(
                        tx, patch.description.root, None
                    )
                    tx.run(
                        CategoryDatabase.LINK_DESCRIPTION_QUERY,
                        category_id=category_id,
                        nomen_id=desc_nomen_id,
                    )

            if "parent_id" in fields_set:
                tx.run(CategoryDatabase.REMOVE_PARENT_QUERY, category_id=category_id)
                if patch.parent_id is not None:
                    result = tx.run(
                        CategoryDatabase.SET_PARENT_QUERY,
                        category_id=category_id,
                        parent_id=patch.parent_id,
                        application=application,
                    )
                    if result.single() is None:
                        raise DataNotFoundError(
                            f"Parent category '{patch.parent_id}' not found for application '{application}'"
                        )

        with self.session as session:
            session.execute_write(update_transaction)
        return self.get(category_id, application)
