from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataValidationError
from identifier import generate_id

from .data_adapter import DataAdapter
from .nomen_database import NomenDatabase

if TYPE_CHECKING:
    from models import CategoryInput, CategoryOutput
    from neo4j import ManagedTransaction, Session

    from .database import Database


class CategoryDatabase:
    GET_ALL_QUERY = """
    MATCH (c:Category {application: $application})
    WHERE ($parent_id IS NULL AND NOT (c)-[:HAS_PARENT]->(:Category))
       OR ($parent_id IS NOT NULL AND (c)-[:HAS_PARENT]->(:Category {id: $parent_id}))
    RETURN {
        id: c.id,
        application: c.application,
        title: [(c)-[:HAS_TITLE]->(n:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
            -[:HAS_LANGUAGE]->(l:Language) | {language: l.code, text: lt.text}],
        parent_id: [(c)-[:HAS_PARENT]->(parent:Category) | parent.id][0],
        has_children: EXISTS { (child:Category)-[:HAS_PARENT]->(c) }
    } AS category
    """

    CREATE_QUERY = """
    MATCH (n:Nomen {id: $nomen_id})
    CREATE (c:Category {id: $category_id, application: $application})
    CREATE (c)-[:HAS_TITLE]->(n)
    WITH c
    OPTIONAL MATCH (parent:Category {id: $parent_id})
    FOREACH (_ IN CASE WHEN parent IS NOT NULL THEN [1] ELSE [] END |
        CREATE (c)-[:HAS_PARENT]->(parent)
    )
    RETURN c.id AS category_id
    """

    FIND_EXISTING_QUERY = """
    MATCH (c:Category {application: $application})
    WHERE ($parent_id IS NULL AND NOT (c)-[:HAS_PARENT]->(:Category))
       OR ($parent_id IS NOT NULL AND (c)-[:HAS_PARENT]->(:Category {id: $parent_id}))
    MATCH (c)-[:HAS_TITLE]->(:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
        -[:HAS_LANGUAGE]->(:Language {code: $language})
    WHERE toLower(lt.text) = toLower($title_text)
    RETURN c.id AS category_id
    LIMIT 1
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

    def create(self, category: CategoryInput) -> str:
        def create_transaction(tx: ManagedTransaction) -> str:
            self._validate_not_exists_tx(tx, category.application, category.title.root, category.parent_id)

            category_id = generate_id()
            nomen_id = NomenDatabase.create_with_transaction(tx, category.title.root, None)

            result = tx.run(
                CategoryDatabase.CREATE_QUERY,
                category_id=category_id,
                application=category.application,
                nomen_id=nomen_id,
                parent_id=category.parent_id,
            )
            record = result.single(strict=True)
            return record["category_id"]

        with self.session as session:
            return str(session.execute_write(create_transaction))

    def _validate_not_exists_tx(
        self, tx: ManagedTransaction, application: str, title: dict[str, str], parent_id: str | None
    ) -> None:
        for language, title_text in title.items():
            result = tx.run(
                CategoryDatabase.FIND_EXISTING_QUERY,
                application=application,
                parent_id=parent_id,
                language=language,
                title_text=title_text,
            )
            record = result.single()
            if record:
                raise DataValidationError(
                    f"Category with title '{title_text}' in language '{language}' "
                    f"already exists for application '{application}'"
                )
