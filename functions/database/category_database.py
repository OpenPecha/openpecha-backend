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

    def create(self, category: CategoryInput, application: str) -> str:
        def create_transaction(tx: ManagedTransaction) -> str:
            self._validate_not_exists_tx(tx, application, category.title.root, category.parent_id)

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
