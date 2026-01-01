from __future__ import annotations

from typing import TYPE_CHECKING

from exceptions import DataValidationError
from identifier import generate_id
from models import CategoryListItemModel

if TYPE_CHECKING:
    from neo4j import Session

    from .database import Database


class CategoryDatabase:
    GET_ALL_QUERY = """
    MATCH (c:Category {application: $application})
    WHERE ($parent_id IS NULL AND NOT (c)-[:HAS_PARENT]->(:Category))
       OR ($parent_id IS NOT NULL AND (c)-[:HAS_PARENT]->(:Category {id: $parent_id}))
    OPTIONAL MATCH (c)-[:HAS_PARENT]->(parent:Category)
    OPTIONAL MATCH (c)-[:HAS_TITLE]->(:Nomen)-[:HAS_LOCALIZATION]->(lt:LocalizedText)
        -[:HAS_LANGUAGE]->(:Language {code: $language})
    OPTIONAL MATCH (child:Category)-[:HAS_PARENT]->(c)
    WITH c, parent, lt, COUNT(DISTINCT child) > 0 AS has_child
    RETURN c.id AS id, parent.id AS parent, lt.text AS title, has_child
    """

    CREATE_QUERY = """
    CREATE (c:Category {id: $category_id, application: $application})
    CREATE (n:Nomen)
    CREATE (c)-[:HAS_TITLE]->(n)
    FOREACH (lt IN $localized_texts |
        MERGE (l:Language {code: lt.language})
        CREATE (n)-[:HAS_LOCALIZATION]->(:LocalizedText {text: lt.text})-[:HAS_LANGUAGE]->(l)
    )
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

    def get_all(self, application: str, language: str, parent_id: str | None = None) -> list[CategoryListItemModel]:
        with self.session as session:
            result = session.run(
                CategoryDatabase.GET_ALL_QUERY,
                application=application,
                parent_id=parent_id,
                language=language,
            )
            categories = []
            for record in result:
                data = record.data()
                if data.get("title") is not None:
                    categories.append(
                        CategoryListItemModel(
                            id=data["id"],
                            parent=data.get("parent"),
                            title=data["title"],
                            has_child=data.get("has_child", False),
                        )
                    )
            return categories

    def create(self, application: str, title: dict[str, str], parent_id: str | None = None) -> str:
        self._validate_not_exists(application, title, parent_id)

        category_id = generate_id()
        localized_texts = [{"language": lang, "text": text} for lang, text in title.items()]

        with self.session as session:
            result = session.run(
                CategoryDatabase.CREATE_QUERY,
                category_id=category_id,
                application=application,
                localized_texts=localized_texts,
                parent_id=parent_id,
            )
            record = result.single(strict=True)
            return record["category_id"]

    def _validate_not_exists(self, application: str, title: dict[str, str], parent_id: str | None) -> None:
        with self.session as session:
            for language, title_text in title.items():
                result = session.run(
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
