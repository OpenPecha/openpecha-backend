from models.base import LocalizedString
from models.category import CategoryOutput
from models.contribution import AIContribution, ContributionOutput
from models.edition import EditionOutput
from models.enums import EditionType, LicenseType
from models.person import PersonOutput
from models.tag import TagOutput
from models.text import TextOutput


class DataAdapter:
    """Adapters for converting database data formats to Pydantic models."""

    @staticmethod
    def contributions(items: list[dict] | None) -> list[ContributionOutput | AIContribution]:
        out: list[ContributionOutput | AIContribution] = []
        for c in items or []:
            if c.get("ai_id"):
                out.append(AIContribution(ai_id=c["ai_id"], role=c["role"]))
            else:
                person_name = c.get("person_name")
                out.append(
                    ContributionOutput(
                        person_id=c.get("person_id"),
                        person_bdrc_id=c.get("person_bdrc_id"),
                        role=c["role"],
                        person_name=LocalizedString(person_name) if person_name else None,
                    )
                )
        return out

    @staticmethod
    def edition(data: dict) -> EditionOutput:
        incipit_title = data.get("incipit_title")
        alt_incipit_titles = [LocalizedString(a) for a in data.get("alt_incipit_titles", []) if a] or None

        return EditionOutput(
            id=data["id"],
            text_id=data["text_id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            type=EditionType(data["type"]),
            source=data.get("source"),
            colophon=data.get("colophon"),
            incipit_title=LocalizedString(incipit_title) if incipit_title else None,
            alt_incipit_titles=alt_incipit_titles,
        )

    @staticmethod
    def text(data: dict) -> TextOutput:
        """Helper method to process text data from query results"""
        return TextOutput(
            id=data["id"],
            bdrc=data.get("bdrc") or None,
            wiki=data.get("wiki") or None,
            commentary_of=data.get("commentary_of") or None,
            translation_of=data.get("translation_of") or None,
            commentaries=data.get("commentaries") or [],
            translations=data.get("translations") or [],
            contributions=DataAdapter.contributions(data.get("contributors")),
            date=data.get("date") or None,
            title=LocalizedString(data.get("title") or {}),
            alt_titles=[LocalizedString(alt) for alt in data.get("alt_titles", []) if alt] or None,
            language=data["language"],
            category_id=data["category_id"],
            license=LicenseType(data.get("license") or LicenseType.PUBLIC_DOMAIN_MARK.value),
            editions=data.get("editions") or [],
            tag_ids=data.get("tag_ids") or [],
        )

    @staticmethod
    def person(data: dict) -> PersonOutput:
        return PersonOutput(
            id=data["id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            name=LocalizedString(data.get("name") or {}),
            alt_names=[LocalizedString(alt) for alt in data.get("alt_names", []) if alt] or None,
        )

    @staticmethod
    def category(data: dict) -> CategoryOutput:
        desc = data.get("description")
        return CategoryOutput(
            id=data["id"],
            title=LocalizedString(data.get("title") or {}),
            description=LocalizedString(desc) if desc else None,
            parent_id=data.get("parent_id"),
            children=data.get("children") or [],
        )

    @staticmethod
    def tag(data: dict) -> TagOutput:
        desc = data.get("description")
        return TagOutput(
            id=data["id"],
            title=LocalizedString(data.get("title") or {}),
            description=LocalizedString(desc) if desc else None,
        )
