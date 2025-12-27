from models import (
    AIContributionModel,
    ContributionOutput,
    CopyrightStatus,
    ExpressionOutput,
    LicenseType,
    LocalizedString,
    ManifestationOutput,
    ManifestationType,
    PersonOutput,
)


class DataAdapter:
    """Adapters for converting database data formats to Pydantic models."""

    @staticmethod
    def localized_text(entries: list[dict[str, str]] | None) -> dict[str, str] | None:
        """Convert list of {language, text} dicts to a localized text dict."""
        return {e["language"]: e["text"] for e in (entries or []) if "language" in e and "text" in e} or None

    @staticmethod
    def contributions(items: list[dict] | None) -> list[ContributionOutput | AIContributionModel]:
        out: list[ContributionOutput | AIContributionModel] = []
        for c in items or []:
            if c.get("ai_id"):
                out.append(AIContributionModel(ai_id=c["ai_id"], role=c["role"]))
            else:
                person_name = DataAdapter.localized_text(c.get("person_name"))
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
    def manifestation(data: dict) -> ManifestationOutput:
        incipit_title = DataAdapter.localized_text(data.get("incipit_title"))
        alt_incipit_titles = [
            LocalizedString(t) for a in data.get("alt_incipit_titles", []) if (t := DataAdapter.localized_text(a))
        ] or None

        return ManifestationOutput(
            id=data["id"],
            text_id=data["expression_id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            type=ManifestationType(data["type"]),
            source=data.get("source"),
            colophon=data.get("colophon"),
            incipit_title=LocalizedString(incipit_title) if incipit_title else None,
            alt_incipit_titles=alt_incipit_titles,
        )

    @staticmethod
    def expression(data: dict) -> ExpressionOutput:
        """Helper method to process expression data from query results"""
        return ExpressionOutput(
            id=data["id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            commentary_of=data.get("commentary_of"),
            translation_of=data.get("translation_of"),
            commentaries=data.get("commentaries") or [],
            translations=data.get("translations") or [],
            contributions=DataAdapter.contributions(data.get("contributors")),
            date=data.get("date"),
            title=LocalizedString(DataAdapter.localized_text(data["title"]) or {}),
            alt_titles=[
                LocalizedString(t) for alt in data.get("alt_titles", []) if (t := DataAdapter.localized_text(alt))
            ]
            or None,
            language=data["language"],
            category_id=data.get("category_id"),
            copyright=CopyrightStatus(data.get("copyright") or CopyrightStatus.PUBLIC_DOMAIN.value),
            license=LicenseType(data.get("license") or LicenseType.PUBLIC_DOMAIN_MARK.value),
            instances=data.get("instances") or [],
        )

    @staticmethod
    def person(data: dict) -> PersonOutput:
        return PersonOutput(
            id=data["id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            name=LocalizedString(DataAdapter.localized_text(data["name"]) or {}),
            alt_names=[
                LocalizedString(t) for alt in data.get("alt_names", []) if (t := DataAdapter.localized_text(alt))
            ]
            or None,
        )
