from models import (
    AIContributionModel,
    AnnotationModel,
    AnnotationType,
    ContributionModel,
    CopyrightStatus,
    ExpressionModelOutput,
    LicenseType,
    ManifestationModelOutput,
    ManifestationType,
    PersonModelOutput,
    TextType,
)


class DataAdapter:
    """Adapters for converting database data formats to Pydantic models."""

    @staticmethod
    def localized_text(entries: list[dict[str, str]] | None) -> dict[str, str] | None:
        """Convert list of {language, text} dicts to a localized text dict."""
        if entries is None:
            return None
        result = {entry["language"]: entry["text"] for entry in entries if "language" in entry and "text" in entry}
        return result or None

    @staticmethod
    def contributions(items: list[dict] | None) -> list[ContributionModel | AIContributionModel]:
        out: list[ContributionModel | AIContributionModel] = []
        for c in items or []:
            if c.get("ai_id"):
                out.append(AIContributionModel(ai_id=c["ai_id"], role=c["role"]))
            else:
                out.append(
                    ContributionModel(
                        person_id=c.get("person_id"),
                        person_bdrc_id=c.get("person_bdrc_id"),
                        role=c["role"],
                    )
                )
        return out

    @staticmethod
    def manifestation(data: dict) -> ManifestationModelOutput:
        annotations = [
            AnnotationModel(
                id=annotation.get("id"),
                type=AnnotationType(annotation.get("type")),
                aligned_to=annotation.get("aligned_to"),
            )
            for annotation in data.get("annotations", [])
        ]

        incipit_title = DataAdapter.localized_text(data.get("incipit_title"))
        alt_incipit_titles = (
            [DataAdapter.localized_text(alt) for alt in data.get("alt_incipit_titles", [])]
            if data.get("alt_incipit_titles")
            else None
        )

        return ManifestationModelOutput(
            id=data["id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            type=ManifestationType(data["type"]),
            annotations=annotations,
            source=data.get("source"),
            colophon=data.get("colophon"),
            incipit_title=incipit_title,
            alt_incipit_titles=alt_incipit_titles,
            alignment_sources=data.get("alignment_sources"),
            alignment_targets=data.get("alignment_targets"),
        )

    @staticmethod
    def expression(data: dict) -> ExpressionModelOutput:
        """Helper method to process expression data from query results"""
        expression_type = TextType(data.get("type"))
        target = data.get("target")

        # Convert None to "N/A" for standalone translations/commentaries
        if expression_type in [TextType.TRANSLATION, TextType.COMMENTARY] and target is None:
            target = "N/A"

        return ExpressionModelOutput(
            id=data.get("id"),
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            type=expression_type,
            contributions=DataAdapter.contributions(data.get("contributors")),
            date=data.get("date"),
            title=DataAdapter.localized_text(data.get("title")),
            alt_titles=[DataAdapter.localized_text(alt) for alt in data.get("alt_titles", [])],
            language=data.get("language"),
            target=target,
            category_id=data.get("category_id"),
            copyright=CopyrightStatus(data.get("copyright") or CopyrightStatus.PUBLIC_DOMAIN.value),
            license=LicenseType(data.get("license") or LicenseType.PUBLIC_DOMAIN_MARK.value),
        )

    @staticmethod
    def person(data: dict) -> PersonModelOutput:
        return PersonModelOutput(
            id=data["id"],
            bdrc=data.get("bdrc"),
            wiki=data.get("wiki"),
            name=DataAdapter.localized_text(data["name"]),
            alt_names=(
                [DataAdapter.localized_text(alt) for alt in data["alt_names"]] if data.get("alt_names") else None
            ),
        )
