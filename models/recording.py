from typing import Annotated

from pydantic import AfterValidator, Field

from .base import LocalizedString, NonEmptyStr, OpenPechaModel, PatchModel
from .contribution import ContributionInputItem, ContributionOutputItem
from .enums import AudioFormat, ContributorRole, LicenseType


def _validate_narrator_roles(contributions: list[ContributionInputItem]) -> list[ContributionInputItem]:
    """Recordings credit the person or AI reading the text, so no other role is accepted."""
    other_roles = sorted({c.role for c in contributions if c.role is not ContributorRole.NARRATOR})
    if other_roles:
        raise ValueError(
            f"Recording contributions must have role '{ContributorRole.NARRATOR}', got: {', '.join(other_roles)}"
        )
    return contributions


NarratorContributions = Annotated[
    list[ContributionInputItem], Field(min_length=1), AfterValidator(_validate_narrator_roles)
]


class RecordingBase(OpenPechaModel):
    title: LocalizedString | None = None
    language: NonEmptyStr | None = None
    license: LicenseType = LicenseType.PUBLIC_DOMAIN_MARK
    date: NonEmptyStr | None = None
    duration_ms: int | None = Field(default=None, ge=0)


class RecordingInput(RecordingBase):
    contributions: NarratorContributions


class RecordingPatch(PatchModel):
    title: LocalizedString | None = None
    language: NonEmptyStr | None = None
    license: LicenseType | None = None
    date: NonEmptyStr | None = None
    duration_ms: int | None = Field(default=None, ge=0)
    contributions: NarratorContributions | None = None


class RecordingOutput(RecordingBase):
    id: NonEmptyStr
    edition_id: NonEmptyStr
    text_id: NonEmptyStr
    contributions: list[ContributionOutputItem]
    format: AudioFormat
    size_bytes: int
