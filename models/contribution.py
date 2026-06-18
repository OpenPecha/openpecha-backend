from typing import Annotated, Literal, Self

from pydantic import Field, model_validator

from .base import LocalizedString, NonEmptyStr, OpenPechaModel
from .enums import ContributorRole


class AIContribution(OpenPechaModel):
    type: Literal["ai"]
    id: NonEmptyStr
    role: ContributorRole


class PersonContributionBase(OpenPechaModel):
    type: Literal["person"]
    id: NonEmptyStr | None = None
    bdrc_id: NonEmptyStr | None = None
    role: ContributorRole


class PersonContributionInput(PersonContributionBase):
    @model_validator(mode="after")
    def validate_person_reference(self) -> Self:
        if self.id is None and self.bdrc_id is None:
            raise ValueError("Either id or bdrc_id must be provided for person contributions")
        if self.id is not None and self.bdrc_id is not None:
            raise ValueError("Only one of id or bdrc_id can be provided for person contributions")
        return self


class PersonContributionOutput(PersonContributionBase):
    name: LocalizedString | None = None


type ContributionInputItem = Annotated[PersonContributionInput | AIContribution, Field(discriminator="type")]
type ContributionOutputItem = Annotated[PersonContributionOutput | AIContribution, Field(discriminator="type")]
