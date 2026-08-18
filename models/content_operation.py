from typing import Annotated, Literal, Self

from pydantic import Field, RootModel, model_validator

from .base import OpenPechaModel


class InsertOperation(OpenPechaModel):
    type: Literal["insert"]
    position: int = Field(..., ge=0, description="Position for INSERT operation")
    text: str = Field(..., min_length=1, description="Text to insert")


class RangedOperation(OpenPechaModel):
    start: int = Field(..., ge=0)
    end: int = Field(..., ge=1)

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        # Unlike an annotation span, an empty range here is a no-op rather than a position.
        if self.start >= self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class DeleteOperation(RangedOperation):
    type: Literal["delete"]


class ReplaceOperation(RangedOperation):
    type: Literal["replace"]
    text: str = Field(..., min_length=1, description="Replacement text")


class ContentOperation(
    RootModel[Annotated[InsertOperation | DeleteOperation | ReplaceOperation, Field(discriminator="type")]]
):
    """Content operation wrapper using discriminated union."""

    @property
    def operation(self) -> InsertOperation | DeleteOperation | ReplaceOperation:
        return self.root
