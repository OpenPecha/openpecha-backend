from typing import Annotated, Literal, Self

from pydantic import Field, RootModel, model_validator

from .base import OpenPechaModel


class SegmentContentInput(OpenPechaModel):
    content: str = Field(..., min_length=1)


class ContentOperationBase(OpenPechaModel):
    pass


class InsertOperation(ContentOperationBase):
    type: Literal["insert"]
    position: int = Field(..., ge=0, description="Position for INSERT operation")
    text: str = Field(..., min_length=1, description="Text to insert")


class DeleteOperation(ContentOperationBase):
    type: Literal["delete"]
    start: int = Field(..., ge=0, description="Start position for DELETE operation")
    end: int = Field(..., ge=1, description="End position for DELETE operation")

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.start >= self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class ReplaceOperation(ContentOperationBase):
    type: Literal["replace"]
    start: int = Field(..., ge=0, description="Start position for REPLACE operation")
    end: int = Field(..., ge=1, description="End position for REPLACE operation")
    text: str = Field(..., min_length=1, description="Replacement text")

    @model_validator(mode="after")
    def validate_range(self) -> Self:
        if self.start >= self.end:
            raise ValueError("'start' must be less than 'end'")
        return self


class ContentOperation(
    RootModel[Annotated[InsertOperation | DeleteOperation | ReplaceOperation, Field(discriminator="type")]]
):
    """Content operation wrapper using discriminated union."""

    @property
    def operation(self) -> InsertOperation | DeleteOperation | ReplaceOperation:
        return self.root
