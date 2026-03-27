from pydantic import Field

from .base import LocalizedString, NonEmptyStr, OpenPechaModel


class CategoryBase(OpenPechaModel):
    title: LocalizedString
    description: LocalizedString | None = None
    parent_id: NonEmptyStr | None = None


class CategoryInput(CategoryBase):
    pass


class CategoryOutput(CategoryBase):
    id: NonEmptyStr
    children: list[str] = Field(default_factory=list)


class CategoryDetailOutput(CategoryBase):
    id: NonEmptyStr
    children: list[CategoryOutput] = Field(default_factory=list)
