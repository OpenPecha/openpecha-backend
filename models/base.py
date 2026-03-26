from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, RootModel, StrictStr, StringConstraints

type NonEmptyStr = Annotated[StrictStr, StringConstraints(min_length=1, strip_whitespace=True)]


def _dedupe[T](items: list[T], exclude: T) -> list[T]:
    """Remove duplicates and excluded item from list."""
    seen: list[T] = []
    for item in items:
        if item != exclude and item not in seen:
            seen.append(item)
    return seen


class OpenPechaModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
    )


class LocalizedString(RootModel[dict[str, NonEmptyStr]]):
    root: dict[str, NonEmptyStr] = Field(min_length=1)

    def __getitem__(self, item: str) -> str:
        return self.root[item]
