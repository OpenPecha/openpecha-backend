from pydantic import BaseModel


class IdResponse(BaseModel):
    id: str


class IdsResponse(BaseModel):
    ids: list[str]


class LanguageResponse(BaseModel):
    code: str
    name: str


class ApplicationResponse(BaseModel):
    id: str
    name: str
