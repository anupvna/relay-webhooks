"""Request bodies. Pydantic validates them before our code ever runs."""
from uuid import UUID

from pydantic import BaseModel, Field, HttpUrl


class EndpointCreate(BaseModel):
    url: HttpUrl


class EventCreate(BaseModel):
    endpoint_id: UUID
    type: str = Field(min_length=1, max_length=100)
    payload: dict
