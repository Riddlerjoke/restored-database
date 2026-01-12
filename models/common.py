from pydantic import BaseModel
from typing import List, Optional


class HealthResponse(BaseModel):
    message: str


class HelloResponse(BaseModel):
    message: str


class CollectionsResponse(BaseModel):
    collections: List[str]


class CountResponse(BaseModel):
    collection: str
    count: int


class ErrorResponse(BaseModel):
    detail: str
