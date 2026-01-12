from pydantic import BaseModel
from typing import List


class RestoreCSVResponse(BaseModel):
    collection: str
    inserted: int
    fields: List[str]
