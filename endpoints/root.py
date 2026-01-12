from fastapi import APIRouter
from models.common import HealthResponse, HelloResponse

router = APIRouter(tags=["root"])


@router.get("/", response_model=HealthResponse)
async def root() -> HealthResponse:
    return HealthResponse(message="Service OK")


@router.get("/hello/{name}", response_model=HelloResponse)
async def say_hello(name: str) -> HelloResponse:
    return HelloResponse(message=f"Hello {name}")
