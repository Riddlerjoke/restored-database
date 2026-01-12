from fastapi import Request, Depends
from motor.motor_asyncio import AsyncIOMotorDatabase


def get_db(request: Request) -> AsyncIOMotorDatabase:
    """Get the AsyncIOMotorDatabase from app state using the current request.
    This allows routers to depend on the DB without capturing the app instance.
    """
    return request.app.state.db
