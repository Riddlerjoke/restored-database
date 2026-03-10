from typing import AsyncGenerator, Callable

from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from .config import Settings


async def init_mongo(app: FastAPI, settings: Settings) -> None:
    """Initialize MongoDB client and attach to app.state."""
    # AMÉLIORATION: paramètres optimisés pour replica set
    app.state.mongo_client = AsyncIOMotorClient(
        settings.mongo_uri,
        serverSelectionTimeoutMS=20000,  # 20s pour sélection du serveur (élections)
        connectTimeoutMS=10000,          # 10s pour connexion initiale
        socketTimeoutMS=30000,           # 30s pour opérations réseau
        retryWrites=True,                # Retry automatique des écritures
        retryReads=True,                 # Retry automatique des lectures
        maxPoolSize=50,                  # Pool de connexions (ajustez selon charge)
        minPoolSize=10,
    )
    app.state.db = app.state.mongo_client[settings.mongo_db]


async def close_mongo(app: FastAPI) -> None:
    """Close MongoDB client from app.state if present."""
    client: AsyncIOMotorClient = getattr(app.state, "mongo_client", None)
    if client:
        client.close()


def get_db_dependency(app: FastAPI) -> Callable[[], AsyncGenerator[AsyncIOMotorDatabase, None]]:
    """Return a FastAPI dependency function that yields the current DB."""
    async def _get_db() -> AsyncGenerator[AsyncIOMotorDatabase, None]:
        db: AsyncIOMotorDatabase = app.state.db
        yield db

    return _get_db