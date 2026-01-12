import csv
import io
from typing import List

from fastapi import FastAPI

from core.config import get_settings
from core.db import init_mongo, close_mongo
from endpoints.root import router as root_router
from endpoints.db_admin import router as db_admin_router
from endpoints.restore import router as restore_router
from endpoints.analyse import router as analyse_router



def get_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(title=settings.app_name, version="0.1.0")

    @app.on_event("startup")
    async def startup_event() -> None:
        await init_mongo(app, settings)

    @app.on_event("shutdown")
    async def shutdown_event() -> None:
        await close_mongo(app)

    # Include routers only; main.py is now responsible for wiring
    app.include_router(root_router)
    app.include_router(db_admin_router)
    app.include_router(restore_router)

    app.include_router(analyse_router)

    return app


app = get_app()
