import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = "RestoreBddproject"
    mongo_uri: str = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    mongo_db: str = os.getenv("MONGO_DB", "restore_db")
    server_host: str = os.getenv("SERVER_HOST", "0.0.0.0")
    server_port: int = int(os.getenv("SERVER_PORT", "8000"))


def get_settings() -> Settings:
    # Simple factory; could be expanded to cache if needed
    return Settings()
