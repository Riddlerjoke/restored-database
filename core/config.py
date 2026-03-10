import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    app_name: str = "RestoreBddproject"
    # CORRECTION: enlever le préfixe MONGO_URI= en double
    mongo_uri: str = os.getenv(
        "MONGO_URI",
        "mongodb://admin:admin123@mongo1:27017,mongo2:27017,mongo3:27017/restore_db?replicaSet=rs0&authSource=admin&retryWrites=true&w=majority"
    )
    mongo_db: str = os.getenv("MONGO_DB", "restore_db")
    server_host: str = os.getenv("SERVER_HOST", "0.0.0.0")
    server_port: int = int(os.getenv("SERVER_PORT", "8000"))


def get_settings() -> Settings:
    return Settings()