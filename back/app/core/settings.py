from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "DMS Dashboard Local"
    api_prefix: str = "/api"
    database_url: str = "sqlite:///./storage/dashboard.db"
    default_timezone: str = "America/Bogota"
    frontend_origins: Annotated[list[str], NoDecode] = [
        "http://localhost:5173",
        "http://127.0.0.1:5173",
    ]
    frontend_origin_regex: str = r"^https?://(localhost|127\.0\.0\.1)(:\d+)?$"
    ingest_mode: str = "live"

    howen_http_base: str = "http://172.86.110.17:9966/vss"
    howen_ws_url: str = "ws://172.86.110.17:36300/ws"
    howen_wss_url: str = "wss://172.86.110.17:36301/ws"
    howen_username: str | None = None
    howen_password: str | None = None
    howen_password_md5: str | None = None
    howen_token: str | None = None
    howen_pid: str | None = None

    late_threshold_minutes: int = 20
    stopped_threshold_minutes: int = 45
    admin_token: str | None = None
    jwt_secret: str = "change-me-local-dms"
    session_cookie_name: str = "dms_session"
    session_ttl_minutes: int = 720
    seed_admin_username: str = "admin"
    seed_admin_password: str = "Admin123!"
    seed_client_password: str = "Cliente123!"
    anomaly_future_tolerance_minutes: int = 5
    live_retention_days: int = 40
    anomaly_retention_days: int = 90

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @field_validator("frontend_origins", mode="before")
    @classmethod
    def _split_frontend_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value

    @field_validator("ingest_mode", mode="before")
    @classmethod
    def _force_live_mode(cls, value: object) -> str:
        mode = str(value or "").strip().lower()
        return "live" if mode != "live" else mode

    @property
    def root_dir(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def company_config_path(self) -> Path:
        return self.root_dir / "app" / "data" / "companies.json"

    @property
    def upload_dir(self) -> Path:
        return self.root_dir / "storage" / "uploads"

    @property
    def session_cache_path(self) -> Path:
        return self.root_dir / "storage" / "howen_session.json"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
