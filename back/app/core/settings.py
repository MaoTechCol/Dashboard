from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import Field
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
    public_dashboard_url: str | None = None
    public_api_url: str | None = None

    late_threshold_minutes: int = 20
    stopped_threshold_minutes: int = 45
    admin_token: str | None = None
    jwt_secret: str = "change-me-local-dms"
    session_cookie_name: str = "dms_session"
    session_cookie_secure: bool = False
    session_ttl_minutes: int = 720
    seed_admin_username: str = "admin"
    seed_admin_password: str = "Admin123!"
    seed_client_password: str = "Cliente123!"
    anomaly_future_tolerance_minutes: int = 5
    live_retention_days: int = 40
    anomaly_retention_days: int = 90
    howen_login_min_interval_seconds: int = 20
    howen_login_rate_limit_cooldown_seconds: int = 75
    howen_request_spacing_seconds: float = 2.5
    howen_request_spacing_max_seconds: float = 8.0
    howen_request_recovery_successes: int = 20
    howen_alarm_source: str = "evidence_bulk"
    howen_evidence_page_size: int = 48
    howen_evidence_max_devices_per_request: int = 50
    howen_evidence_fallback_to_device_api: bool = True
    harvest_cut_interval_minutes: int = 15
    harvest_overlap_minutes: int = 30
    harvest_check_interval_seconds: int = 20
    harvest_window_lag_seconds: int = 45
    harvest_max_cuts_per_cycle: int = 4
    catchup_overlap_minutes: int = 10
    catchup_bootstrap_hours: int = 6
    catchup_stale_after_minutes: int = 90
    catchup_max_window_minutes: int = 20
    catchup_device_batch_size: int = 1
    catchup_check_interval_minutes: int = 5
    catchup_run_time_budget_seconds: int = 300
    catchup_batch_pause_seconds: float = 0.0
    catchup_rate_limit_base_seconds: int = 300
    catchup_rate_limit_max_seconds: int = 3600
    catchup_error_retry_seconds: int = 300
    backfill_rate_limit_max_retries: int = 4
    backfill_rate_limit_cooldown_seconds: int = 20
    backfill_rate_limit_max_cooldown_seconds: int = 180
    historical_rebuild_chunk_days: int = 30
    historical_rebuild_max_concurrency: int = 1
    historical_batch_mode: str = "activation_only"
    historical_batch_size: int = 500
    reconciliation_cache_ttl_minutes: int = 120
    process_role: str = "api"
    worker_poll_interval_seconds: float = 1.0
    worker_scheduler_interval_seconds: int = 15
    worker_harvest_concurrency: int = 4
    worker_lease_seconds: int = 90
    worker_heartbeat_seconds: int = 20
    worker_retry_base_seconds: int = 30
    worker_retry_max_seconds: int = 900
    worker_max_attempts: int = 5
    database_connect_timeout_seconds: int = 5
    database_lock_timeout_ms: int = 5_000
    api_database_statement_timeout_ms: int = 12_000
    worker_database_statement_timeout_ms: int = 300_000
    api_database_pool_timeout_seconds: int = 5
    worker_database_pool_timeout_seconds: int = 30
    memory_monitor_interval_seconds: int = 15
    api_memory_warning_mb: int = 450
    api_memory_critical_mb: int = 750
    worker_memory_warning_mb: int = 1_100
    worker_memory_critical_mb: int = 2_000

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        env_parse_none_str="",
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

    @field_validator("historical_batch_mode", mode="before")
    @classmethod
    def _validate_historical_batch_mode(cls, value: object) -> str:
        mode = str(value or "activation_only").strip().lower()
        if mode not in {"off", "activation_only", "all_historical"}:
            raise ValueError("HISTORICAL_BATCH_MODE must be off, activation_only, or all_historical")
        return mode

    @field_validator("howen_alarm_source", mode="before")
    @classmethod
    def _validate_howen_alarm_source(cls, value: object) -> str:
        source = str(value or "evidence_bulk").strip().lower()
        if source not in {"evidence_bulk", "official_device"}:
            raise ValueError("HOWEN_ALARM_SOURCE must be evidence_bulk or official_device")
        return source

    @field_validator("process_role", mode="before")
    @classmethod
    def _validate_process_role(cls, value: object) -> str:
        role = str(value or "api").strip().lower()
        if role not in {"api", "worker", "all"}:
            raise ValueError("PROCESS_ROLE must be api, worker, or all")
        return role

    @property
    def root_dir(self) -> Path:
        return Path(__file__).resolve().parents[2]

    @property
    def company_config_path(self) -> Path:
        return self.root_dir / "storage" / "companies.json"

    @property
    def company_seed_config_path(self) -> Path:
        return self.root_dir / "app" / "data" / "companies.json"

    @property
    def upload_dir(self) -> Path:
        return self.root_dir / "storage" / "uploads"

    @property
    def session_cache_path(self) -> Path:
        return self.root_dir / "storage" / "howen_session.json"

    @property
    def database_statement_timeout_ms(self) -> int:
        if self.process_role == "worker":
            return self.worker_database_statement_timeout_ms
        return self.api_database_statement_timeout_ms

    @property
    def database_pool_timeout_seconds(self) -> int:
        if self.process_role == "worker":
            return self.worker_database_pool_timeout_seconds
        return self.api_database_pool_timeout_seconds

    @property
    def memory_warning_mb(self) -> int:
        if self.process_role == "worker":
            return self.worker_memory_warning_mb
        return self.api_memory_warning_mb

    @property
    def memory_critical_mb(self) -> int:
        if self.process_role == "worker":
            return self.worker_memory_critical_mb
        return self.api_memory_critical_mb


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
