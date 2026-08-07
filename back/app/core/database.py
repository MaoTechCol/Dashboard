from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.settings import get_settings


class Base(DeclarativeBase):
    pass


settings = get_settings()

connect_args = {}
if settings.database_url.startswith("sqlite"):
    connect_args["check_same_thread"] = False

engine = create_engine(settings.database_url, future=True, connect_args=connect_args)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


def init_db() -> None:
    from app import models  # noqa: F401

    Base.metadata.create_all(bind=engine)
    _run_compat_migrations()


def get_session() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def _run_compat_migrations() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "ingest_state" in existing_tables:
        _ensure_column("ingest_state", "last_cycle_received_at", "DATETIME")
        _ensure_column("ingest_state", "last_event_observed_at", "DATETIME")
        _ensure_column("ingest_state", "last_anomaly_at", "DATETIME")
    if "devices" in existing_tables:
        _ensure_column("devices", "last_received_at", "DATETIME")
        _ensure_column("devices", "record_source", "VARCHAR(32)")
        _ensure_column("devices", "fleet_name", "VARCHAR(128)")
    if "daily_mileage_snapshots" in existing_tables:
        _ensure_column("daily_mileage_snapshots", "source", "VARCHAR(32)")
    if "alarm_events" in existing_tables:
        _ensure_column("alarm_events", "mapping_source", "VARCHAR(32)")
        _ensure_column("alarm_events", "source", "VARCHAR(32)")
    _backfill_legacy_sources(existing_tables)


def _ensure_column(table_name: str, column_name: str, column_sql: str) -> None:
    inspector = inspect(engine)
    columns = {column["name"] for column in inspector.get_columns(table_name)}
    if column_name in columns:
        return
    with engine.begin() as connection:
        try:
            connection.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"))
        except OperationalError as exc:
            if "duplicate column name" not in str(exc).lower():
                raise


def _backfill_legacy_sources(existing_tables: set[str]) -> None:
    statements: list[str] = []
    if "devices" in existing_tables:
        statements.append("UPDATE devices SET record_source = 'live' WHERE record_source IS NULL OR record_source = ''")
    if "alarm_events" in existing_tables:
        statements.append("UPDATE alarm_events SET source = 'live' WHERE source IS NULL OR source = ''")
        statements.append("UPDATE alarm_events SET mapping_source = 'unknown' WHERE mapping_source IS NULL OR mapping_source = ''")
    if "daily_mileage_snapshots" in existing_tables:
        statements.append("UPDATE daily_mileage_snapshots SET source = 'live' WHERE source IS NULL OR source = ''")
    if "mileage_readings" in existing_tables:
        statements.append("UPDATE mileage_readings SET source = 'status' WHERE source IS NULL OR source = ''")
    if not statements:
        return
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
