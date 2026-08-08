from __future__ import annotations

from contextlib import contextmanager
import json
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
        _ensure_column("devices", "company_slug", "VARCHAR(64)")
        _ensure_column("devices", "last_received_at", "DATETIME")
        _ensure_column("devices", "record_source", "VARCHAR(32)")
        _ensure_column("devices", "fleet_name", "VARCHAR(128)")
        _ensure_column("devices", "raw_total_value", "VARCHAR(64)")
        _ensure_column("devices", "raw_day_value", "VARCHAR(64)")
        _ensure_column("devices", "km_validation_status", "VARCHAR(32)")
        _ensure_column("devices", "km_validation_reason", "VARCHAR(255)")
    if "daily_mileage_snapshots" in existing_tables:
        _ensure_column("daily_mileage_snapshots", "company_slug", "VARCHAR(64)")
        _ensure_column("daily_mileage_snapshots", "source", "VARCHAR(32)")
        _ensure_column("daily_mileage_snapshots", "raw_total_value", "VARCHAR(64)")
        _ensure_column("daily_mileage_snapshots", "raw_day_value", "VARCHAR(64)")
        _ensure_column("daily_mileage_snapshots", "km_validation_status", "VARCHAR(32)")
        _ensure_column("daily_mileage_snapshots", "km_validation_reason", "VARCHAR(255)")
        _ensure_column("daily_mileage_snapshots", "repair_reason", "VARCHAR(255)")
        _ensure_column("daily_mileage_snapshots", "repaired_at", "DATETIME")
    if "alarm_events" in existing_tables:
        _ensure_column("alarm_events", "company_slug", "VARCHAR(64)")
        _ensure_column("alarm_events", "mapping_source", "VARCHAR(32)")
        _ensure_column("alarm_events", "classification_status", "VARCHAR(32)")
        _ensure_column("alarm_events", "visibility_status", "VARCHAR(32)")
        _ensure_column("alarm_events", "source", "VARCHAR(32)")
        _ensure_column("alarm_events", "raw_alarm_type", "VARCHAR(128)")
        _ensure_column("alarm_events", "raw_tp", "VARCHAR(32)")
        _ensure_column("alarm_events", "raw_event_code", "VARCHAR(32)")
        _ensure_column("alarm_events", "received_at", "DATETIME")
        _ensure_column("alarm_events", "raw_event_time", "VARCHAR(128)")
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
        statements.append(
            """
            UPDATE alarm_events
            SET classification_status = CASE
                WHEN category IN ('Uso de celular', 'Fatiga en progresion', 'Ojos cerrados', 'Riesgo de colision', 'Bostezo', 'Camara cubierta', 'Fumando', 'Distraccion')
                    THEN 'classified_dms'
                WHEN category = 'Sin clasificar'
                    THEN 'unmapped'
                ELSE 'classified_non_dms'
            END
            WHERE classification_status IS NULL OR classification_status = ''
            """
        )
        statements.append(
            """
            UPDATE alarm_events
            SET visibility_status = CASE
                WHEN classification_status = 'classified_dms'
                    THEN 'candidate'
                WHEN classification_status = 'classified_non_dms'
                    THEN 'hidden_non_dms'
                ELSE 'hidden_unmapped'
            END
            WHERE visibility_status IS NULL OR visibility_status = ''
            """
        )
        statements.append("UPDATE alarm_events SET raw_tp = subtype WHERE raw_tp IS NULL AND subtype IS NOT NULL")
        statements.append("UPDATE alarm_events SET raw_event_code = event_code WHERE raw_event_code IS NULL AND event_code IS NOT NULL")
        statements.append("UPDATE alarm_events SET received_at = occurred_at WHERE received_at IS NULL AND occurred_at IS NOT NULL")
    if "daily_mileage_snapshots" in existing_tables:
        statements.append("UPDATE daily_mileage_snapshots SET source = 'live' WHERE source IS NULL OR source = ''")
    if "mileage_readings" in existing_tables:
        statements.append("UPDATE mileage_readings SET source = 'status' WHERE source IS NULL OR source = ''")
    if not statements:
        _backfill_company_slugs(existing_tables)
        return
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
    _backfill_company_slugs(existing_tables)


def _backfill_company_slugs(existing_tables: set[str]) -> None:
    try:
        payload = json.loads(settings.company_config_path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return
    except json.JSONDecodeError:
        return

    assignments: list[tuple[str, list[str], list[str]]] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        slug = str(item.get("slug") or "").strip()
        if not slug:
            continue
        fleet_ids = [str(value).strip() for value in item.get("fleet_ids") or [] if str(value).strip()]
        device_ids = [str(value).strip() for value in item.get("device_ids") or [] if str(value).strip()]
        if not fleet_ids and not device_ids:
            continue
        assignments.append((slug, fleet_ids, device_ids))

    if not assignments:
        return

    table_specs = []
    if "devices" in existing_tables:
        table_specs.append(("devices", True))
    if "alarm_events" in existing_tables:
        table_specs.append(("alarm_events", True))
    if "daily_mileage_snapshots" in existing_tables:
        table_specs.append(("daily_mileage_snapshots", True))
    if "ingestion_anomalies" in existing_tables:
        table_specs.append(("ingestion_anomalies", False))

    if not table_specs:
        return

    with engine.begin() as connection:
        for table_name, has_fleet_id in table_specs:
            for slug, fleet_ids, device_ids in assignments:
                clauses: list[str] = []
                params: dict[str, str] = {"slug": slug}
                if device_ids:
                    device_params = []
                    for index, device_id in enumerate(device_ids):
                        key = f"device_id_{index}"
                        params[key] = device_id
                        device_params.append(f":{key}")
                    clauses.append(f"device_id IN ({', '.join(device_params)})")
                if has_fleet_id and fleet_ids:
                    fleet_params = []
                    for index, fleet_id in enumerate(fleet_ids):
                        key = f"fleet_id_{index}"
                        params[key] = fleet_id
                        fleet_params.append(f":{key}")
                    clauses.append(f"fleet_id IN ({', '.join(fleet_params)})")
                if not clauses:
                    continue
                connection.execute(
                    text(
                        f"""
                        UPDATE {table_name}
                        SET company_slug = :slug
                        WHERE (company_slug IS NULL OR company_slug = '')
                          AND ({' OR '.join(clauses)})
                        """
                    ),
                    params,
                )
