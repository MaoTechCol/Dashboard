from __future__ import annotations

from contextlib import contextmanager
import json
from typing import Iterator

from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.settings import get_settings
from app.services.company_registry import normalize_plate_label


class Base(DeclarativeBase):
    pass


settings = get_settings()

def _engine_configuration() -> tuple[dict[str, object], dict[str, object]]:
    connect_args: dict[str, object] = {}
    engine_options: dict[str, object] = {}
    if settings.database_url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
        return connect_args, engine_options

    if settings.database_url.startswith("postgresql"):
        statement_timeout = max(int(settings.database_statement_timeout_ms), 1_000)
        lock_timeout = max(int(settings.database_lock_timeout_ms), 500)
        idle_timeout = max(statement_timeout * 2, 30_000)
        connect_args.update(
            {
                "connect_timeout": max(int(settings.database_connect_timeout_seconds), 1),
                "application_name": f"dms-dashboard-{settings.process_role}",
                "options": (
                    f"-c statement_timeout={statement_timeout} "
                    f"-c lock_timeout={lock_timeout} "
                    f"-c idle_in_transaction_session_timeout={idle_timeout}"
                ),
            }
        )
        engine_options.update(
            {
                "pool_timeout": max(int(settings.database_pool_timeout_seconds), 1),
                "pool_recycle": 900,
            }
        )
    return connect_args, engine_options


connect_args, engine_options = _engine_configuration()

engine = create_engine(
    settings.database_url,
    future=True,
    connect_args=connect_args,
    pool_pre_ping=True,
    **engine_options,
)
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


def is_database_timeout(exc: BaseException) -> bool:
    """Recognize PostgreSQL cancellations without depending on a driver class."""
    current: BaseException | None = exc
    messages: list[str] = []
    seen: set[int] = set()
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        messages.append(str(current).lower())
        current = current.__cause__ or current.__context__
    message = " ".join(messages)
    return any(
        marker in message
        for marker in (
            "statement timeout",
            "canceling statement due to statement timeout",
            "query canceled",
            "query cancelled",
            "lock timeout",
            "queuepool limit",
            "connection timed out",
        )
    )


def _run_compat_migrations() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "ingest_state" in existing_tables:
        _ensure_column("ingest_state", "last_cycle_received_at", "DATETIME")
        _ensure_column("ingest_state", "last_event_observed_at", "DATETIME")
        _ensure_column("ingest_state", "last_live_alarm_message_at", "DATETIME")
        _ensure_column("ingest_state", "last_live_dms_at", "DATETIME")
        _ensure_column("ingest_state", "last_live_unmapped_at", "DATETIME")
        _ensure_column("ingest_state", "last_anomaly_at", "DATETIME")
        _ensure_column("ingest_state", "maintenance_mode", "BOOLEAN DEFAULT FALSE")
        _ensure_column("ingest_state", "maintenance_reason", "TEXT")
        _ensure_column("ingest_state", "maintenance_started_at", "DATETIME")
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
        _ensure_index(
            "ix_daily_snapshots_company_date",
            "daily_mileage_snapshots",
            ["company_slug", "snapshot_date"],
        )
    if "alarm_events" in existing_tables:
        _ensure_column("alarm_events", "provider_event_key", "VARCHAR(255)")
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
        _ensure_index(
            "ix_alarm_events_provider_event_key",
            "alarm_events",
            ["provider_event_key"],
            unique=True,
        )
        _ensure_index(
            "ix_alarm_events_company_device_occurred",
            "alarm_events",
            ["company_slug", "device_id", "occurred_at"],
        )
        _ensure_index(
            "ix_alarm_events_company_occurred",
            "alarm_events",
            ["company_slug", "occurred_at"],
        )
    if "alarm_event_audit" in existing_tables:
        _ensure_column("alarm_event_audit", "provider_event_key", "VARCHAR(255)")
        _ensure_index(
            "ix_alarm_event_audit_provider_event_key",
            "alarm_event_audit",
            ["provider_event_key"],
        )
    if "howen_alarm_raw" in existing_tables:
        _ensure_column("howen_alarm_raw", "provider_event_key", "VARCHAR(255)")
        _ensure_column("howen_alarm_raw", "raw_event_time", "VARCHAR(128)")
        _ensure_index(
            "ix_howen_alarm_raw_provider_event_key",
            "howen_alarm_raw",
            ["provider_event_key"],
            unique=True,
        )
        _ensure_index(
            "ix_howen_alarm_raw_company_device_occurred",
            "howen_alarm_raw",
            ["company_slug", "device_id", "occurred_at"],
        )
        _ensure_index(
            "ix_howen_alarm_raw_company_occurred",
            "howen_alarm_raw",
            ["company_slug", "occurred_at"],
        )
        _ensure_index(
            "ix_howen_alarm_raw_company_received",
            "howen_alarm_raw",
            ["company_slug", "received_at"],
        )
    if "alarm_event_audit" in existing_tables:
        _ensure_index(
            "ix_alarm_event_audit_company_device_received",
            "alarm_event_audit",
            ["company_slug", "device_id", "received_at"],
        )
        _ensure_index(
            "ix_alarm_event_audit_company_received",
            "alarm_event_audit",
            ["company_slug", "received_at"],
        )
    if "ingestion_anomalies" in existing_tables:
        _ensure_index(
            "ix_ingestion_anomalies_company_received",
            "ingestion_anomalies",
            ["company_slug", "received_at"],
        )
    if "reconciliation_reviews" in existing_tables:
        _ensure_index(
            "ix_reconciliation_reviews_company_observed",
            "reconciliation_reviews",
            ["company_slug", "observed_at"],
        )
        _ensure_index(
            "ix_reconciliation_reviews_company_created",
            "reconciliation_reviews",
            ["company_slug", "created_at"],
        )
    if "catchup_cursor" in existing_tables:
        _ensure_column("catchup_cursor", "last_successful_catchup_cursor_at", "DATETIME")
        _ensure_column("catchup_cursor", "pending_range_start_at", "DATETIME")
        _ensure_column("catchup_cursor", "pending_range_end_at", "DATETIME")
        _ensure_column("catchup_cursor", "next_device_offset", "INTEGER")
        _ensure_column("catchup_cursor", "next_retry_at", "DATETIME")
        _ensure_column("catchup_cursor", "rate_limit_streak", "INTEGER")
    if "company_historical_rebuild_jobs" in existing_tables:
        _ensure_column("company_historical_rebuild_jobs", "next_retry_at", "DATETIME")
        _ensure_column("company_historical_rebuild_jobs", "phase", "VARCHAR(32)")
        _ensure_column("company_historical_rebuild_jobs", "rows_total", "INTEGER DEFAULT 0")
        _ensure_column("company_historical_rebuild_jobs", "rows_processed", "INTEGER DEFAULT 0")
        _ensure_column("company_historical_rebuild_jobs", "current_device_id", "VARCHAR(128)")
        _ensure_column("company_historical_rebuild_jobs", "last_heartbeat_at", "DATETIME")
    _backfill_legacy_sources(existing_tables)
    _normalize_legacy_plate_labels(existing_tables)
    _backfill_raw_alarm_store()
    _prune_alarm_event_store(existing_tables)
    _repair_postgres_sequences(existing_tables)


def _ensure_column(table_name: str, column_name: str, column_sql: str) -> None:
    column_sql = _normalize_column_sql(column_sql)
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


def _normalize_column_sql(column_sql: str) -> str:
    if column_sql != "DATETIME":
        return column_sql
    if engine.dialect.name.startswith("postgres"):
        return "TIMESTAMP WITH TIME ZONE"
    return "DATETIME"


def _ensure_index(index_name: str, table_name: str, columns: list[str], *, unique: bool = False) -> None:
    inspector = inspect(engine)
    indexes = {index["name"] for index in inspector.get_indexes(table_name)}
    if index_name in indexes:
        return
    unique_sql = "UNIQUE " if unique else ""
    column_sql = ", ".join(columns)
    with engine.begin() as connection:
        try:
            connection.execute(text(f"CREATE {unique_sql}INDEX IF NOT EXISTS {index_name} ON {table_name} ({column_sql})"))
        except OperationalError:
            if unique:
                # Older deployments may contain duplicated legacy rows. In that case
                # we keep the column and let the new pipeline populate unique values
                # on subsequent rewrites instead of failing startup.
                return
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
    if "catchup_cursor" in existing_tables:
        statements.append(
            """
            UPDATE catchup_cursor
            SET last_successful_catchup_cursor_at = last_successful_catchup_observed_at
            WHERE last_successful_catchup_cursor_at IS NULL AND last_successful_catchup_observed_at IS NOT NULL
            """
        )
        statements.append(
            """
            UPDATE catchup_cursor
            SET next_device_offset = 0
            WHERE next_device_offset IS NULL
            """
        )
        statements.append(
            """
            UPDATE catchup_cursor
            SET rate_limit_streak = 0
            WHERE rate_limit_streak IS NULL
            """
        )
    if "ingest_state" in existing_tables:
        statements.append(
            """
            UPDATE ingest_state
            SET maintenance_mode = FALSE
            WHERE maintenance_mode IS NULL
            """
        )
    if not statements:
        _backfill_company_slugs(existing_tables)
        return
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))
        if "howen_alarm_raw" in existing_tables and "alarm_events" in existing_tables:
            connection.execute(
                text(
                    """
                    UPDATE howen_alarm_raw
                    SET raw_event_time = (
                        SELECT alarms.raw_event_time
                        FROM alarm_events AS alarms
                        WHERE alarms.guid = howen_alarm_raw.guid
                          AND alarms.raw_event_time IS NOT NULL
                          AND alarms.raw_event_time <> ''
                        LIMIT 1
                    )
                    WHERE (raw_event_time IS NULL OR raw_event_time = '')
                      AND EXISTS (
                        SELECT 1
                        FROM alarm_events AS alarms
                        WHERE alarms.guid = howen_alarm_raw.guid
                          AND alarms.raw_event_time IS NOT NULL
                          AND alarms.raw_event_time <> ''
                      )
                    """
                )
            )
    _backfill_company_slugs(existing_tables)


def _prune_alarm_event_store(existing_tables: set[str]) -> None:
    if "alarm_events" not in existing_tables:
        return
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                DELETE FROM alarm_events
                WHERE classification_status IS NULL
                   OR classification_status <> 'classified_dms'
                """
            )
        )


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


def _backfill_raw_alarm_store() -> None:
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    if "alarm_events" not in existing_tables or "howen_alarm_raw" not in existing_tables:
        return
    with engine.begin() as connection:
        connection.execute(
            text(
                """
                INSERT INTO howen_alarm_raw (
                    guid,
                    company_slug,
                    device_id,
                    fleet_id,
                    plate_no,
                    source,
                    occurred_at,
                    received_at,
                    raw_alarm_type,
                    raw_tp,
                    raw_event_code,
                    raw_event_time,
                    classification_status,
                    mapped_category,
                    mapping_source,
                    temporal_status,
                    ingest_result,
                    payload_json,
                    updated_at
                )
                SELECT
                    guid,
                    company_slug,
                    device_id,
                    fleet_id,
                    plate_no,
                    COALESCE(source, 'live'),
                    occurred_at,
                    COALESCE(received_at, occurred_at),
                    raw_alarm_type,
                    raw_tp,
                    raw_event_code,
                    raw_event_time,
                    classification_status,
                    category,
                    mapping_source,
                    'accepted',
                    CASE
                        WHEN classification_status = 'classified_dms' THEN 'inserted_alarm_event'
                        WHEN classification_status = 'classified_non_dms' THEN 'kept_raw_only'
                        WHEN classification_status = 'unmapped' THEN 'kept_raw_only'
                        ELSE 'legacy_unknown'
                    END,
                    COALESCE(raw_payload, '{}'),
                    COALESCE(received_at, occurred_at)
                FROM alarm_events
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM howen_alarm_raw raw
                    WHERE raw.guid = alarm_events.guid
                )
                """
            )
        )


def _normalize_legacy_plate_labels(existing_tables: set[str]) -> None:
    inspector = inspect(engine)
    candidate_tables = [
        table_name
        for table_name in existing_tables
        if "plate_no" in {column["name"] for column in inspector.get_columns(table_name)}
    ]
    if not candidate_tables:
        return
    with engine.begin() as connection:
        for table_name in candidate_tables:
            raw_plates = connection.execute(
                text(f"SELECT DISTINCT plate_no FROM {table_name} WHERE plate_no IS NOT NULL AND plate_no <> ''")
            ).scalars().all()
            for raw_plate in raw_plates:
                normalized_plate = normalize_plate_label(raw_plate)
                if not normalized_plate or normalized_plate == raw_plate:
                    continue
                connection.execute(
                    text(f"UPDATE {table_name} SET plate_no = :normalized WHERE plate_no = :raw"),
                    {"normalized": normalized_plate, "raw": raw_plate},
                )


def _repair_postgres_sequences(existing_tables: set[str]) -> None:
    if not engine.dialect.name.startswith("postgres"):
        return

    sequence_tables = (
        ("daily_mileage_snapshots", "id"),
        ("alarm_event_audit", "id"),
        ("ingestion_anomalies", "id"),
        ("report_assets", "id"),
        ("user_accounts", "id"),
    )
    with engine.begin() as connection:
        for table_name, column_name in sequence_tables:
            if table_name not in existing_tables:
                continue
            sequence_name = connection.execute(
                text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
                {
                    "table_name": table_name,
                    "column_name": column_name,
                },
            ).scalar()
            if not sequence_name:
                continue
            max_id = connection.execute(
                text(f"SELECT COALESCE(MAX({column_name}), 0) FROM {table_name}")
            ).scalar_one()
            if max_id and int(max_id) > 0:
                connection.execute(
                    text("SELECT setval(:sequence_name, :max_id, true)"),
                    {
                        "sequence_name": sequence_name,
                        "max_id": int(max_id),
                    },
                )
            else:
                connection.execute(
                    text("SELECT setval(:sequence_name, 1, false)"),
                    {
                        "sequence_name": sequence_name,
                    },
                )
