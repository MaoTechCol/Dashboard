from __future__ import annotations

import argparse
from collections.abc import Iterable
from pathlib import Path
import sys
from typing import Any

from sqlalchemy import create_engine, delete, func, select, text
from sqlalchemy.orm import Session, sessionmaker

BACK_ROOT = Path(__file__).resolve().parents[1] / "back"
if str(BACK_ROOT) not in sys.path:
    sys.path.insert(0, str(BACK_ROOT))

from app.core.database import Base
from app.models import (
    AlarmEvent,
    AlarmEventAudit,
    DailyMileageSnapshot,
    DeviceRecord,
    IngestState,
    IngestionAnomaly,
    MileageReading,
    ReportAsset,
    UserAccount,
)


TABLE_ORDER = [
    UserAccount,
    DeviceRecord,
    MileageReading,
    DailyMileageSnapshot,
    AlarmEvent,
    AlarmEventAudit,
    ReportAsset,
    IngestionAnomaly,
    IngestState,
]


def build_engine(url: str):
    connect_args: dict[str, Any] = {}
    if url.startswith("sqlite"):
        connect_args["check_same_thread"] = False
    return create_engine(url, future=True, connect_args=connect_args, pool_pre_ping=True)


def row_to_dict(instance: Any) -> dict[str, Any]:
    return {column.name: getattr(instance, column.name) for column in instance.__table__.columns}


def chunked(items: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    for index in range(0, len(items), size):
        yield items[index : index + size]


def sync_postgres_sequences(target_session: Session, models: list[type[Any]]) -> None:
    bind = target_session.get_bind()
    if bind is None or bind.dialect.name != "postgresql":
        return

    for model in models:
        table = model.__table__
        id_column = table.c.get("id")
        if id_column is None:
            continue

        schema_prefix = f"{table.schema}." if table.schema else "public."
        seq_name = target_session.execute(
            text("SELECT pg_get_serial_sequence(:table_name, :column_name)"),
            {"table_name": f"{schema_prefix}{table.name}", "column_name": id_column.name},
        ).scalar_one_or_none()
        if not seq_name:
            continue

        max_id = target_session.execute(select(func.max(id_column))).scalar() or 0
        target_value = max(max_id, 1)
        target_session.execute(
            text("SELECT setval(:seq_name, :target_value, true)"),
            {"seq_name": seq_name, "target_value": target_value},
        )
        print(f"[seq] {table.name}.{id_column.name}: {target_value}")

    target_session.commit()


def migrate(source_url: str, target_url: str, *, truncate: bool) -> None:
    source_engine = build_engine(source_url)
    target_engine = build_engine(target_url)
    SourceSession = sessionmaker(bind=source_engine, autoflush=False, autocommit=False, future=True)
    TargetSession = sessionmaker(bind=target_engine, autoflush=False, autocommit=False, future=True)

    Base.metadata.create_all(bind=target_engine)

    with SourceSession() as source_session, TargetSession() as target_session:
        if truncate:
            for model in reversed(TABLE_ORDER):
                target_session.execute(delete(model))
            target_session.commit()

        for model in TABLE_ORDER:
            records = [row_to_dict(row) for row in source_session.scalars(select(model))]
            if not records:
                print(f"[skip] {model.__tablename__}: 0")
                continue
            for batch in chunked(records, 500):
                target_session.execute(model.__table__.insert(), batch)
            target_session.commit()
            print(f"[ok] {model.__tablename__}: {len(records)}")

        sync_postgres_sequences(target_session, TABLE_ORDER)


def main() -> int:
    parser = argparse.ArgumentParser(description="Migra la base local SQLite a PostgreSQL/Supabase")
    parser.add_argument("--source-url", required=True, help="URL SQLAlchemy de origen, por ejemplo sqlite:///./storage/dashboard.db")
    parser.add_argument("--target-url", required=True, help="URL SQLAlchemy destino, por ejemplo postgresql+psycopg://...")
    parser.add_argument("--truncate", action="store_true", help="Vaciar las tablas destino antes de insertar")
    args = parser.parse_args()

    migrate(args.source_url, args.target_url, truncate=args.truncate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
