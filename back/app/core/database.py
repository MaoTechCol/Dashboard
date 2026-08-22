from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

from app.core.settings import get_settings


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
                "pool_size": max(int(settings.database_pool_size), 1),
                "max_overflow": max(int(settings.database_max_overflow), 0),
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

    # SQLite remains convenient for local tests. PostgreSQL schema changes are
    # versioned with Alembic and never trigger data repairs during startup.
    if engine.dialect.name == "sqlite":
        Base.metadata.create_all(bind=engine)


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
