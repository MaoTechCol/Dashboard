from __future__ import annotations

from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ensure_utc(value: datetime | None) -> datetime | None:
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def parse_timestamp(value: object, timezone_name: str = "UTC") -> datetime | None:
    if not value:
        return None
    tz = ZoneInfo(timezone_name)
    if isinstance(value, datetime):
        localized = value if value.tzinfo else value.replace(tzinfo=tz)
        return localized.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 1_000_000_000_000:
            timestamp /= 1000
        if timestamp > 1_000_000_000:
            return datetime.fromtimestamp(timestamp, tz=timezone.utc)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        try:
            timestamp = float(text)
            if timestamp > 1_000_000_000_000:
                timestamp /= 1000
            if timestamp > 1_000_000_000:
                return datetime.fromtimestamp(timestamp, tz=timezone.utc)
        except ValueError:
            pass
    candidates = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    )
    for fmt in candidates:
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.replace(tzinfo=tz).astimezone(timezone.utc)
        except ValueError:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo:
            return parsed.astimezone(timezone.utc)
        return parsed.replace(tzinfo=tz).astimezone(timezone.utc)
    except ValueError:
        return None


def as_timezone(value: datetime | None, timezone_name: str) -> datetime | None:
    value = ensure_utc(value)
    if value is None:
        return None
    return value.astimezone(ZoneInfo(timezone_name))


def to_local_date(value: datetime, timezone_name: str) -> date:
    return as_timezone(value, timezone_name).date()
