from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, AsyncIterator
from uuid import uuid4

import httpx
import websockets

from app.core.time import parse_timestamp
from app.schemas import NormalizedAlarm, NormalizedStatus
from app.services.company_registry import CompanyRegistry


AUTH_ERROR_HINTS = (
    "session has expired",
    "please log in again",
    "token invalid",
    "pid invalid",
    "login failed",
    "invalid token",
    "invalid pid",
    "re-login",
)

LOGIN_RATE_LIMIT_HINTS = (
    "login too frequently",
    "too frequent",
)

HISTORICAL_ALARM_TYPE_MAP = {
    "eye closed": "Ojos cerrados",
    "yawning": "Bostezo",
    "fcw (forward collision warning)": "Riesgo de colision",
    "phone call alarm": "Uso de celular",
    "distracted driving": "Distraccion",
    "dms camera covered": "Camara cubierta",
    "camera undetected": "Camara cubierta",
    "ir-blocking sunglasses": "Camara cubierta",
    "smoking": "Fumando",
    "fatigue driving alarm": "Fatiga en progresion",
}


@dataclass
class HowenSession:
    token: str
    pid: str


class HowenClient:
    def __init__(
        self,
        *,
        settings: Any,
        registry: CompanyRegistry,
    ) -> None:
        self.settings = settings
        self.registry = registry

    def _password_md5(self) -> str | None:
        if self.settings.howen_password_md5:
            return self.settings.howen_password_md5
        if self.settings.howen_password:
            return hashlib.md5(self.settings.howen_password.encode("utf-8")).hexdigest()
        return None

    def has_durable_credentials(self) -> bool:
        return bool(self.settings.howen_username and self._password_md5())

    def is_auth_error(self, error: object) -> bool:
        message = str(error).lower()
        return any(hint in message for hint in AUTH_ERROR_HINTS)

    def is_login_rate_limited(self, error: object) -> bool:
        message = str(error).lower()
        return any(hint in message for hint in LOGIN_RATE_LIMIT_HINTS)

    def is_ignorable_historical_alarm(self, payload: dict[str, Any]) -> bool:
        alarm_type = _normalize_alarm_type(payload.get("alarmTypeValue"))
        return bool(alarm_type) and alarm_type not in HISTORICAL_ALARM_TYPE_MAP

    def _bootstrap_session(self) -> HowenSession | None:
        if self.settings.howen_token and self.settings.howen_pid:
            return HowenSession(token=self.settings.howen_token, pid=self.settings.howen_pid)
        return None

    def _load_cached_session(self) -> HowenSession | None:
        if not self.settings.session_cache_path.exists():
            return None
        payload = json.loads(self.settings.session_cache_path.read_text(encoding="utf-8"))
        token = payload.get("token")
        pid = payload.get("pid")
        if token and pid:
            return HowenSession(token=token, pid=pid)
        return None

    def cache_session(self, session: HowenSession) -> None:
        self.settings.session_cache_path.write_text(
            json.dumps({"token": session.token, "pid": session.pid}, indent=2),
            encoding="utf-8",
        )

    def clear_cached_session(self) -> None:
        if self.settings.session_cache_path.exists():
            self.settings.session_cache_path.unlink()

    async def login(self) -> HowenSession:
        if not self.settings.howen_username:
            raise RuntimeError("HOWEN_USERNAME is required for live ingestion")
        password_md5 = self._password_md5()
        if not password_md5:
            raise RuntimeError("HOWEN_PASSWORD or HOWEN_PASSWORD_MD5 is required for live ingestion")
        url = f"{self.settings.howen_http_base.rstrip('/')}/user/apiLogin.action"
        async with httpx.AsyncClient(timeout=20.0) as client:
            response = await client.post(
                url,
                json={
                    "username": self.settings.howen_username,
                    "password": password_md5,
                },
            )
            response.raise_for_status()
            payload = response.json()
        if payload.get("status") != 10000:
            raise RuntimeError(payload.get("msg") or "Unable to authenticate against Howen VSS")
        data = payload.get("data") or {}
        session = HowenSession(token=data["token"], pid=data["pid"])
        self.cache_session(session)
        return session

    async def resolve_session(self, *, force_login: bool = False) -> HowenSession:
        cached = self._load_cached_session()
        bootstrap = self._bootstrap_session()

        if not force_login:
            if cached:
                return cached
            if bootstrap:
                return bootstrap

        if self.has_durable_credentials():
            try:
                return await self.login()
            except Exception as exc:
                if not force_login and self.is_login_rate_limited(exc):
                    fallback = cached or bootstrap
                    if fallback:
                        return fallback
                raise

        fallback = cached or bootstrap
        if fallback:
            return fallback
        return await self.login()

    async def fetch_devices(self, token: str) -> list[dict[str, Any]]:
        url = f"{self.settings.howen_http_base.rstrip('/')}/vehicle/findAll.action"
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(
                url,
                json={
                    "token": token,
                    "pageNum": "-1",
                    "pageCount": "-1",
                },
            )
            response.raise_for_status()
            payload = response.json()
        if payload.get("status") != 10000:
            raise RuntimeError(payload.get("msg") or "Unable to fetch device catalog")
        return _extract_rows(payload)

    async def fetch_historical_alarms(
        self,
        token: str,
        *,
        device_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        url = f"{self.settings.howen_http_base.rstrip('/')}/alarm/apiFindAllByTime.action"
        body = {
            "token": token,
            "deviceID": device_id,
            "deviceno": device_id,
            "deviceid": device_id,
            "startTime": start_at.strftime("%Y-%m-%d %H:%M:%S"),
            "endTime": end_at.strftime("%Y-%m-%d %H:%M:%S"),
            "pageNum": "-1",
            "pageCount": "-1",
        }
        async with httpx.AsyncClient(timeout=40.0) as client:
            response = await client.post(url, json=body)
            response.raise_for_status()
            payload = response.json()
        if payload.get("status") != 10000:
            raise RuntimeError(payload.get("msg") or "Unable to fetch historical alarms")
        return _extract_rows(payload)

    async def listen(self, session: HowenSession) -> AsyncIterator[dict[str, Any]]:
        if not self.settings.howen_username:
            raise RuntimeError("HOWEN_USERNAME is required for websocket login")
        heartbeat_task: asyncio.Task[None] | None = None
        async with websockets.connect(self.settings.howen_ws_url, ping_interval=None) as websocket:
            await websocket.send(
                json.dumps(
                    {
                        "action": "80000",
                        "payload": {
                            "username": self.settings.howen_username,
                            "pid": session.pid,
                            "token": session.token,
                            "subscribe": [80003, 80004],
                        },
                    }
                )
            )
            login_reply = json.loads(await websocket.recv())
            if (login_reply.get("payload") or {}).get("result") == "fail":
                raise RuntimeError((login_reply.get("payload") or {}).get("msg") or "Howen websocket login failed")

            await websocket.send(json.dumps({"action": "80001", "payload": ""}))
            try:
                subscribe_reply = json.loads(await asyncio.wait_for(websocket.recv(), timeout=5))
                yield subscribe_reply
            except Exception:
                pass

            async def heartbeat() -> None:
                while True:
                    await asyncio.sleep(60)
                    try:
                        await websocket.send(
                            json.dumps(
                                {
                                    "action": "80009",
                                    "payload": {
                                        "username": self.settings.howen_username,
                                        "token": session.token,
                                    },
                                }
                            )
                        )
                    except websockets.ConnectionClosed:
                        return

            heartbeat_task = asyncio.create_task(heartbeat())
            async for raw in websocket:
                yield json.loads(raw)
        if heartbeat_task:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

    def normalize_status(self, payload: dict[str, Any]) -> NormalizedStatus | None:
        device_id = str(payload.get("deviceID") or payload.get("deviceno") or payload.get("deviceid") or "").strip()
        if not device_id:
            return None
        fleet_id = _pick_value(payload, "fleetID", "fleetId", "fleetid")
        plate_no = _pick_value(payload, "plateNo", "plateno", "plate")
        driver = payload.get("driver") or {}
        event_time = (
            _pick_value(payload, "dtu")
            or _pick_value(payload.get("location") or {}, "dtu")
            or _pick_value(payload.get("ext") or {}, "reportTime")
        )
        timezone_name = self.registry.timezone_for(
            device_id=device_id,
            fleet_id=fleet_id,
            fallback=self.settings.default_timezone,
        )
        observed_at = parse_timestamp(event_time, timezone_name)
        if not observed_at:
            return None
        mileage = payload.get("mileage") or {}
        return NormalizedStatus(
            device_id=device_id,
            observed_at=observed_at,
            total_km=_normalize_distance_km(mileage.get("total")),
            day_km=_normalize_distance_km(mileage.get("todayDay")),
            plate_no=plate_no,
            fleet_id=fleet_id,
            driver_name=_pick_value(driver, "name", "drivername"),
            device_name=_pick_value(payload.get("ext") or {}, "deviceName", "devicename") or plate_no,
            raw_event_time=str(event_time) if event_time else None,
            raw=payload,
        )

    def normalize_alarm(self, payload: dict[str, Any]) -> NormalizedAlarm | None:
        device_id = str(payload.get("deviceID") or payload.get("deviceno") or payload.get("deviceid") or "").strip()
        if not device_id:
            return None
        detail = payload.get("payload") or payload
        det = detail.get("det") or {}
        fleet_id = _pick_value(payload, "fleetID", "fleetId", "fleetid") or _pick_value(detail, "fleetID", "fleetId", "fleetid")
        plate_no = _pick_value(payload, "plateNo", "plateno", "plate") or _pick_value(detail, "plateNo", "plateno", "plate")
        guid = str(
            payload.get("alarmID")
            or payload.get("guid")
            or detail.get("uuid")
            or uuid4().hex
        )
        timezone_name = self.registry.timezone_for(
            device_id=device_id,
            fleet_id=fleet_id,
            fallback=self.settings.default_timezone,
        )
        if det:
            subtype = str(det.get("tp")) if det.get("tp") is not None else None
            mapped_category = self.registry.subtype_map().get(subtype or "")
            payload_category = payload.get("category") or detail.get("category")
            category = mapped_category or payload_category or "Sin clasificar"
            if mapped_category:
                mapping_source = "subtype_map"
            elif payload_category:
                mapping_source = "payload_category"
            else:
                mapping_source = "unclassified"
            location = payload.get("location") or {}
            event_time = detail.get("dtu") or detail.get("st") or location.get("dtu") or payload.get("dtu")
            occurred_at = parse_timestamp(event_time, timezone_name)
            if not occurred_at:
                return None
            return NormalizedAlarm(
                guid=guid,
                device_id=device_id,
                occurred_at=occurred_at,
                start_at=parse_timestamp(detail.get("st"), timezone_name),
                end_at=parse_timestamp(detail.get("et"), timezone_name),
                category=category,
                subtype=subtype,
                mapping_source=mapping_source,
                event_code=str(detail.get("ec")) if detail.get("ec") is not None else None,
                plate_no=plate_no,
                fleet_id=fleet_id,
                driver_name=_pick_value(detail, "drname", "drivername"),
                latitude=_safe_float(location.get("latitude")),
                longitude=_safe_float(location.get("longitude")),
                total_mileage_km=_normalize_distance_km(
                    payload.get("totalMileage")
                    or detail.get("totalMileage")
                    or (payload.get("mileage") or {}).get("total")
                ),
                raw_event_time=str(event_time) if event_time else None,
                raw=payload,
            )

        historical_type = _normalize_alarm_type(detail.get("alarmTypeValue"))
        historical_category = HISTORICAL_ALARM_TYPE_MAP.get(historical_type)
        if not historical_category:
            return None
        event_time = detail.get("reportTime") or detail.get("startTime") or detail.get("endTime")
        occurred_at = parse_timestamp(event_time, timezone_name)
        if not occurred_at:
            return None
        latitude, longitude = _parse_alarm_gps(detail.get("alarmGps"))
        return NormalizedAlarm(
            guid=guid,
            device_id=device_id,
            occurred_at=occurred_at,
            start_at=parse_timestamp(detail.get("startTime"), timezone_name),
            end_at=parse_timestamp(detail.get("endTime"), timezone_name),
            category=historical_category,
            subtype=str(detail.get("alarmTypeValue")).strip() if detail.get("alarmTypeValue") else None,
            mapping_source="history_alarm_type",
            event_code=None,
            plate_no=plate_no,
            fleet_id=fleet_id,
            driver_name=_pick_value(detail, "driverName", "drivername"),
            latitude=latitude,
            longitude=longitude,
            total_mileage_km=_normalize_distance_km(detail.get("totalMileage")),
            raw_event_time=str(event_time) if event_time else None,
            raw=payload,
        )


def _extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = payload.get("data") or {}
    rows = data.get("list") or data.get("rows") or data.get("result") or data.get("items") or data.get("dataList") or []
    if isinstance(rows, dict):
        rows = rows.get("list") or rows.get("rows") or rows.get("items") or rows.get("dataList") or []
    return [row for row in rows if isinstance(row, dict)]


def _pick_value(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    return None


def _safe_float(value: object) -> float | None:
    if value in (None, "", "-"):
        return None
    try:
        return float(str(value).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _normalize_alarm_type(value: object) -> str:
    return str(value or "").strip().lower()


def _parse_alarm_gps(value: object) -> tuple[float | None, float | None]:
    if value in (None, "", "-"):
        return None, None
    raw = str(value)
    if "," not in raw:
        return None, None
    lon, lat = raw.split(",", 1)
    return _safe_float(lat), _safe_float(lon)


def _normalize_distance_km(value: object) -> float | None:
    parsed = _safe_float(value)
    if parsed is None:
        return None
    if parsed >= 1_000_000:
        return round(parsed / 1000, 1)
    if parsed >= 10_000:
        return round(parsed / 1000, 1)
    return round(parsed, 1)
