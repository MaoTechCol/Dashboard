from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from time import monotonic
from typing import Any, AsyncIterator, ClassVar
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
    "requests too frequent",
)

NO_DATA_HINTS = (
    "no data",
    "no record",
    "not data",
)

HISTORICAL_ALARM_TYPE_MAP = {
    "eye closed": "Ojos cerrados",
    "eyes closed": "Ojos cerrados",
    "yawning": "Bostezo",
    "yawn": "Bostezo",
    "fcw (forward collision warning)": "Riesgo de colision",
    "forward collision warning": "Riesgo de colision",
    "phone call alarm": "Uso de celular",
    "using phone while driving": "Uso de celular",
    "distracted driving": "Distraccion",
    "distraction alarm": "Distraccion",
    "distractions alarm": "Distraccion",
    "dms camera covered": "Camara cubierta",
    "camera covered": "Camara cubierta",
    "camera undetected": "Camara cubierta",
    "occlusion": "Camara cubierta",
    "ir-blocking sunglasses": "Camara cubierta",
    "smoking": "Fumando",
    "driver smoking": "Fumando",
    "fatigue driving alarm": "Fatiga en progresion",
}

LIVE_TP_MAP = {
    "17": "Riesgo de colision",
    "34": "Uso de celular",
    "35": "Fumando",
    "65": "Ojos cerrados",
    "66": "Bostezo",
    "68": "Distraccion",
}

LIVE_EVENT_CODE_MAP = {
    "110": "Riesgo de colision",
    "116": "Uso de celular",
    "117": "Fumando",
    "121": "Ojos cerrados",
    "122": "Bostezo",
    "124": "Distraccion",
}
PLATE_LIKE_RE = re.compile(r"^[A-Z]{3}\d{3}$")


@dataclass
class HowenSession:
    token: str
    pid: str


class HowenRateLimitError(RuntimeError):
    pass


class HowenClient:
    _shared_sessions: ClassVar[dict[str, HowenSession]] = {}
    _login_locks: ClassVar[dict[str, asyncio.Lock]] = {}
    _request_locks: ClassVar[dict[str, asyncio.Lock]] = {}
    _last_login_at: ClassVar[dict[str, float]] = {}
    _login_cooldown_until: ClassVar[dict[str, float]] = {}
    _next_request_at: ClassVar[dict[str, float]] = {}
    _adaptive_request_spacing: ClassVar[dict[str, float]] = {}
    _successful_request_streak: ClassVar[dict[str, int]] = {}

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

    @property
    def _account_key(self) -> str:
        return f"{self.settings.howen_http_base.rstrip('/')}|{self.settings.howen_username or ''}"

    def _get_login_lock(self) -> asyncio.Lock:
        lock = self._login_locks.get(self._account_key)
        if lock is None:
            lock = asyncio.Lock()
            self._login_locks[self._account_key] = lock
        return lock

    def _get_request_lock(self) -> asyncio.Lock:
        lock = self._request_locks.get(self._account_key)
        if lock is None:
            lock = asyncio.Lock()
            self._request_locks[self._account_key] = lock
        return lock

    def _current_request_spacing(self) -> float:
        base = max(float(self.settings.howen_request_spacing_seconds), 0.0)
        return max(self._adaptive_request_spacing.get(self._account_key, base), base)

    def _register_request_success(self) -> None:
        streak = self._successful_request_streak.get(self._account_key, 0) + 1
        recovery_successes = max(int(self.settings.howen_request_recovery_successes), 1)
        if streak < recovery_successes:
            self._successful_request_streak[self._account_key] = streak
            return
        base = max(float(self.settings.howen_request_spacing_seconds), 0.0)
        current = self._current_request_spacing()
        self._adaptive_request_spacing[self._account_key] = max(base, current - 0.5)
        self._successful_request_streak[self._account_key] = 0

    def _register_rate_limit(self) -> None:
        base = max(float(self.settings.howen_request_spacing_seconds), 0.0)
        maximum = max(float(self.settings.howen_request_spacing_max_seconds), base)
        current = self._current_request_spacing()
        self._adaptive_request_spacing[self._account_key] = min(
            maximum,
            max(current * 1.5, base + 0.5),
        )
        self._successful_request_streak[self._account_key] = 0
        cooldown = max(float(self.settings.backfill_rate_limit_cooldown_seconds), 0.0)
        self._next_request_at[self._account_key] = max(
            self._next_request_at.get(self._account_key, 0.0),
            monotonic() + max(cooldown, self._current_request_spacing()),
        )

    def is_auth_error(self, error: object) -> bool:
        message = str(error).lower()
        return any(hint in message for hint in AUTH_ERROR_HINTS)

    def is_login_rate_limited(self, error: object) -> bool:
        message = str(error).lower()
        return any(hint in message for hint in LOGIN_RATE_LIMIT_HINTS)

    def is_rate_limited(self, error: object) -> bool:
        return self.is_login_rate_limited(error)

    def is_no_data_error(self, error: object) -> bool:
        message = str(error).lower()
        return any(hint in message for hint in NO_DATA_HINTS)

    def is_ignorable_historical_alarm(self, payload: dict[str, Any]) -> bool:
        return False

    def _bootstrap_session(self) -> HowenSession | None:
        if self.settings.howen_token and self.settings.howen_pid:
            return HowenSession(token=self.settings.howen_token, pid=self.settings.howen_pid)
        return None

    def _load_disk_cached_session(self) -> HowenSession | None:
        if not self.settings.session_cache_path.exists():
            return None
        payload = json.loads(self.settings.session_cache_path.read_text(encoding="utf-8"))
        token = payload.get("token")
        pid = payload.get("pid")
        if token and pid:
            return HowenSession(token=token, pid=pid)
        return None

    def _load_cached_session(self) -> HowenSession | None:
        in_memory = self._shared_sessions.get(self._account_key)
        if in_memory:
            return in_memory
        disk_cached = self._load_disk_cached_session()
        if disk_cached:
            self._shared_sessions[self._account_key] = disk_cached
            return disk_cached
        bootstrap = self._bootstrap_session()
        if bootstrap:
            self._shared_sessions[self._account_key] = bootstrap
        return bootstrap

    def cache_session(self, session: HowenSession) -> None:
        self._shared_sessions[self._account_key] = session
        self.settings.session_cache_path.write_text(
            json.dumps({"token": session.token, "pid": session.pid}, indent=2),
            encoding="utf-8",
        )

    def clear_cached_session(self) -> None:
        self._shared_sessions.pop(self._account_key, None)
        if self.settings.session_cache_path.exists():
            self.settings.session_cache_path.unlink()

    async def invalidate_session(self) -> None:
        self.clear_cached_session()

    async def _wait_for_login_window(self) -> None:
        now = monotonic()
        cooldown_until = self._login_cooldown_until.get(self._account_key, 0.0)
        if cooldown_until > now:
            await asyncio.sleep(cooldown_until - now)
            now = monotonic()
        last_login_at = self._last_login_at.get(self._account_key)
        min_interval = float(self.settings.howen_login_min_interval_seconds)
        if last_login_at is not None:
            remaining = (last_login_at + min_interval) - now
            if remaining > 0:
                await asyncio.sleep(remaining)

    async def _post_json(self, url: str, body: dict[str, Any], *, timeout: float) -> dict[str, Any]:
        async with self._get_request_lock():
            now = monotonic()
            next_request_at = self._next_request_at.get(self._account_key, 0.0)
            if next_request_at > now:
                await asyncio.sleep(next_request_at - now)
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(url, json=body)
                    if response.status_code == 429:
                        self._register_rate_limit()
                    response.raise_for_status()
                    payload = response.json()
                    if self.is_rate_limited(payload.get("msg") or ""):
                        self._register_rate_limit()
                    elif payload.get("status") == 10000:
                        self._register_request_success()
                    return payload
            finally:
                self._next_request_at[self._account_key] = max(
                    self._next_request_at.get(self._account_key, 0.0),
                    monotonic() + self._current_request_spacing(),
                )

    async def _post_form(
        self,
        url: str,
        body: dict[str, Any],
        *,
        token: str,
        timeout: float,
    ) -> dict[str, Any]:
        headers = {
            "token": token,
            "platform": "web",
            "version": "v2",
            "X-Requested-With": "XMLHttpRequest",
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        }
        async with self._get_request_lock():
            now = monotonic()
            next_request_at = self._next_request_at.get(self._account_key, 0.0)
            if next_request_at > now:
                await asyncio.sleep(next_request_at - now)
            try:
                async with httpx.AsyncClient(timeout=timeout) as client:
                    response = await client.post(url, data=body, headers=headers)
                    if response.status_code == 429:
                        self._register_rate_limit()
                    response.raise_for_status()
                    payload = response.json()
                    if self.is_rate_limited(payload.get("msg") or ""):
                        self._register_rate_limit()
                    elif payload.get("status") == 10000:
                        self._register_request_success()
                    return payload
            finally:
                self._next_request_at[self._account_key] = max(
                    self._next_request_at.get(self._account_key, 0.0),
                    monotonic() + self._current_request_spacing(),
                )

    async def login(self) -> HowenSession:
        if not self.settings.howen_username:
            raise RuntimeError("HOWEN_USERNAME is required for live ingestion")
        password_md5 = self._password_md5()
        if not password_md5:
            raise RuntimeError("HOWEN_PASSWORD or HOWEN_PASSWORD_MD5 is required for live ingestion")
        url = f"{self.settings.howen_http_base.rstrip('/')}/user/apiLogin.action"
        payload = await self._post_json(
            url,
            {
                "username": self.settings.howen_username,
                "password": password_md5,
            },
            timeout=20.0,
        )
        if payload.get("status") != 10000:
            raise RuntimeError(payload.get("msg") or "Unable to authenticate against Howen VSS")
        data = payload.get("data") or {}
        session = HowenSession(token=data["token"], pid=data["pid"])
        self.cache_session(session)
        return session

    async def resolve_session(self, *, force_login: bool = False) -> HowenSession:
        fallback = self._load_cached_session()
        if fallback and not force_login:
            return fallback

        if not self.has_durable_credentials():
            if fallback:
                return fallback
            raise RuntimeError("Howen durable credentials are not configured")

        async with self._get_login_lock():
            fallback = self._load_cached_session()
            if fallback and not force_login:
                return fallback

            await self._wait_for_login_window()
            try:
                session = await self.login()
            except Exception as exc:
                if self.is_login_rate_limited(exc):
                    self._login_cooldown_until[self._account_key] = monotonic() + float(
                        self.settings.howen_login_rate_limit_cooldown_seconds
                    )
                    if fallback and not force_login:
                        return fallback
                raise
            self._last_login_at[self._account_key] = monotonic()
            self._login_cooldown_until.pop(self._account_key, None)
            return session

    def extract_plate_candidate(self, payload: dict[str, Any]) -> str | None:
        detail = _as_dict(payload.get("payload")) or payload
        ext = _as_dict(payload.get("ext"))
        detail_ext = _as_dict(detail.get("ext"))
        return _pick_plate_value(payload, detail=detail, ext=ext or detail_ext)

    async def fetch_devices(self, token: str) -> list[dict[str, Any]]:
        url = f"{self.settings.howen_http_base.rstrip('/')}/vehicle/findAll.action"
        payload = await self._post_json(
            url,
            {
                "token": token,
                "pageNum": "-1",
                "pageCount": "-1",
            },
            timeout=30.0,
        )
        if payload.get("status") != 10000:
            raise RuntimeError(payload.get("msg") or "Unable to fetch device catalog")
        return _extract_rows(payload)

    async def fetch_devices_authorized(self, *, force_login: bool = False) -> list[dict[str, Any]]:
        session = await self.resolve_session(force_login=force_login)
        try:
            return await self.fetch_devices(session.token)
        except Exception as exc:
            if not self.is_auth_error(exc):
                raise
            await self.invalidate_session()
            session = await self.resolve_session(force_login=True)
            return await self.fetch_devices(session.token)

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
        payload = await self._post_json(url, body, timeout=40.0)
        if payload.get("status") != 10000:
            if self.is_no_data_error(payload.get("msg") or ""):
                return []
            if self.is_rate_limited(payload.get("msg") or ""):
                raise HowenRateLimitError(payload.get("msg") or "Requests too frequent, please try again later")
            raise RuntimeError(payload.get("msg") or "Unable to fetch historical alarms")
        return _extract_rows(payload)

    async def fetch_historical_alarms_authorized(
        self,
        *,
        device_id: str,
        start_at: datetime,
        end_at: datetime,
        force_login: bool = False,
    ) -> list[dict[str, Any]]:
        session = await self.resolve_session(force_login=force_login)
        try:
            return await self.fetch_historical_alarms(
                session.token,
                device_id=device_id,
                start_at=start_at,
                end_at=end_at,
            )
        except Exception as exc:
            if not self.is_auth_error(exc):
                raise
            await self.invalidate_session()
            session = await self.resolve_session(force_login=True)
            return await self.fetch_historical_alarms(
                session.token,
                device_id=device_id,
                start_at=start_at,
                end_at=end_at,
            )

    async def fetch_track_mileage(
        self,
        token: str,
        *,
        device_id: str,
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        """Read the documented VSS track feed used by Mileage Record.

        The API reports mileage fields in units of ten metres. Conversion is
        intentionally deferred to ingestion so the original values remain
        available for audit and future reprocessing.
        """
        url = f"{self.settings.howen_http_base.rstrip('/')}/track/getTrackList.action"
        page_size = max(int(getattr(self.settings, "mileage_track_page_size", 500)), 1)
        rows: list[dict[str, Any]] = []
        page_num = 1
        previous_fingerprint: tuple[str, ...] | None = None

        while True:
            body = {
                "token": token,
                "deviceID": device_id,
                "deviceId": device_id,
                "deviceid": device_id,
                "beginTime": start_at.strftime("%Y-%m-%d %H:%M:%S"),
                "endTime": end_at.strftime("%Y-%m-%d %H:%M:%S"),
                "pageNum": str(page_num),
                "pageCount": str(page_size),
            }
            payload = await self._post_json(url, body, timeout=60.0)
            if payload.get("status") != 10000:
                message = payload.get("msg") or "Unable to fetch historical mileage"
                if self.is_no_data_error(message):
                    return rows
                if self.is_rate_limited(message):
                    raise HowenRateLimitError(message)
                raise RuntimeError(message)

            page_rows = _extract_rows(payload)
            if not page_rows:
                break
            fingerprint = tuple(
                str(row.get("guid") or row.get("id") or row.get("dtu") or row.get("time") or index)
                for index, row in enumerate(page_rows[:5])
            )
            if fingerprint == previous_fingerprint:
                break
            previous_fingerprint = fingerprint
            rows.extend(page_rows)
            if len(page_rows) < page_size:
                break
            page_num += 1

        return rows

    async def fetch_track_mileage_authorized(
        self,
        *,
        device_id: str,
        start_at: datetime,
        end_at: datetime,
        force_login: bool = False,
    ) -> list[dict[str, Any]]:
        session = await self.resolve_session(force_login=force_login)
        try:
            return await self.fetch_track_mileage(
                session.token,
                device_id=device_id,
                start_at=start_at,
                end_at=end_at,
            )
        except Exception as exc:
            if not self.is_auth_error(exc):
                raise
            await self.invalidate_session()
            session = await self.resolve_session(force_login=True)
            return await self.fetch_track_mileage(
                session.token,
                device_id=device_id,
                start_at=start_at,
                end_at=end_at,
            )

    async def fetch_daily_mileage_report(
        self,
        token: str,
        *,
        device_ids: list[str],
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        """Use the same aggregate endpoint as VSS Daily Mileage Report."""
        normalized_ids = sorted({str(value).strip() for value in device_ids if str(value).strip()})
        if not normalized_ids:
            return []
        url = f"{self.settings.howen_http_base.rstrip('/')}/mileage/mileageStatDY.action"
        payload = await self._post_json(
            url,
            {
                "token": token,
                "startDate": start_at.strftime("%Y-%m-%d %H:%M:%S"),
                "endDate": end_at.strftime("%Y-%m-%d %H:%M:%S"),
                "deviceIdList": normalized_ids,
                "dimen": "DAY",
            },
            timeout=90.0,
        )
        if payload.get("status") != 10000:
            message = payload.get("msg") or "Unable to fetch daily mileage report"
            if self.is_no_data_error(message):
                return []
            if self.is_rate_limited(message):
                raise HowenRateLimitError(message)
            raise RuntimeError(message)
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
        return _extract_rows(payload)

    async def fetch_daily_mileage_report_authorized(
        self,
        *,
        device_ids: list[str],
        start_at: datetime,
        end_at: datetime,
        force_login: bool = False,
    ) -> list[dict[str, Any]]:
        session = await self.resolve_session(force_login=force_login)
        try:
            return await self.fetch_daily_mileage_report(
                session.token,
                device_ids=device_ids,
                start_at=start_at,
                end_at=end_at,
            )
        except Exception as exc:
            if not self.is_auth_error(exc):
                raise
            await self.invalidate_session()
            session = await self.resolve_session(force_login=True)
            return await self.fetch_daily_mileage_report(
                session.token,
                device_ids=device_ids,
                start_at=start_at,
                end_at=end_at,
            )

    async def fetch_evidence_alarms(
        self,
        token: str,
        *,
        device_ids: list[str],
        start_at: datetime,
        end_at: datetime,
    ) -> list[dict[str, Any]]:
        """Fetch every video-backed alarm shown by Howen's Alarm Clips view."""
        normalized_ids = sorted({str(device_id).strip() for device_id in device_ids if str(device_id).strip()})
        if not normalized_ids:
            return []

        url = f"{self.settings.howen_http_base.rstrip('/')}/record/findEvidences.action"
        page_size = max(int(getattr(self.settings, "howen_evidence_page_size", 100)), 1)
        max_devices = max(int(getattr(self.settings, "howen_evidence_max_devices_per_request", 50)), 1)
        rows: list[dict[str, Any]] = []

        for offset in range(0, len(normalized_ids), max_devices):
            device_batch = normalized_ids[offset : offset + max_devices]
            page_num = 1
            previous_fingerprint: tuple[str, ...] | None = None
            while True:
                body = {
                    "token": token,
                    "scheme": "http",
                    "lang": "en_US",
                    "conditionName": ",".join(device_batch),
                    "startTime": start_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "endTime": end_at.strftime("%Y-%m-%d %H:%M:%S"),
                    "alarmType": "",
                    "takeType": "",
                    "reviewType": "",
                    "driverCardId": "",
                    "pageNum": str(page_num),
                    "pageCount": str(page_size),
                }
                payload = await self._post_form(url, body, token=token, timeout=45.0)
                if payload.get("status") != 10000:
                    if self.is_no_data_error(payload.get("msg") or ""):
                        break
                    if self.is_rate_limited(payload.get("msg") or ""):
                        raise HowenRateLimitError(
                            payload.get("msg") or "Requests too frequent, please try again later"
                        )
                    raise RuntimeError(payload.get("msg") or "Unable to fetch Howen alarm evidences")

                page_rows = _extract_rows(payload)
                if not page_rows:
                    break
                fingerprint = tuple(
                    str(row.get("alarmGuid") or row.get("alarmID") or row.get("guid") or "")
                    for row in page_rows
                )
                if previous_fingerprint == fingerprint:
                    break
                rows.extend(page_rows)
                previous_fingerprint = fingerprint
                if len(page_rows) < page_size:
                    break
                page_num += 1

        return rows

    async def fetch_evidence_alarms_authorized(
        self,
        *,
        device_ids: list[str],
        start_at: datetime,
        end_at: datetime,
        force_login: bool = False,
    ) -> list[dict[str, Any]]:
        session = await self.resolve_session(force_login=force_login)
        try:
            return await self.fetch_evidence_alarms(
                session.token,
                device_ids=device_ids,
                start_at=start_at,
                end_at=end_at,
            )
        except Exception as exc:
            if not self.is_auth_error(exc):
                raise
            await self.invalidate_session()
            session = await self.resolve_session(force_login=True)
            return await self.fetch_evidence_alarms(
                session.token,
                device_ids=device_ids,
                start_at=start_at,
                end_at=end_at,
            )

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
            login_reply = _decode_ws_message(await websocket.recv())
            if _message_payload_value(login_reply, "result") == "fail":
                raise RuntimeError(_message_payload_value(login_reply, "msg") or "Howen websocket login failed")

            await websocket.send(json.dumps({"action": "80001", "payload": ""}))
            try:
                subscribe_reply = _decode_ws_message(await asyncio.wait_for(websocket.recv(), timeout=5))
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
                yield _decode_ws_message(raw)
        if heartbeat_task:
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

    def normalize_status(self, payload: dict[str, Any]) -> NormalizedStatus | None:
        device_id = str(payload.get("deviceID") or payload.get("deviceno") or payload.get("deviceid") or "").strip()
        if not device_id:
            return None
        fleet_id = _pick_value(payload, "fleetID", "fleetId", "fleetid")
        company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
        raw_plate_no = self.extract_plate_candidate(payload)
        plate_no = self.registry.normalize_plate(company, raw_plate_no) if company else self.registry.normalize_plate_any(raw_plate_no)
        driver = _as_dict(payload.get("driver"))
        location = _as_dict(payload.get("location"))
        ext = _as_dict(payload.get("ext"))
        event_time = (
            _pick_value(payload, "dtu")
            or _pick_value(location, "dtu")
            or _pick_value(ext, "reportTime")
        )
        timezone_name = self.registry.timezone_for(
            device_id=device_id,
            fleet_id=fleet_id,
            fallback=self.settings.default_timezone,
        )
        observed_at = parse_timestamp(event_time, timezone_name)
        if not observed_at:
            return None
        mileage = _as_dict(payload.get("mileage"))
        return NormalizedStatus(
            device_id=device_id,
            observed_at=observed_at,
            total_km=_normalize_distance_field_km(mileage.get("total")),
            day_km=_normalize_distance_field_km(mileage.get("todayDay")),
            plate_no=plate_no,
            fleet_id=fleet_id,
            driver_name=_pick_value(driver, "name", "drivername"),
            device_name=_pick_value(ext, "deviceName", "devicename") or plate_no,
            raw_event_time=str(event_time) if event_time else None,
            raw_total_value=_raw_string(mileage.get("total")),
            raw_day_value=_raw_string(mileage.get("todayDay")),
            raw=payload,
        )

    def normalize_alarm(self, payload: dict[str, Any]) -> NormalizedAlarm | None:
        device_id = str(payload.get("deviceID") or payload.get("deviceno") or payload.get("deviceid") or "").strip()
        if not device_id:
            return None
        nested_payload = _as_dict(payload.get("payload"))
        detail = nested_payload or payload
        det = _as_dict(detail.get("det"))
        detail_meta = _as_dict(detail.get("detail"))
        fleet_id = _pick_value(payload, "fleetID", "fleetId", "fleetid") or _pick_value(detail, "fleetID", "fleetId", "fleetid")
        company = self.registry.resolve_company(device_id=device_id, fleet_id=fleet_id)
        raw_plate_no = self.extract_plate_candidate(payload)
        plate_no = self.registry.normalize_plate(company, raw_plate_no) if company else self.registry.normalize_plate_any(raw_plate_no)
        guid = str(
            payload.get("alarmID")
            or payload.get("alarmGuid")
            or payload.get("guid")
            or detail.get("uuid")
            or uuid4().hex
        )
        timezone_name = self.registry.timezone_for(
            device_id=device_id,
            fleet_id=fleet_id,
            fallback=self.settings.default_timezone,
        )
        raw_alarm_type = _pick_alarm_text(payload, detail)
        raw_tp = _pick_alarm_tp(payload, detail)
        raw_event_code = _pick_alarm_event_code(payload, detail)
        category, mapping_source, classification_status = _classify_alarm(
            raw_alarm_type=raw_alarm_type,
            raw_tp=raw_tp,
            raw_event_code=raw_event_code,
            payload_category=payload.get("category") or detail.get("category"),
            detail_category=detail_meta.get("category"),
            registry_subtype_map=self.registry.subtype_map(),
        )
        visibility_status = _visibility_for_classification(classification_status)

        if det:
            location = _as_dict(payload.get("location"))
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
                subtype=raw_tp or raw_alarm_type,
                mapping_source=mapping_source,
                event_code=raw_event_code,
                raw_alarm_type=raw_alarm_type,
                raw_tp=raw_tp,
                raw_event_code=raw_event_code,
                classification_status=classification_status,
                visibility_status=visibility_status,
                plate_no=plate_no,
                fleet_id=fleet_id,
                driver_name=_pick_value(detail, "drname", "drivername"),
                latitude=_safe_float(location.get("latitude")),
                longitude=_safe_float(location.get("longitude")),
                total_mileage_km=_normalize_distance_field_km(
                    payload.get("totalMileage")
                    or detail.get("totalMileage")
                    or _as_dict(payload.get("mileage")).get("total")
                ),
                raw_event_time=str(event_time) if event_time else None,
                raw=payload,
            )

        # In Howen historical alarms, reportTime is often stored five hours ahead of
        # the portal-visible event time. Prefer the operational end/start time first.
        event_time = (
            detail.get("alarmTime")
            or detail.get("alarmTimeEnd")
            or detail.get("endTime")
            or detail.get("startTime")
            or detail.get("reportTime")
        )
        occurred_at = parse_timestamp(event_time, timezone_name)
        if not occurred_at:
            return None
        latitude, longitude = _parse_alarm_gps(detail.get("alarmGps"))
        return NormalizedAlarm(
            guid=guid,
            device_id=device_id,
            occurred_at=occurred_at,
            start_at=parse_timestamp(
                detail.get("alarmTime") or detail.get("startTime") or detail.get("endTime"),
                timezone_name,
            ),
            end_at=parse_timestamp(
                detail.get("alarmTimeEnd")
                or detail.get("alarmTime")
                or detail.get("endTime")
                or detail.get("reportTime"),
                timezone_name,
            ),
            category=category,
            subtype=raw_alarm_type or raw_tp,
            mapping_source=mapping_source,
            event_code=raw_event_code,
            raw_alarm_type=raw_alarm_type,
            raw_tp=raw_tp,
            raw_event_code=raw_event_code,
            classification_status=classification_status,
            visibility_status=visibility_status,
            plate_no=plate_no,
            fleet_id=fleet_id,
            driver_name=_pick_value(detail, "driverName", "drivername"),
            latitude=latitude,
            longitude=longitude,
            total_mileage_km=_normalize_distance_field_km(detail.get("totalMileage")),
            raw_event_time=str(event_time) if event_time else None,
            raw=payload,
        )


def _extract_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    data = _as_dict(payload.get("data"))
    rows = data.get("list") or data.get("rows") or data.get("result") or data.get("items") or data.get("dataList") or []
    if isinstance(rows, dict):
        row_dict = _as_dict(rows)
        rows = row_dict.get("list") or row_dict.get("rows") or row_dict.get("items") or row_dict.get("dataList") or []
    return [row for row in rows if isinstance(row, dict)]


def _pick_value(payload: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = payload.get(key)
        if value not in (None, ""):
            return str(value)
    for nested_key in ("payload", "basic", "detail", "det", "ext", "location", "mileage", "module"):
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            nested_value = _pick_value(nested, *keys)
            if nested_value:
                return nested_value
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


def _raw_string(value: object) -> str | None:
    if value in (None, "", "-"):
        return None
    return str(value).strip()


def _pick_alarm_text(payload: dict[str, Any], detail: dict[str, Any]) -> str | None:
    for candidate in (
        detail.get("alarmTypeValue"),
        payload.get("alarmTypeValue"),
        detail.get("alarmType"),
        payload.get("alarmType"),
        detail.get("alarmtypeValue"),
        payload.get("alarmtypeValue"),
        detail.get("alarmName"),
        payload.get("alarmName"),
        detail.get("typeName"),
        payload.get("typeName"),
        detail.get("name"),
        payload.get("name"),
        _extract_alarm_type_from_alarm_detail(detail.get("alarmDetail")),
        _extract_alarm_type_from_alarm_detail(payload.get("alarmDetail")),
        _extract_alarm_type_from_alarm_detail((detail.get("detail") or {}).get("alarmDetail") if isinstance(detail.get("detail"), dict) else None),
        _extract_alarm_type_from_alarm_detail((payload.get("detail") or {}).get("alarmDetail") if isinstance(payload.get("detail"), dict) else None),
    ):
        raw = _raw_string(candidate)
        if raw:
            return raw
    return None


def _pick_alarm_tp(payload: dict[str, Any], detail: dict[str, Any]) -> str | None:
    det = _as_dict(detail.get("det"))
    direct = _raw_string(det.get("tp")) or _raw_string(detail.get("tp")) or _raw_string(payload.get("tp"))
    if direct:
        return direct
    for candidate in (_raw_string(detail.get("alarmvalue")), _raw_string(payload.get("alarmvalue")), _raw_string(payload.get("alarmDetail"))):
        if not candidate:
            continue
        match = re.search(r"tp:(\d+)", candidate)
        if match:
            return match.group(1)
    return None


def _pick_alarm_event_code(payload: dict[str, Any], detail: dict[str, Any]) -> str | None:
    return (
        _raw_string(detail.get("ec"))
        or _raw_string(payload.get("ec"))
        or _raw_string(detail.get("alarmtype"))
        or _raw_string(payload.get("alarmtype"))
        or _raw_string(detail.get("alarmType"))
        or _raw_string(payload.get("alarmType"))
    )


def _classify_alarm(
    *,
    raw_alarm_type: str | None,
    raw_tp: str | None,
    raw_event_code: str | None,
    payload_category: object,
    detail_category: object,
    registry_subtype_map: dict[str, str],
) -> tuple[str, str, str]:
    text_key = _normalize_alarm_type(raw_alarm_type)
    if text_key in HISTORICAL_ALARM_TYPE_MAP:
        return HISTORICAL_ALARM_TYPE_MAP[text_key], "text_alarm_type", "classified_dms"

    normalized_tp = _raw_string(raw_tp)
    tp_map = registry_subtype_map.get(normalized_tp or "") or LIVE_TP_MAP.get(normalized_tp or "")
    if tp_map:
        return tp_map, "subtype_map", "classified_dms"

    normalized_event_code = _raw_string(raw_event_code)
    event_map = LIVE_EVENT_CODE_MAP.get(normalized_event_code or "")
    if event_map:
        return event_map, "event_code_map", "classified_dms"

    for candidate in (payload_category, detail_category):
        normalized_candidate = _normalize_alarm_type(candidate)
        if normalized_candidate in HISTORICAL_ALARM_TYPE_MAP:
            return HISTORICAL_ALARM_TYPE_MAP[normalized_candidate], "payload_category", "classified_dms"
    for candidate in (raw_tp, raw_event_code):
        normalized_candidate = _normalize_alarm_type(candidate)
        if normalized_candidate in HISTORICAL_ALARM_TYPE_MAP:
            return HISTORICAL_ALARM_TYPE_MAP[normalized_candidate], "historical_equivalent", "classified_dms"

    for candidate in (raw_alarm_type, raw_tp, raw_event_code, payload_category, detail_category):
        normalized_candidate = _normalize_alarm_type(candidate)
        if _looks_like_dms_text(normalized_candidate):
            return "Sin clasificar", "dms_like_unmapped", "unmapped"
        if normalized_candidate:
            return "No DMS", "non_dms_text", "classified_non_dms"

    return "Sin clasificar", "unclassified", "unmapped"


def _pick_plate_value(
    payload: dict[str, Any],
    *,
    detail: dict[str, Any] | None = None,
    ext: dict[str, Any] | None = None,
) -> str | None:
    detail = detail or {}
    ext = ext or {}
    for candidate in (
        _pick_value(payload, "plateNo", "plateno", "plate"),
        _pick_value(detail, "plateNo", "plateno", "plate"),
        _pick_value(ext, "plateNo", "plateno", "plate"),
    ):
        raw = _raw_string(candidate)
        if raw:
            return raw

    for candidate in (
        _pick_value(payload, "devicename", "deviceName"),
        _pick_value(detail, "devicename", "deviceName"),
        _pick_value(ext, "devicename", "deviceName"),
    ):
        raw = _raw_string(candidate)
        if raw and _looks_like_vehicle_plate(raw):
            return raw

    last_status_json = payload.get("lastStatusJson")
    if isinstance(last_status_json, str) and last_status_json.strip():
        try:
            parsed = json.loads(last_status_json)
        except json.JSONDecodeError:
            parsed = {}
        parsed_ext = _as_dict(parsed.get("ext"))
        for candidate in (
            _pick_value(parsed_ext, "devicename", "deviceName"),
            _pick_value(parsed, "devicename", "deviceName"),
        ):
            raw = _raw_string(candidate)
            if raw and _looks_like_vehicle_plate(raw):
                return raw
    return None


def _visibility_for_classification(classification_status: str) -> str:
    if classification_status == "classified_dms":
        return "candidate"
    if classification_status == "classified_non_dms":
        return "hidden_non_dms"
    return "hidden_unmapped"


def _parse_alarm_gps(value: object) -> tuple[float | None, float | None]:
    if value in (None, "", "-"):
        return None, None
    raw = str(value)
    if "," not in raw:
        return None, None
    lon, lat = raw.split(",", 1)
    return _safe_float(lat), _safe_float(lon)


def _normalize_distance_field_km(value: object) -> float | None:
    if value in (None, "", "-"):
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if isinstance(value, int):
        return round(value / 1000, 1)
    if isinstance(value, float) and not value.is_integer():
        return round(value, 1)
    if re.fullmatch(r"-?\d+", raw):
        return round(float(raw) / 1000, 1)
    parsed = _safe_float(raw)
    if parsed is None:
        return None
    return round(parsed, 1)


def _looks_like_dms_text(value: str) -> bool:
    if not value:
        return False
    keywords = (
        "eye",
        "yawn",
        "phone",
        "smok",
        "distract",
        "fatigue",
        "collision",
        "camera",
        "sunglass",
        "dms",
    )
    return any(keyword in value for keyword in keywords)


def _looks_like_vehicle_plate(value: str) -> bool:
    normalized = re.sub(r"[^A-Za-z0-9]", "", value or "").upper()
    return bool(PLATE_LIKE_RE.fullmatch(normalized))


def _extract_alarm_type_from_alarm_detail(value: object) -> str | None:
    raw = _raw_string(value)
    if not raw:
        return None
    match = re.search(r"type\s*:\s*([^;]+)", raw, re.IGNORECASE)
    if not match:
        return None
    return _raw_string(match.group(1))


def _decode_ws_message(raw: str) -> dict[str, Any]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return {
            "action": "__raw_text__",
            "payload": {
                "raw_message": raw,
                "raw_type": "text",
            },
        }
    if isinstance(payload, dict):
        return payload
    return {
        "action": "__non_dict__",
        "payload": {
            "raw_message": payload,
            "raw_type": type(payload).__name__,
        },
    }


def _message_payload_value(message: dict[str, Any], key: str) -> str | None:
    payload = message.get("payload")
    if not isinstance(payload, dict):
        return None
    value = payload.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
