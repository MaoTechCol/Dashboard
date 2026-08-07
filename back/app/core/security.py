from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any


def hash_password(password: str, *, salt: str | None = None) -> str:
    salt = salt or secrets.token_hex(16)
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120_000)
    return f"{salt}${derived.hex()}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        salt, expected = password_hash.split("$", 1)
    except ValueError:
        return False
    candidate = hash_password(password, salt=salt)
    return hmac.compare_digest(candidate, f"{salt}${expected}")


def create_token(payload: dict[str, Any], *, secret: str, ttl_minutes: int) -> str:
    header = {"alg": "HS256", "typ": "JWT"}
    claims = dict(payload)
    claims["exp"] = int((datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)).timestamp())
    signing_input = f"{_b64(header)}.{_b64(claims)}"
    signature = hmac.new(secret.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
    return f"{signing_input}.{_urlsafe(signature)}"


def decode_token(token: str, *, secret: str) -> dict[str, Any]:
    try:
        header_b64, payload_b64, signature_b64 = token.split(".", 2)
    except ValueError as exc:
        raise ValueError("Malformed token") from exc
    signing_input = f"{header_b64}.{payload_b64}"
    expected = hmac.new(secret.encode("utf-8"), signing_input.encode("utf-8"), hashlib.sha256).digest()
    if not hmac.compare_digest(_urlsafe(expected), signature_b64):
        raise ValueError("Invalid token signature")
    payload = json.loads(_unb64(payload_b64))
    exp = payload.get("exp")
    if exp is None or int(exp) < int(datetime.now(timezone.utc).timestamp()):
        raise ValueError("Expired token")
    return payload


def _b64(value: dict[str, Any]) -> str:
    return _urlsafe(json.dumps(value, separators=(",", ":"), ensure_ascii=True).encode("utf-8"))


def _unb64(value: str) -> str:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(f"{value}{padding}".encode("ascii")).decode("utf-8")


def _urlsafe(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")
