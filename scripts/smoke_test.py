from __future__ import annotations

import argparse
import json
import sys
from urllib import error, request


def fetch(url: str, *, method: str = "GET", data: dict | None = None, cookie: str | None = None) -> tuple[int, dict, str | None]:
    headers = {"Content-Type": "application/json"}
    if cookie:
        headers["Cookie"] = cookie
    payload = json.dumps(data).encode("utf-8") if data is not None else None
    req = request.Request(url, data=payload, headers=headers, method=method)
    try:
        with request.urlopen(req, timeout=20) as response:
            body = response.read().decode("utf-8")
            set_cookie = response.headers.get("Set-Cookie")
            return response.status, json.loads(body or "{}"), set_cookie
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8")
        try:
            payload = json.loads(body or "{}")
        except json.JSONDecodeError:
            payload = {"detail": body}
        return exc.code, payload, None


def require_ok(name: str, status: int, payload: dict) -> None:
    if status >= 400:
        raise RuntimeError(f"{name} fallo con {status}: {payload}")
    print(f"[ok] {name}: {status}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Smoke test del dashboard DMS")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--company", default="ismocol")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")

    status, payload, _ = fetch(f"{base_url}/healthz")
    require_ok("healthz", status, payload)

    status, payload, _ = fetch(f"{base_url}/readyz")
    require_ok("readyz", status, payload)

    status, payload, set_cookie = fetch(
        f"{base_url}/auth/login",
        method="POST",
        data={"username": args.username, "password": args.password},
    )
    require_ok("auth/login", status, payload)
    cookie = (set_cookie or "").split(";", 1)[0]
    if not cookie:
        raise RuntimeError("auth/login no devolvio cookie de sesion")

    status, payload, _ = fetch(f"{base_url}/auth/me", cookie=cookie)
    require_ok("auth/me", status, payload)

    status, payload, _ = fetch(f"{base_url}/dashboard?company={args.company}", cookie=cookie)
    require_ok("dashboard", status, payload)

    status, payload, _ = fetch(f"{base_url}/feed?company={args.company}", cookie=cookie)
    require_ok("feed", status, payload)

    print("[done] smoke test completo")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"[fail] {exc}", file=sys.stderr)
        raise SystemExit(1)
