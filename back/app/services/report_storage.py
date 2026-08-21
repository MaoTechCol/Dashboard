from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx


@dataclass(frozen=True)
class StoredReport:
    backend: str
    key: str
    file_path: str


class ReportStorage:
    def __init__(self, settings: Any) -> None:
        self.settings = settings
        requested = str(settings.report_storage_backend or "auto").strip().lower()
        has_supabase = bool(settings.supabase_url and settings.supabase_service_role_key)
        self.backend = "supabase" if requested in {"auto", "supabase"} and has_supabase else "local"
        if requested == "supabase" and not has_supabase:
            raise RuntimeError("Supabase report storage requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY")
        self._bucket_ready = False

    def store(self, *, company_slug: str, year: int, month: int, payload: bytes, content_type: str) -> StoredReport:
        key = f"{company_slug}/{year}/{month:02d}.pdf"
        if self.backend == "supabase":
            self._ensure_supabase_bucket()
            self._supabase_request("PUT", key, content=payload, content_type=content_type)
            return StoredReport(backend="supabase", key=key, file_path="")

        target = self.settings.upload_dir / company_slug / str(year) / f"{month:02d}.pdf"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return StoredReport(backend="local", key=key, file_path=str(target))

    def read(self, *, backend: str | None, key: str | None, file_path: str) -> bytes:
        if (backend or "local") == "supabase":
            if not key:
                raise FileNotFoundError("Report storage key is missing")
            return self._supabase_request("GET", key).content
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(str(path))
        return path.read_bytes()

    def delete(self, *, backend: str | None, key: str | None, file_path: str) -> None:
        if (backend or "local") == "supabase":
            if key:
                self._supabase_request("DELETE", key)
            return
        path = Path(file_path)
        if path.exists():
            path.unlink()

    @staticmethod
    def content_disposition(filename: str) -> str:
        return f"attachment; filename*=UTF-8''{quote(filename)}"

    def _supabase_request(
        self,
        method: str,
        key: str,
        *,
        content: bytes | None = None,
        content_type: str = "application/pdf",
    ) -> httpx.Response:
        base = str(self.settings.supabase_url).rstrip("/")
        bucket = quote(str(self.settings.supabase_reports_bucket), safe="")
        encoded_key = "/".join(quote(part, safe="") for part in key.split("/"))
        token = str(self.settings.supabase_service_role_key)
        response = httpx.request(
            method,
            f"{base}/storage/v1/object/{bucket}/{encoded_key}",
            headers={
                "Authorization": f"Bearer {token}",
                "apikey": token,
                "Content-Type": content_type,
                "x-upsert": "true",
            },
            content=content,
            timeout=30.0,
        )
        if response.status_code == 404:
            raise FileNotFoundError(key)
        response.raise_for_status()
        return response

    def _ensure_supabase_bucket(self) -> None:
        if self._bucket_ready:
            return
        base = str(self.settings.supabase_url).rstrip("/")
        bucket = str(self.settings.supabase_reports_bucket)
        token = str(self.settings.supabase_service_role_key)
        headers = {
            "Authorization": f"Bearer {token}",
            "apikey": token,
        }
        response = httpx.get(
            f"{base}/storage/v1/bucket/{quote(bucket, safe='')}",
            headers=headers,
            timeout=15.0,
        )
        if response.status_code == 404:
            response = httpx.post(
                f"{base}/storage/v1/bucket",
                headers={**headers, "Content-Type": "application/json"},
                json={"id": bucket, "name": bucket, "public": False},
                timeout=15.0,
            )
        response.raise_for_status()
        self._bucket_ready = True
