from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from typing import Any

from sqlalchemy import select

from app.core.catalog import CATEGORY_ORDER, DEFAULT_SUBTYPE_MAP
from app.schemas import CompanyConfig, QualityNoteView


class CompanyRegistry:
    def __init__(
        self,
        config_path: Path,
        *,
        seed_path: Path | None = None,
        session_factory: Any | None = None,
    ) -> None:
        self._config_path = config_path
        self._seed_path = seed_path
        self._session_factory = session_factory
        self._ensure_config_exists()
        self._seed_managed_companies_once()
        self._companies = self._load()

    def _ensure_config_exists(self) -> None:
        self._config_path.parent.mkdir(parents=True, exist_ok=True)
        if self._config_path.exists():
            return
        if self._seed_path and self._seed_path.exists():
            self._config_path.write_text(self._seed_path.read_text(encoding="utf-8"), encoding="utf-8")
            return
        self._config_path.write_text("[]\n", encoding="utf-8")

    def _load_payload(self) -> list[dict[str, Any]]:
        payload = json.loads(self._config_path.read_text(encoding="utf-8"))
        return [item for item in payload if isinstance(item, dict)]

    def _write_payload(self, payload: list[dict[str, Any]]) -> None:
        if self._session_factory:
            return
        payload.sort(
            key=lambda item: (
                item.get("slug") != "ismocol",
                str(item.get("name") or item.get("slug") or "").lower(),
            )
        )
        self._config_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=True) + "\n",
            encoding="utf-8",
        )

    def _seed_managed_companies_once(self) -> None:
        """Use JSON once as a seed; the database is authoritative afterwards."""
        if not self._session_factory:
            return
        from app.models import ManagedCompany, PublishedDashboardSnapshot, SystemSetting, UserAccount

        marker_key = "managed_companies_seeded_v2"
        with self._session_factory() as session:
            if session.get(SystemSetting, marker_key):
                return
            existing_slugs = set(session.scalars(select(ManagedCompany.slug)))
            seed_empty_registry = not existing_slugs
            for item in self._load_payload():
                slug = str(item.get("slug") or "").strip()
                if not slug or slug in existing_slugs:
                    continue
                has_active_user = session.scalar(
                    select(UserAccount.id).where(
                        UserAccount.company_slug == slug,
                        UserAccount.is_active.is_(True),
                    ).limit(1)
                )
                has_published_snapshot = session.scalar(
                    select(PublishedDashboardSnapshot.company_slug).where(
                        PublishedDashboardSnapshot.company_slug == slug,
                        PublishedDashboardSnapshot.snapshot_json.is_not(None),
                    ).limit(1)
                )
                if seed_empty_registry or has_active_user or has_published_snapshot:
                    session.add(
                        ManagedCompany(
                            slug=slug,
                            config_json=json.dumps(item, ensure_ascii=True),
                            is_active=True,
                        )
                    )
            session.add(SystemSetting(key=marker_key, value_json=json.dumps({"seeded": True})))
            session.commit()

    def _mutable_payload(self) -> list[dict[str, Any]]:
        if not self._session_factory:
            return self._load_payload()
        return [company.model_dump(mode="json") for company in self._companies.values()]

    def _load_managed_payload(self) -> list[dict[str, Any]]:
        if not self._session_factory:
            return []
        from app.models import ManagedCompany

        with self._session_factory() as session:
            rows = list(
                session.scalars(
                    select(ManagedCompany)
                    .where(ManagedCompany.is_active.is_(True))
                    .order_by(ManagedCompany.slug.asc())
                )
            )
        payload: list[dict[str, Any]] = []
        for row in rows:
            try:
                item = json.loads(row.config_json)
            except json.JSONDecodeError:
                continue
            if isinstance(item, dict):
                payload.append(item)
        return payload

    def _persist_managed_company(self, item: dict[str, Any]) -> None:
        if not self._session_factory:
            return
        from app.models import ManagedCompany

        slug = str(item.get("slug") or "").strip()
        if not slug:
            return
        with self._session_factory() as session:
            row = session.get(ManagedCompany, slug) or ManagedCompany(slug=slug)
            row.config_json = json.dumps(item, ensure_ascii=True)
            row.is_active = True
            session.add(row)
            session.commit()

    def _delete_managed_company(self, slug: str) -> None:
        if not self._session_factory:
            return
        from app.models import ManagedCompany

        with self._session_factory() as session:
            row = session.get(ManagedCompany, slug)
            if not row:
                return
            session.delete(row)
            session.commit()

    def _restore_company_from_operational_rows(self, slug: str) -> CompanyConfig | None:
        if not self._session_factory:
            return None
        from app.models import AlarmEvent, DeviceRecord, HowenAlarmRaw

        normalized_slug = _normalize_company_slug(slug)
        if not normalized_slug:
            return None

        discovered_name: str | None = None
        fleet_ids: set[str] = set()
        device_ids: set[str] = set()

        with self._session_factory() as session:
            device_rows = list(
                session.scalars(
                    select(DeviceRecord)
                    .where(DeviceRecord.company_slug == normalized_slug)
                    .order_by(DeviceRecord.last_received_at.desc(), DeviceRecord.last_seen_at.desc())
                    .limit(25)
                )
            )
            for row in device_rows:
                if row.fleet_name and not discovered_name:
                    discovered_name = row.fleet_name.strip()
                if row.fleet_id:
                    fleet_ids.add(row.fleet_id)
                if row.device_id:
                    device_ids.add(row.device_id)

            if not fleet_ids and not device_ids:
                raw_rows = list(
                    session.scalars(
                        select(HowenAlarmRaw)
                        .where(HowenAlarmRaw.company_slug == normalized_slug)
                        .order_by(HowenAlarmRaw.received_at.desc(), HowenAlarmRaw.occurred_at.desc())
                        .limit(50)
                    )
                )
                for row in raw_rows:
                    if row.fleet_id:
                        fleet_ids.add(row.fleet_id)
                    if row.device_id:
                        device_ids.add(row.device_id)

            if not fleet_ids and not device_ids:
                alarm_rows = list(
                    session.scalars(
                        select(AlarmEvent)
                        .where(AlarmEvent.company_slug == normalized_slug)
                        .order_by(AlarmEvent.received_at.desc(), AlarmEvent.occurred_at.desc())
                        .limit(50)
                    )
                )
                for row in alarm_rows:
                    if row.fleet_id:
                        fleet_ids.add(row.fleet_id)
                    if row.device_id:
                        device_ids.add(row.device_id)

        if not fleet_ids and not device_ids:
            return None

        restored_name = discovered_name or normalized_slug.replace("-", " ").strip().title() or normalized_slug
        restored_item = {
            "slug": normalized_slug,
            "name": restored_name,
            "customer": restored_name,
            "timezone": "America/Bogota",
            "subdomain": None,
            "fleet_ids": sorted(fleet_ids),
            "device_ids": sorted(device_ids),
            "allowed_categories": list(CATEGORY_ORDER),
            "subtype_map": {},
            "plate_aliases": {},
            "notes": "Restaurada automaticamente desde datos operativos locales.",
            "quality_notes": [],
            "brand": _default_company_brand(restored_name),
        }
        payload = self._mutable_payload()
        payload = [item for item in payload if item.get("slug") != normalized_slug]
        payload.append(restored_item)
        self._write_payload(payload)
        self._persist_managed_company(restored_item)
        self.reload()
        return self._companies.get(normalized_slug)

    def _load(self) -> dict[str, CompanyConfig]:
        merged: dict[str, CompanyConfig] = {}
        source = self._load_managed_payload() if self._session_factory else self._load_payload()
        for item in source:
            company = CompanyConfig.model_validate(item)
            merged[company.slug] = company
        return merged

    def all(self) -> list[CompanyConfig]:
        return list(self._companies.values())

    def reload(self) -> None:
        self._companies = self._load()

    def get(self, slug: str) -> CompanyConfig:
        normalized_slug = _normalize_company_slug(slug)
        try:
            return self._companies[normalized_slug]
        except KeyError as exc:
            raise KeyError(f"Unknown company slug: {normalized_slug}") from exc

    def update_assignment(self, *, slug: str, fleet_ids: list[str], device_ids: list[str]) -> CompanyConfig:
        payload = self._mutable_payload()
        updated = False
        persisted_item: dict[str, Any] | None = None
        for item in payload:
            if item.get("slug") != slug:
                continue
            item["fleet_ids"] = sorted({value.strip() for value in fleet_ids if value and value.strip()})
            item["device_ids"] = sorted({value.strip() for value in device_ids if value and value.strip()})
            updated = True
            persisted_item = item
            break
        if not updated:
            raise KeyError(f"Unknown company slug: {slug}")
        self._write_payload(payload)
        if persisted_item is not None:
            self._persist_managed_company(persisted_item)
        self.reload()
        return self.get(slug)

    def deactivate_company(self, *, slug: str) -> CompanyConfig:
        payload = self._mutable_payload()
        updated = False
        persisted_item: dict[str, Any] | None = None
        for item in payload:
            if item.get("slug") != slug:
                continue
            item["fleet_ids"] = []
            item["device_ids"] = []
            updated = True
            persisted_item = item
            break
        if not updated:
            raise KeyError(f"Unknown company slug: {slug}")
        self._write_payload(payload)
        if persisted_item is not None:
            self._persist_managed_company(persisted_item)
        self.reload()
        return self.get(slug)

    def delete_company(self, *, slug: str) -> None:
        payload = self._mutable_payload()
        filtered = [item for item in payload if item.get("slug") != slug]
        if len(filtered) == len(payload):
            raise KeyError(f"Unknown company slug: {slug}")
        self._write_payload(filtered)
        self._delete_managed_company(slug)
        self.reload()

    def upsert_company(
        self,
        *,
        slug: str,
        name: str,
        customer: str | None,
        timezone: str,
        subdomain: str | None,
        fleet_ids: list[str],
        device_ids: list[str],
        notes: str | None,
    ) -> CompanyConfig:
        normalized_slug = _normalize_company_slug(slug)
        normalized_name = " ".join((name or "").strip().split())
        normalized_customer = " ".join((customer or normalized_name).strip().split())
        normalized_timezone = (timezone or "").strip() or "America/Bogota"
        normalized_subdomain = (subdomain or "").strip() or None
        normalized_notes = (notes or "").strip() or None
        normalized_fleet_ids = _normalize_identity_list(fleet_ids)
        normalized_device_ids = _normalize_identity_list(device_ids)

        if not normalized_slug:
            raise ValueError("Debes indicar un slug valido para la empresa")
        if not normalized_name:
            raise ValueError("Debes indicar el nombre visible de la empresa")
        if not normalized_fleet_ids and not normalized_device_ids:
            raise ValueError("Debes indicar al menos un fleet_id o device_id para activar la empresa")

        payload = self._mutable_payload()
        updated = False
        persisted_item: dict[str, Any] | None = None
        for item in payload:
            if item.get("slug") != normalized_slug:
                continue
            item["name"] = normalized_name
            item["customer"] = normalized_customer
            item["timezone"] = normalized_timezone
            item["subdomain"] = normalized_subdomain
            item["fleet_ids"] = normalized_fleet_ids
            item["device_ids"] = normalized_device_ids
            item["notes"] = normalized_notes
            item.setdefault("allowed_categories", list(CATEGORY_ORDER))
            item.setdefault("subtype_map", {})
            item.setdefault("plate_aliases", {})
            item.setdefault("quality_notes", [])
            item.setdefault("brand", _default_company_brand(normalized_name))
            updated = True
            persisted_item = item
            break

        if not updated:
            persisted_item = {
                "slug": normalized_slug,
                "name": normalized_name,
                "customer": normalized_customer,
                "timezone": normalized_timezone,
                "subdomain": normalized_subdomain,
                "fleet_ids": normalized_fleet_ids,
                "device_ids": normalized_device_ids,
                "allowed_categories": list(CATEGORY_ORDER),
                "subtype_map": {},
                "plate_aliases": {},
                "notes": normalized_notes,
                "quality_notes": [],
                "brand": _default_company_brand(normalized_name),
            }
            payload.append(persisted_item)

        self._write_payload(payload)
        if persisted_item is not None:
            self._persist_managed_company(persisted_item)
        self.reload()
        return self.get(normalized_slug)

    def subtype_map(self) -> dict[str, str]:
        merged = dict(DEFAULT_SUBTYPE_MAP)
        for company in self._companies.values():
            merged.update(company.subtype_map)
        return merged

    def normalize_plate(self, company: CompanyConfig, plate_no: str | None) -> str | None:
        normalized = normalize_plate_label(plate_no)
        if not normalized:
            return None
        alias_map = {normalize_plate_label(alias): normalize_plate_label(target) for alias, target in company.plate_aliases.items()}
        return alias_map.get(normalized, normalized)

    def normalize_plate_any(self, plate_no: str | None) -> str | None:
        return normalize_plate_label(plate_no)

    def canonical_plate(self, device_id: str | None, *candidates: str | None) -> str | None:
        """Prefer a real Colombian plate over provider identifiers or stale labels."""
        normalized_device_id = normalize_plate_label(device_id)
        normalized_candidates = [
            normalized
            for value in candidates
            if (normalized := normalize_plate_label(value))
        ]
        for candidate in normalized_candidates:
            if is_colombian_plate_label(candidate):
                return candidate
        for candidate in normalized_candidates:
            if candidate != normalized_device_id and not candidate.isdigit():
                return candidate
        return normalized_device_id or (normalized_candidates[0] if normalized_candidates else None)

    def plate_alias_applied(self, company: CompanyConfig, plate_no: str | None) -> tuple[str | None, bool]:
        normalized = normalize_plate_label(plate_no)
        if not normalized:
            return None, False
        canonical = self.normalize_plate(company, plate_no)
        return canonical, canonical is not None and canonical != normalized

    def plates_match(self, company: CompanyConfig, left: str | None, right: str | None) -> bool:
        normalized_left = self.normalize_plate(company, left)
        normalized_right = self.normalize_plate(company, right)
        if normalized_left and normalized_right:
            return normalized_left == normalized_right
        return False

    def resolve_company(self, *, device_id: str | None = None, fleet_id: str | None = None, slug: str | None = None) -> CompanyConfig | None:
        if slug:
            return self._companies.get(slug)
        for company in self._companies.values():
            if self.device_belongs(company, device_id, fleet_id):
                return company
        return None

    def timezone_for(self, *, device_id: str | None = None, fleet_id: str | None = None, slug: str | None = None, fallback: str) -> str:
        company = self.resolve_company(device_id=device_id, fleet_id=fleet_id, slug=slug)
        return company.timezone if company else fallback

    def active_quality_notes(self, company: CompanyConfig, *, range_start: date, range_end: date) -> list[QualityNoteView]:
        notes: list[QualityNoteView] = []
        for note in company.quality_notes:
            end_date = note.end_date or range_end
            if note.start_date <= range_end and end_date >= range_start:
                notes.append(
                    QualityNoteView(
                        title=note.title,
                        message=note.message,
                        severity=note.severity,
                        start_date=note.start_date,
                        end_date=note.end_date,
                    )
                )
        return notes

    @staticmethod
    def is_operational(company: CompanyConfig) -> bool:
        return bool(company.device_ids or company.fleet_ids)

    @staticmethod
    def device_belongs(company: CompanyConfig, device_id: str | None, fleet_id: str | None) -> bool:
        if company.device_ids and device_id:
            return device_id in set(company.device_ids)
        if company.fleet_ids and fleet_id:
            return fleet_id in set(company.fleet_ids)
        if company.device_ids or company.fleet_ids:
            return False
        return True

    @staticmethod
    def category_allowed(company: CompanyConfig, category: str) -> bool:
        if not company.allowed_categories:
            return True
        return category in set(company.allowed_categories)


def normalize_plate_label(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = "".join(char for char in value.upper().strip() if char.isalnum())
    if not cleaned:
        return None
    if len(cleaned) == 6:
        if cleaned[:3].isalpha() and cleaned[3:].isdigit():
            return cleaned
        if cleaned[3:].isdigit():
            candidate_prefix = cleaned[:3].replace("0", "O")
            candidate = f"{candidate_prefix}{cleaned[3:]}"
            if candidate[:3].isalpha() and candidate[3:].isdigit():
                return candidate
    return cleaned


def is_colombian_plate_label(value: str | None) -> bool:
    normalized = normalize_plate_label(value)
    return bool(
        normalized
        and len(normalized) == 6
        and normalized[:3].isalpha()
        and normalized[3:].isdigit()
    )


def _normalize_plate_key(value: str | None) -> str | None:
    return normalize_plate_label(value)


def _normalize_identity_list(values: list[str]) -> list[str]:
    return sorted({value.strip() for value in values if value and value.strip()})


def _normalize_company_slug(value: str | None) -> str:
    raw = (value or "").strip().lower()
    slug = []
    last_was_dash = False
    for char in raw:
        if char.isalnum():
            slug.append(char)
            last_was_dash = False
            continue
        if char in {" ", "-", "_"} and not last_was_dash:
            slug.append("-")
            last_was_dash = True
    return "".join(slug).strip("-")


def _default_company_brand(name: str) -> dict[str, str]:
    return {
        "eyebrow": "Monitoreo de Conduccion · DMS",
        "title": f"{name} — Panel de Seguridad de Flota",
        "subtitle": "Captura Howen VSS · Dashboard multiempresa local",
        "accent": "#10b981",
        "warning": "#f97316",
        "danger": "#ef4444",
        "muted": "#8a90a8",
    }
