from __future__ import annotations

import json
from datetime import date
from pathlib import Path

from app.core.catalog import DEFAULT_SUBTYPE_MAP
from app.schemas import CompanyConfig, QualityNoteView


class CompanyRegistry:
    def __init__(self, config_path: Path) -> None:
        self._config_path = config_path
        self._companies = self._load()

    def _load(self) -> dict[str, CompanyConfig]:
        payload = json.loads(self._config_path.read_text(encoding="utf-8"))
        companies = [CompanyConfig.model_validate(item) for item in payload]
        return {company.slug: company for company in companies}

    def all(self) -> list[CompanyConfig]:
        return list(self._companies.values())

    def reload(self) -> None:
        self._companies = self._load()

    def get(self, slug: str) -> CompanyConfig:
        try:
            return self._companies[slug]
        except KeyError as exc:
            raise KeyError(f"Unknown company slug: {slug}") from exc

    def update_assignment(self, *, slug: str, fleet_ids: list[str], device_ids: list[str]) -> CompanyConfig:
        payload = json.loads(self._config_path.read_text(encoding="utf-8"))
        updated = False
        for item in payload:
            if item.get("slug") != slug:
                continue
            item["fleet_ids"] = sorted({value.strip() for value in fleet_ids if value and value.strip()})
            item["device_ids"] = sorted({value.strip() for value in device_ids if value and value.strip()})
            updated = True
            break
        if not updated:
            raise KeyError(f"Unknown company slug: {slug}")
        self._config_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
        self.reload()
        return self.get(slug)

    def subtype_map(self) -> dict[str, str]:
        merged = dict(DEFAULT_SUBTYPE_MAP)
        for company in self._companies.values():
            merged.update(company.subtype_map)
        return merged

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
