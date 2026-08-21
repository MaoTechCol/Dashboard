from __future__ import annotations

import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.database import Base
from app.models import ManagedCompany, PublishedDashboardSnapshot
from app.schemas import CompanyBrand, CompanyConfig
from app.services.company_registry import CompanyRegistry


def _company(slug: str) -> dict[str, object]:
    return CompanyConfig(
        slug=slug,
        name=slug.upper(),
        customer=slug.upper(),
        timezone="America/Bogota",
        fleet_ids=[f"fleet-{slug}"],
        brand=CompanyBrand(eyebrow="DMS", title="DMS", subtitle="DMS"),
    ).model_dump(mode="json")


class DurableCompanyRegistryTests(unittest.TestCase):
    def test_transition_seeds_missing_company_with_published_snapshot(self) -> None:
        with TemporaryDirectory() as temp_dir:
            config_path = Path(temp_dir) / "companies.json"
            config_path.write_text(
                json.dumps([_company("existing"), _company("legacy")]),
                encoding="utf-8",
            )
            engine = create_engine(
                "sqlite+pysqlite:///:memory:",
                connect_args={"check_same_thread": False},
                poolclass=StaticPool,
                future=True,
            )
            Base.metadata.create_all(engine)
            sessions = sessionmaker(bind=engine, autoflush=False, expire_on_commit=True, future=True)
            with sessions() as session:
                session.add(
                    ManagedCompany(
                        slug="existing",
                        config_json=json.dumps(_company("existing")),
                        is_active=True,
                    )
                )
                session.add(
                    PublishedDashboardSnapshot(
                        company_slug="legacy",
                        cut_status="published",
                        snapshot_json="{}",
                    )
                )
                session.commit()

            registry = CompanyRegistry(config_path, session_factory=sessions)

            self.assertEqual({company.slug for company in registry.all()}, {"existing", "legacy"})
            with sessions() as session:
                self.assertIsNotNone(session.get(ManagedCompany, "legacy"))


if __name__ == "__main__":
    unittest.main()
