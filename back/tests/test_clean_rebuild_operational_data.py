from scripts.clean_rebuild_operational_data import OPERATIONAL_TABLES, PRESERVED_TABLES


def test_clean_rebuild_never_purges_identity_configuration_or_reports() -> None:
    protected = {
        "managed_companies",
        "user_accounts",
        "report_assets",
        "company_lifecycle_audit",
        "system_settings",
        "alembic_version",
    }

    assert protected.issubset(PRESERVED_TABLES)
    assert protected.isdisjoint(OPERATIONAL_TABLES)


def test_clean_rebuild_clears_all_published_and_derived_layers() -> None:
    required = {
        "howen_alarm_raw",
        "alarm_events",
        "alarm_event_audit",
        "ingestion_anomalies",
        "mileage_observations",
        "mileage_readings",
        "daily_mileage_snapshots",
        "company_daily_aggregates",
        "company_window_aggregates",
        "published_dashboard_snapshots",
        "reconciliation_reviews",
        "background_jobs",
        "devices",
    }

    assert required.issubset(OPERATIONAL_TABLES)
    assert len(OPERATIONAL_TABLES) == len(set(OPERATIONAL_TABLES))
