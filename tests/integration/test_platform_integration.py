"""Container-backed acceptance path for the complete ForgeFlow incident lifecycle."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import psycopg
import pytest
from psycopg import sql

from forgeflow.config import Settings
from forgeflow.errors import WarehouseError
from forgeflow.models import (
    FailureScenario,
    QuarantinedRecord,
    QuarantineReason,
    RunStatus,
    RunSummary,
)
from forgeflow.object_store import S3ObjectStore
from forgeflow.pipeline import PipelineRunner
from forgeflow.service import ForgeFlowService
from forgeflow.warehouse import PostgresRepository

pytestmark = pytest.mark.integration


def test_healthy_idempotent_incident_and_recovery(tmp_path: Path) -> None:
    """Exercise real MinIO, PostgreSQL, dbt, evidence, impact, and safe recovery."""
    base_settings = Settings()
    settings = Settings(
        database_url=_isolated_test_database_url(base_settings),
        generated_days=3,
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
    )
    repository = PostgresRepository(settings)
    repository.initialize()
    repository.clean_demo_state(confirmed=True)
    object_store = S3ObjectStore(settings)
    runner = PipelineRunner(settings, repository, object_store)
    batch_date = datetime.now(UTC).date() - timedelta(days=2)

    baseline = runner.run_generated(FailureScenario.CLEAN, batch_date=batch_date)
    assert baseline.summary.status == RunStatus.HEALTHY
    assert baseline.summary.accepted_row_count > 0
    assert baseline.summary.quarantined_row_count == 0
    assert baseline.summary.model_row_counts
    assert all(count >= 0 for count in baseline.summary.model_row_counts.values())
    baseline_stages = repository.list_run_stages(baseline.summary.run_id)
    assert {stage["stage_name"] for stage in baseline_stages} == {
        "source_generation",
        "raw_landing",
        "contract_validation",
        "warehouse_load",
        "dbt_build_and_freshness",
        "observability_finalization",
        "incident_linkage",
    }
    assert all(stage["status"] == "succeeded" for stage in baseline_stages)

    replay = runner.run_generated(FailureScenario.CLEAN, batch_date=batch_date)
    assert replay.summary.status == RunStatus.HEALTHY
    assert replay.summary.skipped_file_count == 10
    assert replay.summary.accepted_row_count == 0

    incident = runner.run_generated(
        FailureScenario.INCIDENT,
        batch_date=batch_date,
        incremental=True,
    )
    assert incident.summary.status == RunStatus.FAILED
    assert incident.summary.quarantined_row_count > 0
    assert incident.summary.failed_checks > 0
    assert incident.summary.freshness_status == "warning"
    assert incident.summary.affected_downstream_models
    assert incident.incident_id is not None

    service = ForgeFlowService(settings, repository, object_store=object_store)
    assert service.get_freshness()
    assert service.get_factory_performance()
    failed = service.list_failed_checks(run_id=incident.summary.run_id, limit=100)
    quarantine = service.list_quarantined_records(run_id=incident.summary.run_id, limit=100)
    handoff = service.generate_engineering_handoff(incident.incident_id)
    assert failed.items
    assert quarantine.items
    assert all("raw_payload" not in row for row in quarantine.items)
    assert handoff["observed_facts"]
    assert handoff["hypotheses"]

    recovery = runner.run_generated(
        FailureScenario.RECOVERY,
        batch_date=batch_date,
        incremental=True,
        recovery_incident_id=incident.incident_id,
    )
    assert recovery.summary.status == RunStatus.HEALTHY
    assert recovery.incident_id == incident.incident_id
    retained = service.get_incident(incident.incident_id)
    assert retained is not None
    assert retained["status"] == "resolved"
    assert retained["failed_run_id"] == str(incident.summary.run_id)
    assert retained["recovery_run_id"] == str(recovery.summary.run_id)


def test_atomic_source_commit_rolls_back_real_postgresql_before_retry() -> None:
    """Prove late file-completion failure rolls back raw and quarantine writes together."""
    base_settings = Settings()
    settings = Settings(database_url=_isolated_test_database_url(base_settings))
    repository = PostgresRepository(settings)
    repository.initialize()
    repository.clean_demo_state(confirmed=True)
    summary = RunSummary(batch_id="atomic-integration", scenario=FailureScenario.CLEAN)
    repository.start_run(summary)
    checksum = "f" * 64
    registration = repository.register_source_file(
        run_id=summary.run_id,
        batch_id=summary.batch_id,
        source_name="factories",
        logical_key=f"{summary.batch_id}/factories.csv",
        object_key=f"incoming/factories/{summary.batch_id}/{checksum}-factories.csv",
        checksum=checksum,
        schema_fingerprint="e" * 64,
        size_bytes=128,
        row_count=2,
    )
    repository.complete_source_file(
        registration.file_id,
        accepted_count=0,
        quarantined_count=0,
        status="failed",
    )
    accepted = {
        "factory_id": "FAC-ATOMIC-001",
        "factory_name": "Atomic Test Factory",
        "country_code": "HU",
        "timezone": "Europe/Budapest",
        "opened_on": date(2020, 1, 2),
        "status": "active",
        "updated_at": datetime(2025, 7, 10, 8, tzinfo=UTC),
        "_source_row_number": 2,
    }
    quarantined = QuarantinedRecord(
        source_name="factories",
        source_row_number=3,
        raw_payload={"factory_id": None},
        reasons=[
            QuarantineReason(
                code="required_value_missing",
                column="factory_id",
                check="not_nullable",
                message="factory_id is required",
            )
        ],
    )

    with pytest.raises(WarehouseError, match="atomic completion"):
        repository.commit_source_result(
            "factories",
            [accepted],
            run_id=summary.run_id,
            batch_id=summary.batch_id,
            file_id=registration.file_id,
            ingested_at=datetime.now(UTC),
            quarantined_records=[quarantined],
            schema_changes=[],
        )

    assert _atomic_test_row_counts(settings, registration.file_id) == (0, 0)

    retry = repository.register_source_file(
        run_id=summary.run_id,
        batch_id=summary.batch_id,
        source_name="factories",
        logical_key=f"{summary.batch_id}/factories.csv",
        object_key=f"incoming/factories/{summary.batch_id}/{checksum}-factories.csv",
        checksum=checksum,
        schema_fingerprint="e" * 64,
        size_bytes=128,
        row_count=2,
    )
    assert retry.file_id == registration.file_id
    assert repository.commit_source_result(
        "factories",
        [accepted],
        run_id=summary.run_id,
        batch_id=summary.batch_id,
        file_id=retry.file_id,
        ingested_at=datetime.now(UTC),
        quarantined_records=[quarantined],
        schema_changes=[],
    ) == (1, 1)
    assert _atomic_test_row_counts(settings, retry.file_id) == (1, 1)
    repository.clean_demo_state(confirmed=True)


def _atomic_test_row_counts(settings: Settings, file_id: UUID) -> tuple[int, int]:
    with psycopg.connect(settings.database_url.get_secret_value()) as connection:
        raw_count = connection.execute(
            "SELECT count(*) FROM raw.factories WHERE factory_id = %s",
            ("FAC-ATOMIC-001",),
        ).fetchone()
        quarantine_count = connection.execute(
            "SELECT count(*) FROM quarantine.records WHERE file_id = %s",
            (file_id,),
        ).fetchone()
    assert raw_count is not None
    assert quarantine_count is not None
    return int(raw_count[0]), int(quarantine_count[0])


def _isolated_test_database_url(settings: Settings) -> str:
    """Create/use only the dedicated loopback integration database."""
    base_url = settings.database_url.get_secret_value()
    parsed = urlsplit(base_url)
    database_name = parsed.path.removeprefix("/")
    if parsed.hostname not in {"127.0.0.1", "localhost", "::1"} or database_name not in {
        "forgeflow",
        "forgeflow_test",
    }:
        pytest.fail("Integration tests require the local forgeflow demo PostgreSQL instance")

    test_url = urlunsplit(parsed._replace(path="/forgeflow_test"))
    admin_url = urlunsplit(parsed._replace(path="/postgres"))
    with (
        psycopg.connect(admin_url, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", ("forgeflow_test",))
        if cursor.fetchone() is None:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier("forgeflow_test")))
    return test_url
