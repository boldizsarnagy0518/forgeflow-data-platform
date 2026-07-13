"""Unit coverage for pipeline reliability, evidence, and replay semantics."""

from __future__ import annotations

import csv
import io
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

import forgeflow.pipeline as pipeline_module
from forgeflow.config import Settings
from forgeflow.contracts import SOURCE_CONTRACTS
from forgeflow.dbt_runner import DbtRunner, DbtRunResult
from forgeflow.errors import ContractError, ForgeFlowError, ObjectStoreError, WarehouseError
from forgeflow.models import (
    ContractResult,
    FailureScenario,
    QualityResult,
    QuarantinedRecord,
    RunStatus,
    RunSummary,
    SchemaChange,
)
from forgeflow.object_store import LandedObject, ObjectStore, schema_fingerprint, sha256_bytes
from forgeflow.pipeline import (
    PipelineRunner,
    contract_quality_results,
    serialize_source_csv,
    volume_anomaly_result,
)
from forgeflow.synthetic import SyntheticDataGenerator
from forgeflow.warehouse import FileRegistration, PostgresRepository

BATCH_DATE = date(2025, 7, 10)


class FakeObjectStore:
    """Record raw-landing calls while behaving like a content-addressed store."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.objects: dict[str, bytes] = {}
        self.failure: ObjectStoreError | None = None

    def ensure_bucket(self) -> None:
        self.events.append("ensure_bucket")

    def put_bytes(
        self,
        *,
        content: bytes,
        source_name: str,
        batch_id: str,
        filename: str,
    ) -> LandedObject:
        self.events.append(f"land:{source_name}")
        if self.failure is not None:
            raise self.failure
        checksum = sha256_bytes(content)
        key = f"incoming/{source_name}/{batch_id}/{checksum[:12]}-{filename}"
        self.objects[key] = content
        return LandedObject("raw", key, checksum, len(content))

    def get_bytes(self, object_key: str) -> bytes:
        return self.objects[object_key]

    def ping(self) -> bool:
        return True


class FakeRepository:
    """In-memory warehouse boundary that retains every pipeline side effect."""

    def __init__(self, events: list[str] | None = None) -> None:
        self.events = events if events is not None else []
        self.duplicate_sources: set[str] = set()
        self.changed_sources: set[str] = set()
        self.started: list[RunSummary] = []
        self.finished: list[RunSummary] = []
        self.finish_attempts = 0
        self.finish_error: WarehouseError | None = None
        self.registrations: list[dict[str, Any]] = []
        self.loaded: dict[str, list[dict[str, Any]]] = {}
        self.quarantined: list[QuarantinedRecord] = []
        self.schema_changes: list[SchemaChange] = []
        self.completed_files: list[tuple[UUID, int, int, str]] = []
        self.quality_results: list[QualityResult] = []
        self.open_incident: dict[str, Any] | None = None
        self.resolutions: list[tuple[UUID, UUID]] = []
        self.incidents: dict[UUID, dict[str, Any]] = {}
        self.volume_history: dict[str, list[int]] = {}
        self.stages: list[tuple[str, str]] = []

    def initialize(self, script_path: Path | None = None) -> None:
        del script_path
        self.events.append("repository_initialized")

    def start_run(self, summary: RunSummary) -> None:
        self.events.append("run_started")
        self.started.append(summary.model_copy(deep=True))

    def finish_run(self, summary: RunSummary, human_summary: Mapping[str, Any]) -> None:
        del human_summary
        self.events.append("run_finished")
        self.finish_attempts += 1
        if self.finish_error is not None:
            raise self.finish_error
        self.finished.append(summary.model_copy(deep=True))

    def start_stage(
        self,
        run_id: UUID,
        stage_name: str,
        *,
        input_row_count: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        del run_id, input_row_count, metadata
        self.stages.append((stage_name, "running"))

    def finish_stage(
        self,
        run_id: UUID,
        stage_name: str,
        *,
        status: str,
        output_row_count: int | None = None,
        error_message: str | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        del run_id, output_row_count, error_message, metadata
        self.stages.append((stage_name, status))

    def register_source_file(
        self,
        *,
        run_id: UUID,
        batch_id: str,
        source_name: str,
        logical_key: str,
        object_key: str,
        checksum: str,
        schema_fingerprint: str,
        size_bytes: int,
        row_count: int,
    ) -> FileRegistration:
        self.events.append(f"register:{source_name}")
        self.registrations.append(
            {
                "run_id": run_id,
                "batch_id": batch_id,
                "source_name": source_name,
                "logical_key": logical_key,
                "object_key": object_key,
                "checksum": checksum,
                "schema_fingerprint": schema_fingerprint,
                "size_bytes": size_bytes,
                "row_count": row_count,
            }
        )
        return FileRegistration(
            file_id=uuid4(),
            duplicate=source_name in self.duplicate_sources,
            changed_logical_file=source_name in self.changed_sources,
        )

    def load_records(
        self,
        source_name: str,
        records: Sequence[Mapping[str, Any]],
        *,
        batch_id: str,
        file_id: UUID,
        ingested_at: datetime,
    ) -> int:
        del batch_id, file_id, ingested_at
        copied = [dict(record) for record in records]
        self.loaded[source_name] = copied
        return len(copied)

    def quarantine_records(
        self,
        *,
        run_id: UUID,
        file_id: UUID,
        records: Sequence[QuarantinedRecord],
    ) -> int:
        del run_id, file_id
        self.quarantined.extend(record.model_copy(deep=True) for record in records)
        return len(records)

    def commit_source_result(
        self,
        source_name: str,
        accepted_records: Sequence[Mapping[str, Any]],
        *,
        run_id: UUID,
        batch_id: str,
        file_id: UUID,
        ingested_at: datetime,
        quarantined_records: Sequence[QuarantinedRecord],
        schema_changes: Sequence[SchemaChange],
    ) -> tuple[int, int]:
        loaded_count = self.load_records(
            source_name,
            accepted_records,
            batch_id=batch_id,
            file_id=file_id,
            ingested_at=ingested_at,
        )
        quarantined_count = self.quarantine_records(
            run_id=run_id,
            file_id=file_id,
            records=quarantined_records,
        )
        self.record_schema_changes(run_id=run_id, file_id=file_id, changes=schema_changes)
        has_breaking_schema = any(change.change_type == "breaking" for change in schema_changes)
        self.complete_source_file(
            file_id,
            accepted_count=loaded_count,
            quarantined_count=quarantined_count,
            status=(
                "quarantined"
                if loaded_count == 0 and (quarantined_count > 0 or has_breaking_schema)
                else "loaded"
            ),
        )
        return loaded_count, quarantined_count

    def record_schema_changes(
        self,
        *,
        run_id: UUID,
        file_id: UUID,
        changes: Sequence[SchemaChange],
    ) -> None:
        del run_id, file_id
        self.schema_changes.extend(change.model_copy(deep=True) for change in changes)

    def complete_source_file(
        self,
        file_id: UUID,
        *,
        accepted_count: int,
        quarantined_count: int,
        status: str,
    ) -> None:
        self.completed_files.append((file_id, accepted_count, quarantined_count, status))

    def record_quality_results(self, results: Sequence[QualityResult]) -> None:
        self.quality_results.extend(result.model_copy(deep=True) for result in results)

    def source_volume_history(
        self,
        source_name: str,
        *,
        before: datetime,
        batch_kind: str,
        limit: int = 7,
    ) -> list[int]:
        del before, batch_kind
        return self.volume_history.get(source_name, [])[:limit]

    def get_run(self, run_id: UUID) -> dict[str, Any] | None:
        for summary in reversed(self.finished):
            if summary.run_id == run_id:
                return summary.model_dump(mode="python")
        return None

    def latest_healthy_run_before(self, started_at: datetime) -> dict[str, Any] | None:
        del started_at
        return None

    def list_failed_checks(
        self,
        *,
        run_id: UUID | None,
        limit: int,
        offset: int,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [
            result.model_dump(mode="json")
            for result in self.quality_results
            if (run_id is None or result.run_id == run_id)
            and result.status in {"failed", "warning"}
        ]
        return rows[offset : offset + limit], len(rows)

    def quarantine_summary(self, run_id: UUID) -> list[dict[str, Any]]:
        del run_id
        counts: Counter[tuple[str, str]] = Counter(
            (record.source_name, reason.code)
            for record in self.quarantined
            for reason in record.reasons
        )
        return [
            {"source_name": source_name, "reason_code": reason_code, "count": count}
            for (source_name, reason_code), count in sorted(counts.items())
        ]

    def create_incident(
        self,
        *,
        incident_id: UUID,
        failed_run_id: UUID,
        baseline_run_id: UUID | None,
        title: str,
        evidence: Mapping[str, Any],
        explanation: Mapping[str, Any],
    ) -> UUID:
        self.incidents[incident_id] = {
            "incident_id": incident_id,
            "failed_run_id": failed_run_id,
            "baseline_run_id": baseline_run_id,
            "title": title,
            "status": "open",
            "evidence": dict(evidence),
            "explanation": dict(explanation),
        }
        return incident_id

    def update_incident_explanation(
        self,
        incident_id: UUID,
        explanation: Mapping[str, Any],
    ) -> None:
        self.incidents[incident_id]["explanation"] = dict(explanation)

    def latest_open_incident(self) -> dict[str, Any] | None:
        return self.open_incident

    def get_incident(self, incident_id: UUID) -> dict[str, Any] | None:
        if self.open_incident and self.open_incident.get("incident_id") == incident_id:
            return self.open_incident
        return self.incidents.get(incident_id)

    def resolve_incident(self, incident_id: UUID, recovery_run_id: UUID) -> None:
        self.resolutions.append((incident_id, recovery_run_id))


class SuccessfulDbtRunner:
    """A deterministic dbt boundary for pipeline-only tests."""

    def build(
        self,
        run_id: UUID,
        batch_id: str,
        *,
        variables: Mapping[str, str] | None = None,
    ) -> DbtRunResult:
        del run_id, batch_id, variables
        return DbtRunResult(succeeded=True, return_code=0)


def _settings(tmp_path: Path) -> Settings:
    return Settings(data_dir=tmp_path / "data", artifact_dir=tmp_path / "artifacts")


def _runner(
    tmp_path: Path,
    repository: FakeRepository,
    object_store: FakeObjectStore,
) -> PipelineRunner:
    return PipelineRunner(
        _settings(tmp_path),
        cast("PostgresRepository", repository),
        cast("ObjectStore", object_store),
        dbt_runner=cast("DbtRunner", SuccessfulDbtRunner()),
    )


def _clean_factories() -> list[dict[str, Any]]:
    dataset = SyntheticDataGenerator(seed=17, generated_days=2).generate_baseline(
        batch_date=BATCH_DATE
    )
    return dataset["factories"]


def test_all_source_bytes_are_landed_and_registered_before_contract_validation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    events: list[str] = []
    repository = FakeRepository(events)
    object_store = FakeObjectStore(events)
    dataset = {
        "factories": _clean_factories(),
        "machines": [
            {
                "machine_id": "MCH-001",
                "production_line_id": "LINE-001",
                "machine_name": "Machine 001",
            }
        ],
    }

    def fail_validation(
        raw_dataset: Mapping[str, Sequence[Mapping[str, Any]]],
    ) -> dict[str, ContractResult]:
        assert raw_dataset is dataset
        events.append("validate")
        raise ContractError("deliberate contract failure")

    monkeypatch.setattr(pipeline_module, "validate_dataset", fail_validation)

    with pytest.raises(ContractError, match="deliberate contract failure"):
        _runner(tmp_path, repository, object_store).run_dataset(
            dataset,
            batch_id="land-first",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert events.index("land:factories") < events.index("validate")
    assert events.index("register:factories") < events.index("validate")
    assert events.index("land:machines") < events.index("validate")
    assert events.index("register:machines") < events.index("validate")
    assert repository.finished[-1].status is RunStatus.FAILED
    assert len(repository.completed_files) == 2
    assert {status for _, _, _, status in repository.completed_files} == {"failed"}


def test_partial_raw_landing_failure_marks_prior_registrations_failed(tmp_path: Path) -> None:
    class FailSecondObjectStore(FakeObjectStore):
        def put_bytes(
            self,
            *,
            content: bytes,
            source_name: str,
            batch_id: str,
            filename: str,
        ) -> LandedObject:
            if source_name == "machines":
                raise ObjectStoreError("second landing failed")
            return super().put_bytes(
                content=content,
                source_name=source_name,
                batch_id=batch_id,
                filename=filename,
            )

    repository = FakeRepository()
    dataset = {
        "factories": _clean_factories(),
        "machines": [{"machine_id": "MCH-001"}],
    }

    with pytest.raises(ObjectStoreError, match="second landing failed"):
        _runner(tmp_path, repository, FailSecondObjectStore()).run_dataset(
            dataset,
            batch_id="partial-landing",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert len(repository.registrations) == 1
    assert len(repository.completed_files) == 1
    assert repository.completed_files[0][1:] == (0, 0, "failed")


def test_duplicate_checksum_is_skipped_without_loading_or_quarantining(
    tmp_path: Path,
) -> None:
    repository = FakeRepository()
    repository.duplicate_sources.add("factories")
    outcome = _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": _clean_factories()},
        batch_id="duplicate-batch",
        scenario=FailureScenario.CLEAN,
        run_dbt=False,
    )

    assert outcome.summary.status is RunStatus.DEGRADED
    assert outcome.summary.source_file_count == 1
    assert outcome.summary.skipped_file_count == 1
    assert outcome.summary.accepted_row_count == 0
    assert repository.loaded == {}
    assert repository.quarantined == []
    duplicate_check = next(
        result for result in repository.quality_results if result.check_type == "idempotency"
    )
    assert duplicate_check.status == "passed"
    assert duplicate_check.evidence["action"] == "skipped"


def test_quarantine_reasons_and_breaking_schema_drift_are_persisted(
    tmp_path: Path,
) -> None:
    invalid = _clean_factories()
    for record in invalid:
        record.pop("country_code")

    repository = FakeRepository()
    outcome = _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": invalid},
        batch_id="contract-evidence",
        scenario=FailureScenario.CLEAN,
        run_dbt=False,
    )

    assert outcome.summary.status is RunStatus.FAILED
    assert outcome.summary.accepted_row_count == 0
    assert outcome.summary.quarantined_row_count == len(invalid)
    assert repository.completed_files[0][1:] == (0, len(invalid), "quarantined")
    assert repository.schema_changes[0].change_type == "breaking"
    assert repository.schema_changes[0].missing_columns == ["country_code"]
    assert len(repository.quarantined) == len(invalid)
    assert all(record.raw_payload for record in repository.quarantined)
    assert all(
        {reason.code for reason in record.reasons} == {"missing_required_column"}
        for record in repository.quarantined
    )
    schema_check = next(
        result for result in repository.quality_results if result.check_type == "schema"
    )
    assert schema_check.status == "failed"
    assert schema_check.severity.value == "error"


def test_late_warehouse_failure_recomputes_persisted_quality_counters(tmp_path: Path) -> None:
    class FailSecondCommitRepository(FakeRepository):
        def __init__(self) -> None:
            super().__init__()
            self.commit_calls = 0

        def commit_source_result(
            self,
            source_name: str,
            accepted_records: Sequence[Mapping[str, Any]],
            *,
            run_id: UUID,
            batch_id: str,
            file_id: UUID,
            ingested_at: datetime,
            quarantined_records: Sequence[QuarantinedRecord],
            schema_changes: Sequence[SchemaChange],
        ) -> tuple[int, int]:
            self.commit_calls += 1
            if self.commit_calls == 2:
                raise WarehouseError("second source commit failed")
            return super().commit_source_result(
                source_name,
                accepted_records,
                run_id=run_id,
                batch_id=batch_id,
                file_id=file_id,
                ingested_at=ingested_at,
                quarantined_records=quarantined_records,
                schema_changes=schema_changes,
            )

    repository = FailSecondCommitRepository()
    invalid_factories = _clean_factories()
    for record in invalid_factories:
        record.pop("country_code")
    dataset = {
        "factories": invalid_factories,
        "machines": [{"machine_id": "MCH-001"}],
    }

    with pytest.raises(WarehouseError, match="second source commit failed"):
        _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
            dataset,
            batch_id="late-warehouse-failure",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    finalized = repository.finished[-1]
    assert finalized.status is RunStatus.FAILED
    assert finalized.failed_checks > 0
    assert finalized.passed_checks > 0


def test_late_arrival_produces_bounded_freshness_evidence() -> None:
    run_id = uuid4()
    updated_at = datetime(2025, 7, 10, 12, tzinfo=UTC)
    result = ContractResult(
        source_name="machine_telemetry",
        source_rows=7,
        accepted_records=[
            {
                "telemetry_id": f"TEL-{index}",
                "event_timestamp": (updated_at - timedelta(hours=25 + index)).isoformat(),
                "updated_at": updated_at.isoformat(),
            }
            for index in range(7)
        ],
    )

    quality = contract_quality_results(
        run_id=run_id,
        source_name="machine_telemetry",
        result=result,
    )
    freshness = next(check for check in quality if check.check_id.startswith("late_arrival:"))

    assert freshness.status == "warning"
    assert freshness.observed_value == 7
    assert freshness.evidence["late_record_count"] == 7
    assert freshness.evidence["example_telemetry_ids"] == [
        "TEL-0",
        "TEL-1",
        "TEL-2",
        "TEL-3",
        "TEL-4",
    ]
    assert freshness.evidence["threshold_hours"] == 24


def test_volume_anomaly_uses_bounded_explainable_median_mad_evidence() -> None:
    stable = volume_anomaly_result(
        run_id=uuid4(),
        source_name="machine_telemetry",
        observed_rows=102,
        healthy_history=[100, 101, 99, 100, 100, 10_000, 98, 97],
    )
    outlier = volume_anomaly_result(
        run_id=uuid4(),
        source_name="machine_telemetry",
        observed_rows=20,
        healthy_history=[100, 101, 99, 100, 100],
    )

    assert stable.status == "passed"
    assert stable.evidence["evaluated"] is True
    assert len(stable.evidence["healthy_history_rows"]) == 7
    assert stable.evidence["method"] == "median_mad"
    assert outlier.status == "warning"
    assert outlier.severity.value == "warning"
    assert outlier.evidence["relative_floor"] == 0.20


def test_volume_anomaly_records_insufficient_history_without_degrading_baseline() -> None:
    result = volume_anomaly_result(
        run_id=uuid4(),
        source_name="factories",
        observed_rows=3,
        healthy_history=[3, 3],
    )

    assert result.status == "passed"
    assert result.evidence["evaluated"] is False
    assert result.evidence["minimum_samples"] == 3


def test_pipeline_step_failure_is_finalized_with_safe_failure_metadata(
    tmp_path: Path,
) -> None:
    repository = FakeRepository()
    object_store = FakeObjectStore()
    object_store.failure = ObjectStoreError("raw landing unavailable")

    with pytest.raises(ObjectStoreError, match="raw landing unavailable"):
        _runner(tmp_path, repository, object_store).run_dataset(
            {"factories": _clean_factories()},
            batch_id="failed-landing",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert repository.finish_attempts == 1
    failed = repository.finished[0]
    assert failed.status is RunStatus.FAILED
    assert failed.finished_at is not None
    assert failed.duration_seconds is not None
    assert failed.duration_seconds >= 0
    assert failed.error_message == "raw landing unavailable"


def test_finalization_failure_does_not_mask_the_original_pipeline_error(
    tmp_path: Path,
) -> None:
    repository = FakeRepository()
    repository.finish_error = WarehouseError("metadata finalization unavailable")
    object_store = FakeObjectStore()
    original = ObjectStoreError("original landing failure")
    object_store.failure = original

    with pytest.raises(ObjectStoreError, match="original landing failure"):
        _runner(tmp_path, repository, object_store).run_dataset(
            {"factories": _clean_factories()},
            batch_id="double-failure",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert repository.finish_attempts == 1


def test_quality_persistence_failure_still_attempts_run_finalization(tmp_path: Path) -> None:
    class QualityFailureRepository(FakeRepository):
        def record_quality_results(self, results: Sequence[QualityResult]) -> None:
            del results
            raise WarehouseError("quality persistence unavailable")

    repository = QualityFailureRepository()
    object_store = FakeObjectStore()
    object_store.failure = ObjectStoreError("primary landing failure")

    with pytest.raises(ObjectStoreError, match="primary landing failure"):
        _runner(tmp_path, repository, object_store).run_dataset(
            {"factories": _clean_factories()},
            batch_id="quality-finalization-failure",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert repository.finish_attempts == 1
    assert repository.finished[0].status is RunStatus.FAILED


def test_generation_failure_finalizes_the_started_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = FakeRepository()

    def fail_generation(
        scenario: FailureScenario,
        *,
        seed: int,
        generated_days: int,
        batch_date: date,
        incremental: bool,
    ) -> dict[str, list[dict[str, Any]]]:
        del scenario, seed, generated_days, batch_date, incremental
        raise ValueError("synthetic generator preflight failed")

    monkeypatch.setattr(pipeline_module, "generate_dataset", fail_generation)

    with pytest.raises(ForgeFlowError, match="Source generation failed"):
        _runner(tmp_path, repository, FakeObjectStore()).run_generated(
            batch_date=BATCH_DATE,
            run_dbt=False,
        )

    assert len(repository.started) == 1
    assert len(repository.finished) == 1
    failed = repository.finished[0]
    assert failed.run_id == repository.started[0].run_id
    assert failed.status is RunStatus.FAILED
    assert failed.error_message == "synthetic generator preflight failed"


def test_generation_error_is_not_masked_when_preflight_finalization_also_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repository = FakeRepository()
    repository.finish_error = WarehouseError("preflight metadata unavailable")

    def fail_generation(
        scenario: FailureScenario,
        *,
        seed: int,
        generated_days: int,
        batch_date: date,
        incremental: bool,
    ) -> dict[str, list[dict[str, Any]]]:
        del scenario, seed, generated_days, batch_date, incremental
        raise ContractError("original generation error")

    monkeypatch.setattr(pipeline_module, "generate_dataset", fail_generation)

    with pytest.raises(ContractError, match="original generation error"):
        _runner(tmp_path, repository, FakeObjectStore()).run_generated(
            batch_date=BATCH_DATE,
            run_dbt=False,
        )

    assert repository.finish_attempts == 1


def test_healthy_recovery_resolves_incident_without_discarding_history(
    tmp_path: Path,
) -> None:
    incident_id = uuid4()
    failed_run_id = uuid4()
    evidence = {"failed_run_id": str(failed_run_id), "failed_checks": ["business_rule"]}
    repository = FakeRepository()
    repository.open_incident = {
        "incident_id": incident_id,
        "failed_run_id": failed_run_id,
        "status": "open",
        "evidence": evidence.copy(),
    }

    outcome = _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": _clean_factories()},
        batch_id="recovery-batch",
        scenario=FailureScenario.RECOVERY,
        recovery_incident_id=incident_id,
    )

    assert outcome.summary.status is RunStatus.HEALTHY
    assert outcome.incident_id == incident_id
    assert repository.resolutions == [(incident_id, outcome.summary.run_id)]
    assert repository.open_incident is not None
    assert repository.open_incident["failed_run_id"] == failed_run_id
    assert repository.open_incident["evidence"] == evidence
    assert repository.finished[-1].scenario is FailureScenario.RECOVERY


def test_recovery_without_explicit_incident_does_not_close_latest_open_incident(
    tmp_path: Path,
) -> None:
    incident_id = uuid4()
    repository = FakeRepository()
    repository.open_incident = {
        "incident_id": incident_id,
        "failed_run_id": uuid4(),
        "status": "open",
        "evidence": {},
    }

    outcome = _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": _clean_factories()},
        batch_id="unbound-recovery-batch",
        scenario=FailureScenario.RECOVERY,
    )

    assert outcome.summary.status is RunStatus.HEALTHY
    assert outcome.incident_id is None
    assert repository.resolutions == []


def test_skip_dbt_is_diagnostic_and_never_reports_a_healthy_run(tmp_path: Path) -> None:
    repository = FakeRepository()

    outcome = _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": _clean_factories()},
        batch_id="diagnostic-ingestion",
        scenario=FailureScenario.CLEAN,
        run_dbt=False,
    )

    assert outcome.summary.status is RunStatus.DEGRADED
    skipped = next(check for check in repository.quality_results if check.check_id == "dbt_skipped")
    assert skipped.status == "warning"
    assert skipped.evidence == {"diagnostic_only": True}


def test_optional_provider_failure_cannot_prevent_deterministic_incident_persistence(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingProvider:
        def explain(self, evidence: object) -> object:
            del evidence
            raise ForgeFlowError("optional provider unavailable")

    settings = Settings(
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
        ai_provider="openai",
        OPENAI_API_KEY="synthetic-test-key",
    )
    repository = FakeRepository()
    invalid = _clean_factories()
    for record in invalid:
        record.pop("country_code")
    monkeypatch.setattr(
        pipeline_module,
        "build_explanation_provider",
        lambda configured: FailingProvider(),
    )
    runner = PipelineRunner(
        settings,
        cast("PostgresRepository", repository),
        cast("ObjectStore", FakeObjectStore()),
        dbt_runner=cast("DbtRunner", SuccessfulDbtRunner()),
    )

    outcome = runner.run_dataset(
        {"factories": invalid},
        batch_id="incident-provider-failure",
        scenario=FailureScenario.INCIDENT,
        run_dbt=False,
    )

    assert outcome.summary.status is RunStatus.FAILED
    assert outcome.incident_id is not None
    persisted = repository.incidents[outcome.incident_id]
    assert cast(dict[str, Any], persisted["explanation"])["provider"] == "deterministic"
    assert persisted["failed_run_id"] == outcome.summary.run_id


def test_csv_serialization_is_deterministic_and_loaded_rows_keep_file_lineage(
    tmp_path: Path,
) -> None:
    records = _clean_factories()
    first_bytes, first_columns = serialize_source_csv(records)
    second_bytes, second_columns = serialize_source_csv(records)

    assert first_bytes == second_bytes
    assert first_columns == second_columns == list(records[0])
    parsed = list(csv.DictReader(io.StringIO(first_bytes.decode("utf-8"))))
    assert [row["factory_id"] for row in parsed] == [row["factory_id"] for row in records]
    assert first_bytes.endswith(b"\n")

    repository = FakeRepository()
    _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": records},
        batch_id="lineage-batch",
        scenario=FailureScenario.CLEAN,
        run_dbt=False,
    )

    assert [row["_source_row_number"] for row in repository.loaded["factories"]] == [2, 3, 4]
    registration = repository.registrations[0]
    assert registration["checksum"] == sha256_bytes(first_bytes)
    assert registration["row_count"] == 3


def test_manual_ingestion_lands_the_exact_validated_source_bytes(tmp_path: Path) -> None:
    records = _clean_factories()
    canonical, _ = serialize_source_csv(records)
    exact_source = canonical.replace(b"\n", b"\r\n")
    repository = FakeRepository()
    object_store = FakeObjectStore()

    _runner(tmp_path, repository, object_store).run_dataset(
        {"factories": records},
        source_bytes={"factories": exact_source},
        batch_id="exact-byte-batch",
        scenario=FailureScenario.CLEAN,
        run_dbt=False,
    )

    assert list(object_store.objects.values()) == [exact_source]
    assert repository.registrations[0]["checksum"] == sha256_bytes(exact_source)


def test_manual_ingestion_rejects_raw_bytes_that_do_not_match_parsed_rows(
    tmp_path: Path,
) -> None:
    records = _clean_factories()
    exact_source, _ = serialize_source_csv(records)
    unrelated = exact_source.replace(b"Factory Alpha", b"Unrelated Name", 1)
    repository = FakeRepository()
    object_store = FakeObjectStore()

    with pytest.raises(ForgeFlowError, match="do not match parsed value"):
        _runner(tmp_path, repository, object_store).run_dataset(
            {"factories": records},
            source_bytes={"factories": unrelated},
            batch_id="mismatched-byte-batch",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert not object_store.objects
    assert not repository.registrations


def test_programmatic_ingestion_enforces_row_bounds_before_landing(tmp_path: Path) -> None:
    records = _clean_factories()
    repository = FakeRepository()
    object_store = FakeObjectStore()
    settings = Settings(
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
        max_source_rows_per_file=2,
    )
    runner = PipelineRunner(
        settings,
        cast("PostgresRepository", repository),
        cast("ObjectStore", object_store),
        dbt_runner=cast("DbtRunner", SuccessfulDbtRunner()),
    )

    with pytest.raises(ForgeFlowError, match="row limit"):
        runner.run_dataset(
            {"factories": records},
            batch_id="oversized-row-batch",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert not object_store.objects
    assert not repository.registrations


def test_programmatic_ingestion_enforces_serialized_byte_bounds_before_put(
    tmp_path: Path,
) -> None:
    records = _clean_factories()
    records[0]["factory_name"] = "x" * 2_000
    repository = FakeRepository()
    object_store = FakeObjectStore()
    settings = Settings(
        data_dir=tmp_path / "data",
        artifact_dir=tmp_path / "artifacts",
        max_source_file_bytes=256,
    )
    runner = PipelineRunner(
        settings,
        cast("PostgresRepository", repository),
        cast("ObjectStore", object_store),
        dbt_runner=cast("DbtRunner", SuccessfulDbtRunner()),
    )

    with pytest.raises(ForgeFlowError, match="byte limit"):
        runner.run_dataset(
            {"factories": records},
            batch_id="oversized-byte-batch",
            scenario=FailureScenario.CLEAN,
            run_dbt=False,
        )

    assert not object_store.objects
    assert not repository.registrations


def test_manual_empty_csv_preserves_its_observed_header_shape(tmp_path: Path) -> None:
    headers = list(SOURCE_CONTRACTS["factories"].expected_columns)
    exact_source = (",".join(headers) + "\r\n").encode()
    repository = FakeRepository()

    outcome = _runner(tmp_path, repository, FakeObjectStore()).run_dataset(
        {"factories": []},
        source_bytes={"factories": exact_source},
        batch_id="empty-header-batch",
        scenario=FailureScenario.CLEAN,
        run_dbt=False,
    )

    assert outcome.summary.schema_changes == []
    assert repository.completed_files[0][1:] == (0, 0, "loaded")
    assert repository.registrations[0]["schema_fingerprint"] == schema_fingerprint(headers)
