"""Unit tests for the shared bounded ForgeFlow read/evidence service."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast
from uuid import UUID

import pytest

from forgeflow.config import Settings
from forgeflow.incident import ExplanationProvider
from forgeflow.service import ForgeFlowService, evidence_from_run
from forgeflow.warehouse import PostgresRepository

LEFT_RUN_ID = UUID("10000000-0000-0000-0000-000000000001")
RIGHT_RUN_ID = UUID("10000000-0000-0000-0000-000000000002")


class _FakeRepository:
    def __init__(self) -> None:
        self.run_rows: list[dict[str, Any]] = []
        self.runs: dict[UUID, dict[str, Any]] = {}
        self.quarantine_rows: list[dict[str, Any]] = []
        self.list_run_calls: list[tuple[int, int]] = []
        self.quarantine_calls: list[tuple[UUID | None, int, int]] = []
        self.failed_check_calls: list[tuple[str, UUID | None]] = []
        self.incidents: dict[UUID, dict[str, Any]] = {}
        self.models: dict[str, dict[str, Any]] = {}
        self.reachable = True

    def ping(self) -> bool:
        return self.reachable

    def list_runs(self, *, limit: int, offset: int) -> tuple[list[dict[str, Any]], int]:
        self.list_run_calls.append((limit, offset))
        return self.run_rows[offset : offset + limit], len(self.run_rows)

    def get_run(self, run_id: UUID) -> dict[str, Any] | None:
        return self.runs.get(run_id)

    def get_incident(self, incident_id: UUID) -> dict[str, Any] | None:
        return self.incidents.get(incident_id)

    def list_quarantined_records(
        self, *, run_id: UUID | None, limit: int, offset: int
    ) -> tuple[list[dict[str, Any]], int]:
        self.quarantine_calls.append((run_id, limit, offset))
        return self.quarantine_rows[offset : offset + limit], len(self.quarantine_rows)

    def get_failed_check(self, check_id: str, run_id: UUID | None) -> dict[str, Any] | None:
        self.failed_check_calls.append((check_id, run_id))
        return {"check_id": check_id} if check_id == "accepted.check" else None

    def get_model(self, model_name: str) -> dict[str, Any] | None:
        return self.models.get(model_name)

    def get_columns(self, model_name: str) -> list[dict[str, Any]]:
        return [{"model_name": model_name, "column_name": "id"}]

    def get_lineage(self, model_name: str) -> dict[str, Any]:
        return {"model_name": model_name, "parents": [], "children": []}

    def get_downstream_impact(self, model_name: str) -> list[dict[str, Any]]:
        return [{"model_name": model_name, "depth": 0}]


def _service(
    repository: _FakeRepository,
    *,
    max_page_size: int = 2,
    max_page_offset: int = 10_000,
) -> ForgeFlowService:
    return ForgeFlowService(
        Settings(max_page_size=max_page_size, max_page_offset=max_page_offset),
        cast(PostgresRepository, repository),
    )


class _FailingProvider:
    def explain(self, evidence: object) -> object:
        del evidence
        raise AssertionError("read paths must not invoke an explanation provider")


def test_run_pagination_caps_limit_and_serializes_boundary_values() -> None:
    repository = _FakeRepository()
    repository.run_rows = [
        {"run_id": LEFT_RUN_ID, "finished_at": datetime(2025, 7, 10, 10, tzinfo=UTC)},
        {"run_id": RIGHT_RUN_ID, "finished_at": datetime(2025, 7, 10, 11, tzinfo=UTC)},
        {"run_id": UUID("10000000-0000-0000-0000-000000000003")},
    ]

    page = _service(repository).list_pipeline_runs(limit=500, offset=1)

    assert repository.list_run_calls == [(2, 1)]
    assert page.total == 3
    assert page.limit == 2
    assert page.offset == 1
    assert page.items[0] == {
        "run_id": str(RIGHT_RUN_ID),
        "finished_at": "2025-07-10T11:00:00+00:00",
    }


@pytest.mark.parametrize("limit", [0, -1])
def test_non_positive_limits_are_rejected_before_repository_access(limit: int) -> None:
    repository = _FakeRepository()

    with pytest.raises(ValueError, match="limit must be at least 1"):
        _service(repository).list_pipeline_runs(limit=limit)

    assert repository.list_run_calls == []


def test_negative_offsets_are_rejected_before_repository_access() -> None:
    repository = _FakeRepository()

    with pytest.raises(ValueError, match="offset must be zero or greater"):
        _service(repository).list_pipeline_runs(offset=-1)

    assert repository.list_run_calls == []


def test_large_offsets_are_capped_before_repository_access() -> None:
    repository = _FakeRepository()

    page = _service(repository, max_page_offset=25).list_pipeline_runs(offset=10**9)

    assert repository.list_run_calls == [(2, 25)]
    assert page.offset == 25


def test_quarantine_surface_never_returns_raw_source_payload() -> None:
    repository = _FakeRepository()
    repository.quarantine_rows = [
        {
            "quarantine_id": UUID("20000000-0000-0000-0000-000000000001"),
            "source_name": "machine_telemetry",
            "source_row_number": 9,
            "raw_payload": {"operator": "must-not-leak", "temperature_c": 999},
            "reasons": [{"code": "out_of_range", "column": "temperature_c"}],
        }
    ]

    page = _service(repository).list_quarantined_records(limit=1)

    assert page.total == 1
    assert "raw_payload" not in page.items[0]
    assert page.items[0]["source_name"] == "machine_telemetry"
    assert page.items[0]["reasons"] == [{"code": "out_of_range", "column": "temperature_c"}]


def test_quarantine_serialization_bounds_untrusted_reason_evidence() -> None:
    repository = _FakeRepository()
    oversized = "do-not-echo-in-full-" * 10_000
    repository.quarantine_rows = [
        {
            "quarantine_id": UUID("20000000-0000-0000-0000-000000000002"),
            "source_name": "machine_telemetry",
            "source_row_number": 10,
            "raw_payload": {"secret": oversized},
            "unapproved_evidence": oversized,
            "reasons": [
                {
                    "code": oversized,
                    "column": oversized,
                    "check": oversized,
                    "message": oversized,
                    "value": float("inf"),
                    "raw_payload": {"secret": oversized},
                }
                for _ in range(1_000)
            ],
        }
    ]

    page = _service(repository).list_quarantined_records(limit=1)
    serialized = page.model_dump_json()

    item = page.items[0]
    assert "raw_payload" not in item
    assert "unapproved_evidence" not in item
    assert len(item["reasons"]) == 10
    assert item["reasons"][-1]["code"] == "additional_reasons_omitted"
    assert item["reasons"][0]["value"] == "<non-finite numeric value>"
    assert oversized not in serialized
    assert "do-not-echo-in-full" not in serialized
    assert len(serialized) < 5_000


@pytest.mark.parametrize(
    "unsafe_name",
    ["quality check", "failed;drop", "x" * 201, ""],
)
def test_check_identifiers_are_validated_before_repository_access(unsafe_name: str) -> None:
    repository = _FakeRepository()

    with pytest.raises(ValueError, match="check_id contains unsupported characters"):
        _service(repository).get_failed_check_details(unsafe_name)

    assert repository.failed_check_calls == []


def test_safe_check_identifier_is_passed_exactly_to_repository() -> None:
    repository = _FakeRepository()

    result = _service(repository).get_failed_check_details("accepted.check", LEFT_RUN_ID)

    assert result == {"check_id": "accepted.check"}
    assert repository.failed_check_calls == [("accepted.check", LEFT_RUN_ID)]


def test_unknown_model_reads_raise_one_consistent_not_found_error() -> None:
    service = _service(_FakeRepository())

    with pytest.raises(LookupError, match="missing_model"):
        service.get_column_metadata("missing_model")
    with pytest.raises(LookupError, match="missing_model"):
        service.get_model_lineage("missing_model")
    with pytest.raises(LookupError, match="missing_model"):
        service.get_downstream_impact("missing_model")


def test_run_comparison_reports_observed_deltas_without_claiming_causation() -> None:
    repository = _FakeRepository()
    repository.runs = {
        LEFT_RUN_ID: {
            "status": "healthy",
            "source_file_count": 10,
            "source_row_count": 100,
            "accepted_row_count": 99,
            "quarantined_row_count": 1,
            "passed_checks": 12,
            "failed_checks": 0,
            "model_row_counts": {"dim_machine": 4, "fct_output": 80},
        },
        RIGHT_RUN_ID: {
            "status": "failed",
            "source_file_count": 11,
            "source_row_count": 120,
            "accepted_row_count": 105,
            "quarantined_row_count": 15,
            "passed_checks": 10,
            "failed_checks": 2,
            "model_row_counts": {"fct_output": 75, "mart_factory": 3},
            "schema_changes": [{"source_name": "machine_telemetry"}],
            "affected_downstream_models": ["mart_factory"],
        },
    }

    comparison = _service(repository).compare_pipeline_runs(LEFT_RUN_ID, RIGHT_RUN_ID)

    assert comparison["status_change"] == {"from": "healthy", "to": "failed"}
    assert comparison["count_deltas"] == {
        "source_file_count": 1,
        "source_row_count": 20,
        "accepted_row_count": 6,
        "quarantined_row_count": 14,
        "passed_checks": -2,
        "failed_checks": 2,
    }
    assert comparison["model_row_count_deltas"] == {"fct_output": -5}
    assert comparison["model_row_count_unavailable"] == ["dim_machine", "mart_factory"]
    assert comparison["right_affected_models"] == ["mart_factory"]
    assert "do not prove causation" in comparison["interpretation"]


def test_run_comparison_identifies_the_missing_run() -> None:
    repository = _FakeRepository()
    repository.runs[LEFT_RUN_ID] = {"status": "healthy"}

    with pytest.raises(LookupError, match=str(RIGHT_RUN_ID)):
        _service(repository).compare_pipeline_runs(LEFT_RUN_ID, RIGHT_RUN_ID)


def test_evidence_builder_caps_inputs_and_preserves_only_compact_evidence() -> None:
    oversized = "untrusted-evidence-" * 1_000
    failed_run = {
        "run_id": RIGHT_RUN_ID,
        "model_row_counts": {"fct_output": 75, "mart_factory": 3},
        "schema_changes": [
            {
                "source_name": f"source_{index}",
                "change_type": "additive",
                "expected_columns": ["id"],
                "actual_columns": ["id", "new_column"],
                "unexpected_columns": ["new_column"],
            }
            for index in range(60)
        ],
        "affected_downstream_models": [f"model_{index}" for index in range(120)],
        "error_message": "dbt test failed",
    }
    baseline_run = {
        "run_id": LEFT_RUN_ID,
        "model_row_counts": {"dim_machine": 4, "fct_output": 80},
    }

    evidence = evidence_from_run(
        incident_id=UUID("30000000-0000-0000-0000-000000000001"),
        failed_run=failed_run,
        baseline_run=baseline_run,
        failed_checks=[
            {
                "check_id": f"check_{index}",
                "evidence": {
                    "value": oversized,
                    "raw_payload": {"secret": oversized},
                },
            }
            for index in range(150)
        ],
        quarantine_summary=[
            {
                "source_name": "machine_telemetry",
                "reason_code": "out_of_range",
                "count": 4,
            }
        ],
    )

    assert evidence.failed_run_id == RIGHT_RUN_ID
    assert evidence.baseline_run_id == LEFT_RUN_ID
    assert len(evidence.failed_checks) == 100
    assert len(evidence.schema_changes) == 50
    assert len(evidence.affected_models) == 100
    assert evidence.row_count_changes == {"fct_output": -5}
    assert evidence.quarantine_reasons == {"machine_telemetry:out_of_range": 4}
    assert evidence.log_events == ["dbt test failed"]
    assert evidence.failed_checks[0]["evidence"] == {
        "value": "<redacted: text exceeds reviewer evidence limit>"
    }


def test_health_reports_dependency_state_without_connection_details() -> None:
    repository = _FakeRepository()
    repository.reachable = False

    result = _service(repository).health()

    assert result == {
        "status": "degraded",
        "warehouse": "unreachable",
        "object_store": "not_checked",
        "writes_enabled": False,
        "ai_provider": "deterministic",
    }
    assert "postgresql" not in str(result).lower()


def test_incident_read_and_handoff_use_only_the_persisted_explanation() -> None:
    incident_id = UUID("30000000-0000-0000-0000-000000000009")
    failed_run_id = UUID("30000000-0000-0000-0000-000000000010")
    repository = _FakeRepository()
    repository.incidents[incident_id] = {
        "incident_id": incident_id,
        "status": "open",
        "failed_run_id": failed_run_id,
        "recovery_run_id": None,
        "evidence": {
            "incident_id": str(incident_id),
            "failed_run_id": str(failed_run_id),
            "affected_models": ["factory_performance"],
        },
        "explanation": {
            "provider": "deterministic",
            "observed_facts": ["Persisted fact."],
            "likely_explanations": [],
            "recommended_next_steps": ["Inspect retained evidence."],
            "uncertainty_note": "No cause is confirmed.",
            "evidence_run_ids": [str(failed_run_id)],
        },
    }
    service = ForgeFlowService(
        Settings(),
        cast(PostgresRepository, repository),
        explanation_provider=cast(ExplanationProvider, _FailingProvider()),
    )

    explanation = service.explain_incident_evidence(incident_id)
    handoff = service.generate_engineering_handoff(incident_id)

    assert explanation.observed_facts == ["Persisted fact."]
    assert handoff["observed_facts"] == ["Persisted fact."]
    assert handoff["affected_models"] == ["factory_performance"]
