"""Shared typed records for pipeline, observability, API, MCP, and AI providers."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_SCHEMA_COLUMN_CHARACTERS = 128
MAX_SCHEMA_COLUMNS_PER_CHANGE = 100
REDACTED_SCHEMA_COLUMN = "<redacted: column name exceeds evidence limit>"
OMITTED_SCHEMA_COLUMNS = "<additional columns omitted>"


def _compact_schema_columns(value: Any) -> Any:
    """Bound column-name evidence while leaving invalid non-list input to Pydantic."""
    if not isinstance(value, (list, tuple)):
        return value
    retain = MAX_SCHEMA_COLUMNS_PER_CHANGE
    has_omitted_columns = len(value) > retain
    if has_omitted_columns:
        retain -= 1
    bounded: list[str] = []
    for column in value[:retain]:
        rendered = str(column)
        bounded.append(
            rendered if len(rendered) <= MAX_SCHEMA_COLUMN_CHARACTERS else REDACTED_SCHEMA_COLUMN
        )
    if has_omitted_columns:
        bounded.append(OMITTED_SCHEMA_COLUMNS)
    return list(dict.fromkeys(bounded))


class RunStatus(StrEnum):
    """Durable pipeline states with consistent product semantics."""

    RUNNING = "running"
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"


class FailureScenario(StrEnum):
    """Named deterministic source generation modes."""

    CLEAN = "clean"
    INCIDENT = "incident"
    RECOVERY = "recovery"


class Severity(StrEnum):
    """Quality result severity."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class QuarantineReason(BaseModel):
    """One machine-readable and human-readable reason for rejecting a record."""

    model_config = ConfigDict(extra="forbid")

    code: str
    column: str | None = None
    check: str
    message: str
    value: str | int | float | bool | None = None


class QuarantinedRecord(BaseModel):
    """A source row retained with complete rejection evidence."""

    source_name: str
    source_row_number: int = Field(ge=2)
    raw_payload: dict[str, Any]
    reasons: list[QuarantineReason] = Field(min_length=1)


class SchemaChange(BaseModel):
    """Observed file shape difference from its registered contract."""

    source_name: str
    change_type: Literal["additive", "breaking"]
    expected_columns: list[str]
    actual_columns: list[str]
    missing_columns: list[str] = Field(default_factory=list)
    unexpected_columns: list[str] = Field(default_factory=list)

    @field_validator(
        "expected_columns",
        "actual_columns",
        "missing_columns",
        "unexpected_columns",
        mode="before",
    )
    @classmethod
    def bound_column_evidence(cls, value: Any) -> Any:
        """Prevent arbitrary source headers from creating unbounded evidence payloads."""
        return _compact_schema_columns(value)


class ContractResult(BaseModel):
    """Accepted rows plus durable contract/quarantine evidence."""

    source_name: str
    source_rows: int = Field(ge=0)
    accepted_records: list[dict[str, Any]] = Field(default_factory=list)
    quarantined_records: list[QuarantinedRecord] = Field(default_factory=list)
    schema_changes: list[SchemaChange] = Field(default_factory=list)


class QualityResult(BaseModel):
    """Normalized result from a contract, dbt test, or observability check."""

    check_id: str
    run_id: UUID
    check_name: str
    check_type: str
    scope: str
    status: Literal["passed", "failed", "warning"]
    severity: Severity
    observed_value: float | int | str | None = None
    expected: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RunSummary(BaseModel):
    """Compact canonical summary shared by every read surface."""

    run_id: UUID = Field(default_factory=uuid4)
    batch_id: str
    scenario: FailureScenario
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    duration_seconds: float | None = Field(default=None, ge=0)
    source_file_count: int = Field(default=0, ge=0)
    source_row_count: int = Field(default=0, ge=0)
    accepted_row_count: int = Field(default=0, ge=0)
    quarantined_row_count: int = Field(default=0, ge=0)
    skipped_file_count: int = Field(default=0, ge=0)
    model_row_counts: dict[str, int] = Field(default_factory=dict)
    test_counts: dict[str, int] = Field(default_factory=dict)
    passed_checks: int = Field(default=0, ge=0)
    failed_checks: int = Field(default=0, ge=0)
    freshness_status: str = "unknown"
    schema_changes: list[SchemaChange] = Field(default_factory=list)
    affected_downstream_models: list[str] = Field(default_factory=list)
    error_message: str | None = None


class IncidentEvidence(BaseModel):
    """Minimal evidence bundle allowed into explanation providers."""

    incident_id: UUID
    failed_run_id: UUID
    baseline_run_id: UUID | None = None
    failed_checks: list[dict[str, Any]] = Field(default_factory=list, max_length=100)
    row_count_changes: dict[str, int] = Field(default_factory=dict)
    quarantine_reasons: dict[str, int] = Field(default_factory=dict)
    schema_changes: list[SchemaChange] = Field(default_factory=list, max_length=50)
    affected_models: list[str] = Field(default_factory=list, max_length=100)
    log_events: list[str] = Field(default_factory=list, max_length=50)


class IncidentExplanation(BaseModel):
    """Evidence-grounded explanation with explicit epistemic labels."""

    provider: Literal["deterministic", "openai"]
    observed_facts: list[str] = Field(min_length=1, max_length=30)
    likely_explanations: list[str] = Field(default_factory=list, max_length=20)
    recommended_next_steps: list[str] = Field(min_length=1, max_length=20)
    uncertainty_note: str
    evidence_run_ids: list[UUID] = Field(min_length=1, max_length=5)
