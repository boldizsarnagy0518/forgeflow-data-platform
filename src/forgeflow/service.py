"""Single bounded read/evidence service shared by API, MCP, CLI, and dashboard."""

from __future__ import annotations

import math
import re
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field

from forgeflow.config import Settings
from forgeflow.incident import ExplanationProvider, build_explanation_provider
from forgeflow.models import IncidentEvidence, IncidentExplanation, SchemaChange
from forgeflow.object_store import ObjectStore
from forgeflow.warehouse import PostgresRepository

SAFE_NAME = re.compile(r"^[A-Za-z0-9_:.\-]{1,200}$")
MAX_REVIEWER_TEXT_CHARACTERS = 500
MAX_REVIEWER_KEY_CHARACTERS = 128
MAX_REVIEWER_COLLECTION_ITEMS = 100
MAX_REVIEWER_NESTING_DEPTH = 6
MAX_QUARANTINE_REASONS = 10
MAX_QUARANTINE_REASON_CHARACTERS = 200
REDACTED_REVIEWER_TEXT = "<redacted: text exceeds reviewer evidence limit>"
OMITTED_REVIEWER_VALUE = "<additional evidence omitted>"
QUARANTINE_PUBLIC_FIELDS = (
    "quarantine_id",
    "run_id",
    "source_name",
    "source_row_number",
    "reasons",
    "quarantined_at",
)
QUARANTINE_REASON_FIELDS = ("code", "column", "check", "message", "value")


class Page(BaseModel):
    """Stable pagination envelope for all list surfaces."""

    items: list[dict[str, Any]]
    total: int = Field(ge=0)
    limit: int = Field(ge=1)
    offset: int = Field(ge=0)


class ForgeFlowService:
    """Canonical product semantics over the PostgreSQL evidence repository."""

    def __init__(
        self,
        settings: Settings,
        repository: PostgresRepository,
        *,
        object_store: ObjectStore | None = None,
        explanation_provider: ExplanationProvider | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._object_store = object_store
        self._explanation_provider = explanation_provider or build_explanation_provider(settings)

    def health(self) -> dict[str, Any]:
        """Report dependency health without exposing connection details."""
        warehouse = self._repository.ping()
        object_store = self._object_store.ping() if self._object_store is not None else None
        healthy = warehouse and object_store is not False
        return {
            "status": "healthy" if healthy else "degraded",
            "warehouse": "reachable" if warehouse else "unreachable",
            "object_store": (
                "not_checked"
                if object_store is None
                else "reachable"
                if object_store
                else "unreachable"
            ),
            "writes_enabled": self._settings.enable_writes,
            "ai_provider": self._settings.ai_provider,
        }

    def list_pipeline_runs(self, *, limit: int = 20, offset: int = 0) -> Page:
        """Return a bounded newest-first run page."""
        bounded = self._bounded_limit(limit)
        resolved_offset = self._bounded_offset(offset)
        rows, total = self._repository.list_runs(limit=bounded, offset=resolved_offset)
        return Page(
            items=[_jsonable_row(row) for row in rows],
            total=total,
            limit=bounded,
            offset=resolved_offset,
        )

    def get_pipeline_run(self, run_id: UUID) -> dict[str, Any] | None:
        """Return one run with its normalized persisted fields."""
        row = self._repository.get_run(run_id)
        return _jsonable_row(row) if row else None

    def get_latest_pipeline_status(self) -> dict[str, Any] | None:
        """Return the latest run summary."""
        row = self._repository.get_latest_run()
        return _jsonable_row(row) if row else None

    def get_data_quality_summary(self, run_id: UUID | None = None) -> dict[str, Any]:
        """Return grouped checks plus quarantine counts for one run."""
        run = self._repository.get_run(run_id) if run_id else self._repository.get_latest_run()
        if run is None:
            return {"run_id": None, "checks": [], "quarantine": [], "state": "empty"}
        resolved_id = UUID(str(run["run_id"]))
        return {
            "run_id": str(resolved_id),
            "run_status": run["status"],
            "checks": [_jsonable_row(row) for row in self._repository.quality_summary(resolved_id)],
            "quarantine": [
                _jsonable_row(row) for row in self._repository.quarantine_summary(resolved_id)
            ],
            "state": "available",
        }

    def list_failed_checks(
        self, *, run_id: UUID | None = None, limit: int = 50, offset: int = 0
    ) -> Page:
        """Return failure/warning evidence with enforced payload bounds."""
        bounded = self._bounded_limit(limit)
        resolved_offset = self._bounded_offset(offset)
        rows, total = self._repository.list_failed_checks(
            run_id=run_id, limit=bounded, offset=resolved_offset
        )
        return Page(
            items=[_jsonable_row(row) for row in rows],
            total=total,
            limit=bounded,
            offset=resolved_offset,
        )

    def get_failed_check_details(
        self, check_id: str, run_id: UUID | None = None
    ) -> dict[str, Any] | None:
        """Return one exact check after identifier validation."""
        _require_safe_name(check_id, "check_id")
        row = self._repository.get_failed_check(check_id, run_id)
        return _jsonable_row(row) if row else None

    def list_quarantined_records(
        self, *, run_id: UUID | None = None, limit: int = 50, offset: int = 0
    ) -> Page:
        """Return rejection evidence but not full raw source payloads."""
        bounded = self._bounded_limit(limit)
        resolved_offset = self._bounded_offset(offset)
        rows, total = self._repository.list_quarantined_records(
            run_id=run_id, limit=bounded, offset=resolved_offset
        )
        return Page(
            items=[_bounded_quarantine_row(row) for row in rows],
            total=total,
            limit=bounded,
            offset=resolved_offset,
        )

    def list_models(self, *, limit: int = 100, offset: int = 0) -> Page:
        """Return dbt model/source metadata."""
        bounded = self._bounded_limit(limit)
        resolved_offset = self._bounded_offset(offset)
        rows, total = self._repository.list_models(limit=bounded, offset=resolved_offset)
        return Page(
            items=[_jsonable_row(row) for row in rows],
            total=total,
            limit=bounded,
            offset=resolved_offset,
        )

    def get_model_metadata(self, model_name: str) -> dict[str, Any] | None:
        """Return one documented dbt resource."""
        _require_safe_name(model_name, "model_name")
        row = self._repository.get_model(model_name)
        return _jsonable_row(row) if row else None

    def get_column_metadata(self, model_name: str) -> list[dict[str, Any]]:
        """Return documented columns for one exact model."""
        _require_safe_name(model_name, "model_name")
        self._require_model(model_name)
        return [_jsonable_row(row) for row in self._repository.get_columns(model_name)]

    def get_model_lineage(self, model_name: str) -> dict[str, Any]:
        """Return direct parent/child edges."""
        _require_safe_name(model_name, "model_name")
        self._require_model(model_name)
        return _jsonable_row(self._repository.get_lineage(model_name))

    def get_downstream_impact(self, model_name: str) -> list[dict[str, Any]]:
        """Return cycle-safe transitive downstream impact."""
        _require_safe_name(model_name, "model_name")
        self._require_model(model_name)
        return [_jsonable_row(row) for row in self._repository.get_downstream_impact(model_name)]

    def _require_model(self, model_name: str) -> None:
        if self._repository.get_model(model_name) is None:
            raise LookupError(f"Model {model_name!r} was not found")

    def get_freshness(self) -> list[dict[str, Any]]:
        """Return actual modeled freshness, capped by the repository."""
        return [_jsonable_row(row) for row in self._repository.freshness()]

    def get_factory_performance(self) -> list[dict[str, Any]]:
        """Return factory performance mart rows for operational views."""
        return [_jsonable_row(row) for row in self._repository.factory_performance()]

    def get_quality_trend(self, *, limit: int = 50) -> list[dict[str, Any]]:
        """Return newest quality trend points."""
        return [
            _jsonable_row(row)
            for row in self._repository.quality_trend(limit=self._bounded_limit(limit))
        ]

    def compare_pipeline_runs(self, left_run_id: UUID, right_run_id: UUID) -> dict[str, Any]:
        """Compare persisted evidence without inferring an unsupported cause."""
        left = self._repository.get_run(left_run_id)
        right = self._repository.get_run(right_run_id)
        if left is None or right is None:
            missing = str(left_run_id if left is None else right_run_id)
            raise LookupError(f"Pipeline run {missing} was not found")
        count_fields = (
            "source_file_count",
            "source_row_count",
            "accepted_row_count",
            "quarantined_row_count",
            "passed_checks",
            "failed_checks",
        )
        deltas = {
            field: int(right.get(field) or 0) - int(left.get(field) or 0) for field in count_fields
        }
        left_models = left.get("model_row_counts") or {}
        right_models = right.get("model_row_counts") or {}
        model_names = set(left_models) & set(right_models)
        model_deltas = {
            name: int(right_models[name]) - int(left_models[name]) for name in sorted(model_names)
        }
        unavailable_models = sorted(set(left_models) ^ set(right_models))
        return _jsonable_row(
            {
                "left_run_id": str(left_run_id),
                "right_run_id": str(right_run_id),
                "status_change": {"from": left["status"], "to": right["status"]},
                "count_deltas": deltas,
                "model_row_count_deltas": model_deltas,
                "model_row_count_unavailable": unavailable_models,
                "new_schema_changes": right.get("schema_changes") or [],
                "right_affected_models": right.get("affected_downstream_models") or [],
                "interpretation": "Deltas are observations; they do not prove causation.",
            }
        )

    def explain_incident_evidence(self, incident_id: UUID) -> IncidentExplanation:
        """Return the explanation persisted when the incident was created."""
        incident = self._repository.get_incident(incident_id)
        if incident is None:
            raise LookupError(f"Incident {incident_id} was not found")
        return IncidentExplanation.model_validate(_json_value(incident["explanation"]))

    def get_incident(self, incident_id: UUID) -> dict[str, Any] | None:
        """Return an incident, its explanation, and recovery state."""
        row = self._repository.get_incident(incident_id)
        return _jsonable_row(row) if row else None

    def generate_engineering_handoff(self, incident_id: UUID) -> dict[str, Any]:
        """Produce a compact handoff referencing persisted evidence rather than raw tables."""
        incident = self._repository.get_incident(incident_id)
        if incident is None:
            raise LookupError(f"Incident {incident_id} was not found")
        evidence = IncidentEvidence.model_validate(_json_value(incident["evidence"]))
        explanation = IncidentExplanation.model_validate(_json_value(incident["explanation"]))
        return _jsonable_row(
            {
                "incident_id": str(incident_id),
                "status": incident["status"],
                "failed_run_id": str(incident["failed_run_id"]),
                "recovery_run_id": (
                    str(incident["recovery_run_id"]) if incident.get("recovery_run_id") else None
                ),
                "observed_facts": explanation.observed_facts,
                "hypotheses": explanation.likely_explanations,
                "next_steps": explanation.recommended_next_steps,
                "uncertainty_note": explanation.uncertainty_note,
                "affected_models": evidence.affected_models[:50],
            }
        )

    def _bounded_limit(self, requested: int) -> int:
        if requested < 1:
            raise ValueError("limit must be at least 1")
        return min(requested, self._settings.max_page_size)

    def _bounded_offset(self, requested: int) -> int:
        if requested < 0:
            raise ValueError("offset must be zero or greater")
        return min(requested, self._settings.max_page_offset)


def build_service(settings: Settings, *, include_object_store: bool = False) -> ForgeFlowService:
    """Build the default service graph at a process boundary."""
    repository = PostgresRepository(settings)
    object_store: ObjectStore | None = None
    if include_object_store:
        from forgeflow.object_store import S3ObjectStore

        object_store = S3ObjectStore(settings)
    return ForgeFlowService(settings, repository, object_store=object_store)


def evidence_from_run(
    *,
    incident_id: UUID,
    failed_run: dict[str, Any],
    baseline_run: dict[str, Any] | None,
    failed_checks: list[dict[str, Any]],
    quarantine_summary: list[dict[str, Any]],
) -> IncidentEvidence:
    """Create the only evidence shape accepted by explanation providers."""
    failed_models = failed_run.get("model_row_counts") or {}
    baseline_models = baseline_run.get("model_row_counts") or {} if baseline_run else {}
    model_names = sorted(set(failed_models) & set(baseline_models))[:MAX_REVIEWER_COLLECTION_ITEMS]
    deltas = {
        _bounded_text(str(name), maximum=MAX_REVIEWER_KEY_CHARACTERS): int(failed_models[name])
        - int(baseline_models[name])
        for name in model_names
    }
    reasons = {
        _bounded_text(
            f"{row['source_name']}:{row['reason_code']}",
            maximum=MAX_REVIEWER_KEY_CHARACTERS,
        ): int(row["count"])
        for row in quarantine_summary[:MAX_REVIEWER_COLLECTION_ITEMS]
    }
    affected_model_names = list(failed_run.get("affected_downstream_models") or [])
    return IncidentEvidence(
        incident_id=incident_id,
        failed_run_id=UUID(str(failed_run["run_id"])),
        baseline_run_id=(UUID(str(baseline_run["run_id"])) if baseline_run else None),
        failed_checks=[_jsonable_row(row) for row in failed_checks[:MAX_REVIEWER_COLLECTION_ITEMS]],
        row_count_changes=deltas,
        quarantine_reasons=reasons,
        schema_changes=[
            SchemaChange.model_validate(_json_value(change))
            for change in (failed_run.get("schema_changes") or [])[:50]
        ],
        affected_models=[
            _bounded_text(str(name))
            for name in affected_model_names[:MAX_REVIEWER_COLLECTION_ITEMS]
        ],
        log_events=(
            [_bounded_text(str(failed_run["error_message"]))]
            if failed_run.get("error_message")
            else []
        ),
    )


def _require_safe_name(value: str, field: str) -> None:
    if not SAFE_NAME.fullmatch(value):
        raise ValueError(f"{field} contains unsupported characters or is too long")


def _jsonable_row(row: dict[str, Any]) -> dict[str, Any]:
    value = _json_value(row)
    return value if isinstance(value, dict) else {}


def _bounded_text(value: str, *, maximum: int = MAX_REVIEWER_TEXT_CHARACTERS) -> str:
    return value if len(value) <= maximum else REDACTED_REVIEWER_TEXT


def _bounded_quarantine_row(row: dict[str, Any]) -> dict[str, Any]:
    public = {field: row[field] for field in QUARANTINE_PUBLIC_FIELDS if field in row}
    public["reasons"] = _bounded_quarantine_reasons(row.get("reasons"))
    return _jsonable_row(public)


def _bounded_quarantine_reasons(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, (list, tuple)):
        return []
    reasons: list[dict[str, Any]] = []
    retain = MAX_QUARANTINE_REASONS
    has_omitted_reasons = len(value) > retain
    if has_omitted_reasons:
        retain -= 1
    for candidate in value[:retain]:
        if not isinstance(candidate, dict):
            reasons.append({"code": "invalid_reason_shape", "message": OMITTED_REVIEWER_VALUE})
            continue
        reason: dict[str, Any] = {}
        for field in QUARANTINE_REASON_FIELDS:
            if field not in candidate:
                continue
            item = candidate[field]
            if isinstance(item, str):
                reason[field] = _bounded_text(
                    item,
                    maximum=MAX_QUARANTINE_REASON_CHARACTERS,
                )
            elif item is None or isinstance(item, (bool, int, float)):
                reason[field] = _json_value(item)
            else:
                reason[field] = OMITTED_REVIEWER_VALUE
        reasons.append(reason)
    if has_omitted_reasons:
        reasons.append(
            {
                "code": "additional_reasons_omitted",
                "message": OMITTED_REVIEWER_VALUE,
            }
        )
    return reasons


def _json_value(value: Any, *, depth: int = 0) -> Any:
    if isinstance(value, str):
        return _bounded_text(value)
    if isinstance(value, bool) or value is None:
        return value
    if isinstance(value, int):
        return value if -(10**18) <= value <= 10**18 else OMITTED_REVIEWER_VALUE
    if isinstance(value, float):
        return value if math.isfinite(value) else "<non-finite numeric value>"
    if isinstance(value, Decimal):
        if not value.is_finite() or len(value.as_tuple().digits) > MAX_REVIEWER_KEY_CHARACTERS:
            return "<non-finite or oversized numeric value>"
        return value
    if isinstance(value, dict):
        if depth >= MAX_REVIEWER_NESTING_DEPTH:
            return OMITTED_REVIEWER_VALUE
        bounded: dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= MAX_REVIEWER_COLLECTION_ITEMS:
                break
            rendered_key = str(key)
            if rendered_key == "raw_payload":
                continue
            safe_key = _bounded_text(rendered_key, maximum=MAX_REVIEWER_KEY_CHARACTERS)
            if safe_key not in bounded:
                bounded[safe_key] = _json_value(item, depth=depth + 1)
        return bounded
    if isinstance(value, (list, tuple)):
        if depth >= MAX_REVIEWER_NESTING_DEPTH:
            return OMITTED_REVIEWER_VALUE
        return [
            _json_value(item, depth=depth + 1) for item in value[:MAX_REVIEWER_COLLECTION_ITEMS]
        ]
    if hasattr(value, "isoformat"):
        return _bounded_text(str(value.isoformat()))
    if isinstance(value, UUID):
        return str(value)
    return _bounded_text(str(value))
