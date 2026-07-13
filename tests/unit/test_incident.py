"""Evidence-grounding tests for deterministic and optional incident explanations."""

from __future__ import annotations

import json
from typing import cast
from uuid import UUID

import pytest
from openai import OpenAI, OpenAIError
from pydantic import ValidationError

from forgeflow.config import Settings
from forgeflow.errors import ForgeFlowError
from forgeflow.incident import DeterministicExplanationProvider, OpenAIExplanationProvider
from forgeflow.models import IncidentEvidence, IncidentExplanation, SchemaChange

INCIDENT_ID = UUID("00000000-0000-0000-0000-000000000010")
FAILED_RUN_ID = UUID("00000000-0000-0000-0000-000000000011")
BASELINE_RUN_ID = UUID("00000000-0000-0000-0000-000000000012")


def _evidence() -> IncidentEvidence:
    return IncidentEvidence(
        incident_id=INCIDENT_ID,
        failed_run_id=FAILED_RUN_ID,
        baseline_run_id=BASELINE_RUN_ID,
        failed_checks=[
            {
                "check_name": "telemetry_freshness",
                "scope": "machine:M-002",
                "observed_value": "31 hours",
                "expected": "fresh within 24 hours",
            },
            {
                "check_name": "production_quantity_tolerance",
                "scope": "order:O-017",
                "observed_value": 175,
                "expected": "actual quantity at most 150% of plan",
            },
        ],
        row_count_changes={"fct_production": -12, "stg_telemetry": 3},
        quarantine_reasons={
            "machine_telemetry:duplicate_identifier": 2,
            "machine_telemetry:out_of_range": 1,
        },
        schema_changes=[
            SchemaChange(
                source_name="maintenance_work_orders",
                change_type="breaking",
                expected_columns=["work_order_id", "priority"],
                actual_columns=["work_order_id", "owner"],
                missing_columns=["priority"],
                unexpected_columns=["owner"],
            )
        ],
        affected_models=["fct_production", "mart_factory_performance"],
        log_events=["dbt test production_quantity_tolerance failed"],
    )


class _ParsedResponse:
    def __init__(self, explanation: IncidentExplanation | None) -> None:
        self.output_parsed = explanation


class _FakeResponses:
    def __init__(self, explanation: IncidentExplanation | None | Exception) -> None:
        self._explanation = explanation
        self.calls: list[dict[str, object]] = []

    def parse(self, **kwargs: object) -> _ParsedResponse:
        self.calls.append(kwargs)
        if isinstance(self._explanation, Exception):
            raise self._explanation
        return _ParsedResponse(self._explanation)


class _FakeOpenAI:
    def __init__(self, explanation: IncidentExplanation | None | Exception) -> None:
        self.responses = _FakeResponses(explanation)


def _openai_settings() -> Settings:
    return Settings(ai_provider="openai", OPENAI_API_KEY="unit-test-key")


def test_deterministic_explanation_separates_recorded_facts_from_hypotheses() -> None:
    result = DeterministicExplanationProvider().explain(_evidence())

    assert result.provider == "deterministic"
    assert result.evidence_run_ids == [FAILED_RUN_ID, BASELINE_RUN_ID]
    assert any("31 hours" in fact and "within 24 hours" in fact for fact in result.observed_facts)
    assert any("Quarantine recorded 2" in fact for fact in result.observed_facts)
    assert any("missing columns: priority" in fact for fact in result.observed_facts)
    assert any("fct_production=-12" in fact for fact in result.observed_facts)
    assert any("mart_factory_performance" in fact for fact in result.observed_facts)
    assert result.likely_explanations
    assert all("not confirmed" in item.lower() for item in result.likely_explanations)
    assert "not confirmed" in result.uncertainty_note.lower()


def test_deterministic_explanation_has_safe_fallback_when_evidence_is_empty() -> None:
    evidence = IncidentEvidence(incident_id=INCIDENT_ID, failed_run_id=FAILED_RUN_ID)

    result = DeterministicExplanationProvider().explain(evidence)

    assert result.likely_explanations == []
    assert result.observed_facts == [
        "No failed-check, quarantine, schema-change, or row-count evidence was recorded."
    ]
    assert result.recommended_next_steps == [
        "Inspect run finalization and artifact capture before inferring a cause."
    ]


def test_openai_provider_regenerates_facts_and_labels_model_hypotheses() -> None:
    candidate = IncidentExplanation(
        provider="openai",
        observed_facts=["The operator deliberately sabotaged the machine."],
        likely_explanations=["A source export may have stopped"],
        recommended_next_steps=["Inspect the retained source object."],
        uncertainty_note="Model-authored note",
        evidence_run_ids=[BASELINE_RUN_ID, FAILED_RUN_ID],
    )
    fake = _FakeOpenAI(candidate)
    provider = OpenAIExplanationProvider(_openai_settings(), cast(OpenAI, fake))

    result = provider.explain(_evidence())

    assert result.provider == "openai"
    assert result.evidence_run_ids == [FAILED_RUN_ID, BASELINE_RUN_ID]
    assert "sabotaged" not in " ".join(result.observed_facts).lower()
    assert (
        result.observed_facts
        == DeterministicExplanationProvider().explain(_evidence()).observed_facts
    )
    assert result.likely_explanations == [
        "Likely explanation (not confirmed): A source export may have stopped"
    ]
    assert "unconfirmed" in result.uncertainty_note.lower()

    call = fake.responses.calls[0]
    sent = json.loads(cast(str, call["input"]))
    assert sent["failed_run_id"] == str(FAILED_RUN_ID)
    assert "raw_payload" not in cast(str, call["input"])
    assert call["store"] is False
    assert call["max_output_tokens"] == 1_200
    assert call["text_format"] is IncidentExplanation


def test_openai_provider_rejects_malicious_evidence_provenance() -> None:
    attacker_run_id = UUID("ffffffff-ffff-ffff-ffff-ffffffffffff")
    candidate = IncidentExplanation(
        provider="openai",
        observed_facts=["Unsupported claim"],
        likely_explanations=["This is definitely proven"],
        recommended_next_steps=["Delete the evidence."],
        uncertainty_note="None",
        evidence_run_ids=[FAILED_RUN_ID, attacker_run_id],
    )
    provider = OpenAIExplanationProvider(_openai_settings(), cast(OpenAI, _FakeOpenAI(candidate)))

    with pytest.raises(ForgeFlowError, match="provenance validation"):
        provider.explain(_evidence())


def test_openai_provider_rejects_missing_structured_output() -> None:
    provider = OpenAIExplanationProvider(_openai_settings(), cast(OpenAI, _FakeOpenAI(None)))

    with pytest.raises(ForgeFlowError, match="no structured incident explanation"):
        provider.explain(_evidence())


def test_openai_provider_rejects_oversized_evidence_before_request() -> None:
    fake = _FakeOpenAI(None)
    provider = OpenAIExplanationProvider(_openai_settings(), cast(OpenAI, fake))
    evidence = _evidence().model_copy(update={"log_events": ["x" * 50_000]})

    with pytest.raises(ForgeFlowError, match="50000-byte"):
        provider.explain(evidence)

    assert fake.responses.calls == []


def test_openai_provider_translates_sdk_failures_without_exposing_details() -> None:
    fake = _FakeOpenAI(OpenAIError("sensitive upstream response"))
    provider = OpenAIExplanationProvider(_openai_settings(), cast(OpenAI, fake))

    with pytest.raises(ForgeFlowError, match="OpenAIError") as exc_info:
        provider.explain(_evidence())

    assert "sensitive" not in str(exc_info.value)


def test_openai_provider_translates_structured_output_validation_failures() -> None:
    with pytest.raises(ValidationError) as validation_error:
        IncidentExplanation.model_validate({"provider": "openai"})
    fake = _FakeOpenAI(validation_error.value)
    provider = OpenAIExplanationProvider(_openai_settings(), cast(OpenAI, fake))

    with pytest.raises(ForgeFlowError, match="ValidationError"):
        provider.explain(_evidence())
