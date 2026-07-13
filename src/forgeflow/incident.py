"""Evidence-grounded deterministic and optional OpenAI incident explanation providers."""

from __future__ import annotations

import json
from typing import Protocol

from openai import OpenAI, OpenAIError
from pydantic import ValidationError

from forgeflow.config import Settings
from forgeflow.errors import ForgeFlowError
from forgeflow.models import IncidentEvidence, IncidentExplanation

_MAX_OPENAI_EVIDENCE_BYTES = 50_000
_MAX_OPENAI_OUTPUT_TOKENS = 1_200
_OPENAI_TIMEOUT_SECONDS = 30.0


class ExplanationProvider(Protocol):
    """Boundary for producing a structured explanation from compact evidence."""

    def explain(self, evidence: IncidentEvidence) -> IncidentExplanation:
        """Explain only the facts present in the evidence bundle."""


class DeterministicExplanationProvider:
    """Offline, testable rules that never upgrade a hypothesis into a fact."""

    def explain(self, evidence: IncidentEvidence) -> IncidentExplanation:
        facts: list[str] = []
        hypotheses: list[str] = []
        next_steps: list[str] = []

        for check in evidence.failed_checks[:10]:
            name = str(check.get("check_name", check.get("check_id", "unknown check")))
            scope = str(check.get("scope", "unknown scope"))
            status = str(check.get("status", "failed"))
            observed = check.get("observed_value")
            expected = check.get("expected", "the configured rule")
            facts.append(
                f"Check {name} recorded status {status} for {scope}: "
                f"observed {observed!s}; expected {expected}."
            )
            lowered = f"{name} {scope} {expected}".lower()
            if "fresh" in lowered or "stale" in lowered:
                hypotheses.append(
                    "Likely explanation (not confirmed): the affected source or machine stopped "
                    "delivering recent events, or a late batch arrived outside its expected window."
                )
                next_steps.append(
                    f"Inspect the latest source object and event timestamps for {scope}."
                )
            if "production" in lowered or "quantity" in lowered:
                hypotheses.append(
                    "Likely explanation (not confirmed): an order export contains an output value "
                    "outside the documented planning tolerance."
                )
                next_steps.append(
                    f"Compare planned and actual order quantities in the source evidence for {scope}."
                )

        for reason_code, count in sorted(evidence.quarantine_reasons.items()):
            facts.append(f"Quarantine recorded {count} occurrence(s) of reason {reason_code}.")
            lowered = reason_code.lower()
            if "duplicate" in lowered:
                hypotheses.append(
                    "Likely explanation (not confirmed): the source replayed a business key within "
                    "the same delivery."
                )
                next_steps.append(
                    "Compare duplicate keys and source-row numbers in quarantine metadata."
                )
            if "range" in lowered or "measurement" in lowered or "enum" in lowered:
                hypotheses.append(
                    "Likely explanation (not confirmed): a source-system mapping or sensor export "
                    "produced a value outside the published contract."
                )
                next_steps.append("Inspect the rejected column, value class, and contract limit.")

        for change in evidence.schema_changes:
            missing = ", ".join(change.missing_columns) or "none"
            unexpected = ", ".join(change.unexpected_columns) or "none"
            facts.append(
                f"{change.source_name} had a {change.change_type} schema change; missing columns: "
                f"{missing}; unexpected columns: {unexpected}."
            )
            hypotheses.append(
                "Likely explanation (not confirmed): the producer changed its export schema before "
                "the ForgeFlow contract was updated."
            )
            next_steps.append(
                f"Compare the {change.source_name} object header with its versioned data contract."
            )

        if evidence.row_count_changes:
            rendered = ", ".join(
                f"{name}={delta:+d}" for name, delta in sorted(evidence.row_count_changes.items())
            )
            facts.append(f"Run comparison recorded row-count changes: {rendered}.")
            next_steps.append(
                "Check whether each row-count delta matches the intended batch scope."
            )

        if evidence.affected_models:
            facts.append(
                "Recorded lineage marks these downstream models as affected: "
                + ", ".join(sorted(evidence.affected_models)[:20])
                + "."
            )
            next_steps.append(
                "Rebuild and retest the smallest affected lineage selection after repair."
            )

        for event in evidence.log_events[:5]:
            facts.append(f"Pipeline log evidence: {event}")

        if not facts:
            facts.append(
                "No failed-check, quarantine, schema-change, or row-count evidence was recorded."
            )
            next_steps.append(
                "Inspect run finalization and artifact capture before inferring a cause."
            )
        if not next_steps:
            next_steps.append(
                "Inspect the failed check evidence and its direct upstream model first."
            )

        return IncidentExplanation(
            provider="deterministic",
            observed_facts=_deduplicate(facts)[:30],
            likely_explanations=_deduplicate(hypotheses)[:20],
            recommended_next_steps=_deduplicate(next_steps)[:20],
            uncertainty_note=(
                "Likely explanations are rule-based hypotheses, not confirmed root causes. "
                "Confirm them against the retained source object and platform evidence."
            ),
            evidence_run_ids=[evidence.failed_run_id]
            + ([evidence.baseline_run_id] if evidence.baseline_run_id else []),
        )


class OpenAIExplanationProvider:
    """Optional Responses API provider constrained to the shared evidence schema."""

    def __init__(self, settings: Settings, client: OpenAI | None = None) -> None:
        if settings.openai_api_key is None:
            raise ForgeFlowError("The OpenAI provider requires OPENAI_API_KEY")
        self._model = settings.openai_model
        self._client = client or OpenAI(
            api_key=settings.openai_api_key.get_secret_value(),
            timeout=_OPENAI_TIMEOUT_SECONDS,
        )

    def explain(self, evidence: IncidentEvidence) -> IncidentExplanation:
        """Send only compact synthetic metadata and parse a validated structured result."""
        serialized_evidence = json.dumps(
            evidence.model_dump(mode="json"),
            ensure_ascii=False,
            separators=(",", ":"),
        )
        evidence_size = len(serialized_evidence.encode("utf-8"))
        if evidence_size > _MAX_OPENAI_EVIDENCE_BYTES:
            raise ForgeFlowError("Incident evidence exceeds the 50000-byte OpenAI request limit")
        try:
            response = self._client.responses.parse(
                model=self._model,
                instructions=(
                    "Explain this synthetic data-platform incident using only supplied evidence. "
                    "Put direct observations in observed_facts. Label every unproven cause as a "
                    "likely explanation. Recommend concrete inspections. Set provider to 'openai'. "
                    "Never claim a root cause is confirmed unless the evidence explicitly proves it."
                ),
                input=serialized_evidence,
                max_output_tokens=_MAX_OPENAI_OUTPUT_TOKENS,
                store=False,
                text_format=IncidentExplanation,
            )
        except (OpenAIError, ValidationError) as exc:
            raise ForgeFlowError(
                f"OpenAI incident explanation failed ({type(exc).__name__})"
            ) from exc
        parsed = response.output_parsed
        if parsed is None:
            raise ForgeFlowError("OpenAI returned no structured incident explanation")
        candidate = parsed
        expected_run_ids = [evidence.failed_run_id] + (
            [evidence.baseline_run_id] if evidence.baseline_run_id else []
        )
        if candidate.provider != "openai" or set(candidate.evidence_run_ids) != set(
            expected_run_ids
        ):
            raise ForgeFlowError("OpenAI explanation failed evidence provenance validation")
        deterministic = DeterministicExplanationProvider().explain(evidence)
        hypotheses = [
            hypothesis
            if "not confirmed" in hypothesis.lower()
            else f"Likely explanation (not confirmed): {hypothesis}"
            for hypothesis in candidate.likely_explanations
        ]
        return IncidentExplanation(
            provider="openai",
            # Facts are rendered locally from evidence; the model may only enrich hypotheses/actions.
            observed_facts=deterministic.observed_facts,
            likely_explanations=hypotheses,
            recommended_next_steps=candidate.recommended_next_steps,
            uncertainty_note=(
                "Model suggestions are unconfirmed. Observed facts were regenerated locally from "
                "the persisted evidence bundle and must be verified against source artifacts."
            ),
            evidence_run_ids=expected_run_ids,
        )


def build_explanation_provider(settings: Settings) -> ExplanationProvider:
    """Select the offline default unless OpenAI was explicitly configured."""
    if settings.ai_provider == "openai":
        return OpenAIExplanationProvider(settings)
    return DeterministicExplanationProvider()


def _deduplicate(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
