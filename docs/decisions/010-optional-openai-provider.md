# ADR-010: The OpenAI provider is optional and schema-compatible

- Status: Accepted and implemented as an opt-in boundary
- Date: 2026-07-10

## Context

ForgeFlow can demonstrate responsible AI integration without making the pipeline depend on network
access or an API key. If an external model is enabled, its input must be minimized and its output must
retain the same evidence/uncertainty semantics as the deterministic provider.

## Decision

Keep `FORGEFLOW_AI_PROVIDER=deterministic` as default. Enable OpenAI only when explicitly selected
with a nonempty `OPENAI_API_KEY`; use the configurable model setting. Persist deterministic incident
evidence/explanation first. Send at most 50,000 UTF-8 bytes of `IncidentEvidence`, set a 30-second
timeout, `max_output_tokens=1200`, and `store=false`, then validate the response into
`IncidentExplanation`. Regenerate observed facts locally and accept model output only for hypotheses
and recommended actions.

## Consequences

- Core demos, tests, and MCP investigation work without external service access.
- Opt-in model output can improve hypotheses/actions while remaining substitutable.
- External processing introduces cost, latency, availability, retention/residency, injection, and
  nondeterminism concerns.
- Generated text remains untrusted explanation, never evidence, permission, or an automated repair.
- Provider errors leave the deterministic incident intact; later reads never call the provider.

## Verification

Mocked boundary tests cover opt-in configuration, request limits, storage flag, output budget,
timeout/error handling, invalid/provenance schema, uncertainty fields, and secret-safe errors. A real
paid API call is deliberately not required for CI.
