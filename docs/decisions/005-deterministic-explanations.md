# ADR-005: Deterministic explanations are the default

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

An LLM can phrase an incident fluently but may be unavailable, costly, nondeterministic, or tempted
to state unsupported causes. ForgeFlow must work without a key and distinguish evidence from
hypothesis in repeatable tests.

## Decision

Build a deterministic provider that converts a bounded typed evidence bundle into observed facts,
likely explanations, recommended next steps, uncertainty, and evidence run IDs. Persist that result
when creating the incident. Optional enrichment may replace only the explanation after persistence;
read surfaces deserialize the stored value and never invoke a provider.

## Consequences

- Offline demos and evaluation fixtures are stable and auditable.
- Explanations cannot discover facts the platform failed to record.
- Deterministic prose may be less natural and less adaptable to novel evidence combinations.
- Provider failure cannot prevent or erase deterministic incident persistence.
- Service/API/MCP/dashboard reads have stable semantics and no paid outbound side effect.

## Verification

Fixtures cover relevant evidence, facts/hypotheses separation, bounded output, stable results, and
refusal to claim a confirmed cause when the bundle does not support one. Execution evidence remains
in `STATUS.md`.
