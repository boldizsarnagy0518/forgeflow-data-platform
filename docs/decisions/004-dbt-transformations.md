# ADR-004: dbt owns warehouse transformations

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

Python-only transformations could share one language with ingestion, but would require ForgeFlow to
recreate SQL model dependency, testing, documentation, source freshness, exposures, and lineage
artifact conventions. The analytical logic is predominantly relational.

## Decision

Use dbt Core with the PostgreSQL adapter for staging, intermediate, marts, incremental/history
models, tests, descriptions, freshness, and artifacts. Keep source validation and quarantine in
Python. Serialize local relation mutation with a PostgreSQL advisory lock and assign every ForgeFlow
run an isolated dbt `--target-path`. Keep build artifacts at the run root and source-freshness
artifacts in a child target, pass identical bounded variables, and correlate the build manifest and
results by dbt invocation ID.

## Consequences

- SQL grains, tests, documentation, and lineage are explicit and reviewer-friendly.
- dbt artifacts provide downstream-impact evidence after both successful and failed invocations;
  zero-exit commands without required, structurally valid, invocation-correlated artifacts fail.
- Manifest-declared safe model relations are queried for actual row counts; adapter
  `rows_affected` is not reported as the canonical relation count.
- Logic spans Python and SQL, so boundaries and metric definitions must remain documented.
- dbt invocation/artifact lifecycle adds failure modes that must be finalized correctly.

## Verification

Current evidence requirements are compile/test on healthy data, the deliberate business-rule
failure, parsed failure artifacts/impact, required-artifact failure, late-arrival/incremental
coverage, actual relation counts, and generated documentation. Results live in `STATUS.md`.
