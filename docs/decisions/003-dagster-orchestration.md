# ADR-003: Dagster coordinates the pipeline

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

A Python script could execute the ten pipeline steps with less framework code. The core demo,
however, needs inspectable dependencies, per-step metadata, safe retries, scheduling, dbt integration,
and failure propagation while still finalizing observability evidence.

## Decision

Model the workflow with three coherent Dagster assets: dependency readiness, the canonical pipeline
run, and the shared-service operational summary. Keep domain work in plain typed services. Retry the
dependency-readiness asset only; do not retry deterministic contract/dbt failures blindly. Treat
both `degraded` and `failed` pipeline outcomes as Dagster failures, and disable Dagster run-resume
attempts locally.

## Consequences

- Asset lineage plus PostgreSQL-persisted pipeline stages make the vertical slice easier to inspect.
- Dagster can coordinate dbt and persist structured materialization metadata.
- The framework adds concepts, dependencies, and deployment processes that a simple script avoids.
- ForgeFlow's PostgreSQL observability schema remains the cross-surface status authority; Dagster
  metadata is not duplicated as product logic.

## Verification

The definitions, daily job, UTC schedule, healthy-only propagation, `max_resume_run_attempts: 0`, and
`uv run poe dagster-validate` gate are implemented. Runtime evidence, rather than the presence of
definitions, is tracked in `STATUS.md`.
