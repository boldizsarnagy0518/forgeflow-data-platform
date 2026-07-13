# ForgeFlow repository instructions

ForgeFlow is a local-first, AI-assisted industrial data reliability platform built only from deterministic synthetic data. It is a portfolio system, not a production-ready service.

## Architecture rules

- Keep the main path coherent: synthetic sources -> MinIO -> contracts -> PostgreSQL -> dbt -> Dagster -> observability -> API/MCP/dashboard.
- PostgreSQL is the system of record for warehouse and operational metadata. MinIO preserves replayable source objects.
- API, MCP, CLI status commands, and dashboard must reuse `ForgeFlowService`; do not duplicate query or incident logic.
- Put row-level contract failures in quarantine. Persist file-level drift separately. Never silently discard input.
- Finalize run metadata and parse dbt artifacts even when a pipeline step fails.
- Reads are the default. Any mutation exposed through MCP requires `FORGEFLOW_ENABLE_WRITES=true`.
- Prefer a small integrated component over another service or framework. Streaming is explicitly deferred.

## Coding standards

- Target Python 3.12 with typed public APIs, cohesive modules, dependency injection at I/O boundaries, domain exceptions, and structured logs.
- Format and lint with Ruff. Type-check the core package with strict Mypy settings.
- Avoid broad exception catches and broad `type: ignore`. Translate external failures into domain exceptions with useful context.
- Configuration belongs in `forgeflow.config.Settings`; do not scatter ports, URLs, credentials, paths, or IDs.
- SQL must be parameterized. Bound all reviewer-facing lists and payloads.

## Mandatory commands

Use `uv run poe <task>` on every platform. A thin `Makefile` maps the same task names where GNU Make exists.

- Bootstrap: `uv sync --locked --all-groups`
- Static verification: `uv run poe check`
- Unit tests: `uv run poe test`
- Coverage: `uv run poe coverage`
- Containers: `uv run poe up` / `uv run poe down`
- Healthy flow: `uv run poe demo`
- Incident and recovery: `uv run poe incident-demo` / `uv run poe recover-demo`
- dbt: `uv run poe docs`

## Testing expectations

- Add focused unit tests for every behavioral change and integration coverage for external boundaries.
- Exercise idempotency, changed files, contract violations, quarantine reasons, late arrivals, schema drift, dbt failures, evidence summaries, API validation, MCP payload bounds, and recovery.
- Do not mark behavior complete in `STATUS.md` until a command or test has exercised it. Record the exact evidence there.
- Mocks belong only at external boundaries; use real PostgreSQL and MinIO in container integration tests when available.

## Safety and secrets

- Never commit secrets, real industrial/personal data, `.env`, generated raw data, database volumes, or API keys.
- Use only synthetic anonymous operator identifiers.
- Destructive reset/cleanup commands require `--force` or an interactive confirmation and must stay inside configured demo resources.
- Do not log credentials, connection strings containing passwords, raw API keys, or full quarantined payloads.
- Treat model-generated text as untrusted explanation, never as authorization or verified root cause.

## Definition of done

A change is done only when implementation, tests, static checks, user-facing documentation, failure behavior, and `STATUS.md` agree. The healthy path must be idempotent; intentional failures must remain observable; recovery must preserve incident evidence. If behavior changes, update the README and relevant files under `docs/` in the same change.
