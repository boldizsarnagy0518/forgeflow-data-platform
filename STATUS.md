# ForgeFlow status

Last updated: 2026-07-13

Status meanings: **completed** means implemented and reviewed; **verified** means exercised by the
evidence below; **in progress** is active work inside the agreed scope; **blocked** requires an
external state change; **deferred** is deliberately outside this local portfolio system.

## Completed

- The coherent path from deterministic synthetic sources through MinIO, contracts, PostgreSQL,
  dbt, Dagster, observability, and the shared API/MCP/dashboard service layer.
- Ten industrial source domains, lineage-bearing raw tables, structured quarantine, file-level
  schema drift, late-arrival handling, volume checks, dbt models/tests/snapshot, incident evidence,
  and explicit recovery.
- Cross-platform Poe tasks, locked dependencies, Docker Compose infrastructure and app profiles,
  least-privilege read surfaces, bounded file/API/MCP payloads, and local safety controls.
- Architecture, data model, contracts, observability, MCP, demo, threat-model, production,
  troubleshooting, decision, portfolio, and reviewer-orientation documentation.
- One pinned, least-privilege GitHub Actions workflow for quality and integration jobs.

## Verified

- Locked Python 3.12 environment; Ruff format/lint and strict Mypy both pass.
- Unit/surface/reliability suite: **226 passed, 1 Windows symlink test skipped, 2 integration tests deselected**.
- Branch-aware core-package coverage: **87.44%**, above the 80% gate. Dashboard and Dagster
  definition modules are intentionally excluded from that numeric gate and are checked separately.
- Security gates: Bandit reported no issues; `pip-audit` reported no known vulnerabilities in
  resolved third-party packages. The local editable project was correctly skipped as a non-PyPI
  distribution.
- Docker Compose configuration, `uv run poe up`, PostgreSQL/MinIO health, application image build,
  API health, and dashboard health all passed locally.
- Container integration suite: **2 passed, 227 deselected in 230.00 seconds**. The lifecycle test proved baseline **333 accepted /
  0 quarantined**; exact replay **10 files skipped / 0 accepted**; incident **95 accepted /
  23 quarantined / 5 failed checks**; recovery **106 accepted / 0 quarantined**, with prior incident
  evidence retained. The second test forced the final per-source warehouse operation to fail, proved
  raw and quarantine writes rolled back atomically, then proved a retry succeeded.
- The local read-only application role returned healthy platform status, **10 freshness rows**, and
  **3 factory-performance rows**.
- Browser verification loaded the live dashboard and FastAPI OpenAPI surface with no browser console
  errors.

## In progress

- None inside the verified local portfolio scope.

## Blocked

- A hosted GitHub Actions run cannot be evidenced until this workspace is initialized as a Git
  repository and pushed to a remote. The workflow is implemented and locally equivalent gates pass,
  but this external execution is not claimed as verified.

## Deferred

- Streaming/Redpanda, cloud infrastructure, Kubernetes, and distributed scale testing.
- Production identity, network policy, TLS, secret-manager integration, HA, backup/restore drills,
  disaster recovery, and organization-specific authorization/audit controls.
- A paid live OpenAI request. The deterministic provider is the default; the optional boundary is
  covered without making CI depend on a key, network, cost, or nondeterministic output.

## Known non-blocking limitation

- FastAPI/Starlette `TestClient` emits an upstream deprecation warning from its current dependency
  combination. Response behavior, OpenAPI, and live browser checks pass; the warning is not hidden.
- One real directory-symlink cleanup test is skipped on this Windows host because directory symlink
  creation is unavailable; simulated symlink/path-containment tests and the rejection code still run.
- Dagster definition validation passes but its current CLI emits a supersession warning recommending
  `dg check defs`; the installed dependency set does not expose that replacement command.
- Generated coverage, dbt targets/logs, bytecode, and normal caches were removed for handoff. A few
  ignored test/cache directories remain because they were created under a different Windows sandbox
  security identity and the current user lacks ownership privileges; they contain only synthetic
  pytest/uv cache data, not source, credentials, or runtime demo evidence.

## Hard acceptance criteria

| # | Criterion | Status | Evidence |
|---:|---|---|---|
| 1 | Clean documented bootstrap | Verified locally | Locked environment supported every recorded gate and demo command. |
| 2 | Docker Compose infrastructure | Verified | Compose config, `poe up`, PostgreSQL, MinIO, app builds, and healthchecks passed. |
| 3 | Healthy demo | Verified | Latest full demo baseline completed healthy with 1,399 accepted and 0 quarantined rows. |
| 4 | Idempotent repeat ingestion | Verified | Exact replay skipped all 10 source files and accepted 0 new rows. |
| 5 | Structured quarantine | Verified | Incident retained 23 rejected rows with contract evidence. |
| 6 | Healthy dbt models/tests | Verified | Baseline and recovery dbt execution/artifacts completed without failed required checks. |
| 7 | Intentional visible incident | Verified | Incident run finished failed by design. |
| 8 | Failed checks and downstream impact | Verified | Five failed checks and parsed dbt/lineage evidence were persisted. |
| 9 | Evidence facts versus hypotheses | Verified | Typed deterministic explanations and uncertainty labels are covered by tests and visible in reads. |
| 10 | Healthy recovery | Verified | Corrected batch accepted 106, quarantined 0, and resolved only its named incident. |
| 11 | Validated FastAPI responses | Verified | Unit validation plus live OpenAPI/browser checks passed. |
| 12 | Compact structured MCP results | Verified | Bounded typed handlers/resources are covered in the 226-test passing suite. |
| 13 | MCP writes disabled by default | Verified | Published MCP surface is read-only; write enablement defaults false. |
| 14 | Dashboard using actual data | Verified | Live browser session rendered persisted run, freshness, and factory data without console errors. |
| 15 | Unit/integration tests | Verified | 226 non-integration tests plus two container integration tests passed. |
| 16 | Ruff | Verified | Format check and lint are green. |
| 17 | Mypy | Verified | Strict Mypy over `src` and `tests` is green. |
| 18 | Secret-free CI | Completed; hosted run blocked | Pinned workflow, minimal permissions, synthetic local credentials, and local security gates exist; no remote is available to run GitHub Actions/gitleaks. |
| 19 | No real credentials/personal data | Verified | Only deterministic synthetic records and documented local-only credentials are used. |
| 20 | README claims verified | Verified locally | Claims are limited to the recorded local commands, results, and explicit limitations. |
| 21 | No placeholders/fake implementations | Verified for acceptance paths | Healthy, replay, incident, recovery, API, dashboard, warehouse, and security paths executed. |
| 22 | Limitations documented | Verified | Local-only, production-control, OpenAI, warning, and hosted-CI limits are explicit. |
| 23 | Branding claims map to implementation | Verified | Reliability, investigation, recovery, AI-assistance, and industrial-domain claims map to exercised components. |

## Evidence log

| Date | Command/evidence | Result |
|---|---|---|
| 2026-07-13 | `uv run poe test` | 226 passed, 1 Windows symlink test skipped, 2 integration tests deselected |
| 2026-07-13 | `uv run poe check` | 226 passed, 1 Windows symlink test skipped, 2 integration tests deselected; 87.44% coverage; Ruff/Mypy/Dagster/Bandit/pip-audit passed |
| 2026-07-13 | `uv run poe integration` | 2 passed, 227 deselected in 230.00 s: full lifecycle plus atomic per-source rollback/retry |
| 2026-07-13 | Compose config and `uv run poe up` | PostgreSQL and MinIO healthy; bucket initialization passed |
| 2026-07-13 | `docker compose --profile app up -d --build --wait` | Final-source API and dashboard images rebuilt; all four services healthy |
| 2026-07-13 | `uv run poe down` | Full app profile stopped and removed; final Compose container list empty |
| 2026-07-13 | Read-only role smoke query | Health healthy; 10 freshness rows; 3 factory-performance rows |
| 2026-07-13 | Live browser verification | Dashboard and FastAPI OpenAPI loaded; no console errors |
| 2026-07-13 | `uv run forgeflow clean --force` | Local demo rows/files removed; MinIO replay objects and volumes retained |
| 2026-07-13 | `uv run poe demo` | Default 14-day baseline healthy: 1,399 accepted/0 quarantined; replay healthy: 10 skipped/0 accepted |
| 2026-07-13 | `uv run poe incident-demo` | Baseline healthy; named incident failed with 95 accepted, 23 quarantined, and 5 failed checks |
| 2026-07-13 | `uv run poe recover-demo` | Healthy: 106 accepted/0 quarantined; resolved the exact persisted incident ID |
| 2026-07-13 | `uv run poe dbt-compile` | 30 models, 1 snapshot, 174 data tests, 12 sources, and 3 exposures compiled |
| 2026-07-13 | `uv run poe dbt-test` | PASS=174, WARN=0, ERROR=0, SKIP=0 |
| 2026-07-13 | `uv run poe docs` | Manifest and PostgreSQL catalog generated successfully |
| 2026-07-13 | `uv run poe dagster-validate` | All code locations passed definition/workspace validation |
| 2026-07-13 | In-process `forgeflow_daily_job` | Dagster run succeeded; canonical pipeline run healthy with 6 accepted/0 quarantined |
