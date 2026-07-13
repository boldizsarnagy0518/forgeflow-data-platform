# ForgeFlow public quality rubric

Final local score: **90/100**. Target: **85/100**, with no security or functional category below
70% of its weight.

Scores are evidence-based weighted points, not feature counts. A perfect score would require broader
environments, exhaustive failure testing, hosted CI evidence, and production controls that are
explicitly outside this portfolio scope.

| Category | Weight | Earned | Evidence and deduction |
|---|---:|---:|---|
| Functional completeness | 14 | 13 | Healthy, replay, incident, and recovery ran end to end; hosted Linux CI remains unexecuted. |
| Architectural coherence | 9 | 8.5 | Every mandatory component participates in one main path and read surfaces reuse `ForgeFlowService`; local deployment is intentionally single-node. |
| Data engineering depth | 11 | 10 | Ten domains, lineage, contracts, quarantine, drift, late data, incremental models, snapshot/history, freshness, anomaly check, and useful marts are implemented; distributed/streaming scale is deferred. |
| Reliability and idempotency | 10 | 9 | Exact replay, changed/error paths, isolated dbt artifacts, explicit incident recovery, and container integration are tested; abrupt process-death reconciliation and multi-process stress are not exhaustive. |
| Data quality | 9 | 8.5 | Contract, relationship, business-rule, freshness, volume, quarantine, and dbt checks produce readable evidence; broader statistical evaluation is outside scope. |
| Observability | 8 | 7 | Durable run/stage/file/check/freshness/schema/artifact/impact/incident evidence is queryable; no production metrics backend, tracing, paging, or SLO alerting is included. |
| MCP and AI usefulness | 8 | 7 | Bounded read-only investigation tools and persisted fact/hypothesis explanations work offline; no paid live OpenAI evaluation is claimed. |
| Software engineering quality | 8 | 7.5 | Typed boundaries, Ruff, strict Mypy, 226 passing unit/surface tests, two integration tests, and 87.44% gated coverage pass; dashboard/Dagster are excluded from the numeric coverage gate and `TestClient` has an upstream warning. |
| Security | 7 | 6 | Parameterized SQL, bounded inputs, path checks, read-only app role/containers, secret-safe defaults, Bandit, dependency audit, and pinned CI actions are present; production IAM/TLS/secrets/network controls are deferred and hosted gitleaks has not run. |
| Developer experience | 5 | 4.5 | Locked bootstrap, Poe task vocabulary, Make shim, Compose healthchecks, demos, and troubleshooting are coherent; a fresh hosted clone has not been exercised. |
| Documentation | 6 | 5 | Architecture, domain, contracts, observability, MCP, demo, threat, production, troubleshooting, ADR, status, and evidence docs are present; they have not been validated by an external user. |
| Portfolio clarity | 5 | 4 | Reviewer path, browser-verified surfaces, deterministic scenario, and honest limitations are clear; there is no Git history, remote CI badge, or published deployment evidence. |
| **Total** | **100** | **90** | **Locally verified portfolio target exceeded by 5 points.** |

## Verification basis

- `uv run poe check`: 226 passed, 1 Windows symlink test skipped, 2 integration tests deselected,
  and 87.44% coverage above the 80% gate.
- Ruff format/lint and strict Mypy: green.
- `uv run poe security`: Bandit found no issues; pip-audit found no known vulnerabilities in
  resolved third-party packages.
- Full PostgreSQL/MinIO/dbt integration: 2 passed, covering healthy/replay/incident/recovery plus
  atomic per-source warehouse rollback and retry.
- Compose infrastructure and app healthchecks, read-only application queries, live dashboard, and
  live OpenAPI were verified locally.

## Scoring method

Each category earns 0-100% of its weight: 0 absent; 25 scaffolding; 50 implemented but weakly
exercised; 70 credible working core; 85 strong and well tested; 95 unusually complete; 100 exhaustive
evidence with no material limitation. Known limitations reduce the relevant category even when they
are deliberate and documented.
