# ForgeFlow decision log

Detailed rationale for ADR-001 through ADR-010 lives under `docs/decisions/`. This index records the
final local verification state and therefore supersedes any proposal-time "verification pending"
wording in those original design notes. Decisions added during final reliability review are recorded
directly here because they refine, rather than replace, the accepted architecture.

| ID | Decision | Why | Final status/evidence |
|---|---|---|---|
| ADR-001 | PostgreSQL is the system of record | It provides realistic schemas, constraints, transactions, dbt adapter support, and operational metadata in one local service. | Accepted and verified by the container integration test and read-only smoke queries. |
| ADR-002 | MinIO is the replayable raw landing boundary | Object bytes, checksums, and file status must survive contract, database, dbt, and recovery failures. | Accepted and verified by healthy, exact-replay, incident, and recovery ingestion. |
| ADR-003 | Dagster exposes orchestration over the pipeline service | Assets, dependency/failure visibility, retry policy, and scheduling are useful without making Dagster a second status authority. | Accepted; definitions/import and service boundary are locally tested. PostgreSQL remains authoritative. |
| ADR-004 | dbt owns relational transformation, tests, freshness, and lineage artifacts | SQL models, snapshots, tests, descriptions, exposures, and artifacts remain explicit and reviewer-friendly. | Accepted and verified in baseline, incident failure, recovery, and direct integration execution. |
| ADR-005 | Deterministic evidence explanation is the default | Investigation must work offline and must not present generated text as evidence. | Accepted and verified by incident/service tests and persisted recovery history. |
| ADR-006 | Streaming is deferred | Batch/incremental reliability, replay, contracts, late data, observability, and recovery are the core learning goal. | Accepted; streaming remains an explicit non-goal rather than an incomplete path. |
| ADR-007 | API, MCP, CLI, and dashboard share `ForgeFlowService` | One bounded query/incident layer prevents inconsistent status and evidence semantics. | Accepted and verified through unit/surface tests plus live API/dashboard reads. |
| ADR-008 | Failed runs still finalize available evidence | Failed checks, stages, artifacts, lineage, impact, and primary error are the diagnosis product. | Accepted and verified by the deliberate failed run: 23 quarantined rows and 5 failed checks remained inspectable after recovery. |
| ADR-009 | Poe is the cross-platform task interface; Make is a thin alias | The audited Windows environment lacks GNU Make, while reviewers should see one stable task vocabulary. | Accepted and verified by local Poe bootstrap/check/Compose/demo tasks. |
| ADR-010 | OpenAI is optional, bounded, and schema-compatible | A model may enrich phrasing, but the core cannot depend on a key, network, cost, or nondeterministic output. | Accepted; the boundary is mocked/tested, requests are bounded and non-stored, and no paid live call is claimed. |
| ADR-011 | dbt uses an isolated artifact target per pipeline run and requires success artifacts | Shared or stale `target` files could attribute evidence to the wrong run or turn missing evidence into false health. | Accepted and covered by dbt-runner/reliability tests and the real incident path. |
| ADR-012 | Recovery must explicitly identify the open incident it resolves | Resolving the latest incident after any healthy batch could close unrelated evidence. | Accepted and verified by incident-specific recovery tests and the 106/0 recovery run. |
| ADR-013 | Persist deterministic explanation before optional AI enrichment; reads never call the model | Provider failure, cost, or latency must not prevent incident creation or turn a read into an outbound side effect. | Accepted and covered at the provider/service boundary. |
| ADR-014 | Operational reads use a dedicated local database reader and no object-store credentials | API/dashboard require observability and mart reads, not warehouse administration or MinIO access. | Accepted and verified by healthy read-only role queries and app-container healthchecks. |
| ADR-015 | Source-volume detection uses a transparent median/MAD rule over bounded healthy history | A simple explainable anomaly check is more appropriate than an opaque model for deterministic portfolio data. | Accepted and unit-tested; evidence records history, bounds, and whether enough samples existed. |
| ADR-016 | Safety bounds and explicit terminal states take precedence over permissive retry | File size/row/path limits, parameterized queries, terminal-file dedupe, and resumable nonterminal failures make retries observable without unsafe input handling. | Accepted and covered across object-store, contract, pipeline, warehouse, and CLI tests. |

## Deliberately unresolved by architecture

- Hosted CI evidence awaits repository/remote creation; local success is not represented as a remote
  run.
- Production IAM, TLS, network isolation, managed secrets, HA, backups, disaster recovery, and SLO
  operations require deployment-specific decisions.
- Abrupt process-death reconciliation and distributed dbt scheduling would need stronger coordination
  than the local single-platform lock and durable run ledger.
