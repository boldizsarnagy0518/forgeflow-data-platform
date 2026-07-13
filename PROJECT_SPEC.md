# ForgeFlow project specification

## Product statement

**ForgeFlow - an AI-assisted industrial data reliability platform** demonstrates how a compact,
local-first data platform can ingest, validate, model, observe, investigate, and safely recover
deterministic synthetic factory data.

It is a portfolio system, not a production-ready service. It uses no employer, university, customer,
personal, or real industrial data.

## Users and questions

The primary user is a data/platform engineer investigating reliability. ForgeFlow answers:

- which machines or sources are stale, late, anomalous, or unreliable;
- which run introduced a failure and what changed;
- which records were quarantined and for what structured reasons;
- which models and read surfaces are downstream of a failed check;
- how output, downtime, maintenance, inspection, and defect metrics differ by factory or machine;
- which statements are observed facts, which are hypotheses, and what evidence to inspect next.

## Domain model

`factory -> production_line -> machine` is the master-data spine. Shifts cover factory time windows
and reference anonymous operator IDs. Production orders belong to lines. Telemetry, downtime, and
maintenance work orders belong to machines. Inspections belong to production orders; defects belong
to inspections.

All source records are deterministic from a seed. Accepted rows receive batch ID, source file ID,
source row number, ingest timestamp, and record checksum lineage. Older updates cannot overwrite a
newer master record.

## System boundaries

- **MinIO** preserves replayable source bytes behind checksum-addressed object identities.
- **Pandera contracts** enforce versioned source shape, types, nullability, enums, ranges,
  timestamps, identifiers, duplicates, and parent relationships. Row failures enter quarantine;
  file drift is persisted separately.
- **PostgreSQL** is the system of record for `raw`, `staging`, `intermediate`, `marts`,
  `observability`, and `quarantine`. Application read containers use a dedicated local read-only role.
- **dbt** owns transformations, incremental late-arrival handling, history, tests, freshness,
  documentation, exposures, and lineage artifacts. Each pipeline run captures artifacts in an
  isolated target directory.
- **Dagster** exposes assets/jobs/scheduling and propagates workflow failure while PostgreSQL remains
  the cross-surface run-status authority.
- **FastAPI, MCP, CLI, and Streamlit** use bounded views over `ForgeFlowService`; transport layers do
  not duplicate incident or comparison logic. MCP is read-only by default.
- **Optional OpenAI provider** may enrich persisted deterministic incident evidence with validated,
  bounded structured output. Deterministic explanation is the offline default; model text is never
  authorization or independently verified root cause.

## Run states

- `healthy`: required stages/checks pass and no rows are quarantined.
- `degraded`: the pipeline completes with quarantine or warning evidence, including an intentionally
  skipped dbt stage.
- `failed`: infrastructure, transformation, required-artifact, or error-severity quality failure
  prevents a healthy result.
- `running`: the terminal state has not yet been persisted.

A run records timestamps, duration, batch and file/row counts, stages, accepted/quarantined rows,
available dbt results/artifacts, checks, schema changes, freshness, downstream impact, and evidence
summary. Failure finalization preserves the primary error and all evidence available at that point;
it does not pretend unavailable evidence exists.

## Deterministic incident scenario

1. Generate and run a clean baseline.
2. Replay identical input and prove no accepted-row duplication.
3. Inject a missing required column, impossible measurement, duplicate event, contract-valid
   business-rule violation, and late/stale event.
4. Preserve every landed object; quarantine contract failures and allow the deliberate dbt rule to
   fail visibly.
5. Persist failed checks, stages, artifacts, drift, and downstream impact even though the run fails.
6. Compare baseline and failed runs without converting unavailable measurements into false zeroes.
7. Produce observed facts, explicitly unconfirmed hypotheses, and next steps from persisted evidence.
8. Run a corrected batch, explicitly name the open incident being recovered, reprocess idempotently,
   and verify healthy recovery without deleting failed-run evidence.

## Verified local acceptance envelope

- Latest full demo baseline: 1,399 accepted, 0 quarantined.
- Exact replay: 10 files skipped, 0 accepted.
- Deliberate incident: 95 accepted, 23 quarantined, 5 failed checks.
- Recovery: 106 accepted, 0 quarantined, named incident resolved with history retained.
- Quality: 226 non-integration tests, two container integration tests, 87.44% coverage, Ruff,
  strict Mypy, Bandit, and dependency audit passing.
- Surfaces: live API and dashboard health, OpenAPI, dashboard browser rendering, and read-only mart
  queries verified locally.

The authoritative per-criterion evidence is in `STATUS.md`. A hosted GitHub Actions execution is not
part of the verified envelope because the workspace has no Git repository or remote.

## Non-goals and production boundary

Kafka/Redpanda, Spark, Kubernetes, Airflow, Terraform, real cloud resources, automatic operational
repair, and a production-readiness claim are not part of the core. Production identity, TLS, network
policy, managed secrets, HA, backup/restore, disaster recovery, organizational audit policy, and
large-scale performance validation are intentionally deferred and must be added before real use.
