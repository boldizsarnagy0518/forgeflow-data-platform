# ForgeFlow portfolio narrative

ForgeFlow is a completed, locally verified portfolio project built only with deterministic
synthetic data. It demonstrates data-platform reliability and incident reasoning; it does not claim
production readiness, real-plant validation, business impact, or measured savings.

## Positioning

### One sentence

ForgeFlow is a local-first industrial data reliability platform that turns deterministic synthetic
factory files into replayable source evidence, contracted warehouse data, observable failures, and
auditable recovery.

### 30-second pitch

I built ForgeFlow to show the part of data engineering that starts when a pipeline is not green. Ten
synthetic factory sources land in MinIO, pass through versioned contracts and structured quarantine,
load into PostgreSQL, and become tested dbt models coordinated by Dagster. One shared service powers
the CLI, FastAPI, MCP, and Streamlit. The deterministic incident injects drift, invalid rows,
duplicates, late data, and a dbt business-rule failure; the recovery fixes current state while the
failed run and all its evidence remain intact.

### Technical summary

The platform separates three identities: content checksum, domain event ID, and run ID. It lands raw
objects before validation, distinguishes file-level drift from row-level quarantine, and carries
batch, file, source-row, ingestion-time, and record-checksum lineage into PostgreSQL. Contracts are
explicitly versioned `1.0.0` across all ten sources.

dbt owns typed staging, intermediate logic, dimensions, facts, marts, tests, snapshots, freshness,
and lineage. Each execution uses an isolated artifact directory; ForgeFlow requires the expected
artifacts and queries actual model relations for row counts. Pipeline-stage status, timing, counts,
metadata, checks, freshness, lineage, comparisons, and incidents are persisted even when dbt fails.

Source-volume monitoring is intentionally explainable: after at least three comparable healthy
runs, it uses the median and MAD, with a tolerance no smaller than 20% of the median or three scaled
MADs. Incident explanations default to deterministic, offline evidence. The optional OpenAI provider
uses the same bounded typed schema and cannot authorize recovery or turn a hypothesis into a fact.

## Verified result

The latest local evidence exercises PostgreSQL, MinIO, contracts, dbt, artifacts, incidents, and
recovery end to end:

| Scenario | Result |
|---|---|
| Baseline | 1,399 accepted, 0 quarantined, `healthy` |
| Exact replay | 10 files skipped, 0 accepted, `healthy` |
| Incident | 95 accepted, 23 quarantined, 5 failed checks, `failed` |
| Recovery | 106 accepted, 0 quarantined, `healthy`, named incident resolved |
| Unit suite | 226 passed, 1 Windows symlink test skipped |
| Coverage | 87.44% configured branch coverage for the core package |
| Integration suite | 2 service-backed tests passed: lifecycle and atomic rollback/retry |
| Static/security gates | Ruff, strict Mypy, Bandit, and `pip-audit` passed |

A GitHub Actions workflow defines the quality and container-integration paths. This material does
not imply that a remote workflow run has been observed.

## Engineering skills demonstrated

| Area | Concrete repository evidence |
|---|---|
| Platform architecture | One coherent MinIO → contracts → PostgreSQL → dbt → observability path, coordinated by Dagster |
| Python | Python 3.12, typed public boundaries, Pydantic settings/models, domain exceptions, dependency injection, structured logs |
| Data contracts | Ten Pandera-backed `1.0.0` contracts; shape, type, null, enum, range, time, key, and cross-source reference checks |
| Reliability | Content and event idempotency, changed-row upserts, retryable file states, late-arrival handling, bounded backfills |
| Data quality | Structured quarantine, separate schema-drift records, dbt tests/freshness, median/MAD volume evidence |
| SQL and modeling | Parameterized operational SQL; dbt staging, intermediate, dimensional facts/marts, and SCD2 machine history |
| Observability | Persisted pipeline stages, normalized checks, artifacts, actual relation row counts, lineage, downstream impact, and run comparison |
| Incident design | Deterministic fixtures, facts/hypotheses separation, explicit incident-bound recovery, retained failure evidence |
| Product surfaces | One bounded `ForgeFlowService` reused by CLI, typed FastAPI, read-only MCP, and Streamlit |
| Security posture | Secret-safe configuration/logging, bounded reads, no arbitrary SQL, loopback services, least-privilege app DB role, reduced containers |
| Verification | 226 passing unit tests, 2 container integration tests, 87.44% coverage, strict typing/lint/security/dependency gates |
| Documentation | Architecture, contracts, model grains, observability, threat model, operations, production gaps, ADRs, and demo guide |

## CV-ready material

- Built a local-first synthetic industrial data reliability platform with Python, PostgreSQL,
  MinIO, dbt, and Dagster, preserving source-to-mart evidence across healthy, failed, replay, and
  incident-bound recovery runs.
- Implemented `1.0.0` source contracts, exact-object landing, structured quarantine and drift,
  checksum/event idempotency, late-arrival processing, median/MAD volume checks, and failure-safe dbt
  artifact capture.
- Unified bounded operational reads behind one typed service for FastAPI, MCP, CLI, and Streamlit;
  verified the core with 226 passing unit tests, 87.44% branch coverage, and real PostgreSQL/MinIO/dbt
  lifecycle and atomic-rollback integration tests.

### Longer project description

Designed and implemented ForgeFlow, a local-first portfolio platform for deterministic synthetic
factory data. It integrates replayable S3-compatible landing, versioned contracts, structured
quarantine, a PostgreSQL warehouse, dbt models/tests/freshness/lineage, Dagster orchestration,
durable run evidence, typed operational APIs, a read-only MCP server, and a Streamlit console. Its
controlled incident proves that raw inputs, invalid rows, downstream impact, and failed dbt evidence
remain inspectable. Recovery is linked to an explicit incident and restores current state without
deleting the failure history.

## Interview story

**Situation:** Many portfolio pipelines demonstrate only a clean batch and dashboard. I wanted to
show how I reason about unreliable data without using any real industrial or personal information.

**Task:** Build one laptop-sized system that could prove raw replay, contract enforcement,
idempotency, late-data handling, warehouse testing, downstream impact, bounded AI-client
investigation, and recovery.

**Action:** I defined source grains and failure semantics first, then implemented a vertical path
from synthetic objects through MinIO, contracts, PostgreSQL, dbt, and shared read surfaces. I kept
schema drift separate from row quarantine, persisted stage and dbt evidence on failure, used
isolated artifacts and actual relation counts, and made recovery require the specific incident ID.
I added deterministic fixtures and boundary-focused tests instead of relying on a scripted happy
path.

**Result:** The latest demo sequence produced a 1,399-row healthy baseline, skipped all ten identical
files on replay, exposed a failed incident with 95 accepted rows, 23 quarantined rows, and five
failed checks, then recovered with 106 accepted rows while preserving and resolving the named
incident. The core has 226 passing unit tests and 87.44% branch coverage, plus two real
container-backed integration tests covering the lifecycle and atomic rollback/retry.

## Design choices worth discussing

### Why both MinIO and PostgreSQL?

MinIO answers “what exact object did I receive and can I replay it?” PostgreSQL answers “what was
accepted, rejected, modeled, checked, and linked to this run?” Combining both concerns in one store
would weaken either replay or transactional operational evidence.

### Why distinguish drift from quarantine?

Drift describes the shape of a file. Quarantine describes parseable rows that violate declared
rules. Keeping them separate avoids invented row counts for an unreadable shape and makes the
remediation boundary explicit.

### Why dbt and Dagster?

dbt makes relational grains, tests, descriptions, exposures, freshness, and lineage inspectable.
Dagster coordinates dependencies and execution stages. Neither is allowed to replace ForgeFlow's
canonical PostgreSQL evidence schema, so CLI/API/MCP/dashboard semantics remain consistent.

### Why deterministic explanations?

Incident investigation must work offline and be reproducible. The deterministic provider organizes
only persisted evidence into observed facts, uncertain hypotheses, and bounded next steps. Optional
model-generated wording is enrichment, not authority.

### What makes recovery credible?

Recovery creates a new run and corrected source identity, and it must name the open incident being
resolved. It updates current warehouse state while retaining the original object, failed checks,
quarantine records, comparison evidence, and explanation.

## Visual evidence

### Operational dashboard

![ForgeFlow dashboard overview](docs/assets/forgeflow-dashboard-overview.png)

### Typed read API

![ForgeFlow FastAPI OpenAPI](docs/assets/forgeflow-api-openapi.png)

## Honest boundaries

- This is a single-user, laptop-scale portfolio system, not a production-ready service.
- There is no production authentication, authorization, TLS, managed secret store, network policy,
  rate limiting, high availability, disaster recovery, or organizational audit process.
- Compose uses fixed local demo credentials on loopback. They are intentionally obvious and unsafe
  outside an isolated development machine.
- The shared file ledger identifies `(source, checksum)` content globally. A later arrival of
  identical bytes is recorded as a skip within a new run, not as its own delivery-attempt entity.
  Production audit would separate content identity from every delivery attempt.
- Some service reads currently return an empty collection when a mart is unavailable; “no data” and
  “model unavailable” are not yet distinct at every read boundary.
- Exact-byte preservation covers validated manual CSVs passed through `run-batch` and generator
  objects. In-memory record-only callers receive deterministic canonical CSV serialization.
- The volume rule needs comparable healthy history and does not model seasonality. Synthetic
  distributions do not prove real sensor behavior, production scale, or business value.
- Processing is batch/incremental. Streaming is deliberately outside the current scope.
- The optional OpenAI path adds privacy, availability, cost, and nondeterminism risk. It is opt-in;
  offline deterministic explanation is the default.

For implementation detail, start with the [README](README.md), [architecture](docs/architecture.md),
[demo walkthrough](docs/demo-walkthrough.md), and [production considerations](docs/production-considerations.md).
