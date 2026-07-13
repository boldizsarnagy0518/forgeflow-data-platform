# Threat model

## Scope and evidence status

This threat model covers the local ForgeFlow portfolio system: deterministic synthetic files,
MinIO, PostgreSQL, Python processes, dbt/Dagster, FastAPI, MCP stdio, Streamlit, and the optional
OpenAI explanation provider. It does not assert that ForgeFlow is secure or production ready.

Controls described here are visible implementation/configuration choices; their current execution
evidence is recorded in `STATUS.md`. Automated scanning can find classes of defects, but a passing
scan is not a security assessment.

## Security objectives

1. Keep credentials and connection strings out of source control, responses, logs, and artifacts.
2. Prevent untrusted input from becoming SQL, instructions, permissions, or silently accepted data.
3. Keep reviewer-facing reads bounded and expose no MCP mutations in the current interface.
4. Preserve the integrity and traceability of raw objects, accepted rows, quarantine, and run
   evidence through failure and recovery.
5. Restrict local services to the developer machine by default.
6. Make security and availability limitations explicit rather than relying on a portfolio label.

## Assets

| Asset | Why it matters |
|---|---|
| Source objects and checksums | Replay and proof of what was received |
| Accepted warehouse rows | Analytical correctness and lineage |
| Quarantine and schema-drift evidence | Explains why input was not accepted |
| Run/check/dbt/incident metadata | Auditability of failure and recovery |
| Database, MinIO, and optional API credentials | Access to local state and optional external service |
| dbt artifacts and model descriptions | Lineage and impact accuracy |
| MCP/API responses | Context passed to humans and AI clients |
| Repository and dependency lock | Reproducibility and supply-chain integrity |

All domain data is deterministic and synthetic. That reduces privacy impact but does not eliminate
integrity, credential, availability, supply-chain, or unsafe-action risk.

## Actors and assumptions

- A legitimate local developer can run CLI/Compose commands and owns the workspace.
- An untrusted source file may contain malformed values, oversized fields, formula-like strings, or
  text intended to influence an AI client.
- A local browser, MCP client, or process may be buggy or malicious.
- A compromised dependency/container image may execute with the privileges granted to it.
- The optional model provider is an external data processor when enabled.
- The local host and Docker daemon are trusted for the portfolio demo. If either is compromised,
  ForgeFlow cannot provide meaningful isolation.

The default local environment has no multi-user authentication or tenant boundary. Loopback binding
is a deployment assumption, not authorization.

## Trust boundaries

| Boundary | Data crossing | Required control |
|---|---|---|
| B1: Host/user -> CLI, API, dashboard | Arguments, query parameters, environment | Typed validation, bounded values, no shell interpolation |
| B2: Generator/source -> MinIO | Exact validated manual bytes or canonical generated CSV | Content-addressed key, checksum, byte/row bounds, symlink/path containment |
| B3: MinIO -> validator/loader | Parsed untrusted records | Strict version `1.0.0` contracts, quarantine/drift separation, missing-parent rejection |
| B4: Python/dbt -> PostgreSQL | Parameterized data and metadata queries | Least-privilege roles, transactions, constraints, statement/time limits |
| B5: Service -> API/dashboard | Structured operational data | Pagination, redaction, safe errors, authentication before production |
| B6: Service -> MCP client over stdio | Tool schemas, evidence, errors | Read-only catalog, hard caps, untrusted-text labeling, stdout discipline |
| B7: Explanation layer -> OpenAI | Compact structured evidence | Explicit opt-in/key, 50 KB request cap, 1,200 output tokens, 30-second timeout, `store=false`, validated response |
| B8: Containers -> host/network | Ports, volumes, process privileges | Loopback ports, dropped capabilities, no-new-privileges, read-only FS where feasible |
| B9: CI/dependency sources -> build | Packages, actions, images | Locking/pinning, provenance review, scanning, controlled updates |

## Threat and control register

| ID | Threat / abuse case | Local design control | Residual risk and production action |
|---|---|---|---|
| T01 | Documented demo credentials are reused or service ports are exposed | `.env` ignored, local-only labels, loopback port binds | Defaults are public knowledge. Use managed identity/Key Vault, rotation, private networking, and deny public access |
| T02 | SQL injection through API/MCP filters or model names | Typed allowlists, parameterized SQL, shared bounded service queries | Add DB roles, statement timeout, query audit, fuzz/property tests, and an authorization policy |
| T03 | Oversized files/fields exhaust memory, disk, DB, or context windows | Manual CSV bytes (25 MiB default), rows (250,000 default), reviewer lists, artifacts, and OpenAI evidence/output are capped | CSV is still read in memory and has no per-field limit. Production needs streaming parsing, gateway/object limits, quotas, budgets, and alerts |
| T04 | A malformed file is partially accepted without trace | Bounded CLI parse first; then exact-byte/parsed-row binding, checksum, breaking-drift state, row quarantine, atomic per-source accepted/quarantine/drift/file completion, failed-file marking, and idempotent retry | Pre-parse failures are not landed, and object storage plus the complete multi-source batch are not one distributed transaction. Production needs a queued boundary, dead-letter ownership, crash tests, and alerts |
| T05 | Same object/event is replayed to inflate facts | Unique source/checksum ledger plus event natural-key deduplication and monotonic `updated_at` upserts | The content row is not an append-only delivery-attempt record; cross-region replay needs global identity policy |
| T06 | An object is overwritten, hiding original incident input | Content-addressed object identity; changed content receives a new key/ledger row | Local MinIO has no WORM enforcement or explicit predecessor FK. Production needs object lock/versioning and separate roles |
| T07 | Quarantined payload or credentials leak in logs/API/MCP | Structured safe logs, redacted responses, reason summaries, no raw payload by default | Developers with DB/object access can still read synthetic raw data; production requires classification and access audit |
| T08 | CSV/formula content executes after export | Treat all source strings as data; avoid spreadsheet export by default | If export is added, neutralize formula prefixes and content-disposition risks and test it |
| T09 | Stored text instructs an AI client to ignore rules | Evidence fields are untrusted; tool schemas separate data from instructions; deterministic provider default | AI clients may still be susceptible. Use content labeling, minimization, policy checks, and human approval for actions |
| T10 | Optional LLM invents a cause or sends excess evidence externally | Deterministic incident persisted first; 50 KB/1,200-token/30-second/`store=false` limits; typed output; facts regenerated locally | Provider processing/residency and model behavior still require organizational legal/security review |
| T11 | MCP client triggers repair/backfill unexpectedly | No mutation tools are registered; `FORGEFLOW_ENABLE_WRITES` does not add one | Any future write requires authenticated principals, authorization, approvals, bounded scope, and audit |
| T12 | MCP stdout logging corrupts protocol or leaks evidence | Protocol on stdout, diagnostics on stderr, bounded safe errors | Add process supervision and integration tests with a real client |
| T13 | Stale or wrong dbt artifacts are attached to a run | Per-run build/freshness target separation, retry cleanup, required-artifact schema checks, build invocation-ID correlation, and PostgreSQL advisory lock | Advisory locking covers ForgeFlow jobs sharing the key/database, not arbitrary external dbt processes; sign/archive artifacts in production |
| T14 | Failure/recovery deletes or rewrites incident evidence | Failed run/checks remain; corrected input gets a new identity; incident status/recovery link updates explicitly; cleanup is guarded | DB administrators can alter local state. Production needs immutable audit export, retention locks, and separation of duties |
| T15 | Container compromise escapes or moves laterally | Compose declares dropped capabilities/no-new-privileges and loopback exposure; app containers read-only where feasible | Docker daemon is highly privileged. Use patched managed nodes, image policy, non-root users, network policy, and runtime monitoring |
| T16 | Vulnerable or malicious dependency/image enters build | `uv.lock`, Bandit and pip-audit tasks, Gitleaks CI, and SHA-pinned GitHub Actions | Scanners and locks do not prove provenance. Pin image digests, generate an SBOM, sign artifacts, and control updates |
| T17 | API/dashboard is accessed by another local process/user | Loopback-only binding, read-only app filesystem, explicit `forgeflow_reader` URL, no `.env`/S3/OpenAI variable injection, and no object-store client | Reader credentials and code defaults are public local values; direct DB access can read quarantine payloads. Add identity, authorization, session controls, CSRF/CORS review, and TLS |
| T18 | Secrets appear in exception strings or configuration diagnostics | Secret types and `safe_summary`; domain errors omit credential-bearing URLs | Review third-party exceptions and central log processors; add redaction tests and secret canaries |
| T19 | Incomplete finalization makes a failed run look healthy/absent | Run created before pipeline work; protected finalization and artifact parsing survive normal dbt failures; timestamps expose old `running` rows | There is no automated stale-run reconciler. Add watchdog, termination tests, and durable queue/orchestrator state in production |
| T20 | Destructive cleanup targets resources outside the demo | Prompt includes the resolved target or `--force`; CLI only permits loopback `/forgeflow` plus a path below the immutable repository-root `.forgeflow/` subtree; the repository requires an explicit confirmation flag and additionally permits isolated `/forgeflow_test`; MinIO/volumes retained | A local DB owner remains powerful. Production cleanup needs separate privileges, approvals, and immutable audit |

## Data integrity controls by stage

- **Landing:** bounded manual parsing precedes landing; accepted manual bytes are then preserved
  exactly, while generated rows use canonical CSV bytes.
- **Contract:** strict columns/types/nullability/ranges/enums/times; breaking drift separate from row
  quarantine.
- **Load:** parameterized writes, transaction boundaries, unique constraints, reconciliation counts.
- **Transform:** dbt tests and manifest lineage; no silent coercion in marts.
- **Observe:** persist named stage/run/check records, isolate/parse dbt artifacts, flag missing
  required evidence, and query actual safe relation counts.
- **Serve:** shared service, validation, pagination/caps, stable redaction.
- **Explain:** persist deterministic output first; optional bounded OpenAI enrichment, epistemic
  labels, typed validation, no authority, no provider calls on reads.
- **Recover:** new run/input identity, explicit open incident UUID, safe bounded reprocessing, old
  evidence retained.

## Privacy and responsible use

Only synthetic operator/technician/inspector IDs are allowed. Fixtures, screenshots, logs, tests,
and examples must be checked for real names, employer/university identifiers, customer data, machine
identifiers, emails, and copied industrial payloads. If the system were adapted to real data, privacy
impact assessment, purpose limitation, retention/deletion, subject rights, regional processing, and
access controls would be required before ingestion.

## Security verification plan

The configured security task is:

```text
uv run poe security
```

It runs Bandit static analysis followed by `pip-audit`. CI separately runs Gitleaks over repository
history. The configured Gitleaks action is appropriate for this personal-account portfolio; an
organization-owned repository would need its licensing/secret strategy reviewed (including fork
pull requests) or should invoke the OSS CLI directly. Container/image scanning is not currently
configured. Verification records tool versions, commands, results, and accepted findings in
`STATUS.md`. A green result does not test authorization design, runtime network exposure, cloud
policy, data governance, social engineering, or unknown vulnerabilities.

## Explicitly out of scope locally

Authentication/authorization, TLS, managed secrets, multi-tenant isolation, immutable regulatory
audit, high availability, disaster recovery, organizational incident response, and cloud perimeter
controls are not implemented by a local Compose demo. Their concrete production treatment is in
[production considerations](production-considerations.md).
