# ADR-001: PostgreSQL is the system of record

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

ForgeFlow needs warehouse schemas, constraints, transactional ingestion/metadata writes, concurrent
read surfaces, and a dbt adapter. An embedded analytical database would make local setup lighter and
could be faster for single-process scans, but it would underrepresent the database boundaries and
concurrency the portfolio is intended to demonstrate.

## Decision

Use PostgreSQL for accepted raw rows, staging/intermediate/marts, quarantine, and operational
metadata. Keep schema ownership explicit: `raw`, `staging`, `intermediate`, `marts`,
`observability`, and `quarantine`. Use parameterized queries and transactions; MinIO remains the
replayable raw-object store.

## Consequences

- One SQL system supports dbt, constraints, idempotency keys, metadata, and bounded service reads.
- Reviewers see realistic connection, migration, and transaction boundaries.
- Docker/daemon availability and database lifecycle add local operational cost.
- PostgreSQL is not assumed to be the economical answer for unlimited analytical scale; a measured
  production workload could justify a separate warehouse while operational metadata stays
  transactional.

## Implementation notes

Compose initializes explicit warehouse/observability schemas plus a local `forgeflow_reader` role.
API/dashboard use that read-only role; ingestion and dbt use the local owner role. Current command and
integration evidence is recorded in `STATUS.md`.
