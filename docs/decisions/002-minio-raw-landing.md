# ADR-002: MinIO is the raw landing boundary

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

A local filesystem is simpler, but direct file ingestion makes object identity, replay, checksum
metadata, overwrite behavior, and cloud-shaped access boundaries easy to ignore. ForgeFlow needs to
show that rejected data remains recoverable independently of warehouse state.

## Decision

After bounded CLI parsing, land the exact successfully parsed manual CSV bytes in an S3-compatible
MinIO bucket before contract validation. Canonically serialize generated rows and land those bytes.
Calculate and record checksum, source, batch, object identity, ingestion time, and processing status.
Preserve changed content as a new content-addressed identity; PostgreSQL stores the content ledger.

## Consequences

- Raw bytes survive contract, database, dbt, and recovery failures.
- Exact duplicate, failed-file retry, and changed-file behavior can be tested explicitly.
- S3-compatible code provides a useful migration seam toward managed object storage.
- MinIO adds credentials, health, bucket initialization, volume, and network failure modes.
- S3 API compatibility does not make MinIO operationally equivalent to a managed cloud service.
- The ledger is unique by source/checksum and is not an append-only record of every delivery
  attempt. Local MinIO does not enforce WORM retention.

## Verification

Evidence requires successful exact-byte/canonical landing, checksum skip, failed-file retry,
changed-content preservation, contract rejection with raw retention, and recovery. Pre-parse manual
file failures are rejected before landing by design.
