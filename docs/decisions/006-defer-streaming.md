# ADR-006: Streaming is deferred from the core

- Status: Accepted and implemented as a scope constraint
- Date: 2026-07-10

## Context

Kafka/Redpanda would demonstrate event transport but introduce brokers, schemas, offsets, consumer
operations, and additional failure modes. The primary learning goal is trustworthy batch/incremental
ingestion, replay, contracts, late data, modeling, observability, and recovery.

## Decision

Use deterministic historical and incremental file batches in the mandatory architecture. Do not add
Kafka, Redpanda, Spark, or a streaming framework to the core. A future Redpanda experiment must be a
clearly separated extension after all core acceptance criteria pass.

## Consequences

- The main demo remains laptop-sized and every mandatory component has a clear role.
- Event-time, late-arrival, deduplication, replay, and idempotency are still exercised without a
  broker.
- ForgeFlow makes no low-latency or streaming-scale claim.
- A future streaming design would need contract registry, offset/replay semantics, backpressure,
  exactly-once/at-least-once decisions, and additional observability.

## Revisit condition

Revisit only when a measured requirement cannot be met by scheduled incremental batches and the core
healthy/incident/recovery flow is already verified.
