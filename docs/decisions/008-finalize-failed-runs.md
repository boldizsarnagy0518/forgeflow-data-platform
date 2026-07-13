# ADR-008: Failed runs still finalize evidence

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

The incident demo's most useful output is the failed checks, dbt artifacts, count deltas, lineage, and
error context. A conventional fail-fast path that exits before metadata finalization would make the
failure invisible or leave it indefinitely `running`.

## Decision

Create the run record before processing and persist each reached named stage. After dbt invocation,
parse available artifacts regardless of exit status. Persist available counts/checks/impact and
finalize in protected success/failure paths. A metadata failure cannot turn the primary result
healthy.

## Consequences

- Failed work remains diagnosable through API, MCP, dashboard, and artifacts.
- Error-handling code is more complex and must preserve primary versus secondary failures.
- Partial evidence is allowed only when fields clearly indicate unavailable/failed collection.
- A stale-running reconciliation policy is still needed for abrupt process termination.

## Verification

Failure-path evidence covers finish/error metadata, parsed failing dbt results, downstream impact,
stable failed state, and retained evidence after recovery; exact results live in `STATUS.md`. Abrupt
process death still needs an automated stale-run reconciler in a production design.
