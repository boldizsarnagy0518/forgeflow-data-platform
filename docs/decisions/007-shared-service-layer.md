# ADR-007: Read surfaces share one service layer

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

FastAPI, MCP, CLI status commands, and Streamlit need the same run, quality, quarantine, lineage,
comparison, and incident answers. Direct SQL in each adapter would duplicate status rules, pagination,
redaction, and error behavior.

## Decision

Put canonical bounded queries and evidence assembly in `ForgeFlowService`. Inject repositories/I/O
boundaries into the service. Transport adapters validate their own inputs and serialize typed service
results but do not reimplement domain queries or calculations.

## Consequences

- Cross-surface answers and limits can be tested once and contract-tested at each adapter.
- SQL parameterization, redaction, and canonical run status are concentrated.
- The service API must avoid becoming a monolith; cohesive query/repository helpers remain separate.
- Surface-specific presentation can differ without changing evidence semantics.

## Verification

The implemented API, MCP, CLI status path, dashboard, and Dagster operational summary reuse
`ForgeFlowService`. Surface and integration evidence is tracked in `STATUS.md`.
