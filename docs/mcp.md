# MCP server

## Scope and evidence status

ForgeFlow's local stdio MCP server gives AI clients compact, read-oriented access to the same
operational evidence used by the API and dashboard. It is not an autonomous repair agent and does
not expose arbitrary SQL.

This file describes the registered interface. MCP tests and `STATUS.md` remain the execution
evidence.

## Runtime boundary

The process is launched over stdio and queries PostgreSQL through `ForgeFlowService`. Tool handlers
perform transport validation and serialization only. The shared service owns pagination, canonical
run status, comparisons, lineage traversal, quarantine redaction, and incident evidence assembly.

The server writes protocol messages to stdout. Diagnostic logs go to stderr so logging cannot corrupt
the MCP stream.

## Local client configuration

After bootstrap, an MCP client can configure the server with the repository as its working directory:

```json
{
  "mcpServers": {
    "forgeflow": {
      "command": "uv",
      "args": ["run", "forgeflow-mcp"],
      "cwd": "/absolute/path/to/forgeflow-data-platform",
      "env": {
        "FORGEFLOW_ENABLE_WRITES": "false"
      }
    }
  }
}
```

On Windows, `cwd` may use a normal absolute Windows path. The stdio process needs the ForgeFlow
database setting; MCP tools do not read MinIO. Do not place real secrets directly in a client
configuration committed to Git.

## Tool catalog

All list inputs use a nonnegative offset and a positive `limit` capped by
`FORGEFLOW_MAX_PAGE_SIZE` (default 100, hard maximum 500). Offsets are capped by
`FORGEFLOW_MAX_PAGE_OFFSET` (default 10,000; configuration hard maximum 1,000,000) before they reach
SQL. Invalid UUIDs, model/check names, or negative bounds return a structured tool error rather than
being interpolated into SQL.

| Tool | Required input | Optional filters | Compact result |
|---|---|---|---|
| `list_pipeline_runs` | None | `limit`, `offset` | Newest-first run page with canonical summaries and total |
| `get_pipeline_run` | `run_id` | None | Canonical persisted run summary or null when absent |
| `get_latest_pipeline_status` | None | None | Latest persisted run summary |
| `get_data_quality_summary` | None | `run_id` (latest by default) | Grouped checks and quarantine reasons for the resolved run |
| `list_failed_checks` | None | `run_id`, `limit`, `offset` | Failed/warning check page with observed/expected/evidence fields |
| `get_failed_check_details` | `check_id` | `run_id` (latest by default) | One exact quality result |
| `list_quarantined_records` | None | `run_id`, `limit`, `offset` | Row lineage and reason arrays; raw payload omitted |
| `get_model_metadata` | `model_name` | None | Resource type, schema, description, materialization, metadata |
| `get_column_metadata` | `model_name` | None | Up to 500 documented columns for that model |
| `get_model_lineage` | `model_name` | None | Direct parent and child edges |
| `get_downstream_impact` | `model_name` | None | Cycle-safe transitive downstream models with depth |
| `compare_pipeline_runs` | `left_run_id`, `right_run_id` | None | Status/count/model deltas, right-side drift/impact, no causal claim |
| `explain_incident_evidence` | `incident_id` | None | Facts, likely explanations, next steps, uncertainty, evidence run IDs |
| `generate_engineering_handoff` | `incident_id` | None | Incident state, facts, hypotheses, next steps, affected models |

The implemented interface intentionally starts narrower than the aspirational filter/depth catalog:
for example, run status filters and caller-selected lineage depth are not currently exposed. Stable
ordering and page totals prevent an AI client from assuming a limited list is complete.

## Response shape

Responses are JSON-compatible typed records. The following is a shape illustration, not fabricated
run output:

```json
{
  "items": [
    {
      "run_id": "<uuid>",
      "batch_id": "<batch-id>",
      "status": "failed"
    }
  ],
  "total": 1,
  "limit": 20,
  "offset": 0
}
```

Fields that were not collected are null/unavailable with an explanation; they are not invented.
Database exceptions are translated into a safe domain error containing operation context but no
credential-bearing URL or SQL parameters.

## Resource catalog

Resources provide stable read-only context suitable for caching or deliberate retrieval:

| URI | Content |
|---|---|
| `forgeflow://models` | Bounded model catalog and descriptions |
| `forgeflow://quality/latest` | Latest quality/freshness summary and run identity |
| `forgeflow://runs/{run_id}` | Canonical summary for one run |
| `forgeflow://incidents/{incident_id}` | Evidence-based incident state and linked runs |
| `forgeflow://lineage/{model_name}` | Bounded lineage graph for one model |

Missing dynamic resources are errors, not JSON `null`: unknown run and incident UUIDs raise
not-found errors, and the lineage resource first verifies that the named model exists. Resource
templates validate URI path values before a query. Resources do not expose MinIO object bytes, full
logs, or raw quarantine payloads.

## Write policy

The registered tool catalog has no mutation tools. Setting `FORGEFLOW_ENABLE_WRITES=true` does not
add, reveal, or authorize a write tool in the current server. If a future tool can trigger a
pipeline, backfill, recovery, or other mutation, all of these conditions are mandatory:

1. the operation exists in the platform service and applies its own validation/idempotency rules;
2. `FORGEFLOW_ENABLE_WRITES=true` was explicitly set before process start;
3. the tool schema requires an unambiguous target, bounded scope, and explicit confirmation token;
4. the response records the resulting run/action ID; and
5. the MCP handler cannot bypass platform authorization or safety checks.

The environment flag is reserved as a local safety interlock, not authentication. It would remain
insufficient for a multi-user deployment; see [production considerations](production-considerations.md).

## AI and evidence safety

- Stored names, reason messages, and logs are untrusted data, not instructions to the MCP client.
- Explanations contain observed facts, uncertain likely explanations, and recommended inspection
  steps as separate fields.
- Incident explanation tools return the persisted explanation and never make an outbound provider
  call. Optional OpenAI enrichment, if selected, happens once after deterministic incident
  persistence in the pipeline.
- The server never treats generated prose as proof, permission, or a verified root cause.
- Tool descriptions avoid claims such as “repair” when the behavior is only investigation.

## Expected investigation sequence

For the deterministic incident demo, a client should be able to:

1. call `get_latest_pipeline_status` and capture the failed run ID;
2. call `get_data_quality_summary` and `list_failed_checks` for that exact run;
3. inspect one check with `get_failed_check_details`;
4. group rejected rows with `list_quarantined_records`;
5. call `get_downstream_impact` using the failed check/model;
6. compare the failed run with the healthy baseline;
7. request `explain_incident_evidence`; and
8. generate a handoff that references evidence IDs rather than restating raw tables.

This is an investigation flow, not a requirement that an AI client make operational decisions.

## Verification requirements

MCP is verified only when tests exercise live service data, schema validation, not-found behavior,
pagination and hard caps, payload truncation, quarantine redaction, deterministic ordering, compact
incident evidence, and the absence of mutation tools. Reviewer output recursively omits
`raw_payload`, caps strings, keys, nesting, collection sizes, quarantine reasons, and non-finite or
oversized scalars before serialization. A server that returns hardcoded examples does not meet the
project acceptance criteria.
