# Demo walkthrough

## Evidence status

This is the reviewer path through the implemented healthy, incident, investigation, and recovery
flows. Consult `STATUS.md` for exact commands, test counts, and results from the current tree; this
walkthrough does not duplicate those changing execution records.

## What the demo proves

The demo is successful only if it shows one connected path:

1. deterministic source objects are preserved in MinIO;
2. accepted rows and operational metadata are persisted in PostgreSQL;
3. contracts and dbt checks make quality failures visible;
4. an identical rerun does not duplicate accepted data;
5. MCP investigation returns bounded live evidence;
6. recovery returns the current state to healthy; and
7. the failed run and incident evidence still exist afterward.

## Prerequisites

- Python 3.12;
- `uv`;
- Docker Engine with Compose v2;
- free loopback ports for PostgreSQL, MinIO, and any optional app services; and
- no API key for the default deterministic explanation provider.

Create a local environment file from `.env.example` without committing it:

```powershell
Copy-Item .env.example .env
```

```bash
cp .env.example .env
```

The example credentials are synthetic and local-only. Do not reuse them outside this demo.

## 1. Bootstrap and infrastructure

Run tasks through Poe on every platform:

```text
uv run poe bootstrap
uv run poe up
```

`up` should wait for PostgreSQL and MinIO health and create the configured raw bucket idempotently.
Before continuing, inspect `docker compose ps`; do not interpret a started but unhealthy container as
a ready platform.

## 2. Healthy baseline and idempotent replay

```text
uv run poe demo
```

Run this command from a clean demo ledger. It intentionally refuses to call an all-duplicate first
run a baseline; use the guarded `uv run poe reset` first if the deterministic batch already exists.
The healthy demo generates a clean seeded batch, lands it, validates and loads it, runs dbt,
persists checks/artifacts, and finalizes a `healthy` run. Capture the emitted run ID and batch ID.

The command reruns the identical logical input as its second step. On the replay, verify:

- the file checksum matches the prior object;
- the file is reported as skipped/already processed;
- accepted business-row totals do not increase;
- a distinct pipeline run and duplicate quality check can still be audited; and
- the final state remains healthy.

Do not rely on “command exited zero” alone. Query the run/file ledger and modeled counts through the
CLI, API, or MCP service view used by the implementation.

Generated inputs are serialized to deterministic UTF-8 CSV before landing. To demonstrate exact
external-byte preservation separately, run `forgeflow generate`, then `forgeflow run-batch --path
<generated-directory>`: the validated manual CSV bytes, rather than a reserialization of parsed
rows, are sent to MinIO. The content ledger is not an append-only delivery-attempt table; a terminal
duplicate reuses the original file identity while the new run records a skip.

## 3. Controlled incident

```text
uv run poe incident-demo
```

The fixture is deterministic and must include all of the following without changing the clean
default generator:

| Injection | Boundary | Expected evidence |
|---|---|---|
| Missing required column (`priority`) | `maintenance_work_orders` file contract | Breaking schema-change record; rows quarantined with `missing_required_column`; raw object retained |
| Unexpected `firmware_revision` column | Telemetry file contract | Additive schema-change warning; raw column retained in the object and contract-known fields projected |
| `temperature_c = 999` | Telemetry row contract | Quarantined row with `out_of_range` and source lineage |
| Duplicate telemetry ID | Telemetry row contract | `duplicate_identifier` evidence; reviewer-facing fact contains one event |
| Inspection result `review` | Inspection row contract | Quarantined row with `invalid_enum` |
| Downtime timestamp in 2100 | Downtime row contract | Quarantined row with `future_timestamp` |
| Defect references unknown inspection | Dataset relationship contract | Quarantined row with `referential_integrity_violation` |
| Actual order quantity over 150% of plan | `assert_actual_quantity_within_plan` dbt test | Error-severity failed check and failed run |
| Telemetry event arriving 36 hours late | Event-time/incremental model | Arrival-lag/late evidence without treating a valid historical timestamp as malformed |

The fixture is older than the 24-hour late threshold but remains inside the incremental telemetry
model's 48-hour lookback. Verify it reaches `int_machine_telemetry` with `is_late_arrival=true` and
increments the daily late-arrival count. Events older than the lookback require an explicit bounded
backfill; raw acceptance alone does not prove late-arrival modeling.

The incident run should retain all possible metadata even though the business-rule/dbt check fails.
Specifically confirm a finish time, duration, counts, failed checks, parsed dbt artifacts, schema
changes, and affected downstream nodes.

## 4. Investigate through MCP

Start the configured `forgeflow-mcp` stdio server in an MCP client and use exact IDs from the run:

1. `get_latest_pipeline_status`
2. `get_data_quality_summary`
3. `list_failed_checks`
4. `get_failed_check_details`
5. `list_quarantined_records`
6. `get_downstream_impact`
7. `compare_pipeline_runs`
8. `explain_incident_evidence`
9. `generate_engineering_handoff`

The explanation must visibly separate:

- **Observed facts:** check IDs, count changes, drift, quarantine reasons, and lineage edges that were
  actually persisted.
- **Likely explanations:** plausible interpretations labeled uncertain.
- **Next steps:** bounded inspections an engineer can perform.

Reject the demo if the MCP response dumps a table, uses hardcoded incident text, omits evidence IDs,
or claims a confirmed root cause unsupported by the records.

## 5. Recover without erasing evidence

```text
uv run poe recover-demo
```

Recovery generates corrected input and reprocesses the bounded fixture. `recover-demo` resolves the
latest open incident to a concrete UUID before invoking the pipeline; the runner resolves only that
explicit ID after a healthy recovery. It
must not edit the historical raw incident object or delete quarantine/failed-check records.

For the production-rule fixture, the correction keeps business key
`ORD-BUSINESS-RULE-001` and changes the accepted quantity from 175 to 100 against a plan of 100. The
raw current-state row is upserted with new file/checksum lineage; the original incident object and
failed-run evidence remain immutable/queryable.

Verify all of these outcomes:

- a new recovery run ID exists;
- corrected input has its own content identity;
- replay/idempotency guards remain active;
- the previously failed business check passes for the recovered current state;
- freshness/current marts return to the expected state;
- the recovery run is healthy;
- the incident links baseline, failed, and recovery runs; and
- querying the old failed run returns the same evidence as before recovery.

## 6. Optional reviewer surfaces

Once implemented and verified, the FastAPI OpenAPI view can demonstrate typed pagination and the
Streamlit dashboard can show overview, run history, freshness, quality trends, failed checks,
quarantine summaries, factory/machine metrics, lineage impact, and the guided demo. These are views
over the shared service, not separate evidence stores.

If either surface is unavailable, the core pipeline/MCP evidence can still be reviewed, but the
corresponding hard acceptance criterion remains unverified.

## 7. Verification suite

After the walkthrough, run the configured gates rather than copying expected output into docs:

```text
uv run poe check
uv run poe test
uv run poe integration
uv run poe dagster-validate
uv run poe dbt-compile
uv run poe dbt-test
```

Container integration and dbt tasks require healthy services. Record command, timestamp, exit code,
test counts, and any environmental limitation in `STATUS.md`. A previous run or a generated badge is
not evidence for the current tree.

## 8. Shut down

```text
uv run poe down
```

Named volumes remain for replay and inspection. `uv run poe reset` invokes interactive
`forgeflow clean`; `forgeflow clean --force` is the noninteractive equivalent. Cleanup refuses any
CLI database except loopback `/forgeflow`, requires the configured data directory to be beneath the
immutable repository-root `.forgeflow/` subtree, prints the resolved deletion target in the prompt,
deletes warehouse/demo rows and that runtime directory, and intentionally retains MinIO objects and
Docker volumes.

## Presenter checklist

Keep these identifiers visible during a review:

| Item | Value to capture during execution |
|---|---|
| Healthy baseline run | `<run_id>` |
| Idempotent replay run | `<run_id>` and unchanged accepted count |
| Incident run | `<run_id>` |
| Failed check | `<check_id>` |
| Breaking drift | `<source_file_id>` / schema-change ID |
| Quarantine reason | `<reason_code>` and count |
| Impacted model | `<dbt unique_id>` |
| Incident | `<incident_id>` |
| Recovery run | `<run_id>` |

Placeholders above are capture fields, not sample results. Replace them only with actual execution
evidence, never invented UUIDs or metrics.
