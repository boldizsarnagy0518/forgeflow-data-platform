# Troubleshooting

## Evidence status and safety

This guide describes diagnostic paths for the implemented local system. A command's presence does
not prove it succeeded in the current environment; see `STATUS.md` for executed evidence.

Preserve the failing run, object, logs, dbt artifacts, and checks before cleanup. Do not paste `.env`,
password-bearing database URLs, API keys, full quarantined payloads, or unbounded logs into an issue.
Do not run reset/volume deletion as an early diagnostic step.

## Fast triage

1. Record the exact command, timestamp, exit code, and run/batch ID if one exists.
2. Check `STATUS.md` for a known environmental limitation.
3. Check services with `docker compose ps` and inspect only the relevant bounded logs.
4. Determine the failed boundary: bootstrap, container health, landing, contract, load, dbt,
   finalization, service query, or client transport.
5. Compare persisted run/file/check evidence with process output; do not rely on one alone.
6. Capture diagnostics before any repair or reset.

## Task reference

Use the cross-platform Poe tasks rather than invoking internal modules directly:

```text
uv run poe bootstrap
uv run poe up
uv run poe down
uv run poe test
uv run poe integration
uv run poe dbt-compile
uv run poe dbt-test
uv run poe dagster-validate
uv run poe check
```

Run `uv run poe reset` only when intentional destructive demo cleanup is appropriate. It prompts via
`forgeflow clean`; use `forgeflow clean --force` only for an intentional noninteractive cleanup.
The CLI restricts the database to loopback `/forgeflow` and requires the configured data directory
to be below the repository-root `.forgeflow/` subtree, independent of the process working directory.
The lower-level repository cleanup also permits the dedicated loopback `/forgeflow_test` database
used by integration tests, but no other name, and requires an explicit confirmation argument from
its caller. Raw MinIO objects and Docker volumes are retained.

## Bootstrap and dependency problems

### `uv` is not found or Python is the wrong version

Verify `uv --version` and `python --version`. ForgeFlow targets Python 3.12; do not solve an
interpreter mismatch by loosening the project constraint. Install/select Python 3.12, then run the
locked bootstrap task.

### Lockfile resolution or sync fails

- Confirm the command is running from the repository root containing `pyproject.toml` and `uv.lock`.
- Keep `--locked`; an unexpected lock update changes the reviewed dependency set.
- Distinguish registry/network/TLS failure from an incompatible platform wheel using the first
  causal error, not the final stack frame.
- If dependencies intentionally changed, update and review the lock in a dedicated change before
  retrying verification.

## Docker and Compose

### Cannot connect to the Docker daemon

Start Docker Engine/Desktop and verify `docker version` reports a server section. Treat daemon
availability as an environment boundary, not an application result.

### A port is already allocated

Inspect the owner of loopback ports configured in `compose.yaml` (PostgreSQL, MinIO, API, dashboard).
Stop the conflicting local service or intentionally change centralized configuration and Compose
together. Do not scatter alternate ports through code or documentation.

### PostgreSQL or MinIO stays unhealthy

Use `docker compose ps` and bounded service logs. Common boundaries are:

- PostgreSQL: data-volume incompatibility after an image/schema change, invalid local settings, or
  init SQL failure;
- MinIO: volume permission/corruption, invalid local credentials, or failed health command;
- `minio-init`: MinIO not yet healthy, alias authentication failure, or invalid bucket name.

Capture logs before recreating anything. Named-volume deletion destroys replay/evidence state and is
not a routine health fix.

## Landing and ingestion

### Object upload succeeds but no rows load

Find the file ledger entry by batch/source/checksum, then inspect its status:

- `duplicate/skipped`: compare with the prior file identity; this may be correct idempotency;
- `breaking drift`: inspect expected, actual, missing, and unexpected columns;
- `validation failed`: group quarantine reasons and reconcile row counts;
- `load failed`: inspect the transaction/domain error and confirm no partial accepted commit.

The raw object alone does not mean rows were accepted.

### Identical input loads twice

Compare exact file checksum, logical source, object identity, batch ID, record checksum, and event
natural key. Differences caused by newline/encoding/serialization can change byte checksums; the
canonical record checksum and unique target keys should still prevent duplicate facts. Treat a real
count increase as an idempotency defect and preserve both runs for diagnosis.

### A changed file is reported as a duplicate

Confirm the checksum is calculated from landed bytes and the ledger uniqueness key is not only the
file name. Changed bytes require a new content-addressed object and ledger row sharing the logical
key. ForgeFlow emits a changed-file warning but does not store an explicit predecessor foreign key.

### A failed file is skipped on retry

Inspect the source-file status. `loaded`, `quarantined`, and `skipped` are terminal and the same
source/checksum is intentionally skipped. `landed`, `validating`, and `failed` are retryable: a later
run should reset the same `file_id` to `landed` and process it again. Note that this reassigns the
content-ledger row to the retrying run; ForgeFlow does not retain a separate append-only row for each
delivery attempt.

### A manual CSV checksum differs after ingestion

`run-batch` should land the exact successfully parsed file bytes. Compare the local SHA-256 with the
ledger/object checksum and confirm the command passed `source_bytes`; do not compare against a CSV
reserialization. Generated in-memory datasets are different: their source object is the canonical
UTF-8/`\n` serialization produced by ForgeFlow.

### Valid rows disappeared with an invalid sibling

First distinguish breaking file drift from row validation. Breaking drift intentionally blocks the
file because rows cannot be interpreted safely. For a parseable, shape-valid file, valid rows should
load and only failing rows should be quarantined once with all reasons. Reconcile source, accepted,
and quarantined counts.

### Unknown parent references

If the parent source file is absent, expect `missing_parent_source` on otherwise-valid child rows. If
the parent is present but the accepted parent-key set lacks the value, expect
`referential_integrity_violation`. Invalid parent rows are not accepted as keys. Do not create
placeholder parent rows merely to satisfy the relationship.

## dbt and model failures

### dbt cannot connect

Check container health, the selected dbt target/profile directory, and the effective host name for
the execution location (`127.0.0.1` from host versus Compose service name inside a container). Use a
safe configuration summary; never print a password-bearing URL.

### Compilation fails

Run the configured compile task and address the first parser/graph error. Verify source/model names,
macro arguments, package state, and YAML indentation. Do not disable a model/test to obtain green
output without documenting the behavioral change.

### A healthy fixture fails a data test

Inspect failing row count and bounded sample evidence, then trace source lineage. Determine whether
the fixture, contract, transformation grain, join cardinality, or test expectation is wrong. Fix the
cause; do not raise a threshold merely because a check is red.

### A failed dbt run has no ForgeFlow evidence

This is a finalization defect. Check the per-run artifact path, dbt invocation result, parser logs,
and whether finalization ran in the failure path. Verify artifacts were not stale files from a prior
run. The run must remain failed; adding metadata later cannot convert it to healthy.

### Concurrent dbt runs block or time out

ForgeFlow serializes relation mutations with a PostgreSQL session advisory lock. Inspect the other
ForgeFlow run and database session rather than deleting an artifact directory. Every run writes to
`<FORGEFLOW_ARTIFACT_DIR>/dbt/<run_id>` and freshness writes beneath its `freshness/` child; shared
`dbt/target` files are not the canonical artifacts. A zero-exit build without structurally valid,
invocation-correlated `manifest.json` and `run_results.json`, or zero-exit freshness without valid
`freshness/sources.json`, is intentionally failed.

### Incremental model misses a late event

Compare event timestamp, ingestion timestamp, incremental watermark, lookback window, unique key,
and bounded backfill range. Late-but-valid records should be accepted and revisited by ingestion-time
or lookback logic. Do not change the event time to make the event appear current.

## Run status and observability

### Run remains `running`

Check process termination and finalization logs, then compare run age to the stale-running policy.
Do not manually mark it healthy. Reconciliation may finalize it failed with explicit interruption
evidence or launch a new attempt while retaining the stale record.

### Counts do not reconcile

Separate files blocked before row evaluation from row-evaluated files. For the latter, compare source
rows with accepted plus quarantined rows at the same boundary. Also distinguish source-row counts
from deduplicated staging/fact counts and report duplicate winners explicitly.

### Volume anomaly seems noisy

Inspect up to seven prior healthy values for the same source and historical/incremental batch kind.
At least three are required. Check the median, MAD, and tolerance
`max(1, 20% * median, 3 * 1.4826 * MAD)`. The evidence payload contains values rather than baseline
run IDs. Seasonal/cadence differences require additional segmentation or a different policy; they
are not evidence that the deterministic calculation is “AI.”

### Freshness looks healthy because of a future event

Future timestamps beyond tolerance must fail the contract and be excluded from `max(event_time)`.
Inspect contract results and the exact max timestamp used by the freshness result.

## API, MCP, and dashboard

### API returns validation or not-found errors

Verify UUID/model/status spelling and bounds. A not-found response is distinct from an empty list.
Check OpenAPI for the implemented route shape; do not bypass validation with direct SQL.

### MCP client cannot start the server

- Use an absolute `cwd` pointing to the repository.
- Confirm `uv run forgeflow-mcp` resolves in that environment.
- Keep protocol output on stdout and diagnostics on stderr.
- Check database settings are available to the child process.
- Run the MCP tests before assuming the client is at fault.

### MCP response is too large or incomplete

Use filters plus `limit`/`offset`. The hard cap is intentional. When `offset + items.length < total`,
request another page; do not ask for an unbounded table dump. Raw quarantine payloads are omitted by
design.

### A write tool is unavailable

This is expected: the current MCP server registers no mutation tools. Setting
`FORGEFLOW_ENABLE_WRITES=true` does not create one. Do not enable writes to investigate an incident.

### OpenAI provider will not start

The default deterministic provider needs no key. Selecting `openai` requires a nonempty
`OPENAI_API_KEY`; fail-fast configuration is expected. Incident creation persists deterministic
evidence/explanation before optional enrichment, so provider/network failure must leave that record
intact. Read tools return the persisted explanation and do not retry the provider. Requests are
limited to 50,000 evidence bytes, 1,200 output tokens, 30 seconds, and `store=false`.

### Dashboard is empty while API/MCP has data

Confirm all surfaces use the same environment and `ForgeFlowService`, then inspect dashboard
filters, selected run, and empty/loading/error state. Do not add dashboard-only SQL or metric logic as
a workaround.

## Test and quality-gate failures

- `uv run poe test` excludes tests marked `integration`; run integration explicitly with healthy
  containers. The integration fixture provisions and cleans only the loopback `forgeflow_test`
  database; it does not truncate the main `forgeflow` demo database.
- `uv run poe dagster-validate` validates `workspace.yaml` definitions without launching a daemon;
  it is also part of `uv run poe check`.
- Coverage below the configured threshold is a signal to add meaningful behavior tests, not to omit
  core modules or write assertion-free tests.
- Ruff formatting and lint are separate checks; run the formatter intentionally, then review the
  diff.
- Strict Mypy errors should be fixed at boundaries with types/adapters, not broad ignores.
- Security findings need triage and documented disposition; passing tools do not prove security.

## Safe escalation bundle

Provide:

- repository revision or diff identity;
- OS, Python, uv, Docker, and Compose versions;
- exact task and exit code;
- run ID, batch ID, source/check/model IDs as applicable;
- `docker compose ps` status;
- a short redacted relevant log excerpt;
- safe configuration summary with secrets removed;
- expected versus observed behavior; and
- actions already attempted.

Keep the original run/object/artifacts available until the issue is understood. Recovery creates new
evidence; it should not rewrite the failing history.
