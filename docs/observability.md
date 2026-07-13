# Observability and data quality

## Scope and evidence status

ForgeFlow observability is the durable evidence needed to answer “what happened, to which data, and
what should be inspected next?” It is not a generic infrastructure-monitoring product. This
document defines run, file, model, check, freshness, drift, comparison, and incident semantics.

The records and algorithms below match the current implementation. `STATUS.md` records which paths
have been exercised in the current environment.

## Evidence model

The physical schema may consolidate tables, but it must preserve these logical records and keys:

| Record | Key/grain | Required evidence |
|---|---|---|
| Pipeline run | One `run_id` per attempt | Batch/scenario, state, timestamps, duration, stage error, all aggregate counts |
| Source file | One landed source/checksum identity | Source, logical/object keys, checksum, size, batch, timestamps, status, and counts |
| Stage execution | One run and named stage | Start/finish, status, optional input/output counts, bounded error context and metadata |
| Quality result | One run and `check_id` | Type, scope, status, severity, observed value, expected rule, structured evidence |
| Quarantine record | One invalid source row | Run/file/row lineage, all reason codes, protected raw payload |
| Schema change | One run/file/change | Additive/breaking, expected/actual/missing/unexpected columns |
| Model metadata/count | One run and dbt node/model | dbt status/duration in artifacts; actual relation row count for whitelisted models where available |
| Freshness result | One run/source or model | Max event time, evaluated time, lag, threshold, status |
| Lineage edge | One parent/child model edge per manifest version | Unique IDs, names, resource types, manifest identity |
| Incident | One investigation | Failed run, optional baseline/recovery runs, state, evidence summary, resolution time |
| Run comparison | Derived from two finalized runs | Core/model count deltas, status change, B-side drift/impact, unavailable model counts |

PostgreSQL is the queryable system of record. JSON artifacts are portable reviewer evidence and a
debugging aid, not an independently editable source of truth.

## Persisted pipeline stages

Generated runs persist `source_generation`; all runs persist `raw_landing`,
`contract_validation`, `warehouse_load`, `dbt_build_and_freshness`,
`observability_finalization`, and `incident_linkage` as they are reached. A diagnostic `--skip-dbt`
marks the dbt stage `skipped` and adds a warning so that run cannot be canonical healthy. Each
`(run_id, stage_name)` has one current stage row; restarting the same named stage resets that row, so
this table is not an append-only per-attempt trace.

## Canonical run summary

Every terminal run exposes at least:

- `run_id`, `batch_id`, and scenario;
- `status`, `started_at`, `finished_at`, and `duration_seconds`;
- source file and row counts;
- accepted, quarantined, and skipped-file counts;
- actual current relation row counts by produced model where available;
- total/passed/failed test and check counts;
- freshness status;
- schema changes;
- affected downstream models; and
- a bounded error message when the run failed.

Counts must reconcile or explain why they cannot. For record-oriented sources that can be enumerated,
including a file with a missing required column, `source rows = accepted rows + quarantined rows` at
the validation boundary. An unreadable object whose rows cannot be enumerated is a file failure and
does not fabricate a row total.

## State derivation

Run state is calculated by pipeline-domain finalization using all evidence available at that point,
then read unchanged through `ForgeFlowService`:

1. `failed` if a required infrastructure/stage operation failed, dbt returned an error, an
   error-severity check failed, artifacts required for diagnosis could not be processed, or
   finalization itself failed.
2. Otherwise `degraded` if at least one row was quarantined, additive drift was observed, or a
   warning-severity quality/freshness/anomaly check warned or failed.
3. Otherwise `healthy`.

`running` is nonterminal. A stale `running` record is an operational problem and must be identifiable
by age; clients must not reinterpret it as success.

## Check taxonomy

All checks normalize to the same result shape even when Pandera, SQL/dbt, or Python computes them.

| Normalized result | Outcome | Severity and run effect |
|---|---|---|
| Contract validity/reason group | `warning` when one or more rows quarantine | `warning`; degrades the run while retaining valid siblings |
| Additive schema drift | `warning` | `warning`; degrades the run |
| Breaking schema drift | `failed` | `error`; fails the run |
| dbt test | `passed`, `warning`, or `failed` from dbt status | `info`, `warning`, or `error`; an error failure fails the run |
| dbt source freshness | `passed`, `warning`, or `failed` from `sources.json` | `info`, `warning`, or `error`; contributes canonical freshness state |
| Telemetry late arrival | `warning` when accepted events arrive over 24 hours late | `warning`; late is not malformed |
| Source volume anomaly | `warning` outside the median/MAD bounds | `warning`; heuristic degrades rather than fails |
| Exact duplicate file | `passed` with action `skipped` | `info`; protects idempotency |

`status` (`passed`, `failed`, `warning`) describes the outcome; `severity` (`info`, `warning`,
`error`) describes consequence. Keeping both prevents an informational observation from failing a
run while allowing an error-severity failure to do so deterministically.

## Freshness

The mart exposes two related ages:

`event_age = evaluation_timestamp - max(valid_business_event_timestamp)`

`ingestion_age = evaluation_timestamp - max(_ingested_at)`

The current target mart exposes both latest ingestion and latest event age. It classifies ingestion
as `fresh` through 24 hours, `warning` through 72 hours, then `stale`; machine reliability uses a
configurable six-hour telemetry-event threshold. These are transparent demo defaults, not universal
production values, and changes require model, test, and documentation updates.

A future timestamp beyond contract tolerance is invalid and cannot make a source look fresh. An
empty source reports `missing`, never zero age. Runtime executes `dbt source freshness` after the
build and normalizes `sources.json` results into shared quality checks; those checks, not the mart
alone, set `RunSummary.freshness_status`.

## Explainable source-volume anomaly

Each processed source gets a deterministic `volume_anomaly:<source>` check:

1. Query up to the seven most recent terminal source-file counts from prior `healthy` runs.
2. Compare only the same source and batch kind: `historical` with historical, or `incremental` with
   incremental. The kind is inferred from the generated batch ID's `-incremental-` marker; a custom
   manual batch ID without that marker is treated as historical. Incident/recovery labels are not an
   additional segmentation key.
3. With fewer than three comparable values, return an informational `passed` result with
   `evaluated=false`; do not call the current value anomalous.
4. Otherwise calculate the median and median absolute deviation (MAD).
5. Use `max(1 row, 20% of median, 3 * 1.4826 * MAD)` as the symmetric tolerance and clamp the lower
   bound to zero.
6. Return a warning when the current row count falls outside those inclusive bounds.

Evidence includes the bounded history values, minimum sample count, median, MAD, scale/multiplier,
tolerance, and bounds. It currently does not persist the baseline run IDs in the quality payload and
does not model seasonality or source-specific cadence. This is a transparent portfolio heuristic,
not an adaptive forecasting system.

## dbt artifact handling

ForgeFlow runs dbt under a PostgreSQL session advisory lock, with a 30-second database statement
timeout while acquiring it, so local ForgeFlow runs cannot mutate the same modeled relations
concurrently. Each run uses `<FORGEFLOW_ARTIFACT_DIR>/dbt/<run_id>/` (default
`.forgeflow/artifacts/dbt/<run_id>/`) through dbt's `--target-path`; known artifact names are removed
before a same-run retry. Build artifacts remain at that root; source freshness uses the isolated
`freshness/` child target with the same bounded dbt variables, so it cannot overwrite the build
manifest. The environment passed to dbt is allowlisted and explicitly disables dbt anonymous usage
statistics.

ForgeFlow consumes `manifest.json`, `run_results.json`, and `sources.json`. The parser associates dbt
nodes/tests with the ForgeFlow run, normalizes status and duration, and extracts dependency edges for
downstream-impact queries. `catalog.json` is optional.

Artifact parsing executes after both success and failure. If `dbt build` exits zero, a missing,
oversized, malformed, or cross-invocation manifest/run-results pair fails the stage/run. Their dbt
invocation IDs must match before lineage and results are correlated. If source freshness exits zero,
`freshness/sources.json` is also required. A non-zero process already fails the stage but any valid
artifacts it produced are still persisted and parsed.

After manifest parsing, ForgeFlow resolves each model's declared schema and alias and issues an
actual `COUNT(*)` for up to 100 safe relations in `staging`, `intermediate`, and `marts`. These counts
replace adapter `rows_affected` in the run summary. Missing counts are omitted, not converted to zero.

## Downstream impact

The run-level affected list starts from failed dbt test dependencies or failed model nodes, traverses
the manifest child map, removes duplicates, stops starting new expansions after the visited set
reaches 500, and stores sorted downstream model/exposure names. A single high-fanout expansion can
cross that soft guard. The summary does not preserve root/depth/truncation metadata.

The interactive `get_downstream_impact(model_name)` query is separate: it traverses the latest
persisted lineage graph to depth 20 and returns at most 500 model/depth rows in stable order.

Impact means “declared graph dependency,” not proof that a business report was wrong or viewed by a
person.

## Run comparison

A comparison between run A and run B reports identifiers, status change, deltas (`B - A`) for source
files, source rows, accepted rows, quarantined rows, passed checks, and failed checks. Model deltas
are calculated only for names present in both actual-relation-count maps; names found on only one side
are listed in `model_row_count_unavailable`. The response also includes B's schema changes and
affected-model list plus an explicit no-causality interpretation.

The current comparison does not classify newly/resolved individual checks, calculate freshness-lag
deltas, resolve schema changes, or emit a scenario-comparability score. Consumers must compare batch
scope themselves.

## Incident explanation contract

The explanation input is bounded structured evidence: failed/warning checks, the persisted bounded
run error message when present, intersecting model-count changes, quarantine reason counts, schema
changes, affected-model names, and failed/baseline run IDs.
The output has four explicit sections:

1. observed facts derived from the persisted evidence bundle;
2. likely explanations, each labeled as uncertain;
3. recommended next inspection steps; and
4. an uncertainty note and the run IDs used.

Incident creation always persists the deterministic explanation and evidence first. If OpenAI is
explicitly selected, enrichment happens afterward; a provider error leaves the deterministic
incident intact. Service, API, MCP, and dashboard reads deserialize the persisted explanation and
never call either provider.

The OpenAI boundary serializes at most 50,000 UTF-8 bytes of evidence, sets a 30-second client
timeout, `max_output_tokens=1200`, and `store=false`, and validates structured output into the same
typed contract. Observed facts are regenerated deterministically; model output may enrich only
hypotheses and recommended actions. Neither provider can mutate data or resolve an incident.

Recovery linkage is explicit in the pipeline boundary. A healthy recovery resolves only the
`recovery_incident_id` supplied to that run after verifying that the incident exists and is open. A
recovery run without an ID resolves nothing. `recover-demo` selects the latest open incident at the
CLI boundary, converts it to a concrete UUID, and passes that UUID to the runner; the runner never
searches for and closes an unrelated incident on its own.

## Logging and payload safety

Structured logs include run/batch/file/check identifiers, stage, status, duration, and counts. They
exclude credentials, password-bearing connection strings, API keys, full quarantined payloads, and
unbounded dbt/compiler output. Errors are translated into domain context without hiding the original
stage.

Reviewer-facing page endpoints are bounded and expose `total`, `limit`, and `offset`. Aggregate
quarantine reason counts are preferred to raw records. Internal incident/lineage evidence also uses
fixed slice/traversal caps, but not every such summary has continuation metadata; those fields must
not be assumed exhaustive.

## Verification checklist

The observability layer is not verified until tests or demo evidence show:

- final metadata after a dbt failure;
- no duplicated accepted data after an identical rerun;
- separate file drift and row quarantine evidence;
- late arrival and stale-source behavior;
- artifact-derived downstream impact;
- accurate run comparison deltas;
- facts/hypotheses separation in explanation fixtures; and
- healthy recovery that retains the failed run and incident evidence.
