# Data contracts

## Scope and evidence status

Data contracts define what ForgeFlow accepts at its ingestion boundary and how it preserves rejected
input. They do not define analytical metrics or dbt mart behavior. The implemented registry and this
catalog describe contract version `1.0.0`; tests and demo runs recorded in `STATUS.md` are the
execution evidence.

A file that passes the bounded manual CSV parser, or a generated canonical CSV, is retained in MinIO
before contract validation. “Rejected” then means “not loaded as accepted warehouse data,” never
“deleted.” Parser/preflight rejection happens before landing and is called out below.

## Contract envelope

Every registered `SourceContract` declares:

- contract name and version;
- exact required and optional columns;
- logical types and explicit parsing formats;
- nullability;
- enum membership and numeric bounds;
- UTC timestamp and interval rules;
- natural-key uniqueness expectations;
- parent-reference expectations where the parent snapshot is available;
- file-level schema policy; and
- stable machine-readable reason codes with safe human-readable messages.

All ten source contracts currently use version `1.0.0`. The version is included in the normalized
contract-validity quality evidence for each source. It is not a migration registry or negotiated
producer protocol: the source-file ledger does not have a separate version column, and production
use would still need ownership, compatibility, and deprecation policy.

Contracts use strict field names. Values are not silently repaired, guessed, or dropped. The manual
CSV reader converts only declared integer and number fields before validation; invalid numeric text
remains text so the contract can reject it. Empty CSV cells become null. Strings, dates, and
timestamps remain source text until contract validation.

## Source-byte preservation

ForgeFlow distinguishes the immutable source-object checksum from the canonical record checksum:

- `forgeflow run-batch --path <directory>` requires exact registered source filenames, rejects any
  other top-level `*.csv`, reads each registered UTF-8 CSV once, validates parser, file-size,
  row-count, regular-file, containment, and symlink constraints, and passes the same byte sequence
  to MinIO. Before landing, the pipeline re-parses the byte envelope and binds its ordered
  header, row count, columns, and values to the supplied parsed records. Newline style, quoting,
  encoding bytes, and unexpected columns are therefore preserved exactly for a successfully parsed
  manual file without allowing unrelated bytes to accompany loaded rows.
- Generated batches do not begin with an external file. ForgeFlow serializes their in-memory rows to
  canonical UTF-8 CSV with `\n` line endings, deterministic source/column iteration, and stable value
  rendering; those canonical bytes are what MinIO receives and what the file checksum identifies.
- Record checksums use the declared field order and canonical value serialization after validation
  normalization. They are not substitutes for the exact object checksum.

An unreadable, non-UTF-8, oversized, over-row-limit, symlinked, or path-escaping manual file is
rejected before landing because ForgeFlow cannot safely establish a validated CLI ingestion object.
That is an explicit limitation of the local CLI path, not a claim that every arbitrary byte stream
is archived.

## File-level decisions

File and row failures are deliberately separate:

| Observation | Classification | Load behavior | Durable evidence |
|---|---|---|---|
| Same source and checksum already terminal | Exact duplicate | Skip accepted/quarantine writes | Current run records a skipped file and idempotency check; the content ledger retains the original file row |
| Same logical key, different checksum | Changed file | Process as a new content-addressed object | Separate ledger rows sharing the logical key plus a changed-file warning; no explicit predecessor foreign key |
| Required column missing | Breaking drift | No rows are accepted; each source row is quarantined with `missing_required_column` | File drift event plus row lineage/reasons |
| Declared column contains wrong-typed values | Row contract failure | Quarantine affected rows | `invalid_type` reasons; structural drift remains a separate column-shape event |
| Unexpected column only | Additive drift | Preserve raw; load the contract-known projection and mark the run at least degraded | Drift event with unexpected columns |
| Column order changed | Non-breaking | Load by column name | No drift event; never positional remapping |
| Empty but parseable CSV | Shape/row decision | Validate its header, load zero rows, and complete the file as validated/loaded | File/check evidence with a zero row count |
| Unreadable manual CSV | CLI preflight failure | Do not land or load | Safe bounded command error; see the source-byte limitation above |

For ForgeFlow's record-oriented synthetic inputs, rows are still enumerable when a column is absent,
so breaking drift produces both one file-level drift observation and row-level quarantine evidence.
An unreadable object whose rows cannot be enumerated remains a file failure and must not fabricate
row counts.

## Row-level decisions

Rows that can be parsed are evaluated independently. A row failing one or more checks is written once
to quarantine with all applicable reasons. Valid siblings in the same file may continue unless the
file has breaking drift.

A quarantine reason has:

```json
{
  "code": "out_of_range",
  "column": "temperature_c",
  "check": "accepted_range",
  "message": "temperature_c must be between -40.0 and 180.0 inclusive",
  "value": 999.0
}
```

Reason codes are stable API/MCP grouping keys. Messages may improve without changing the code.
Sensitive or excessively large values must be redacted/truncated before being logged or returned;
full synthetic payloads stay in restricted internal storage.

## Source contract catalog

The fields below match the implemented `1.0.0` registry. Any incompatible change requires a version
bump plus fixtures, model/documentation updates, and recorded verification.

### Master and schedule sources

All listed fields are required unless marked nullable. Identifier fields match the uppercase
synthetic pattern `^[A-Z][A-Z0-9-]{2,63}$`; free-text fields must be nonblank.

| Source | Fields | Rules |
|---|---|---|
| `factories` | `factory_id`, `factory_name`, `country_code`, `timezone`, `opened_on`, `status`, `updated_at` | Unique ID; country `HU/DE/CZ`; timezone `Europe/Budapest`, `Europe/Berlin`, or `Europe/Prague`; nonfuture ISO date/update; status `active/inactive` |
| `production_lines` | `production_line_id`, `factory_id`, `line_name`, `product_family`, `nominal_capacity_per_hour`, `status`, `updated_at` | Unique line; known factory; family `motor/pump/gearbox`; capacity integer `[1, 10,000]`; status `active/inactive` |
| `machines` | `machine_id`, `production_line_id`, `machine_name`, `machine_type`, `manufacturer`, `model`, `installed_on`, `status`, `updated_at` | Unique machine; known line; type `cnc/press/robot/welder/inspection`; installation nonfuture; status `active/maintenance/retired` |
| `shifts` | `shift_id`, `factory_id`, `shift_name`, `started_at`, `ended_at`, `operator_id`, `updated_at` | Unique shift; known factory; shift `morning/afternoon/night`; end after start; update not before end; anonymous identifier pattern |

### Operational event sources

| Source | Fields | Rules |
|---|---|---|
| `production_orders` | `production_order_id`, `production_line_id`, `product_code`, `planned_start_at`, `planned_end_at`, nullable `actual_start_at`, nullable `actual_end_at`, `planned_quantity`, `actual_quantity`, `status`, `updated_at` | Unique order; known line; plan `[1, 1,000,000]`; actual `[0, 1,000,000]`; plan end after start; actual times both null or both set and ordered; status `planned/in_progress/completed/cancelled` |
| `machine_telemetry` | `telemetry_id`, `machine_id`, `event_timestamp`, `temperature_c`, `vibration_mm_s`, `pressure_bar`, `energy_kwh`, `operating_state`, `updated_at` | Unique event; known machine; temperature `[-40, 180]`; vibration `[0, 50]`; pressure `[0, 500]`; energy `[0, 100,000]`; state `running/idle/stopped`; update not before event |
| `downtime_events` | `downtime_event_id`, `machine_id`, `started_at`, nullable `ended_at`, `downtime_type`, `reason_code`, `updated_at` | Unique event; known machine; end after start when set; type `planned/unplanned`; reason `maintenance/breakdown/changeover/material_shortage`; update not before start |
| `maintenance_work_orders` | `maintenance_work_order_id`, `machine_id`, `created_at`, `scheduled_for`, nullable `completed_at`, `maintenance_type`, `priority`, `status`, `technician_id`, `updated_at` | Unique work order; known machine; completion not before creation; type `preventive/corrective/inspection`; priority `low/medium/high/critical`; status `open/in_progress/completed/cancelled`; anonymous technician ID |
| `quality_inspections` | `quality_inspection_id`, `production_order_id`, `inspected_at`, `sample_size`, `passed_units`, `failed_units`, `result`, `inspector_id`, `updated_at` | Unique inspection; known order; counts each `[0, 1,000,000]` with sample >= 1; passed + failed = sample; result is `pass` exactly when failed = 0; anonymous inspector ID |
| `product_defects` | `product_defect_id`, `quality_inspection_id`, `detected_at`, `defect_type`, `severity`, `defect_count`, `updated_at` | Unique defect record; known inspection; type `dimensional/surface/assembly/material/functional`; severity `minor/major/critical`; count `[1, 1,000,000]`; update not before detection |

Planned order timestamps and scheduled maintenance may be future-dated by design. Other timestamp/date
fields marked nonfuture use the shared five-minute timestamp tolerance where applicable. Numeric
ranges are synthetic plausibility boundaries, not universal industrial safety limits.

## Timestamp policy

- Exchange timestamps are ISO-8601 strings with an explicit offset and are normalized to UTC for
  comparison/storage.
- Factory time zones are used only for reporting windows and shift labels.
- Naive or unparseable timestamps fail validation; the loader does not assume local host time.
- A non-planned event beyond the shared five-minute future tolerance is quarantined.
- A valid historical event is accepted even when late. Arrival lag is observed separately.
- Interval ends must not precede starts. Open downtime/maintenance intervals use null, not a sentinel
  future date.

Late is not synonymous with stale. “Late” describes arrival relative to event time; “stale” describes
the latest available event relative to a freshness threshold.

## Referential integrity timing

Foreign keys are evaluated after per-source row validation:

- if the required parent source is absent from the dataset, otherwise-valid child rows are
  quarantined with `missing_parent_source`;
- if the parent source is present but the referenced accepted key is absent, the reason is
  `referential_integrity_violation`; and
- invalid parent rows do not enter the accepted parent-key set.

dbt relationship tests remain a second defense against load-order and historical-model mistakes. No
placeholder "unknown" master row is fabricated.

## Contract-valid business failures

Contracts protect source integrity, not every analytical rule. The incident scenario intentionally
includes an order that is type-correct and referentially valid but whose actual quantity exceeds 150%
of its plan. That row reaches `raw`; `assert_actual_quantity_within_plan` is expected to fail and its
downstream impact is persisted. This proves that quarantine and warehouse-quality failures are
distinct controls. The fixture value and dbt threshold must be tested together before this behavior
is called verified.

## Idempotency, retry, and current-state upserts

File checksums use the exact landed bytes. The content ledger is unique on `(source_name,
checksum)`. A terminal `loaded`, `quarantined`, or `skipped` identity is treated as a duplicate. A
nonterminal or `failed` identity is reset to `landed` and retried using the same `file_id`; interrupted
files are marked `failed`, and quarantine rows are upserted by file and source-row number on retry.

This is deliberately a content ledger, not a complete delivery-attempt ledger. A duplicate terminal
delivery does not create another `source_files` row, and retrying a failed identity replaces that
row's run/batch association. The pipeline run still records its skipped count and duplicate quality
check, but organizations needing every transport attempt would add a separate append-only delivery
table.

Changed bytes receive a different content-addressed MinIO key and a new ledger identity. Raw tables
are current-state relations keyed by the source business ID. An upsert changes a current row only
when the record checksum differs and the incoming `updated_at` is equal to or newer than the stored
value; older records cannot roll current state backward. The original file bytes and prior run/file
evidence remain available even though the raw relation is not event-versioned.

## Contract change workflow

1. Add or revise the declared contract and bump its version.
2. Add focused clean, invalid-row, additive-drift, and breaking-drift fixtures.
3. Define compatibility and replay/backfill behavior.
4. Update source/dbt descriptions and affected downstream tests.
5. Run contract, idempotency, integration, incident, and recovery checks.
6. Record exact evidence in `STATUS.md`; until then, label the change pending.

An additive source column is not automatically an analytical requirement. It can be preserved in the
raw object and observed before any warehouse model adopts it.
