CREATE TABLE IF NOT EXISTS observability.pipeline_runs (
    run_id uuid PRIMARY KEY,
    batch_id text NOT NULL,
    scenario text NOT NULL CHECK (scenario IN ('clean', 'incident', 'recovery')),
    status text NOT NULL CHECK (status IN ('running', 'healthy', 'degraded', 'failed')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    duration_seconds double precision CHECK (duration_seconds IS NULL OR duration_seconds >= 0),
    source_file_count integer NOT NULL DEFAULT 0 CHECK (source_file_count >= 0),
    source_row_count integer NOT NULL DEFAULT 0 CHECK (source_row_count >= 0),
    accepted_row_count integer NOT NULL DEFAULT 0 CHECK (accepted_row_count >= 0),
    quarantined_row_count integer NOT NULL DEFAULT 0 CHECK (quarantined_row_count >= 0),
    skipped_file_count integer NOT NULL DEFAULT 0 CHECK (skipped_file_count >= 0),
    model_row_counts jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(model_row_counts) = 'object'),
    test_counts jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(test_counts) = 'object'),
    passed_checks integer NOT NULL DEFAULT 0 CHECK (passed_checks >= 0),
    failed_checks integer NOT NULL DEFAULT 0 CHECK (failed_checks >= 0),
    freshness_status text NOT NULL DEFAULT 'unknown'
        CHECK (freshness_status IN ('unknown', 'fresh', 'warning', 'stale', 'error')),
    schema_changes jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(schema_changes) = 'array'),
    affected_downstream_models jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(affected_downstream_models) = 'array'),
    error_message text,
    summary jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(summary) = 'object'),
    CHECK (finished_at IS NULL OR finished_at >= started_at),
    CHECK (
        (status = 'running' AND finished_at IS NULL)
        OR (status <> 'running' AND finished_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_pipeline_runs_started_at
    ON observability.pipeline_runs (started_at DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_status_started_at
    ON observability.pipeline_runs (status, started_at DESC);
CREATE INDEX IF NOT EXISTS ix_pipeline_runs_batch_id
    ON observability.pipeline_runs (batch_id);

CREATE TABLE IF NOT EXISTS observability.pipeline_stages (
    stage_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE CASCADE,
    stage_name text NOT NULL,
    status text NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'skipped')),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    duration_seconds double precision CHECK (duration_seconds IS NULL OR duration_seconds >= 0),
    input_row_count bigint CHECK (input_row_count IS NULL OR input_row_count >= 0),
    output_row_count bigint CHECK (output_row_count IS NULL OR output_row_count >= 0),
    error_message text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(metadata) = 'object'),
    UNIQUE (run_id, stage_name),
    CHECK (finished_at IS NULL OR finished_at >= started_at)
);

CREATE TABLE IF NOT EXISTS observability.source_files (
    file_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE RESTRICT,
    batch_id text NOT NULL,
    source_name text NOT NULL,
    logical_key text NOT NULL,
    object_key text NOT NULL,
    checksum text NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),
    schema_fingerprint text NOT NULL CHECK (schema_fingerprint ~ '^[0-9a-f]{64}$'),
    size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
    row_count integer NOT NULL CHECK (row_count >= 0),
    accepted_count integer NOT NULL DEFAULT 0 CHECK (accepted_count >= 0),
    quarantined_count integer NOT NULL DEFAULT 0 CHECK (quarantined_count >= 0),
    status text NOT NULL CHECK (
        status IN ('landed', 'validating', 'loaded', 'quarantined', 'skipped', 'failed')
    ),
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed_at timestamptz,
    UNIQUE (source_name, checksum),
    UNIQUE (object_key),
    CHECK (accepted_count + quarantined_count <= row_count),
    CHECK (processed_at IS NULL OR processed_at >= created_at)
);

CREATE INDEX IF NOT EXISTS ix_source_files_run_id
    ON observability.source_files (run_id);
CREATE INDEX IF NOT EXISTS ix_source_files_batch_source
    ON observability.source_files (batch_id, source_name);

CREATE TABLE IF NOT EXISTS observability.schema_changes (
    change_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE CASCADE,
    file_id uuid REFERENCES observability.source_files (file_id) ON DELETE SET NULL,
    source_name text NOT NULL,
    change_type text NOT NULL CHECK (change_type IN ('additive', 'breaking')),
    expected_columns jsonb NOT NULL CHECK (jsonb_typeof(expected_columns) = 'array'),
    actual_columns jsonb NOT NULL CHECK (jsonb_typeof(actual_columns) = 'array'),
    missing_columns jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(missing_columns) = 'array'),
    unexpected_columns jsonb NOT NULL DEFAULT '[]'::jsonb
        CHECK (jsonb_typeof(unexpected_columns) = 'array'),
    detected_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS ix_schema_changes_run_source
    ON observability.schema_changes (run_id, source_name);

CREATE TABLE IF NOT EXISTS observability.quality_results (
    check_id text NOT NULL,
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE CASCADE,
    check_name text NOT NULL,
    check_type text NOT NULL,
    scope text NOT NULL,
    status text NOT NULL CHECK (status IN ('passed', 'failed', 'warning')),
    severity text NOT NULL CHECK (severity IN ('info', 'warning', 'error')),
    observed_value text,
    expected text NOT NULL,
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(evidence) = 'object'),
    occurred_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, check_id)
);

CREATE INDEX IF NOT EXISTS ix_quality_results_run_status
    ON observability.quality_results (run_id, status, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_quality_results_check_name
    ON observability.quality_results (check_name, occurred_at DESC);

CREATE TABLE IF NOT EXISTS observability.dbt_artifacts (
    artifact_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE CASCADE,
    artifact_type text NOT NULL CHECK (
        artifact_type IN ('manifest', 'run_results', 'catalog', 'sources')
    ),
    artifact_json jsonb NOT NULL CHECK (jsonb_typeof(artifact_json) = 'object'),
    captured_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, artifact_type)
);

CREATE TABLE IF NOT EXISTS observability.model_metadata (
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE CASCADE,
    unique_id text NOT NULL,
    model_name text NOT NULL,
    resource_type text NOT NULL,
    database_name text,
    schema_name text,
    relation_name text,
    description text NOT NULL DEFAULT '',
    materialization text,
    tags jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tags) = 'array'),
    meta jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(meta) = 'object'),
    depends_on jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(depends_on) = 'array'),
    captured_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, unique_id)
);

CREATE INDEX IF NOT EXISTS ix_model_metadata_name_captured
    ON observability.model_metadata (model_name, captured_at DESC);

CREATE TABLE IF NOT EXISTS observability.model_columns (
    run_id uuid NOT NULL,
    model_unique_id text NOT NULL,
    column_name text NOT NULL,
    data_type text,
    description text NOT NULL DEFAULT '',
    tests jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(tests) = 'array'),
    ordinal_position integer CHECK (ordinal_position IS NULL OR ordinal_position > 0),
    PRIMARY KEY (run_id, model_unique_id, column_name),
    FOREIGN KEY (run_id, model_unique_id)
        REFERENCES observability.model_metadata (run_id, unique_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS observability.lineage_edges (
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE CASCADE,
    parent_unique_id text NOT NULL,
    child_unique_id text NOT NULL,
    parent_name text NOT NULL,
    child_name text NOT NULL,
    edge_type text NOT NULL DEFAULT 'depends_on'
        CHECK (edge_type IN ('depends_on', 'exposure')),
    discovered_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (run_id, parent_unique_id, child_unique_id),
    CHECK (parent_unique_id <> child_unique_id)
);

CREATE INDEX IF NOT EXISTS ix_lineage_edges_parent
    ON observability.lineage_edges (parent_name, run_id);
CREATE INDEX IF NOT EXISTS ix_lineage_edges_child
    ON observability.lineage_edges (child_name, run_id);

CREATE TABLE IF NOT EXISTS observability.incidents (
    incident_id uuid PRIMARY KEY,
    failed_run_id uuid NOT NULL UNIQUE
        REFERENCES observability.pipeline_runs (run_id) ON DELETE RESTRICT,
    baseline_run_id uuid REFERENCES observability.pipeline_runs (run_id) ON DELETE SET NULL,
    recovery_run_id uuid REFERENCES observability.pipeline_runs (run_id) ON DELETE SET NULL,
    status text NOT NULL CHECK (status IN ('open', 'investigating', 'resolved')),
    title text NOT NULL DEFAULT 'Data quality incident',
    created_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at timestamptz,
    explanation jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(explanation) = 'object'),
    evidence jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(evidence) = 'object'),
    CHECK (resolved_at IS NULL OR resolved_at >= created_at),
    CHECK (
        (status = 'resolved' AND resolved_at IS NOT NULL AND recovery_run_id IS NOT NULL)
        OR (status <> 'resolved' AND resolved_at IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS ix_incidents_status_detected
    ON observability.incidents (status, created_at DESC);

CREATE TABLE IF NOT EXISTS quarantine.records (
    quarantine_id uuid PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES observability.pipeline_runs (run_id) ON DELETE RESTRICT,
    file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    source_name text NOT NULL,
    source_row_number integer NOT NULL CHECK (source_row_number >= 2),
    raw_payload jsonb NOT NULL CHECK (jsonb_typeof(raw_payload) = 'object'),
    reasons jsonb NOT NULL CHECK (jsonb_typeof(reasons) = 'array' AND jsonb_array_length(reasons) > 0),
    quarantined_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (file_id, source_row_number)
);

CREATE INDEX IF NOT EXISTS ix_quarantine_records_run_source
    ON quarantine.records (run_id, source_name, quarantined_at DESC);
