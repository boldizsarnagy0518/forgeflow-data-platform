CREATE TABLE IF NOT EXISTS raw.factories (
    factory_id text PRIMARY KEY,
    factory_name text NOT NULL,
    country_code text NOT NULL CHECK (country_code IN ('HU', 'DE', 'CZ')),
    timezone text NOT NULL CHECK (timezone IN ('Europe/Budapest', 'Europe/Berlin', 'Europe/Prague')),
    opened_on date NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'inactive')),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS raw.production_lines (
    production_line_id text PRIMARY KEY,
    factory_id text NOT NULL REFERENCES raw.factories (factory_id) ON DELETE RESTRICT,
    line_name text NOT NULL,
    product_family text NOT NULL CHECK (product_family IN ('motor', 'pump', 'gearbox')),
    nominal_capacity_per_hour numeric(12, 3) NOT NULL
        CHECK (nominal_capacity_per_hour BETWEEN 1 AND 10000),
    status text NOT NULL CHECK (status IN ('active', 'inactive')),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS ix_production_lines_factory
    ON raw.production_lines (factory_id);

CREATE TABLE IF NOT EXISTS raw.machines (
    machine_id text PRIMARY KEY,
    production_line_id text NOT NULL
        REFERENCES raw.production_lines (production_line_id) ON DELETE RESTRICT,
    machine_name text NOT NULL,
    machine_type text NOT NULL CHECK (machine_type IN ('cnc', 'press', 'robot', 'welder', 'inspection')),
    manufacturer text NOT NULL,
    model text NOT NULL,
    installed_on date NOT NULL,
    status text NOT NULL CHECK (status IN ('active', 'maintenance', 'retired')),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$')
);

CREATE INDEX IF NOT EXISTS ix_machines_line
    ON raw.machines (production_line_id);

CREATE TABLE IF NOT EXISTS raw.shifts (
    shift_id text PRIMARY KEY,
    factory_id text NOT NULL REFERENCES raw.factories (factory_id) ON DELETE RESTRICT,
    shift_name text NOT NULL CHECK (shift_name IN ('morning', 'afternoon', 'night')),
    started_at timestamptz NOT NULL,
    ended_at timestamptz NOT NULL,
    operator_id text NOT NULL,
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (ended_at > started_at),
    CHECK (updated_at >= ended_at)
);

CREATE INDEX IF NOT EXISTS ix_shifts_factory_started
    ON raw.shifts (factory_id, started_at);

CREATE TABLE IF NOT EXISTS raw.production_orders (
    production_order_id text PRIMARY KEY,
    production_line_id text NOT NULL
        REFERENCES raw.production_lines (production_line_id) ON DELETE RESTRICT,
    product_code text NOT NULL,
    planned_start_at timestamptz NOT NULL,
    planned_end_at timestamptz NOT NULL,
    actual_start_at timestamptz,
    actual_end_at timestamptz,
    planned_quantity integer NOT NULL CHECK (planned_quantity BETWEEN 1 AND 1000000),
    actual_quantity integer NOT NULL CHECK (actual_quantity BETWEEN 0 AND 1000000),
    status text NOT NULL CHECK (status IN ('planned', 'in_progress', 'completed', 'cancelled')),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (planned_end_at > planned_start_at),
    CHECK (
        (actual_start_at IS NULL AND actual_end_at IS NULL)
        OR (actual_start_at IS NOT NULL AND actual_end_at IS NOT NULL)
    ),
    CHECK (actual_end_at IS NULL OR actual_end_at >= actual_start_at)
);

CREATE INDEX IF NOT EXISTS ix_production_orders_line_planned_start
    ON raw.production_orders (production_line_id, planned_start_at);

CREATE TABLE IF NOT EXISTS raw.machine_telemetry (
    telemetry_id text PRIMARY KEY,
    machine_id text NOT NULL REFERENCES raw.machines (machine_id) ON DELETE RESTRICT,
    event_timestamp timestamptz NOT NULL,
    temperature_c numeric(8, 3) NOT NULL CHECK (temperature_c BETWEEN -40 AND 180),
    vibration_mm_s numeric(8, 3) NOT NULL CHECK (vibration_mm_s BETWEEN 0 AND 50),
    pressure_bar numeric(9, 3) NOT NULL CHECK (pressure_bar BETWEEN 0 AND 500),
    energy_kwh numeric(12, 3) NOT NULL CHECK (energy_kwh BETWEEN 0 AND 100000),
    operating_state text NOT NULL CHECK (operating_state IN ('running', 'idle', 'stopped')),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (updated_at >= event_timestamp)
);

CREATE INDEX IF NOT EXISTS ix_machine_telemetry_machine_event
    ON raw.machine_telemetry (machine_id, event_timestamp DESC);
CREATE INDEX IF NOT EXISTS ix_machine_telemetry_ingested
    ON raw.machine_telemetry (_ingested_at DESC);

CREATE TABLE IF NOT EXISTS raw.downtime_events (
    downtime_event_id text PRIMARY KEY,
    machine_id text NOT NULL REFERENCES raw.machines (machine_id) ON DELETE RESTRICT,
    started_at timestamptz NOT NULL,
    ended_at timestamptz,
    downtime_type text NOT NULL CHECK (downtime_type IN ('planned', 'unplanned')),
    reason_code text NOT NULL CHECK (
        reason_code IN ('maintenance', 'breakdown', 'changeover', 'material_shortage')
    ),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (ended_at IS NULL OR ended_at > started_at),
    CHECK (updated_at >= started_at)
);

CREATE INDEX IF NOT EXISTS ix_downtime_events_machine_started
    ON raw.downtime_events (machine_id, started_at DESC);

CREATE TABLE IF NOT EXISTS raw.maintenance_work_orders (
    maintenance_work_order_id text PRIMARY KEY,
    machine_id text NOT NULL REFERENCES raw.machines (machine_id) ON DELETE RESTRICT,
    created_at timestamptz NOT NULL,
    scheduled_for timestamptz NOT NULL,
    completed_at timestamptz,
    maintenance_type text NOT NULL CHECK (maintenance_type IN ('preventive', 'corrective', 'inspection')),
    priority text NOT NULL CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    status text NOT NULL CHECK (status IN ('open', 'in_progress', 'completed', 'cancelled')),
    technician_id text NOT NULL,
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (completed_at IS NULL OR completed_at >= created_at),
    CHECK (updated_at >= created_at)
);

CREATE INDEX IF NOT EXISTS ix_maintenance_work_orders_machine_status
    ON raw.maintenance_work_orders (machine_id, status, scheduled_for);

CREATE TABLE IF NOT EXISTS raw.quality_inspections (
    quality_inspection_id text PRIMARY KEY,
    production_order_id text NOT NULL
        REFERENCES raw.production_orders (production_order_id) ON DELETE RESTRICT,
    inspected_at timestamptz NOT NULL,
    sample_size integer NOT NULL CHECK (sample_size BETWEEN 1 AND 1000000),
    passed_units integer NOT NULL CHECK (passed_units BETWEEN 0 AND 1000000),
    failed_units integer NOT NULL CHECK (failed_units BETWEEN 0 AND 1000000),
    result text NOT NULL CHECK (result IN ('pass', 'fail')),
    inspector_id text NOT NULL,
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (passed_units + failed_units = sample_size),
    CHECK (
        (failed_units = 0 AND result = 'pass')
        OR (failed_units > 0 AND result = 'fail')
    ),
    CHECK (updated_at >= inspected_at)
);

CREATE INDEX IF NOT EXISTS ix_quality_inspections_order_inspected
    ON raw.quality_inspections (production_order_id, inspected_at DESC);

CREATE TABLE IF NOT EXISTS raw.product_defects (
    product_defect_id text PRIMARY KEY,
    quality_inspection_id text NOT NULL
        REFERENCES raw.quality_inspections (quality_inspection_id) ON DELETE RESTRICT,
    detected_at timestamptz NOT NULL,
    defect_type text NOT NULL CHECK (
        defect_type IN ('dimensional', 'surface', 'assembly', 'material', 'functional')
    ),
    severity text NOT NULL CHECK (severity IN ('minor', 'major', 'critical')),
    defect_count integer NOT NULL CHECK (defect_count BETWEEN 1 AND 1000000),
    updated_at timestamptz NOT NULL,
    _batch_id text NOT NULL,
    _source_file_id uuid NOT NULL REFERENCES observability.source_files (file_id) ON DELETE RESTRICT,
    _source_row_number integer NOT NULL CHECK (_source_row_number >= 2),
    _ingested_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    _record_checksum text NOT NULL CHECK (_record_checksum ~ '^[0-9a-f]{64}$'),
    CHECK (updated_at >= detected_at)
);

CREATE INDEX IF NOT EXISTS ix_product_defects_inspection_detected
    ON raw.product_defects (quality_inspection_id, detected_at DESC);

COMMENT ON COLUMN raw.production_orders.actual_quantity IS
    'Contract-valid quantity. The <= 150% of planned quantity business rule is intentionally enforced in dbt.';
COMMENT ON COLUMN raw.machine_telemetry.event_timestamp IS
    'Source event time; late arrivals are preserved and handled by the incremental dbt lookback.';
