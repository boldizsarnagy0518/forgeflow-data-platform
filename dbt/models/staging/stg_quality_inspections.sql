select
    quality_inspection_id::text as quality_inspection_id,
    production_order_id::text as production_order_id,
    inspected_at::timestamptz as inspected_at,
    sample_size::integer as sample_size,
    passed_units::integer as passed_units,
    failed_units::integer as failed_units,
    result::text as result,
    inspector_id::text as inspector_id,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'quality_inspections') }}
