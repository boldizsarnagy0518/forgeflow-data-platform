select
    production_order_id::text as production_order_id,
    production_line_id::text as production_line_id,
    product_code::text as product_code,
    planned_start_at::timestamptz as planned_start_at,
    planned_end_at::timestamptz as planned_end_at,
    actual_start_at::timestamptz as actual_start_at,
    actual_end_at::timestamptz as actual_end_at,
    planned_quantity::integer as planned_quantity,
    actual_quantity::integer as actual_quantity,
    status::text as status,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'production_orders') }}
