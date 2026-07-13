select
    production_line_id::text as production_line_id,
    factory_id::text as factory_id,
    line_name::text as line_name,
    product_family::text as product_family,
    nominal_capacity_per_hour::numeric(12, 3) as nominal_capacity_per_hour,
    status::text as status,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'production_lines') }}
