select
    machine_id::text as machine_id,
    production_line_id::text as production_line_id,
    machine_name::text as machine_name,
    machine_type::text as machine_type,
    manufacturer::text as manufacturer,
    model::text as model,
    installed_on::date as installed_on,
    status::text as status,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'machines') }}
