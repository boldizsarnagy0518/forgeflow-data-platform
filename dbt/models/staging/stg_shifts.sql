select
    shift_id::text as shift_id,
    factory_id::text as factory_id,
    shift_name::text as shift_name,
    started_at::timestamptz as started_at,
    ended_at::timestamptz as ended_at,
    operator_id::text as operator_id,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'shifts') }}
