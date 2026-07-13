select
    factory_id::text as factory_id,
    factory_name::text as factory_name,
    country_code::text as country_code,
    timezone::text as timezone,
    opened_on::date as opened_on,
    status::text as status,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'factories') }}
