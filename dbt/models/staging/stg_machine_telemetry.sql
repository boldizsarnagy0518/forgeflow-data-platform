select
    telemetry_id::text as telemetry_id,
    machine_id::text as machine_id,
    event_timestamp::timestamptz as event_timestamp,
    temperature_c::numeric(8, 3) as temperature_c,
    vibration_mm_s::numeric(8, 3) as vibration_mm_s,
    pressure_bar::numeric(9, 3) as pressure_bar,
    energy_kwh::numeric(12, 3) as energy_kwh,
    operating_state::text as operating_state,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'machine_telemetry') }}
