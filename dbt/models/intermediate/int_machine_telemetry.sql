{{
    config(
        materialized='incremental',
        unique_key='telemetry_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        indexes=[
            {'columns': ['telemetry_id'], 'unique': true},
            {'columns': ['machine_id', 'event_timestamp']}
        ]
    )
}}

select
    t.telemetry_id,
    t.machine_id,
    c.machine_name,
    c.machine_type,
    c.production_line_id,
    c.line_name,
    c.factory_id,
    c.factory_name,
    t.event_timestamp,
    t.temperature_c,
    t.vibration_mm_s,
    t.pressure_bar,
    t.energy_kwh,
    t.operating_state,
    t.updated_at,
    t._batch_id,
    t._source_file_id,
    t._source_row_number,
    t._ingested_at,
    t._record_checksum,
    extract(epoch from (t._ingested_at - t.event_timestamp)) / 3600.0 as arrival_lag_hours,
    t.event_timestamp < t._ingested_at - interval '24 hours' as is_late_arrival
from {{ ref('stg_machine_telemetry') }} as t
inner join {{ ref('int_machine_context') }} as c
    on t.machine_id = c.machine_id
where 1 = 1
{{ incremental_event_time_filter('t.event_timestamp') }}
