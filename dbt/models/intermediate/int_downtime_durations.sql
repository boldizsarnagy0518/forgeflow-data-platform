select
    d.downtime_event_id,
    d.machine_id,
    c.machine_name,
    c.machine_type,
    c.production_line_id,
    c.line_name,
    c.factory_id,
    c.factory_name,
    d.started_at,
    d.ended_at,
    d.downtime_type,
    d.reason_code,
    extract(epoch from (coalesce(d.ended_at, current_timestamp) - d.started_at)) / 60.0
        as downtime_minutes,
    d.downtime_type = 'unplanned' and d.reason_code = 'breakdown' as is_failure,
    d.ended_at is null as is_open,
    d._batch_id,
    d._source_file_id,
    d._record_checksum
from {{ ref('stg_downtime_events') }} as d
inner join {{ ref('int_machine_context') }} as c
    on d.machine_id = c.machine_id
