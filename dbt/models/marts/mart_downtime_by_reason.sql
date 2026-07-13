select
    factory_id,
    factory_name,
    production_line_id,
    line_name,
    downtime_type,
    reason_code,
    count(*) as downtime_event_count,
    sum(downtime_minutes) / 60.0 as downtime_hours,
    avg(downtime_minutes) / 60.0 as average_event_hours,
    count(*) filter (where is_open) as open_event_count
from {{ ref('fct_downtime') }}
group by
    factory_id,
    factory_name,
    production_line_id,
    line_name,
    downtime_type,
    reason_code
