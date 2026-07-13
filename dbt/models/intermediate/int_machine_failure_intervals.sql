with failures as (
    select
        downtime_event_id,
        machine_id,
        factory_id,
        production_line_id,
        started_at,
        ended_at,
        downtime_minutes,
        lag(ended_at) over (partition by machine_id order by started_at) as previous_failure_ended_at
    from {{ ref('int_downtime_durations') }}
    where is_failure
)

select
    downtime_event_id,
    machine_id,
    factory_id,
    production_line_id,
    started_at,
    ended_at,
    downtime_minutes as repair_minutes,
    case
        when previous_failure_ended_at is null then null
        else greatest(
            0,
            extract(epoch from (started_at - previous_failure_ended_at)) / 3600.0
        )
    end as hours_since_previous_failure
from failures
