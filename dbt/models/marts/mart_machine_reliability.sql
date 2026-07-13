with failure_metrics as (
    select
        machine_id,
        count(*) as breakdown_count,
        count(*) filter (where ended_at is not null) as resolved_breakdown_count,
        avg(repair_minutes) filter (where ended_at is not null) / 60.0
            as mean_time_to_repair_hours,
        avg(hours_since_previous_failure) as mean_time_between_failures_hours
    from {{ ref('int_machine_failure_intervals') }}
    group by machine_id
),
downtime_metrics as (
    select
        machine_id,
        sum(downtime_minutes) / 60.0 as total_downtime_hours,
        sum(downtime_minutes) filter (where downtime_type = 'unplanned') / 60.0
            as unplanned_downtime_hours,
        count(*) filter (where is_open) as open_downtime_events
    from {{ ref('fct_downtime') }}
    group by machine_id
),
telemetry_metrics as (
    select
        machine_id,
        sum(sample_count) as telemetry_sample_count,
        sum(late_arrival_count) as late_arrival_count,
        max(latest_event_at) as latest_telemetry_at,
        max(latest_ingested_at) as latest_telemetry_ingested_at
    from {{ ref('fct_machine_telemetry_daily') }}
    group by machine_id
)

select
    m.machine_id,
    m.machine_name,
    m.machine_type,
    m.status as machine_status,
    m.production_line_id,
    m.line_name,
    m.factory_id,
    m.factory_name,
    coalesce(f.breakdown_count, 0) as breakdown_count,
    coalesce(f.resolved_breakdown_count, 0) as resolved_breakdown_count,
    f.mean_time_to_repair_hours,
    f.mean_time_between_failures_hours,
    coalesce(d.total_downtime_hours, 0) as total_downtime_hours,
    coalesce(d.unplanned_downtime_hours, 0) as unplanned_downtime_hours,
    coalesce(d.open_downtime_events, 0) as open_downtime_events,
    coalesce(t.telemetry_sample_count, 0) as telemetry_sample_count,
    coalesce(t.late_arrival_count, 0) as late_arrival_count,
    t.latest_telemetry_at,
    t.latest_telemetry_ingested_at,
    extract(epoch from (current_timestamp - t.latest_telemetry_at)) / 3600.0
        as telemetry_age_hours,
    t.latest_telemetry_at is null
        or t.latest_telemetry_at
            < current_timestamp - make_interval(hours => {{ var('telemetry_stale_after_hours', 6) | int }})
        as has_stale_telemetry
from {{ ref('dim_machine') }} as m
left join failure_metrics as f on m.machine_id = f.machine_id
left join downtime_metrics as d on m.machine_id = d.machine_id
left join telemetry_metrics as t on m.machine_id = t.machine_id
