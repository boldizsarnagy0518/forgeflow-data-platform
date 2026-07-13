{{ config(alias='factory_performance') }}

with output as (
    select
        factory_id,
        sum(planned_quantity) filter (where status <> 'cancelled') as planned_units,
        coalesce(sum(actual_quantity) filter (where status <> 'cancelled'), 0) as actual_units,
        count(*) filter (where status = 'completed') as completed_orders,
        count(*) filter (where status <> 'cancelled') as active_or_completed_orders
    from {{ ref('fct_production_output') }}
    group by factory_id
),
quality as (
    select
        factory_id,
        sum(sample_size) as inspected_units,
        sum(failed_units) as failed_inspected_units,
        sum(defect_occurrences) as defect_occurrences,
        sum(critical_defect_occurrences) as critical_defect_occurrences
    from {{ ref('fct_quality_inspections') }}
    group by factory_id
),
downtime as (
    select
        factory_id,
        sum(downtime_minutes) / 60.0 as downtime_hours,
        sum(downtime_minutes) filter (where downtime_type = 'unplanned') / 60.0
            as unplanned_downtime_hours
    from {{ ref('fct_downtime') }}
    group by factory_id
)

select
    f.factory_id,
    f.factory_name,
    f.country_code,
    coalesce(o.planned_units, 0) as planned_units,
    coalesce(o.actual_units, 0) as actual_units,
    coalesce(o.completed_orders, 0) as completed_orders,
    coalesce(o.active_or_completed_orders, 0) as active_or_completed_orders,
    case
        when coalesce(o.planned_units, 0) = 0 then null
        else o.actual_units::numeric / o.planned_units
    end as target_attainment_ratio,
    coalesce(q.inspected_units, 0) as inspected_units,
    coalesce(q.failed_inspected_units, 0) as failed_inspected_units,
    coalesce(q.defect_occurrences, 0) as defect_occurrences,
    coalesce(q.critical_defect_occurrences, 0) as critical_defect_occurrences,
    case
        when coalesce(q.inspected_units, 0) = 0 then null
        else q.failed_inspected_units::numeric / q.inspected_units
    end as sampled_defect_rate,
    coalesce(d.downtime_hours, 0) as downtime_hours,
    coalesce(d.unplanned_downtime_hours, 0) as unplanned_downtime_hours
from {{ ref('dim_factory') }} as f
left join output as o on f.factory_id = o.factory_id
left join quality as q on f.factory_id = q.factory_id
left join downtime as d on f.factory_id = d.factory_id
