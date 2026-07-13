{{ config(alias='data_freshness') }}

with source_watermarks as (
    select 'factories' as source_name, max(_ingested_at) as latest_ingested_at, max(updated_at) as latest_event_at
    from {{ ref('stg_factories') }}
    union all
    select 'production_lines', max(_ingested_at), max(updated_at)
    from {{ ref('stg_production_lines') }}
    union all
    select 'machines', max(_ingested_at), max(updated_at)
    from {{ ref('stg_machines') }}
    union all
    select 'shifts', max(_ingested_at), max(ended_at)
    from {{ ref('stg_shifts') }}
    union all
    select 'production_orders', max(_ingested_at), max(coalesce(actual_end_at, planned_start_at))
    from {{ ref('stg_production_orders') }}
    union all
    select 'machine_telemetry', max(_ingested_at), max(event_timestamp)
    from {{ ref('stg_machine_telemetry') }}
    union all
    select 'downtime_events', max(_ingested_at), max(coalesce(ended_at, started_at))
    from {{ ref('stg_downtime_events') }}
    union all
    select 'maintenance_work_orders', max(_ingested_at), max(updated_at)
    from {{ ref('stg_maintenance_work_orders') }}
    union all
    select 'quality_inspections', max(_ingested_at), max(inspected_at)
    from {{ ref('stg_quality_inspections') }}
    union all
    select 'product_defects', max(_ingested_at), max(detected_at)
    from {{ ref('stg_product_defects') }}
)

select
    source_name,
    latest_ingested_at,
    latest_event_at,
    coalesce(latest_event_at, latest_ingested_at) as latest_recorded_at,
    extract(epoch from (current_timestamp - latest_ingested_at)) / 3600.0 as ingestion_age_hours,
    extract(epoch from (current_timestamp - latest_event_at)) / 3600.0 as event_age_hours,
    case
        when latest_ingested_at is null then 'missing'
        when latest_ingested_at >= current_timestamp - interval '24 hours' then 'fresh'
        when latest_ingested_at >= current_timestamp - interval '72 hours' then 'warning'
        else 'stale'
    end as freshness_status
from source_watermarks
