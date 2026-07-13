select
    o.production_order_id,
    o.production_line_id,
    l.line_name,
    l.factory_id,
    f.factory_name,
    o.product_code,
    o.planned_start_at,
    o.planned_end_at,
    o.actual_start_at,
    o.actual_end_at,
    o.planned_quantity,
    o.actual_quantity,
    o.status,
    o.actual_quantity - o.planned_quantity as quantity_variance,
    case
        when o.planned_quantity = 0 then null
        else o.actual_quantity::numeric / o.planned_quantity
    end as target_attainment_ratio,
    extract(epoch from (o.actual_end_at - o.actual_start_at)) / 3600.0 as actual_duration_hours,
    o._batch_id,
    o._source_file_id,
    o._record_checksum
from {{ ref('stg_production_orders') }} as o
inner join {{ ref('stg_production_lines') }} as l
    on o.production_line_id = l.production_line_id
inner join {{ ref('stg_factories') }} as f
    on l.factory_id = f.factory_id
