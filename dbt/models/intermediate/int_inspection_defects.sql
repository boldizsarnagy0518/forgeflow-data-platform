with defects as (
    select
        quality_inspection_id,
        sum(defect_count) as defect_occurrences,
        sum(defect_count) filter (where severity = 'critical') as critical_defect_occurrences,
        count(distinct defect_type) as defect_categories
    from {{ ref('stg_product_defects') }}
    group by quality_inspection_id
)

select
    i.quality_inspection_id,
    i.production_order_id,
    o.production_line_id,
    l.factory_id,
    i.inspected_at,
    i.sample_size,
    i.passed_units,
    i.failed_units,
    i.result,
    coalesce(d.defect_occurrences, 0) as defect_occurrences,
    coalesce(d.critical_defect_occurrences, 0) as critical_defect_occurrences,
    coalesce(d.defect_categories, 0) as defect_categories,
    case
        when i.sample_size = 0 then null
        else i.failed_units::numeric / i.sample_size
    end as sampled_defect_rate,
    i._batch_id,
    i._source_file_id,
    i._record_checksum
from {{ ref('stg_quality_inspections') }} as i
inner join {{ ref('stg_production_orders') }} as o
    on i.production_order_id = o.production_order_id
inner join {{ ref('stg_production_lines') }} as l
    on o.production_line_id = l.production_line_id
left join defects as d
    on i.quality_inspection_id = d.quality_inspection_id
