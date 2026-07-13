select
    i.factory_id,
    f.factory_name,
    (i.inspected_at at time zone 'UTC')::date as inspection_date,
    count(*) as inspection_count,
    sum(i.sample_size) as inspected_units,
    sum(i.passed_units) as passed_units,
    sum(i.failed_units) as failed_units,
    sum(i.defect_occurrences) as defect_occurrences,
    sum(i.critical_defect_occurrences) as critical_defect_occurrences,
    case
        when sum(i.sample_size) = 0 then null
        else sum(i.failed_units)::numeric / sum(i.sample_size)
    end as sampled_defect_rate
from {{ ref('fct_quality_inspections') }} as i
inner join {{ ref('dim_factory') }} as f
    on i.factory_id = f.factory_id
group by i.factory_id, f.factory_name, (i.inspected_at at time zone 'UTC')::date
