select
    l.production_line_id,
    l.line_name,
    l.factory_id,
    f.factory_name,
    l.product_family,
    l.nominal_capacity_per_hour,
    l.status,
    l.updated_at
from {{ ref('stg_production_lines') }} as l
inner join {{ ref('stg_factories') }} as f
    on l.factory_id = f.factory_id
