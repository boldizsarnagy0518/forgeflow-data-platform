select
    m.machine_id,
    m.machine_name,
    m.machine_type,
    m.manufacturer,
    m.model,
    m.installed_on,
    m.status as machine_status,
    m.updated_at as machine_updated_at,
    l.production_line_id,
    l.line_name,
    l.product_family,
    l.nominal_capacity_per_hour,
    l.status as production_line_status,
    f.factory_id,
    f.factory_name,
    f.country_code,
    f.timezone,
    f.status as factory_status
from {{ ref('stg_machines') }} as m
inner join {{ ref('stg_production_lines') }} as l
    on m.production_line_id = l.production_line_id
inner join {{ ref('stg_factories') }} as f
    on l.factory_id = f.factory_id
