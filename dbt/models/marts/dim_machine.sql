select
    machine_id,
    machine_name,
    machine_type,
    manufacturer,
    model,
    installed_on,
    machine_status as status,
    machine_updated_at as updated_at,
    production_line_id,
    line_name,
    product_family,
    factory_id,
    factory_name,
    country_code,
    timezone
from {{ ref('int_machine_context') }}
