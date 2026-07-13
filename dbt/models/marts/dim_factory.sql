select
    factory_id,
    factory_name,
    country_code,
    timezone,
    opened_on,
    status,
    updated_at
from {{ ref('stg_factories') }}
