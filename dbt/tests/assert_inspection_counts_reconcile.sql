{{ config(severity='error', store_failures=true, tags=['business_rule']) }}

select
    quality_inspection_id,
    sample_size,
    passed_units,
    failed_units
from {{ ref('fct_quality_inspections') }}
where passed_units + failed_units > sample_size
