{{ config(severity='error', store_failures=true, tags=['incident', 'business_rule']) }}

-- Contract-valid quantities remain visible, but more than 150% of plan is an error.
-- fct_production_output -> mart_factory_performance -> dashboard/API exposures makes
-- the downstream impact discoverable in manifest.json and persisted lineage edges.
select
    production_order_id,
    factory_id,
    production_line_id,
    planned_quantity,
    actual_quantity,
    _batch_id,
    _source_file_id
from {{ ref('fct_production_output') }}
where actual_quantity is not null
  and actual_quantity > 1.5 * planned_quantity
