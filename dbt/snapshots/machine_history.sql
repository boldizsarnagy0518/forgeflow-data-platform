{% snapshot machine_history %}

{{
    config(
        unique_key='machine_id',
        strategy='timestamp',
        updated_at='updated_at',
        invalidate_hard_deletes=True
    )
}}

select
    machine_id,
    production_line_id,
    machine_name,
    machine_type,
    manufacturer,
    model,
    installed_on,
    status,
    updated_at,
    _batch_id,
    _source_file_id,
    _record_checksum
from {{ ref('stg_machines') }}

{% endsnapshot %}
