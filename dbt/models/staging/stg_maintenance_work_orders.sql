select
    maintenance_work_order_id::text as maintenance_work_order_id,
    machine_id::text as machine_id,
    created_at::timestamptz as created_at,
    scheduled_for::timestamptz as scheduled_for,
    completed_at::timestamptz as completed_at,
    maintenance_type::text as maintenance_type,
    priority::text as priority,
    status::text as status,
    technician_id::text as technician_id,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'maintenance_work_orders') }}
