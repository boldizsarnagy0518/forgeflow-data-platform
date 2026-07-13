select
    w.maintenance_work_order_id,
    w.machine_id,
    c.machine_name,
    c.machine_type,
    c.production_line_id,
    c.line_name,
    c.factory_id,
    c.factory_name,
    w.maintenance_type,
    w.priority,
    w.status,
    w.created_at,
    w.scheduled_for,
    extract(epoch from (current_timestamp - w.created_at)) / 86400.0 as backlog_age_days,
    w.scheduled_for < current_timestamp as is_overdue,
    w.technician_id
from {{ ref('stg_maintenance_work_orders') }} as w
inner join {{ ref('int_machine_context') }} as c
    on w.machine_id = c.machine_id
where w.status in ('open', 'in_progress')
