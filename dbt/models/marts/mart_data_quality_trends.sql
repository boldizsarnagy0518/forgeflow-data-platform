select
    (occurred_at at time zone 'UTC')::date as quality_date,
    check_type,
    scope,
    count(*) as check_count,
    count(*) filter (where status = 'passed') as passed_check_count,
    count(*) filter (where status = 'failed') as failed_check_count,
    count(*) filter (where status = 'warning') as warning_check_count,
    count(*) filter (where severity = 'error' and status = 'failed') as error_check_count
from {{ source('observability', 'quality_results') }}
group by (occurred_at at time zone 'UTC')::date, check_type, scope
