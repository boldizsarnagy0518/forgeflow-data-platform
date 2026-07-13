select
    quality_inspection_id,
    production_order_id,
    production_line_id,
    factory_id,
    inspected_at,
    sample_size,
    passed_units,
    failed_units,
    result,
    defect_occurrences,
    critical_defect_occurrences,
    defect_categories,
    sampled_defect_rate,
    _batch_id,
    _source_file_id,
    _record_checksum
from {{ ref('int_inspection_defects') }}
