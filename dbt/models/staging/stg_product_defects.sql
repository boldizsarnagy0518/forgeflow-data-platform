select
    product_defect_id::text as product_defect_id,
    quality_inspection_id::text as quality_inspection_id,
    detected_at::timestamptz as detected_at,
    defect_type::text as defect_type,
    severity::text as severity,
    defect_count::integer as defect_count,
    updated_at::timestamptz as updated_at,
    _batch_id::text as _batch_id,
    _source_file_id::uuid as _source_file_id,
    _source_row_number::integer as _source_row_number,
    _ingested_at::timestamptz as _ingested_at,
    _record_checksum::text as _record_checksum
from {{ source('raw', 'product_defects') }}
