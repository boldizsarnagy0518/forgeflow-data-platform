{% macro incremental_event_time_filter(source_expression, persisted_column='event_timestamp') -%}
    {%- set backfill_start = var('backfill_start', none) -%}
    {%- set backfill_end = var('backfill_end', none) -%}
    {%- if backfill_start %}
        and {{ source_expression }} >= '{{ backfill_start | replace("'", "''") }}'::timestamptz
        {%- if backfill_end %}
        and {{ source_expression }} < '{{ backfill_end | replace("'", "''") }}'::timestamptz
        {%- endif %}
    {%- elif is_incremental() %}
        and {{ source_expression }} >= (
            select coalesce(
                max({{ persisted_column }})
                    - make_interval(hours => {{ var('telemetry_lookback_hours', 48) | int }}),
                '1900-01-01 00:00:00+00'::timestamptz
            )
            from {{ this }}
        )
    {%- endif %}
{%- endmacro %}
