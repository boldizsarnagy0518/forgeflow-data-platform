{% docs target_attainment_ratio %}
Actual reported units divided by planned units. Cancelled orders are excluded from factory totals,
and a zero planned quantity returns null rather than an infinite or misleading ratio. A separate
error-severity dbt test rejects actual quantity above 150% of plan.
{% enddocs %}

{% docs sampled_defect_rate %}
Failed units in inspected samples divided by total sampled units. This is a sample outcome, not a
claim about every produced unit. Empty samples return null. Defect occurrences can exceed failed
units because one unit can exhibit multiple defect categories.
{% enddocs %}

{% docs mtbf %}
Mean time between failures is the average operating time from the previous completed unplanned
breakdown's end to the next breakdown's start. The first observed breakdown and intervals following
an unclosed breakdown are excluded. It is an observed-history metric, not a reliability forecast.
{% enddocs %}

{% docs mttr %}
Mean time to repair is the average elapsed duration of unplanned breakdown events. Open breakdowns
are excluded because their repair duration is not final.
{% enddocs %}

{% docs data_freshness %}
Ingestion freshness measures time since the latest accepted row, while event freshness measures time
since the newest domain event. The default source thresholds are warning after 24 hours and error
after 72 hours. Telemetry reliability separately flags event age above six hours. These are explicit
demo thresholds, not learned anomaly boundaries.
{% enddocs %}
