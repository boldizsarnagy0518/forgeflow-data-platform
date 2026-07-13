{% test end_after_start(model, column_name, start_column) %}
select *
from {{ model }}
where {{ column_name }} is not null
  and {{ column_name }} < {{ start_column }}
{% endtest %}
