{# Scalar subquery for the newest load date of a snapshot table.
   Staging models use it to expose only the most recent full snapshot. #}
{% macro latest_ds(relation) %}
    (select max(ds) from {{ relation }})
{% endmacro %}
