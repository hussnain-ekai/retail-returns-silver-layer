{% macro standardize_string(column) %}
    upper(trim({{ column }}))
{% endmacro %}