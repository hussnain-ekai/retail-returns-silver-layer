{% macro generate_hash_key(*args) %}
    md5(concat({{ args | join(', ') }}))
{% endmacro %}