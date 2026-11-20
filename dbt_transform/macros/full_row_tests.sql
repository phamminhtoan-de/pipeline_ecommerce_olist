{% macro default__test_not_null(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} is null

{% endmacro %}


{% macro default__test_unique(model, column_name) %}

select *
from (
    select
        *,
        count(*) over (partition by {{ column_name }}) as _duplicate_count
    from {{ model }}
    where {{ column_name }} is not null
) as _dup
where _duplicate_count > 1

{% endmacro %}


{% macro default__test_accepted_values(model, column_name, values, quote=True) %}

select *
from {{ model }}
where {{ column_name }} not in (
    {% for value in values -%}
        {% if quote -%}
        '{{ value }}'
        {%- else -%}
        {{ value }}
        {%- endif -%}
        {%- if not loop.last -%},{%- endif %}
    {%- endfor %}
)

{% endmacro %}


{% macro default__test_relationships(model, column_name, to, field) %}

select child.*
from {{ model }} as child
left join {{ to }} as parent
    on child.{{ column_name }} = parent.{{ field }}
where child.{{ column_name }} is not null
  and parent.{{ field }} is null

{% endmacro %}


{% test positive_value(model, column_name) %}

select *
from {{ model }}
where {{ column_name }} < 0

{% endtest %}
