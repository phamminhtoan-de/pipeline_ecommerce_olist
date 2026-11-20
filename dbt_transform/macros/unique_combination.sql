{% test unique_combination(model, combination_of_columns) %}
select *
from (
    select
        *,
        count(*) over (partition by {{ combination_of_columns | join(', ') }}) as _duplicate_count
    from {{ model }}
) as _dup
where _duplicate_count > 1
{% endtest %}
