{% test too_many_missings(model, column_name, tolerance) %}

with validation as (

    select
        {{ column_name }} as target_column

    from {{ model }}

),

validation_errors as (

    select
        count(*) as total,
        total - count(target_column) as missings

    from validation

)

select *, (missings::float/total)  as missings_pct
from validation_errors
where missings_pct > {{ tolerance }}

{% endtest %}
