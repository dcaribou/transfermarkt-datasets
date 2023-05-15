{% macro parse_market_value(expression) %}

    {% set factor = regexp_extract %}
    {% set regex = '(£|€)([0-9\.]+)(Th|k|m|M)' %}

    {% set number %}
        regexp_extract({{ expression }}, '{{ regex }}', 2)::float
    {% endset %}

    {% set str_factor %}
        regexp_extract({{ expression }}, '{{ regex }}', 3)
    {% endset %}
    
    {% set factor %}
        case
            when {{ str_factor }} in ('Th', 'k') then 1e3
            when {{ str_factor }} in ('m', 'M') then 1e6
        end
    {% endset %}

    (({{ number }})*({{ factor }}))::integer
  
{% endmacro %}
