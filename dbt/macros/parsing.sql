{% macro parse_market_value(expression) %}

    {% set regex = '(£|€)([0-9\.]+)(Th|k|m|M)' %}

    {% set str_number %}
        regexp_extract({{ expression }}, '{{ regex }}', 2)
    {% endset %}

    {% set number %}
        case when len({{ str_number }}) > 0 then ({{ str_number }})::float end
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


{% macro parse_contract_expiration_date(expression) %}

    {% set month_and_day_str %}
      str_split({{ expression }}, ',')[1]
    {% endset %}

    {% set year_str %}
      trim(str_split({{ expression }}, ',')[2])
    {% endset %}

    {% set month_and_day %}
        case
            when len({{ month_and_day_str }}) = 0 then null
            when ({{ month_and_day_str }}) ~ '[a-zA-Z]{3} [0-9]+' then {{ month_and_day_str }}
            else {{ month_and_day_str }} || ' 1'
        end
    {% endset %}

    strptime({{ month_and_day }} || ', ' || {{ year_str }}, '%b %d, %Y')
  
{% endmacro %}
