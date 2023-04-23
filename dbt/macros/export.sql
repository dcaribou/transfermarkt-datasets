{% macro export_table(model_name) %}
  {% call statement(name, fetch_result=True) %}
    COPY {{ ref(model_name) }} TO '../data/prep/appearances.csv' (HEADER, DELIMITER ',')
  {% endcall %}
{% endmacro %}
