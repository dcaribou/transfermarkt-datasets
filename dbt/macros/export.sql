{% macro export_table(model_name) %}
  COPY {{ ref(model_name) }} TO '../../data/prep/appearances.csv' (HEADER, DELIMITER ',')
{% endmacro %}
