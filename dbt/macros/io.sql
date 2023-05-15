{% macro export_table(model) %}
  {% call statement(name, fetch_result=True) %}
    COPY {{ model.name }} TO '../data/prep_new/{{ model.name }}.csv' (HEADER, DELIMITER ',')
  {% endcall %}
{% endmacro %}
