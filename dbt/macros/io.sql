{% macro export_table(model) %}
  {% call statement(name, fetch_result=True) %}
    COPY {{ model.name }} TO '../data/prep/{{ model.name }}.csv.gz' (HEADER, DELIMITER ',', COMPRESSION gzip)
  {% endcall %}
{% endmacro %}
