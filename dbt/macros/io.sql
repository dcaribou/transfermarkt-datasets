{#
    Export a model to the prep folder in a CSV format.

    Arguments:
      - relation: the model to be exported.
#}
{% macro export_table(relation) %}

  {% set model_config = get_model_config(relation.identifier) %}
  {{ log(model_config) }}

  {% if model_config.enabled %}
      {% call statement(name, fetch_result=True) %}
        COPY {{ relation }} TO '../data/prep/{{ model.name }}.csv.gz' (HEADER, DELIMITER ',', COMPRESSION gzip)
      {% endcall %}
  {% else %}
      SELECT 1
  {% endif %}

{% endmacro %}
