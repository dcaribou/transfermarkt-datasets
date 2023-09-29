{#
    Get a model configuration from the node graph
    https://docs.getdbt.com/reference/dbt-jinja-functions/graph#accessing-models

    Arguments:
        - model_name: the name of the models to be fetched
#}
{% macro get_model_config(model_name) %}
    {% if execute %}
        {% for node in graph.nodes.values() %}
            {% if node.name == model_name %}
                {{ return(node.config) }}
            {% endif %}            
        {% endfor %}
        {{ return(none) }}  
    {% endif %}
        {{ return(none) }}
{% endmacro %}
