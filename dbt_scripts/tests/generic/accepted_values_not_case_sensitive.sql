{% test accepted_values_not_case_sensitive(model, column_name, values) %}

    WITH accepted_values AS (
        SELECT {{ column_name }}
        FROM {{ model }}
        WHERE LOWER({{ column_name }}::STRING) NOT IN (
            {% for value in values -%}
                '{{ value|lower }}'
            {%- endfor %}
        )
    )
    SELECT *
    FROM accepted_values

{% endtest %}
