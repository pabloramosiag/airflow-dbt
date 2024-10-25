{% test accepted_values_not_case_sensitive(model, values, column_name) %}

    WITH accepted_values AS (
        SELECT {{ column_name }}
        FROM {{ model }}
        WHERE LOWER({{ column_name }}::STRING) NOT IN (
            {% for value in values -%}
                '{{ value|lower }}'
                {%- if not loop.last -%},{%- endif %}
            {%- endfor %}
        )
    )
    SELECT *
    FROM accepted_values

{% endtest %}
