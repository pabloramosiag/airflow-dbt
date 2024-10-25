{% test valid_numeric_date_yyyymm(model, column_name) %}

    WITH invalid_date AS (
        SELECT {{ column_name }}
        FROM {{ model }}
        WHERE TO_DATE({{ column_name }} || 01, 'YYYYMMDD') > CURRENT_DATE
    )
    SELECT *
    FROM invalid_date

{% endtest %}
