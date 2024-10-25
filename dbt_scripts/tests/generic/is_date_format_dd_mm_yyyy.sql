{% test is_date_format_dd_mm_yyyy(model, column_name) %}

    WITH dates_validated AS (
        SELECT
            {{ column_name }},
            CASE
                WHEN try_cast( {{ column_name }} as date) IS NOT NULL
                THEN true
                ELSE false
            END AS is_valid_date
        FROM {{ model }}
        WHERE {{ column_name }} IS NOT NULL
    )
    SELECT *
    FROM dates_validated
    WHERE is_valid_date = false

{% endtest %}
