{% test is_castable_to_float(model, column_name) %}

  WITH invalid_casts AS (
      SELECT {{ column_name }}
      FROM {{ model }}
      WHERE try_cast({{ column_name }} as float) IS NULL
        AND {{ column_name }} IS NOT NULL
  )
  SELECT *
  FROM invalid_casts

{% endtest %}
