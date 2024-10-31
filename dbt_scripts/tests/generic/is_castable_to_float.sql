{% test is_castable_to_float(model, column_name, is_null=True) %}

  WITH invalid_casts AS (
      SELECT {{ column_name }}
      FROM {{ model }}
      WHERE try_cast({{ column_name }} as float) IS NULL

      {%- if is_null -%}

          AND {{ column_name }} IS NOT NULL

      {%- endif -%}
  )
  SELECT *
  FROM invalid_casts

{% endtest %}
