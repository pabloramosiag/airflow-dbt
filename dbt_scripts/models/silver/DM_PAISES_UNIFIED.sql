{{
  config(
    materialized = "table"
  )
}}

WITH DM_PAISES AS (
    SELECT *
    FROM {{ 'ref(DM_PAISES)' }}
),
DM_PAISES_FR AS (
    SELECT *
    FROM {{ 'ref(DM_PAISES_FR)' }}
    WHERE ID_PAIS != 1
)
SELECT
    ID_PAIS,
    PAIS_NAME
FROM DM_PAISES_FR
UNION
SELECT
    ID_PAIS,
    PAIS_NAME
FROM DM_PAISES
