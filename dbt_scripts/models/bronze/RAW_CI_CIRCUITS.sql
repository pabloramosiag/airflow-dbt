{{
  config(
    materialized = "table"
  )
}}

WITH raw_ci_circuits AS (
    SELECT DISTINCT *
	FROM public."RSG_CI_CIRCUITS"
)
SELECT 
    "ID" as ID_CI_CIRCUITS, 
    "NOM" AS NOMBRE, 
    "OBSERVACIONS" AS OBSERVACIONES, 
    "DATA_ACTUALITZACIO" AS DATA_ACTUALIZACION, 
    "ID_USUARI_ACTUALITZACIO" AS ID_USUARIO_ACTUAALIZACION
FROM raw_ci_circuits