WITH raw_acceso_externo AS (
    SELECT * FROM public."RSG_ACCES_EXTERNS"
)
SELECT "ID_ACCES_EXTERNS", 
    "USUARI", 
    "ROLES", 
    "MOTIVO", 
    "ACCESO", 
    TO_DATE("DATA_ACCES", 'DD/MM/YY') AS "DATA_ACCESO", 
    "RESULTADO", 
    "ID_EXPEDIENT", 
    "NUM_DOCUMENT"
FROM raw_acceso_externo