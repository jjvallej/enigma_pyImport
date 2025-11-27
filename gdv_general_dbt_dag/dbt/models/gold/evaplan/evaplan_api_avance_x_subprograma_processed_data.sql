{{
  config(
    materialized='view',
    schema='gold_dpt_planeacion_municipal_dev',
    alias='evaplan_api_avance_x_subprograma_processed_data'
  )
}}

-- Modelo gold: Une la tabla de periodos con la tabla de avance_x_subprograma
-- JOIN por peri_idp para tener toda la información del periodo junto con los datos de avance_x_subprograma

SELECT
  p.*,
  a.* EXCEPT(peri_idp, fecha_lectura)  -- Excluir peri_idp y fecha_lectura duplicados (ya están en p.*)
FROM {{ source('silver_evaplan', 'evaplan_api_periodos_transformed_data') }} p
INNER JOIN {{ source('silver_evaplan', 'evaplan_api_avance_x_subprograma_transformed_data') }} a
  ON CAST(p.peri_idp AS INT64) = CAST(a.peri_idp AS INT64)

