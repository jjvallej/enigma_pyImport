{{
  config(
    materialized='table',
    schema='silver_dpt_planeacion_municipal_dev',
    alias='idi_transformed_data_consolidated'
  )
}}

-- Modelo consolidado IDI - Une 2024 y 2023 homologado

WITH datos_2024 AS (
  SELECT 
    2024 AS anio,
    UPPER(REGEXP_REPLACE(NORMALIZE(departamento, NFD), r'\pM', '')) AS departamento,
    * EXCEPT(departamento)
  FROM {{ ref('idi_transformed_data_2024') }}
),

datos_2023 AS (
  SELECT 
    2023 AS anio,
    UPPER(REGEXP_REPLACE(NORMALIZE(departamento, NFD), r'\pM', '')) AS departamento,
    * EXCEPT(departamento)
  FROM {{ ref('idi_transformed_data_2023') }}
)

SELECT * FROM datos_2024
UNION ALL
SELECT * FROM datos_2023
ORDER BY anio DESC, departamento