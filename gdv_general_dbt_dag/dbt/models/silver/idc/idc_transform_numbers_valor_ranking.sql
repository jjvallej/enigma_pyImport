{{
  config(
    materialized='table',
    schema='test_idc_silver'
  )
}}

-- Modelo 3: Transforma números a enteros (vacíos = 0)
-- Transforma automáticamente todas las columnas excepto: departamento, ano_idc, fecha_lectura, nombre_hoja
-- 
-- IMPORTANTE: Este modelo debe ser generado usando el script:
--   python3 dbt/scripts/generate_idc_transform_models.py
-- 
-- El script consultará las columnas reales en BigQuery y generará el SQL completo
-- con todas las transformaciones aplicadas usando las macros clean_integer.

SELECT
  -- Mantener columnas de texto sin cambios
  departamento,
  ano_idc,
  fecha_lectura,
  nombre_hoja,
  
  -- TODO: Ejecutar el script generate_idc_transform_models.py para generar
  -- las transformaciones de todas las columnas numéricas aquí
  
  -- Por ahora, mantener todas las demás columnas sin transformar
  * EXCEPT(departamento, ano_idc, fecha_lectura, nombre_hoja)
  
FROM {{ ref('idc_normalize_text_valor_ranking') }}
