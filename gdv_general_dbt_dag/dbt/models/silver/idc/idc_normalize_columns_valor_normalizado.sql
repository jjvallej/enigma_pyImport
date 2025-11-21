{{
  config(
    materialized='view',
    schema='test_idc_silver'
  )
}}

-- Modelo 1: Normaliza nombres de columnas a snake_case
-- Solo transforma nombres de columnas, NO transforma el contenido

SELECT
  -- Normalizar nombre de columna Departamento a snake_case
  Departamento AS departamento,
  
  -- Normalizar nombre de columna Año IDC a snake_case
  `Año IDC` AS ano_idc,
  
  -- Mantener columnas que ya están en snake_case
  fecha_lectura,
  
  -- TODO: Normalizar nombres de todas las demás columnas a snake_case
  * EXCEPT(Departamento, `Año IDC`, fecha_lectura)
  
FROM {{ source('bronze_idc', 'idc_raw_data_valor_normalizado') }}

