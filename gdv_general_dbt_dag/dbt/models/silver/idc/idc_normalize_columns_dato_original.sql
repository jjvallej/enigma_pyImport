{{
  config(
    materialized='table',
    schema='test_idc_silver'
  )
}}

-- Modelo: Normaliza nombres de columnas a snake_case
-- Convierte todas las columnas a formato texto_texto (snake_case)
-- Incluye columnas con mayúsculas, guiones y espacios

SELECT
  -- Normalizar columnas conocidas explícitamente
  Departamento AS departamento,
  `Año IDC` AS ano_idc,
  fecha_lectura,
  
  -- Normalizar todas las demás columnas a snake_case
  -- Para columnas con guiones (ej: INS-1-1), convertir guiones a guiones bajos y a minúsculas
  -- Para columnas con mayúsculas y espacios, convertir a minúsculas y reemplazar espacios con guiones bajos
  -- 
  -- Patrón para columnas con guiones: `INS-1-1` AS ins_1_1
  -- Patrón para columnas con mayúsculas: `Texto_Texto` AS texto_texto
  --
  -- NOTA: Se deben agregar explícitamente todas las columnas aquí.
  -- Para columnas con caracteres especiales, usar backticks: `Nombre-Columna` AS nombre_columna
  
  -- Por ahora, mantener todas las demás columnas (se deben agregar explícitamente arriba)
  * EXCEPT(Departamento, `Año IDC`, fecha_lectura)
  
FROM {{ source('bronze_idc', 'idc_raw_data_dato_original') }}
