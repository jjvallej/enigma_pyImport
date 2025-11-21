{{
  config(
    materialized='view',
    schema='test_idc_silver'
  )
}}

-- Modelo 2: Normaliza texto a mayúsculas sin caracteres especiales
-- Solo transforma el contenido de texto, NO los nombres de columnas

SELECT
  -- Normalizar columna departamento: texto a mayúsculas, solo quitar acentos y caracteres especiales
  -- Ejemplo: "alcalá" -> "ALCALA" (mantiene todas las letras, solo quita acentos)
  -- Usar TRANSLATE para reemplazar acentos y luego REGEXP_REPLACE para caracteres especiales
  REGEXP_REPLACE(
    TRANSLATE(
      UPPER(COALESCE(CAST(departamento AS STRING), '')),
      'ÁÀÂÄÉÈÊËÍÌÎÏÓÒÔÖÚÙÛÜÑáàâäéèêëíìîïóòôöúùûüñ',
      'AAAEEEEIIIOOOUUUUNaaaeeeeiiiiooouuuun'
    ),
    r'[^A-Z0-9\s]', ''
  ) AS departamento,
  
  -- Mantener columnas que no son de texto
  ano_idc,
  fecha_lectura,
  
  -- TODO: Normalizar texto en todas las demás columnas de texto
  -- Para cada columna de texto adicional, usar la misma lógica:
  -- UPPER(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(COALESCE(CAST(nombre_columna AS STRING), ''), r'á', 'a'), r'é', 'e'), r'í', 'i'), r'ó', 'o'), r'ú', 'u'), r'ñ', 'n')) AS nombre_columna,
  -- 
  -- Para columnas numéricas, mantener como están (se transformarán en el siguiente modelo)
  
  -- Por ahora, mantener todas las demás columnas (se normalizarán cuando se agreguen las líneas arriba)
  * EXCEPT(departamento, ano_idc, fecha_lectura)
  
FROM {{ ref('idc_normalize_columns_dato_original') }}

