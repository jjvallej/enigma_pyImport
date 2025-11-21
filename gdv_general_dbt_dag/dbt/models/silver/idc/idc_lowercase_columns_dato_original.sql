{{
  config(
    materialized='table',
    schema='test_idc_silver'
  )
}}

-- Modelo: Convierte todos los nombres de columnas a minúsculas y asegura formato snake_case (texto_texto)
-- Toma las columnas del modelo anterior y convierte sus nombres a minúsculas

SELECT
  -- Convertir columnas conocidas (los valores se mantienen, solo se asegura que los nombres estén en minúsculas)
  departamento,
  ano_idc,
  fecha_lectura,
  
  -- Convertir todas las demás columnas
  -- NOTA: En SQL no se pueden cambiar dinámicamente los nombres de columnas sin listarlas explícitamente.
  -- Los nombres de columnas ya deberían estar en snake_case del modelo anterior.
  -- Si alguna columna tiene mayúsculas en el nombre, se debe agregar explícitamente aquí.
  -- 
  -- Patrón: Si una columna se llama "NombreColumna", agregar: NombreColumna AS nombre_columna
  -- Si ya está en snake_case pero con mayúsculas como "Nombre_Columna", agregar: Nombre_Columna AS nombre_columna
  
  -- Por ahora, mantener todas las demás columnas (se deben agregar explícitamente arriba si tienen mayúsculas)
  * EXCEPT(departamento, ano_idc, fecha_lectura)
  
FROM {{ ref('idc_normalize_columns_dato_original') }}

