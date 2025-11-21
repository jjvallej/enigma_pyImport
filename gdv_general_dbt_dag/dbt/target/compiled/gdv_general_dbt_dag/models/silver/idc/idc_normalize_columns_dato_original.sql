

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
  -- Para cada columna adicional, agregar una línea como:
  -- `INS-1-1` AS ins_1_1,
  -- `INS-1-2` AS ins_1_2,
  -- `INF-1-1` AS inf_1_1,
  -- etc.
  
  -- Por ahora, mantener todas las demás columnas (se normalizarán cuando se agreguen las líneas arriba)
  * EXCEPT(Departamento, `Año IDC`, fecha_lectura)
  
FROM `datagov-473122`.`test_idc_bronze`.`idc_raw_data_dato_original`