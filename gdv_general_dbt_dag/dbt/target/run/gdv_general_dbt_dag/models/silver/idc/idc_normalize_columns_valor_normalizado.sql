

  create or replace view `datagov-473122`.`test_idc_silver`.`idc_normalize_columns_valor_normalizado`
  OPTIONS()
  as 

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
  
FROM `datagov-473122`.`test_idc_bronze`.`idc_raw_data_valor_normalizado`;

