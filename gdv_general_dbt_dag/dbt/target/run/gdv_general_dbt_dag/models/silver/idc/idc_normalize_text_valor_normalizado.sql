

  create or replace view `datagov-473122`.`test_idc_silver`.`idc_normalize_text_valor_normalizado`
  OPTIONS()
  as 

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
  -- Por ahora, mantener todas las demás columnas
  * EXCEPT(departamento, ano_idc, fecha_lectura)
  
FROM `datagov-473122`.`test_idc_silver`.`idc_normalize_columns_valor_normalizado`;

