
  
    

    create or replace table `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`ipm_transformed_data`
      
    
    

    OPTIONS()
    as (
      

with __dbt__cte__ipm_transform_stg as (


-- Modelo staging: lee de bronze y normaliza nombres de columnas a snake_case
SELECT
    cod_mpio,
    Municipio AS municipio,
    Total AS total,
    IPM_Pobre_Abs AS ipm_pobre_abs,
    IPM_No_Pobre_Abs AS ipm_no_pobre_abs,
    IPM_Pobre_Porc AS ipm_pobre_porc,
    IPM_No_Pobre_Porc AS ipm_no_pobre_porc,
    I1_Con_Privacion_Abs AS i1_con_privacion_abs,
    I1_Sin_Privacion_Abs AS i1_sin_privacion_abs,
    I1_Con_Privacion_Porc AS i1_con_privacion_porc,
    I1_Sin_Privacion_Porc AS i1_sin_privacion_porc,
    I2_Con_Privacion_Abs AS i2_con_privacion_abs,
    I2_Sin_Privacion_Abs AS i2_sin_privacion_abs,
    I2_Con_Privacion_Porc AS i2_con_privacion_porc,
    I2_Sin_Privacion_Porc AS i2_sin_privacion_porc,
    I3_Con_Privacion_Abs AS i3_con_privacion_abs,
    I3_Sin_Privacion_Abs AS i3_sin_privacion_abs,
    I3_Con_Privacion_Porc AS i3_con_privacion_porc,
    I3_Sin_Privacion_Porc AS i3_sin_privacion_porc,
    I4_Con_Privacion_Abs AS i4_con_privacion_abs,
    I4_Sin_Privacion_Abs AS i4_sin_privacion_abs,
    I4_Con_Privacion_Porc AS i4_con_privacion_porc,
    I4_Sin_Privacion_Porc AS i4_sin_privacion_porc,
    I5_Con_Privacion_Abs AS i5_con_privacion_abs,
    I5_Sin_Privacion_Abs AS i5_sin_privacion_abs,
    I5_Con_Privacion_Porc AS i5_con_privacion_porc,
    I5_Sin_Privacion_Porc AS i5_sin_privacion_porc,
    I6_Con_Privacion_Abs AS i6_con_privacion_abs,
    I6_Sin_Privacion_Abs AS i6_sin_privacion_abs,
    I6_Con_Privacion_Porc AS i6_con_privacion_porc,
    I6_Sin_Privacion_Porc AS i6_sin_privacion_porc,
    I7_Con_Privacion_Abs AS i7_con_privacion_abs,
    I7_Sin_Privacion_Abs AS i7_sin_privacion_abs,
    I7_Con_Privacion_Porc AS i7_con_privacion_porc,
    I7_Sin_Privacion_Porc AS i7_sin_privacion_porc,
    I8_Con_Privacion_Abs AS i8_con_privacion_abs,
    I8_Sin_Privacion_Abs AS i8_sin_privacion_abs,
    I8_Con_Privacion_Porc AS i8_con_privacion_porc,
    I8_Sin_Privacion_Porc AS i8_sin_privacion_porc,
    I9_Con_Privacion_Abs AS i9_con_privacion_abs,
    I9_Sin_Privacion_Abs AS i9_sin_privacion_abs,
    I9_Con_Privacion_Porc AS i9_con_privacion_porc,
    I9_Sin_Privacion_Porc AS i9_sin_privacion_porc,
    I10_Con_Privacion_Abs AS i10_con_privacion_abs,
    I10_Sin_Privacion_Abs AS i10_sin_privacion_abs,
    I10_Con_Privacion_Porc AS i10_con_privacion_porc,
    I10_Sin_Privacion_Porc AS i10_sin_privacion_porc,
    I11_Con_Privacion_Abs AS i11_con_privacion_abs,
    I11_Sin_Privacion_Abs AS i11_sin_privacion_abs,
    I11_Con_Privacion_Porc AS i11_con_privacion_porc,
    I11_Sin_Privacion_Porc AS i11_sin_privacion_porc,
    I12_Con_Privacion_Abs AS i12_con_privacion_abs,
    I12_Sin_Privacion_Abs AS i12_sin_privacion_abs,
    I12_Con_Privacion_Porc AS i12_con_privacion_porc,
    I12_Sin_Privacion_Porc AS i12_sin_privacion_porc,
    I13_Con_Privacion_Abs AS i13_con_privacion_abs,
    I13_Sin_Privacion_Abs AS i13_sin_privacion_abs,
    I13_Con_Privacion_Porc AS i13_con_privacion_porc,
    I13_Sin_Privacion_Porc AS i13_sin_privacion_porc,
    I14_Con_Privacion_Abs AS i14_con_privacion_abs,
    I14_Sin_Privacion_Abs AS i14_sin_privacion_abs,
    I14_Con_Privacion_Porc AS i14_con_privacion_porc,
    I14_Sin_Privacion_Porc AS i14_sin_privacion_porc,
    I15_Con_Privacion_Abs AS i15_con_privacion_abs,
    I15_Sin_Privacion_Abs AS i15_sin_privacion_abs,
    I15_Con_Privacion_Porc AS i15_con_privacion_porc,
    I15_Sin_Privacion_Porc AS i15_sin_privacion_porc,
    fecha_lectura
FROM `datagov-473122`.`bronze_dpt_planeacion_municipal_dev`.`ipm_raw_data`
),  __dbt__cte__ipm_transform_normalize_text as (


-- Modelo para normalizar texto: eliminar acentos y convertir a mayúsculas
SELECT
    -- Identificadores: eliminar acentos y convertir a mayúsculas
    UPPER(TRANSLATE(LOWER(CAST(cod_mpio AS STRING)), 'áéíóúñÁÉÍÓÚÑ', 'aeiounAEIOUN')) AS cod_mpio,
    UPPER(TRANSLATE(LOWER(CAST(municipio AS STRING)), 'áéíóúñÁÉÍÓÚÑ', 'aeiounAEIOUN')) AS municipio,
    
    -- Resto de columnas sin cambios
    total,
    ipm_pobre_abs,
    ipm_no_pobre_abs,
    ipm_pobre_porc,
    ipm_no_pobre_porc,
    i1_con_privacion_abs,
    i1_sin_privacion_abs,
    i1_con_privacion_porc,
    i1_sin_privacion_porc,
    i2_con_privacion_abs,
    i2_sin_privacion_abs,
    i2_con_privacion_porc,
    i2_sin_privacion_porc,
    i3_con_privacion_abs,
    i3_sin_privacion_abs,
    i3_con_privacion_porc,
    i3_sin_privacion_porc,
    i4_con_privacion_abs,
    i4_sin_privacion_abs,
    i4_con_privacion_porc,
    i4_sin_privacion_porc,
    i5_con_privacion_abs,
    i5_sin_privacion_abs,
    i5_con_privacion_porc,
    i5_sin_privacion_porc,
    i6_con_privacion_abs,
    i6_sin_privacion_abs,
    i6_con_privacion_porc,
    i6_sin_privacion_porc,
    i7_con_privacion_abs,
    i7_sin_privacion_abs,
    i7_con_privacion_porc,
    i7_sin_privacion_porc,
    i8_con_privacion_abs,
    i8_sin_privacion_abs,
    i8_con_privacion_porc,
    i8_sin_privacion_porc,
    i9_con_privacion_abs,
    i9_sin_privacion_abs,
    i9_con_privacion_porc,
    i9_sin_privacion_porc,
    i10_con_privacion_abs,
    i10_sin_privacion_abs,
    i10_con_privacion_porc,
    i10_sin_privacion_porc,
    i11_con_privacion_abs,
    i11_sin_privacion_abs,
    i11_con_privacion_porc,
    i11_sin_privacion_porc,
    i12_con_privacion_abs,
    i12_sin_privacion_abs,
    i12_con_privacion_porc,
    i12_sin_privacion_porc,
    i13_con_privacion_abs,
    i13_sin_privacion_abs,
    i13_con_privacion_porc,
    i13_sin_privacion_porc,
    i14_con_privacion_abs,
    i14_sin_privacion_abs,
    i14_con_privacion_porc,
    i14_sin_privacion_porc,
    i15_con_privacion_abs,
    i15_sin_privacion_abs,
    i15_con_privacion_porc,
    i15_sin_privacion_porc,
    fecha_lectura
FROM __dbt__cte__ipm_transform_stg
),  __dbt__cte__ipm_transform_clean_numbers as (


-- Paso 1: Limpiar letras y caracteres especiales de columnas numéricas
-- Eliminar espacios, letras y caracteres especiales (excepto coma, punto y signo menos)
-- Convertir a INT64 después de limpiar

SELECT
  cod_mpio,
  municipio,
  
  -- Limpiar cod_mpio: eliminar letras, espacios y caracteres especiales, dejar solo números
  REGEXP_REPLACE(REGEXP_REPLACE(CAST(cod_mpio AS STRING), r'\s', ''), r'[^0-9]', '') AS cod_mpio_cleaned,
  
  -- Limpiar total: 
  -- 1. Manejar NULL o cadena vacía
  -- 2. Eliminar espacios
  -- 3. Eliminar letras y caracteres especiales (excepto coma, punto y signo menos)
  -- 4. Eliminar comas/puntos al final
  -- 5. Eliminar comas/puntos internos (convertir a entero)
  SAFE_CAST(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            COALESCE(TRIM(CAST(total AS STRING)), ''),  -- Manejar NULL y espacios, convertir a string
            r'\s', ''  -- Eliminar espacios
          ),
          r'[^0-9.,\-]', ''  -- Eliminar letras y caracteres especiales (excepto coma, punto, menos)
        ),
        r'[,.]$', ''  -- Eliminar coma o punto al final
      ),
      r'[.,]', ''  -- Eliminar comas/puntos internos
    ) AS INT64
  ) AS total_cleaned,
  
  -- Limpiar IPM: mismo proceso
  SAFE_CAST(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(CAST(ipm_pobre_abs AS STRING), r'\s', ''),
          r'[^0-9.,\-]', ''
        ),
        r'[,.]$', ''
      ),
      r'[.,]', ''
    ) AS INT64
  ) AS ipm_pobre_abs_cleaned,
  
  SAFE_CAST(
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(CAST(ipm_no_pobre_abs AS STRING), r'\s', ''),
          r'[^0-9.,\-]', ''
        ),
        r'[,.]$', ''
      ),
      r'[.,]', ''
    ) AS INT64
  ) AS ipm_no_pobre_abs_cleaned,
  
  -- Limpiar todos los indicadores I1-I15: con_privacion_abs y sin_privacion_abs
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i1_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i1_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i1_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i1_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i2_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i2_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i2_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i2_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i3_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i3_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i3_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i3_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i4_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i4_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i4_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i4_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i5_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i5_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i5_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i5_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i6_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i6_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i6_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i6_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i7_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i7_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i7_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i7_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i8_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i8_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i8_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i8_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i9_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i9_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i9_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i9_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i10_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i10_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i10_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i10_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i11_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i11_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i11_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i11_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i12_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i12_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i12_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i12_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i13_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i13_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i13_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i13_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i14_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i14_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i14_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i14_sin_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i15_con_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i15_con_privacion_abs_cleaned,
  SAFE_CAST(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(CAST(i15_sin_privacion_abs AS STRING), r'\s', ''), r'[^0-9.,\-]', ''), r'[,.]$', ''), r'[.,]', '') AS INT64) AS i15_sin_privacion_abs_cleaned,
  
  -- Mantener porcentajes y fecha sin cambios por ahora
  ipm_pobre_porc,
  ipm_no_pobre_porc,
  i1_con_privacion_porc,
  i1_sin_privacion_porc,
  i2_con_privacion_porc,
  i2_sin_privacion_porc,
  i3_con_privacion_porc,
  i3_sin_privacion_porc,
  i4_con_privacion_porc,
  i4_sin_privacion_porc,
  i5_con_privacion_porc,
  i5_sin_privacion_porc,
  i6_con_privacion_porc,
  i6_sin_privacion_porc,
  i7_con_privacion_porc,
  i7_sin_privacion_porc,
  i8_con_privacion_porc,
  i8_sin_privacion_porc,
  i9_con_privacion_porc,
  i9_sin_privacion_porc,
  i10_con_privacion_porc,
  i10_sin_privacion_porc,
  i11_con_privacion_porc,
  i11_sin_privacion_porc,
  i12_con_privacion_porc,
  i12_sin_privacion_porc,
  i13_con_privacion_porc,
  i13_sin_privacion_porc,
  i14_con_privacion_porc,
  i14_sin_privacion_porc,
  i15_con_privacion_porc,
  i15_sin_privacion_porc,
  fecha_lectura
  
FROM __dbt__cte__ipm_transform_normalize_text
),  __dbt__cte__ipm_transform_detect_negatives as (


-- Paso 2: Detectar valores negativos y agregar flags de validación
-- Si hay algún valor negativo en columnas numéricas, marcar el registro
-- También detectar si total es NULL o 0 después de la limpieza

SELECT
  *,
  
  -- Detectar si hay algún valor negativo en las columnas numéricas
  CASE 
    WHEN COALESCE(total_cleaned, 0) < 0 
      OR COALESCE(ipm_pobre_abs_cleaned, 0) < 0 
      OR COALESCE(ipm_no_pobre_abs_cleaned, 0) < 0
      OR COALESCE(i1_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i1_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i2_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i2_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i3_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i3_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i4_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i4_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i5_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i5_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i6_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i6_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i7_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i7_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i8_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i8_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i9_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i9_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i10_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i10_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i11_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i11_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i12_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i12_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i13_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i13_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i14_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i14_sin_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i15_con_privacion_abs_cleaned, 0) < 0
      OR COALESCE(i15_sin_privacion_abs_cleaned, 0) < 0
    THEN TRUE
    ELSE FALSE
  END AS has_negative_value,
  
  -- Detectar si total NO se pudo limpiar (quedó NULL después de la limpieza) o si es 0
  -- Solo en estos casos se debe poner todo en 0
  -- Si total se limpia bien (> 0), usar el valor limpio aunque otras columnas estén vacías
  CASE 
    WHEN total_cleaned IS NULL OR COALESCE(total_cleaned, 0) = 0 THEN TRUE
    ELSE FALSE
  END AS is_total_zero_or_empty
  
FROM __dbt__cte__ipm_transform_clean_numbers
),  __dbt__cte__ipm_transform_apply_validations as (


-- Paso 3: Aplicar validaciones finales
-- 1. Si hay algún valor negativo, poner TODAS las numéricas del registro en 0
-- 2. Si total = 0 o NULL, poner todas las columnas numéricas en 0

SELECT
  cod_mpio_cleaned AS cod_mpio,
  municipio,
  
  -- Total: si hay negativo, es 0, o no se pudo limpiar (NULL), queda en 0
  -- Si total_cleaned > 0, usar ese valor (se limpió correctamente)
  CASE 
    WHEN has_negative_value THEN 0
    WHEN is_total_zero_or_empty THEN 0
    ELSE COALESCE(total_cleaned, 0)
  END AS total,
  
  -- IPM y todos los indicadores: si hay negativo o total es 0/vacío, poner en 0
  -- Si total se limpia bien (> 0), usar valores individuales (las columnas vacías quedan en 0, pero no afectan el total)
  CASE 
    WHEN has_negative_value OR is_total_zero_or_empty THEN 0
    ELSE COALESCE(ipm_pobre_abs_cleaned, 0)
  END AS ipm_pobre_abs,
  
  CASE 
    WHEN has_negative_value OR is_total_zero_or_empty THEN 0
    ELSE COALESCE(ipm_no_pobre_abs_cleaned, 0)
  END AS ipm_no_pobre_abs,
  
  -- I1-I15 con_privacion_abs
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i1_con_privacion_abs_cleaned, 0) END AS i1_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i1_sin_privacion_abs_cleaned, 0) END AS i1_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i2_con_privacion_abs_cleaned, 0) END AS i2_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i2_sin_privacion_abs_cleaned, 0) END AS i2_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i3_con_privacion_abs_cleaned, 0) END AS i3_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i3_sin_privacion_abs_cleaned, 0) END AS i3_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i4_con_privacion_abs_cleaned, 0) END AS i4_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i4_sin_privacion_abs_cleaned, 0) END AS i4_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i5_con_privacion_abs_cleaned, 0) END AS i5_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i5_sin_privacion_abs_cleaned, 0) END AS i5_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i6_con_privacion_abs_cleaned, 0) END AS i6_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i6_sin_privacion_abs_cleaned, 0) END AS i6_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i7_con_privacion_abs_cleaned, 0) END AS i7_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i7_sin_privacion_abs_cleaned, 0) END AS i7_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i8_con_privacion_abs_cleaned, 0) END AS i8_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i8_sin_privacion_abs_cleaned, 0) END AS i8_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i9_con_privacion_abs_cleaned, 0) END AS i9_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i9_sin_privacion_abs_cleaned, 0) END AS i9_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i10_con_privacion_abs_cleaned, 0) END AS i10_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i10_sin_privacion_abs_cleaned, 0) END AS i10_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i11_con_privacion_abs_cleaned, 0) END AS i11_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i11_sin_privacion_abs_cleaned, 0) END AS i11_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i12_con_privacion_abs_cleaned, 0) END AS i12_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i12_sin_privacion_abs_cleaned, 0) END AS i12_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i13_con_privacion_abs_cleaned, 0) END AS i13_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i13_sin_privacion_abs_cleaned, 0) END AS i13_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i14_con_privacion_abs_cleaned, 0) END AS i14_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i14_sin_privacion_abs_cleaned, 0) END AS i14_sin_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i15_con_privacion_abs_cleaned, 0) END AS i15_con_privacion_abs,
  CASE WHEN has_negative_value OR is_total_zero_or_empty THEN 0 ELSE COALESCE(i15_sin_privacion_abs_cleaned, 0) END AS i15_sin_privacion_abs,
  
  -- Porcentajes: si total es 0 o vacío, poner en 0, sino convertir de STRING a FLOAT64 y mantener el valor original
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(ipm_pobre_porc AS FLOAT64), 0.0) END AS ipm_pobre_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(ipm_no_pobre_porc AS FLOAT64), 0.0) END AS ipm_no_pobre_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i1_con_privacion_porc AS FLOAT64), 0.0) END AS i1_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i1_sin_privacion_porc AS FLOAT64), 0.0) END AS i1_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i2_con_privacion_porc AS FLOAT64), 0.0) END AS i2_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i2_sin_privacion_porc AS FLOAT64), 0.0) END AS i2_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i3_con_privacion_porc AS FLOAT64), 0.0) END AS i3_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i3_sin_privacion_porc AS FLOAT64), 0.0) END AS i3_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i4_con_privacion_porc AS FLOAT64), 0.0) END AS i4_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i4_sin_privacion_porc AS FLOAT64), 0.0) END AS i4_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i5_con_privacion_porc AS FLOAT64), 0.0) END AS i5_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i5_sin_privacion_porc AS FLOAT64), 0.0) END AS i5_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i6_con_privacion_porc AS FLOAT64), 0.0) END AS i6_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i6_sin_privacion_porc AS FLOAT64), 0.0) END AS i6_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i7_con_privacion_porc AS FLOAT64), 0.0) END AS i7_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i7_sin_privacion_porc AS FLOAT64), 0.0) END AS i7_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i8_con_privacion_porc AS FLOAT64), 0.0) END AS i8_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i8_sin_privacion_porc AS FLOAT64), 0.0) END AS i8_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i9_con_privacion_porc AS FLOAT64), 0.0) END AS i9_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i9_sin_privacion_porc AS FLOAT64), 0.0) END AS i9_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i10_con_privacion_porc AS FLOAT64), 0.0) END AS i10_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i10_sin_privacion_porc AS FLOAT64), 0.0) END AS i10_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i11_con_privacion_porc AS FLOAT64), 0.0) END AS i11_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i11_sin_privacion_porc AS FLOAT64), 0.0) END AS i11_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i12_con_privacion_porc AS FLOAT64), 0.0) END AS i12_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i12_sin_privacion_porc AS FLOAT64), 0.0) END AS i12_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i13_con_privacion_porc AS FLOAT64), 0.0) END AS i13_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i13_sin_privacion_porc AS FLOAT64), 0.0) END AS i13_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i14_con_privacion_porc AS FLOAT64), 0.0) END AS i14_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i14_sin_privacion_porc AS FLOAT64), 0.0) END AS i14_sin_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i15_con_privacion_porc AS FLOAT64), 0.0) END AS i15_con_privacion_porc,
  CASE WHEN is_total_zero_or_empty THEN 0.0 ELSE COALESCE(SAFE_CAST(i15_sin_privacion_porc AS FLOAT64), 0.0) END AS i15_sin_privacion_porc,
  fecha_lectura
  
FROM __dbt__cte__ipm_transform_detect_negatives
) -- Modelo final: transforma fecha_lectura a DATE y materializa como tabla
SELECT
    cod_mpio,
    municipio,
    total,
    ipm_pobre_abs,
    ipm_no_pobre_abs,
    ipm_pobre_porc,
    ipm_no_pobre_porc,
    i1_con_privacion_abs,
    i1_sin_privacion_abs,
    i1_con_privacion_porc,
    i1_sin_privacion_porc,
    i2_con_privacion_abs,
    i2_sin_privacion_abs,
    i2_con_privacion_porc,
    i2_sin_privacion_porc,
    i3_con_privacion_abs,
    i3_sin_privacion_abs,
    i3_con_privacion_porc,
    i3_sin_privacion_porc,
    i4_con_privacion_abs,
    i4_sin_privacion_abs,
    i4_con_privacion_porc,
    i4_sin_privacion_porc,
    i5_con_privacion_abs,
    i5_sin_privacion_abs,
    i5_con_privacion_porc,
    i5_sin_privacion_porc,
    i6_con_privacion_abs,
    i6_sin_privacion_abs,
    i6_con_privacion_porc,
    i6_sin_privacion_porc,
    i7_con_privacion_abs,
    i7_sin_privacion_abs,
    i7_con_privacion_porc,
    i7_sin_privacion_porc,
    i8_con_privacion_abs,
    i8_sin_privacion_abs,
    i8_con_privacion_porc,
    i8_sin_privacion_porc,
    i9_con_privacion_abs,
    i9_sin_privacion_abs,
    i9_con_privacion_porc,
    i9_sin_privacion_porc,
    i10_con_privacion_abs,
    i10_sin_privacion_abs,
    i10_con_privacion_porc,
    i10_sin_privacion_porc,
    i11_con_privacion_abs,
    i11_sin_privacion_abs,
    i11_con_privacion_porc,
    i11_sin_privacion_porc,
    i12_con_privacion_abs,
    i12_sin_privacion_abs,
    i12_con_privacion_porc,
    i12_sin_privacion_porc,
    i13_con_privacion_abs,
    i13_sin_privacion_abs,
    i13_con_privacion_porc,
    i13_sin_privacion_porc,
    i14_con_privacion_abs,
    i14_sin_privacion_abs,
    i14_con_privacion_porc,
    i14_sin_privacion_porc,
    i15_con_privacion_abs,
    i15_sin_privacion_abs,
    i15_con_privacion_porc,
    i15_sin_privacion_porc,
    
    -- Fecha de lectura: convertir a DATE (YYYY-MM-DD) sin hora
    DATE(fecha_lectura) AS fecha_lectura
    
FROM __dbt__cte__ipm_transform_apply_validations
    );
  