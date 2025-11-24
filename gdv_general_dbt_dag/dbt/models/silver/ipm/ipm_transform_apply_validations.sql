{{
  config(
    materialized='ephemeral',
    schema='silver_dpt_planeacion_municipal_dev'
  )
}}

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
  
FROM {{ ref('ipm_transform_detect_negatives') }}


