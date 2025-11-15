{{
  config(
    materialized='view',
    schema='silver_dpt_planeacion_municipal_dev'
  )
}}

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
  
FROM {{ ref('rawdata_ipmv2_clean_numbers') }}

