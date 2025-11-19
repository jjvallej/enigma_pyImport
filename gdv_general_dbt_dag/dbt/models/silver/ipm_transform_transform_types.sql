{{
  config(
    materialized='view',
    schema='silver_dpt_planeacion_municipal_dev'
  )
}}

-- Modelo para transformar tipos de datos: enteros para absolutos, floats con 2 decimales para porcentajes
-- Usa SAFE_CAST para manejar valores inválidos (serán NULL y se limpiarán en validate_numbers)
-- En bronze todo viene como STRING, así que intentamos convertir aquí
SELECT
    cod_mpio,
    municipio,
    
    -- Total: convertir a entero usando SAFE_CAST (valores inválidos -> NULL)
    SAFE_CAST(total AS INT64) AS total,
    
    -- IPM: absolutos como enteros, porcentajes como FLOAT con 2 decimales
    SAFE_CAST(ipm_pobre_abs AS INT64) AS ipm_pobre_abs,
    SAFE_CAST(ipm_no_pobre_abs AS INT64) AS ipm_no_pobre_abs,
    ROUND(SAFE_CAST(ipm_pobre_porc AS FLOAT64), 2) AS ipm_pobre_porc,
    ROUND(SAFE_CAST(ipm_no_pobre_porc AS FLOAT64), 2) AS ipm_no_pobre_porc,
    
    -- Indicadores I1 a I15: absolutos como enteros, porcentajes como FLOAT con 2 decimales
    -- Usa SAFE_CAST para manejar valores inválidos (serán NULL y se limpiarán en validate_numbers)
    SAFE_CAST(i1_con_privacion_abs AS INT64) AS i1_con_privacion_abs,
    SAFE_CAST(i1_sin_privacion_abs AS INT64) AS i1_sin_privacion_abs,
    ROUND(SAFE_CAST(i1_con_privacion_porc AS FLOAT64), 2) AS i1_con_privacion_porc,
    ROUND(SAFE_CAST(i1_sin_privacion_porc AS FLOAT64), 2) AS i1_sin_privacion_porc,
    
    SAFE_CAST(i2_con_privacion_abs AS INT64) AS i2_con_privacion_abs,
    SAFE_CAST(i2_sin_privacion_abs AS INT64) AS i2_sin_privacion_abs,
    ROUND(SAFE_CAST(i2_con_privacion_porc AS FLOAT64), 2) AS i2_con_privacion_porc,
    ROUND(SAFE_CAST(i2_sin_privacion_porc AS FLOAT64), 2) AS i2_sin_privacion_porc,
    
    SAFE_CAST(i3_con_privacion_abs AS INT64) AS i3_con_privacion_abs,
    SAFE_CAST(i3_sin_privacion_abs AS INT64) AS i3_sin_privacion_abs,
    ROUND(SAFE_CAST(i3_con_privacion_porc AS FLOAT64), 2) AS i3_con_privacion_porc,
    ROUND(SAFE_CAST(i3_sin_privacion_porc AS FLOAT64), 2) AS i3_sin_privacion_porc,
    
    SAFE_CAST(i4_con_privacion_abs AS INT64) AS i4_con_privacion_abs,
    SAFE_CAST(i4_sin_privacion_abs AS INT64) AS i4_sin_privacion_abs,
    ROUND(SAFE_CAST(i4_con_privacion_porc AS FLOAT64), 2) AS i4_con_privacion_porc,
    ROUND(SAFE_CAST(i4_sin_privacion_porc AS FLOAT64), 2) AS i4_sin_privacion_porc,
    
    SAFE_CAST(i5_con_privacion_abs AS INT64) AS i5_con_privacion_abs,
    SAFE_CAST(i5_sin_privacion_abs AS INT64) AS i5_sin_privacion_abs,
    ROUND(SAFE_CAST(i5_con_privacion_porc AS FLOAT64), 2) AS i5_con_privacion_porc,
    ROUND(SAFE_CAST(i5_sin_privacion_porc AS FLOAT64), 2) AS i5_sin_privacion_porc,
    
    SAFE_CAST(i6_con_privacion_abs AS INT64) AS i6_con_privacion_abs,
    SAFE_CAST(i6_sin_privacion_abs AS INT64) AS i6_sin_privacion_abs,
    ROUND(SAFE_CAST(i6_con_privacion_porc AS FLOAT64), 2) AS i6_con_privacion_porc,
    ROUND(SAFE_CAST(i6_sin_privacion_porc AS FLOAT64), 2) AS i6_sin_privacion_porc,
    
    SAFE_CAST(i7_con_privacion_abs AS INT64) AS i7_con_privacion_abs,
    SAFE_CAST(i7_sin_privacion_abs AS INT64) AS i7_sin_privacion_abs,
    ROUND(SAFE_CAST(i7_con_privacion_porc AS FLOAT64), 2) AS i7_con_privacion_porc,
    ROUND(SAFE_CAST(i7_sin_privacion_porc AS FLOAT64), 2) AS i7_sin_privacion_porc,
    
    SAFE_CAST(i8_con_privacion_abs AS INT64) AS i8_con_privacion_abs,
    SAFE_CAST(i8_sin_privacion_abs AS INT64) AS i8_sin_privacion_abs,
    ROUND(SAFE_CAST(i8_con_privacion_porc AS FLOAT64), 2) AS i8_con_privacion_porc,
    ROUND(SAFE_CAST(i8_sin_privacion_porc AS FLOAT64), 2) AS i8_sin_privacion_porc,
    
    SAFE_CAST(i9_con_privacion_abs AS INT64) AS i9_con_privacion_abs,
    SAFE_CAST(i9_sin_privacion_abs AS INT64) AS i9_sin_privacion_abs,
    ROUND(SAFE_CAST(i9_con_privacion_porc AS FLOAT64), 2) AS i9_con_privacion_porc,
    ROUND(SAFE_CAST(i9_sin_privacion_porc AS FLOAT64), 2) AS i9_sin_privacion_porc,
    
    SAFE_CAST(i10_con_privacion_abs AS INT64) AS i10_con_privacion_abs,
    SAFE_CAST(i10_sin_privacion_abs AS INT64) AS i10_sin_privacion_abs,
    ROUND(SAFE_CAST(i10_con_privacion_porc AS FLOAT64), 2) AS i10_con_privacion_porc,
    ROUND(SAFE_CAST(i10_sin_privacion_porc AS FLOAT64), 2) AS i10_sin_privacion_porc,
    
    SAFE_CAST(i11_con_privacion_abs AS INT64) AS i11_con_privacion_abs,
    SAFE_CAST(i11_sin_privacion_abs AS INT64) AS i11_sin_privacion_abs,
    ROUND(SAFE_CAST(i11_con_privacion_porc AS FLOAT64), 2) AS i11_con_privacion_porc,
    ROUND(SAFE_CAST(i11_sin_privacion_porc AS FLOAT64), 2) AS i11_sin_privacion_porc,
    
    SAFE_CAST(i12_con_privacion_abs AS INT64) AS i12_con_privacion_abs,
    SAFE_CAST(i12_sin_privacion_abs AS INT64) AS i12_sin_privacion_abs,
    ROUND(SAFE_CAST(i12_con_privacion_porc AS FLOAT64), 2) AS i12_con_privacion_porc,
    ROUND(SAFE_CAST(i12_sin_privacion_porc AS FLOAT64), 2) AS i12_sin_privacion_porc,
    
    SAFE_CAST(i13_con_privacion_abs AS INT64) AS i13_con_privacion_abs,
    SAFE_CAST(i13_sin_privacion_abs AS INT64) AS i13_sin_privacion_abs,
    ROUND(SAFE_CAST(i13_con_privacion_porc AS FLOAT64), 2) AS i13_con_privacion_porc,
    ROUND(SAFE_CAST(i13_sin_privacion_porc AS FLOAT64), 2) AS i13_sin_privacion_porc,
    
    SAFE_CAST(i14_con_privacion_abs AS INT64) AS i14_con_privacion_abs,
    SAFE_CAST(i14_sin_privacion_abs AS INT64) AS i14_sin_privacion_abs,
    ROUND(SAFE_CAST(i14_con_privacion_porc AS FLOAT64), 2) AS i14_con_privacion_porc,
    ROUND(SAFE_CAST(i14_sin_privacion_porc AS FLOAT64), 2) AS i14_sin_privacion_porc,
    
    SAFE_CAST(i15_con_privacion_abs AS INT64) AS i15_con_privacion_abs,
    SAFE_CAST(i15_sin_privacion_abs AS INT64) AS i15_sin_privacion_abs,
    ROUND(SAFE_CAST(i15_con_privacion_porc AS FLOAT64), 2) AS i15_con_privacion_porc,
    ROUND(SAFE_CAST(i15_sin_privacion_porc AS FLOAT64), 2) AS i15_sin_privacion_porc,
    
    fecha_lectura
FROM {{ ref('ipm_transform_normalize_text') }}


