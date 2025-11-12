

  create or replace view `datagov-473122`.`gdv_ipmv2_silver`.`rawdata_ipmv2_transform_types`
  OPTIONS()
  as 

-- Modelo para transformar tipos de datos: enteros para absolutos, floats con 2 decimales para porcentajes
SELECT
    cod_mpio,
    municipio,
    
    -- Total: convertir a entero
    CAST(total AS INT64) AS total,
    
    -- IPM: absolutos como enteros, porcentajes como FLOAT con 2 decimales
    CAST(ipm_pobre_abs AS INT64) AS ipm_pobre_abs,
    CAST(ipm_no_pobre_abs AS INT64) AS ipm_no_pobre_abs,
    ROUND(CAST(ipm_pobre_porc AS FLOAT64), 2) AS ipm_pobre_porc,
    ROUND(CAST(ipm_no_pobre_porc AS FLOAT64), 2) AS ipm_no_pobre_porc,
    
    -- Indicadores I1 a I15: absolutos como enteros, porcentajes como FLOAT con 2 decimales
    CAST(i1_con_privacion_abs AS INT64) AS i1_con_privacion_abs,
    CAST(i1_sin_privacion_abs AS INT64) AS i1_sin_privacion_abs,
    ROUND(CAST(i1_con_privacion_porc AS FLOAT64), 2) AS i1_con_privacion_porc,
    ROUND(CAST(i1_sin_privacion_porc AS FLOAT64), 2) AS i1_sin_privacion_porc,
    
    CAST(i2_con_privacion_abs AS INT64) AS i2_con_privacion_abs,
    CAST(i2_sin_privacion_abs AS INT64) AS i2_sin_privacion_abs,
    ROUND(CAST(i2_con_privacion_porc AS FLOAT64), 2) AS i2_con_privacion_porc,
    ROUND(CAST(i2_sin_privacion_porc AS FLOAT64), 2) AS i2_sin_privacion_porc,
    
    CAST(i3_con_privacion_abs AS INT64) AS i3_con_privacion_abs,
    CAST(i3_sin_privacion_abs AS INT64) AS i3_sin_privacion_abs,
    ROUND(CAST(i3_con_privacion_porc AS FLOAT64), 2) AS i3_con_privacion_porc,
    ROUND(CAST(i3_sin_privacion_porc AS FLOAT64), 2) AS i3_sin_privacion_porc,
    
    CAST(i4_con_privacion_abs AS INT64) AS i4_con_privacion_abs,
    CAST(i4_sin_privacion_abs AS INT64) AS i4_sin_privacion_abs,
    ROUND(CAST(i4_con_privacion_porc AS FLOAT64), 2) AS i4_con_privacion_porc,
    ROUND(CAST(i4_sin_privacion_porc AS FLOAT64), 2) AS i4_sin_privacion_porc,
    
    CAST(i5_con_privacion_abs AS INT64) AS i5_con_privacion_abs,
    CAST(i5_sin_privacion_abs AS INT64) AS i5_sin_privacion_abs,
    ROUND(CAST(i5_con_privacion_porc AS FLOAT64), 2) AS i5_con_privacion_porc,
    ROUND(CAST(i5_sin_privacion_porc AS FLOAT64), 2) AS i5_sin_privacion_porc,
    
    CAST(i6_con_privacion_abs AS INT64) AS i6_con_privacion_abs,
    CAST(i6_sin_privacion_abs AS INT64) AS i6_sin_privacion_abs,
    ROUND(CAST(i6_con_privacion_porc AS FLOAT64), 2) AS i6_con_privacion_porc,
    ROUND(CAST(i6_sin_privacion_porc AS FLOAT64), 2) AS i6_sin_privacion_porc,
    
    CAST(i7_con_privacion_abs AS INT64) AS i7_con_privacion_abs,
    CAST(i7_sin_privacion_abs AS INT64) AS i7_sin_privacion_abs,
    ROUND(CAST(i7_con_privacion_porc AS FLOAT64), 2) AS i7_con_privacion_porc,
    ROUND(CAST(i7_sin_privacion_porc AS FLOAT64), 2) AS i7_sin_privacion_porc,
    
    CAST(i8_con_privacion_abs AS INT64) AS i8_con_privacion_abs,
    CAST(i8_sin_privacion_abs AS INT64) AS i8_sin_privacion_abs,
    ROUND(CAST(i8_con_privacion_porc AS FLOAT64), 2) AS i8_con_privacion_porc,
    ROUND(CAST(i8_sin_privacion_porc AS FLOAT64), 2) AS i8_sin_privacion_porc,
    
    CAST(i9_con_privacion_abs AS INT64) AS i9_con_privacion_abs,
    CAST(i9_sin_privacion_abs AS INT64) AS i9_sin_privacion_abs,
    ROUND(CAST(i9_con_privacion_porc AS FLOAT64), 2) AS i9_con_privacion_porc,
    ROUND(CAST(i9_sin_privacion_porc AS FLOAT64), 2) AS i9_sin_privacion_porc,
    
    CAST(i10_con_privacion_abs AS INT64) AS i10_con_privacion_abs,
    CAST(i10_sin_privacion_abs AS INT64) AS i10_sin_privacion_abs,
    ROUND(CAST(i10_con_privacion_porc AS FLOAT64), 2) AS i10_con_privacion_porc,
    ROUND(CAST(i10_sin_privacion_porc AS FLOAT64), 2) AS i10_sin_privacion_porc,
    
    CAST(i11_con_privacion_abs AS INT64) AS i11_con_privacion_abs,
    CAST(i11_sin_privacion_abs AS INT64) AS i11_sin_privacion_abs,
    ROUND(CAST(i11_con_privacion_porc AS FLOAT64), 2) AS i11_con_privacion_porc,
    ROUND(CAST(i11_sin_privacion_porc AS FLOAT64), 2) AS i11_sin_privacion_porc,
    
    CAST(i12_con_privacion_abs AS INT64) AS i12_con_privacion_abs,
    CAST(i12_sin_privacion_abs AS INT64) AS i12_sin_privacion_abs,
    ROUND(CAST(i12_con_privacion_porc AS FLOAT64), 2) AS i12_con_privacion_porc,
    ROUND(CAST(i12_sin_privacion_porc AS FLOAT64), 2) AS i12_sin_privacion_porc,
    
    CAST(i13_con_privacion_abs AS INT64) AS i13_con_privacion_abs,
    CAST(i13_sin_privacion_abs AS INT64) AS i13_sin_privacion_abs,
    ROUND(CAST(i13_con_privacion_porc AS FLOAT64), 2) AS i13_con_privacion_porc,
    ROUND(CAST(i13_sin_privacion_porc AS FLOAT64), 2) AS i13_sin_privacion_porc,
    
    CAST(i14_con_privacion_abs AS INT64) AS i14_con_privacion_abs,
    CAST(i14_sin_privacion_abs AS INT64) AS i14_sin_privacion_abs,
    ROUND(CAST(i14_con_privacion_porc AS FLOAT64), 2) AS i14_con_privacion_porc,
    ROUND(CAST(i14_sin_privacion_porc AS FLOAT64), 2) AS i14_sin_privacion_porc,
    
    CAST(i15_con_privacion_abs AS INT64) AS i15_con_privacion_abs,
    CAST(i15_sin_privacion_abs AS INT64) AS i15_sin_privacion_abs,
    ROUND(CAST(i15_con_privacion_porc AS FLOAT64), 2) AS i15_con_privacion_porc,
    ROUND(CAST(i15_sin_privacion_porc AS FLOAT64), 2) AS i15_sin_privacion_porc,
    
    fecha_lectura
FROM `datagov-473122`.`gdv_ipmv2_silver`.`rawdata_ipmv2_normalize_text`;

