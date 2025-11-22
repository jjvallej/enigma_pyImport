{{
  config(
    materialized='view',
    schema='silver_dpt_planeacion_municipal_dev'
  )
}}

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
  
FROM {{ ref('ipm_transform_normalize_text') }}


