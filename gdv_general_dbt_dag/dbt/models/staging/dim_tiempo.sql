{{ config(materialized='table', schema='gdv_ipm_sisben_dims') }}

-- dim_tiempo: una fila por periodo de reporte
-- Los valores llegan vía vars: anio (INT) y periodo (STRING)
select
  cast({{ var('anio', 2025) }} as int64)    as anio,
  cast({{ var('periodo', '2025-10') }} as string) as periodo

