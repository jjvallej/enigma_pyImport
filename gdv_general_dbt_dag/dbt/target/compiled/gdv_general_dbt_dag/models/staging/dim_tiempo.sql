

-- dim_tiempo: una fila por periodo de reporte
-- Los valores llegan vía vars: anio (INT) y periodo (STRING)
select
  cast(2025 as int64)    as anio,
  cast(2025-10 as string) as periodo