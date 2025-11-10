{{ config(materialized='table', schema='gdv_ipm_sisben_dims') }}

-- Una fila por municipio con sus valores IPM (sin nombre de municipio)
with src as (
  select
    cast(cod_mpio as string) as cod_mpio,
    cast(IPM_Pobre as int64)    as pobre,
    cast(IPM_No_Pobre as int64) as no_pobre
  from {{ source('bronze','ipm_sisben_clean') }}
  where trim(cod_mpio) != '' and trim(Municipio) != ''
)

select
  cod_mpio,
  'IPM' as cod_np,   -- categoría fija
  'IPM' as nombre,   -- por ahora igual que cod_np
  pobre,
  no_pobre
from src
order by cod_mpio

