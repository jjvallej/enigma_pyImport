

select
  cast(cod_mpio as string)  as cod_mpio,
  cast(Municipio as string) as nombre,
  cast(Total as int64)      as cantidad_de_habitantes
from `datagov-473122`.`gdv_ipm_sisben_bronze`.`ipm_sisben_clean`
where trim(cod_mpio) != '' and trim(Municipio) != ''