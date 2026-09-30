

-- Una fila por municipio *y* por categoría I1..I15 (sin nombre de municipio)
with src as (
  select
    cast(cod_mpio as string) as cod_mpio,
    -- tomamos todas las columnas I# para despivotar
    I1_CON_PRIVACION,  I1_SIN_PRIVACION,
    I2_CON_PRIVACION,  I2_SIN_PRIVACION,
    I3_CON_PRIVACION,  I3_SIN_PRIVACION,
    I4_CON_PRIVACION,  I4_SIN_PRIVACION,
    I5_CON_PRIVACION,  I5_SIN_PRIVACION,
    I6_CON_PRIVACION,  I6_SIN_PRIVACION,
    I7_CON_PRIVACION,  I7_SIN_PRIVACION,
    I8_CON_PRIVACION,  I8_SIN_PRIVACION,
    I9_CON_PRIVACION,  I9_SIN_PRIVACION,
    I10_CON_PRIVACION, I10_SIN_PRIVACION,
    I11_CON_PRIVACION, I11_SIN_PRIVACION,
    I12_CON_PRIVACION, I12_SIN_PRIVACION,
    I13_CON_PRIVACION, I13_SIN_PRIVACION,
    I14_CON_PRIVACION, I14_SIN_PRIVACION,
    I15_CON_PRIVACION, I15_SIN_PRIVACION
  from `datagov-473122`.`gdv_ipm_sisben_bronze`.`ipm_sisben_clean`
  where trim(cod_mpio) != '' and trim(Municipio) != ''
),

flat as (
  select
    s.cod_mpio,
    cat.cod  as cod_categoria,
    cat.cod  as nombre, -- por ahora el mismo código
    cast(cat.con as int64) as con_privacion,
    cast(cat.sin as int64) as sin_privacion
  from src s,
  unnest([
    struct('I1'  as cod, s.I1_CON_PRIVACION  as con, s.I1_SIN_PRIVACION  as sin),
    struct('I2'  as cod, s.I2_CON_PRIVACION  as con, s.I2_SIN_PRIVACION  as sin),
    struct('I3'  as cod, s.I3_CON_PRIVACION  as con, s.I3_SIN_PRIVACION  as sin),
    struct('I4'  as cod, s.I4_CON_PRIVACION  as con, s.I4_SIN_PRIVACION  as sin),
    struct('I5'  as cod, s.I5_CON_PRIVACION  as con, s.I5_SIN_PRIVACION  as sin),
    struct('I6'  as cod, s.I6_CON_PRIVACION  as con, s.I6_SIN_PRIVACION  as sin),
    struct('I7'  as cod, s.I7_CON_PRIVACION  as con, s.I7_SIN_PRIVACION  as sin),
    struct('I8'  as cod, s.I8_CON_PRIVACION  as con, s.I8_SIN_PRIVACION  as sin),
    struct('I9'  as cod, s.I9_CON_PRIVACION  as con, s.I9_SIN_PRIVACION  as sin),
    struct('I10' as cod, s.I10_CON_PRIVACION as con, s.I10_SIN_PRIVACION as sin),
    struct('I11' as cod, s.I11_CON_PRIVACION as con, s.I11_SIN_PRIVACION as sin),
    struct('I12' as cod, s.I12_CON_PRIVACION as con, s.I12_SIN_PRIVACION as sin),
    struct('I13' as cod, s.I13_CON_PRIVACION as con, s.I13_SIN_PRIVACION as sin),
    struct('I14' as cod, s.I14_CON_PRIVACION as con, s.I14_SIN_PRIVACION as sin),
    struct('I15' as cod, s.I15_CON_PRIVACION as con, s.I15_SIN_PRIVACION as sin)
  ]) as cat
)

select
  cod_mpio,
  cod_categoria,
  nombre,
  con_privacion,
  sin_privacion
from flat
order by cod_mpio, cod_categoria