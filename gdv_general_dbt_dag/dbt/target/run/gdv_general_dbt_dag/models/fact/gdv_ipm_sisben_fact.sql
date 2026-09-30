
  
    

    create or replace table `datagov-473122`.`gdv_ipm_sisben_gold`.`gdv_ipm_sisben_fact`
      
    
    

    OPTIONS()
    as (
      

-- Fact "wide" construida SOLO desde las dimensiones en gdv_ipm_sisben_dims

with
-- Municipios: nombre y total de habitantes
m as (
  select
    cast(cod_mpio as string) as cod_mpio,
    trim(upper(cast(nombre as string))) as Municipio,
    cast(cantidad_de_habitantes as int64) as Total
  from `datagov-473122`.`gdv_ipm_sisben_dims`.`dim_municipio`
),

-- Nivel de pobreza por municipio
np as (
  select
    cast(cod_mpio as string) as cod_mpio,
    cast(pobre as int64)     as IPM_Pobre,
    cast(no_pobre as int64)  as IPM_No_Pobre
  from `datagov-473122`.`gdv_ipm_sisben_dims`.`dim_nivel_pobreza`
),

-- Categorías I1..I15 por municipio (despivotadas en la dim). Aquí las volvemos a pivotear a columnas wide.
cat as (
  select
    cast(cod_mpio as string) as cod_mpio,
    trim(upper(cod_categoria)) as cod_categoria,
    cast(con_privacion as int64) as con_privacion,
    cast(sin_privacion as int64) as sin_privacion
  from `datagov-473122`.`gdv_ipm_sisben_dims`.`dim_categoria_ipm`
),
cat_wide as (
  select
    cod_mpio,

    max(if(cod_categoria='I1',  con_privacion, null)) as I1_CON_PRIVACION,
    max(if(cod_categoria='I1',  sin_privacion, null)) as I1_SIN_PRIVACION,

    max(if(cod_categoria='I2',  con_privacion, null)) as I2_CON_PRIVACION,
    max(if(cod_categoria='I2',  sin_privacion, null)) as I2_SIN_PRIVACION,

    max(if(cod_categoria='I3',  con_privacion, null)) as I3_CON_PRIVACION,
    max(if(cod_categoria='I3',  sin_privacion, null)) as I3_SIN_PRIVACION,

    max(if(cod_categoria='I4',  con_privacion, null)) as I4_CON_PRIVACION,
    max(if(cod_categoria='I4',  sin_privacion, null)) as I4_SIN_PRIVACION,

    max(if(cod_categoria='I5',  con_privacion, null)) as I5_CON_PRIVACION,
    max(if(cod_categoria='I5',  sin_privacion, null)) as I5_SIN_PRIVACION,

    max(if(cod_categoria='I6',  con_privacion, null)) as I6_CON_PRIVACION,
    max(if(cod_categoria='I6',  sin_privacion, null)) as I6_SIN_PRIVACION,

    max(if(cod_categoria='I7',  con_privacion, null)) as I7_CON_PRIVACION,
    max(if(cod_categoria='I7',  sin_privacion, null)) as I7_SIN_PRIVACION,

    max(if(cod_categoria='I8',  con_privacion, null)) as I8_CON_PRIVACION,
    max(if(cod_categoria='I8',  sin_privacion, null)) as I8_SIN_PRIVACION,

    max(if(cod_categoria='I9',  con_privacion, null)) as I9_CON_PRIVACION,
    max(if(cod_categoria='I9',  sin_privacion, null)) as I9_SIN_PRIVACION,

    max(if(cod_categoria='I10', con_privacion, null)) as I10_CON_PRIVACION,
    max(if(cod_categoria='I10', sin_privacion, null)) as I10_SIN_PRIVACION,

    max(if(cod_categoria='I11', con_privacion, null)) as I11_CON_PRIVACION,
    max(if(cod_categoria='I11', sin_privacion, null)) as I11_SIN_PRIVACION,

    max(if(cod_categoria='I12', con_privacion, null)) as I12_CON_PRIVACION,
    max(if(cod_categoria='I12', sin_privacion, null)) as I12_SIN_PRIVACION,

    max(if(cod_categoria='I13', con_privacion, null)) as I13_CON_PRIVACION,
    max(if(cod_categoria='I13', sin_privacion, null)) as I13_SIN_PRIVACION,

    max(if(cod_categoria='I14', con_privacion, null)) as I14_CON_PRIVACION,
    max(if(cod_categoria='I14', sin_privacion, null)) as I14_SIN_PRIVACION,

    max(if(cod_categoria='I15', con_privacion, null)) as I15_CON_PRIVACION,
    max(if(cod_categoria='I15', sin_privacion, null)) as I15_SIN_PRIVACION

  from cat
  group by cod_mpio
)

select
  m.cod_mpio,
  m.Municipio,
  m.Total,
  np.IPM_Pobre,
  np.IPM_No_Pobre,

  cw.I1_CON_PRIVACION,  cw.I1_SIN_PRIVACION,
  cw.I2_CON_PRIVACION,  cw.I2_SIN_PRIVACION,
  cw.I3_CON_PRIVACION,  cw.I3_SIN_PRIVACION,
  cw.I4_CON_PRIVACION,  cw.I4_SIN_PRIVACION,
  cw.I5_CON_PRIVACION,  cw.I5_SIN_PRIVACION,
  cw.I6_CON_PRIVACION,  cw.I6_SIN_PRIVACION,
  cw.I7_CON_PRIVACION,  cw.I7_SIN_PRIVACION,
  cw.I8_CON_PRIVACION,  cw.I8_SIN_PRIVACION,
  cw.I9_CON_PRIVACION,  cw.I9_SIN_PRIVACION,
  cw.I10_CON_PRIVACION, cw.I10_SIN_PRIVACION,
  cw.I11_CON_PRIVACION, cw.I11_SIN_PRIVACION,
  cw.I12_CON_PRIVACION, cw.I12_SIN_PRIVACION,
  cw.I13_CON_PRIVACION, cw.I13_SIN_PRIVACION,
  cw.I14_CON_PRIVACION, cw.I14_SIN_PRIVACION,
  cw.I15_CON_PRIVACION, cw.I15_SIN_PRIVACION

from m
left join np       using (cod_mpio)
left join cat_wide cw using (cod_mpio)
order by m.cod_mpio
    );
  