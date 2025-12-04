{{
  config(
    materialized='view',
    schema=var('gold_dataset'),
    alias='FACT_SISBEN',
    description='Modelo gold que selecciona columnas de descripción (texto) y valores numéricos de la capa silver para análisis final.'
  )
}}

SELECT
  -- Identificación básica
  _2_num_ficha              AS FICHA,
  _3_cod_departamento       AS CODIGO_DEPARTAMENTO,
  departamento              AS DEPARTAMENTO,
  _4_cod_municipio          AS CODIGO_MUNICIPIO,
  _6_cod_clase_DESCRIPCION  AS CODIGO_CLASE,      -- texto (CABECERA, RURAL...)

  _192_grupo                AS GRUPO_SISBEN,
  _193_nivel                AS NIVEL_SISBEN,

  -- Vivienda / servicios (texto)
  _25_tipo_vivienda_DESCRIPCION          AS TIPO_VIVIENDA,
  _26_tipo_material_paredes_DESCRIPCION  AS MATERIAL_PAREDES,
  _51_elimina_basura_DESCRIPCION         AS ELIMINACION_BASURA,
  _28_energia_DESCRIPCION                AS SERVICIO_ENERGIA,
  _29_estrato_energia_DESCRIPCION        AS ESTRATO_ENERGIA,
  _30_alcantarillado_DESCRIPCION         AS SERVICIO_ALCANTARILLADO,
  _31_gas_DESCRIPCION                    AS SERVICIO_GAS,
  _32_recoleccion_basura_DESCRIPCION     AS SERVICIO_RECOLECCION_BASURA,
  _33_acueducto_DESCRIPCION              AS SERVICIO_ACUEDUCTO,
  _34_estrato_energia_ACUEDUCTO_DESCRIPCION AS ESTRATO_ACUDUCTO,

  _35_cuartos               AS CANTIDAD_CUARTOS,
  _36_hogares               AS CANTIDAD_HOGARES,
  _37_ide_hogar             AS ID_HOGAR,
  _38_tip_ocupacion_DESCRIPCION AS TIPO_OCUPACION_VIVIENDA,

  -- Agua (texto donde aplica)
  _45_orig_agua_DESCRIPCION AS ORIGEN_AGUA,
  _46_agua_7_dias_DESCRIPCION AS AGUA_7_DIAS,
  _47_num_dias_agua         AS DIAS_AGUA,
  _49_horas_agua            AS HORAS_AGUA,
  _50_agua_beber_DESCRIPCION AS USO_AGUA_BEBER,

  -- Gastos (valores en número, no hay *_DESCRIPCION)
  _65_gasto                 AS GASTO_ALIMENTO,
  _67_gasto_transporte      AS GASTO_TRANSPORTE,
  _69_gasto_educ            AS GASTO_EDUCACION,
  _71_gasto_salud           AS GASTO_SALUD,
  _73_gasto_servpub         AS GASTO_SERVICIOS_PUBLICOS,
  _75_gasto_cel             AS GASTO_CELULAR,
  _77_gasto_arrendo         AS GASTO_ARRIENDO,
  _79_gastos_otros          AS GASTOS_OTROS,
  _80_num_hab_DESCRIPCION   AS TIEMPO_VIVIENDA,

  -- Eventos / desastres
  _82_num_inundacion        AS NUM_INUNDACIONES,
  _84_num_avalancha         AS NUM_AVALANCHAS,
  _86_num_terremoto         AS NUM_TERREMOTOS,
  _88_num_incendio          AS NUM_INCENDIOS,
  _90_num_vendaval          AS NUM_VENDAVALES,
  _92_num_hundimiento       AS NUM_HUNDIMIENTOS,
  _93_num_pers_hogar        AS NUM_PERSONAS_HOGAR,

  -- Persona
  _101_sexo_DESCRIPCION     AS SEXO,
  _102_tip_doc_DESCRIPCION  AS TIPO_DOCUMENTO,
  _105_edad                 AS EDAD,

  -- Discapacidad (texto SI/NO)
  _117_disca_ver_DESCRIPCION       AS DISCAPACIDAD_VISUAL,
  _118_disca_oir_DESCRIPCION       AS DISCAPACIDAD_AUDITIVA,
  _119_disca_hablar_DESCRIPCION    AS DISCAPACIDAD_HABLA,
  _120_disca_moverse_DESCRIPCION   AS DISCAPACIDAD_MOVIMIENTO,
  _121_disca_banarse_DESCRIPCION   AS DISCAPACIDAD_BANARSE,
  _122_disca_salir_DESCRIPCION     AS DISCAPACIDAD_SALIR,
  _123_disca_entender_DESCRIPCION  AS DISCAPACIDAD_ENTENDER,
  _124_disca_ninguna_DESCRIPCION   AS SIN_DISCAPACIDAD,

  -- Salud
  _125_seg_social_DESCRIPCION      AS SEGURIDAD_SOCIAL,
  _126_enfermo_DESCRIPCION         AS ENFERMENDAD_30_DIAS,
  _127_acudio_salud_DESCRIPCION    AS ACUDIO_SALUD,
  _128_fue_atendido_DESCRIPCION    AS ATENDIDO,
  _129_estaba_embarazada_DESCRIPCION AS EMBARAZADA,
  _130_tuvo_hijos_DESCRIPCION      AS TIENE_HIJOS,
  _131_cuidado_hijos_DESCRIPCION   AS TIPO_CUIDADO_HIJOS,
  _132_recibe_comida_DESCRIPCION   AS RECIBE_COMIDA_HIJO,

  -- Educación
  _133_sabe_leer_DESCRIPCION       AS SABE_LEER,
  _134_estudia_DESCRIPCION         AS ESTUDIA,
  _135_nivel_educativo_DESCRIPCION AS NIVEL_EDUCATIVO,
  _136_grado                       AS GRADO_ACADEMICO,

  -- Trabajo / pensiones
  _137_fondo_pensiones_DESCRIPCION AS FONDO_PENSIONES,
  _138_actividad_mes_DESCRIPCION   AS ACTIVIDAD_MES,
  _139_sem_buscando               AS SEMANAS_BUSCANDO_TRABAJO,
  _140_empleado_DESCRIPCION       AS TIPO_EMPLEO,

  -- Ingresos (indicador en texto, valor numérico)
  _141_ingreso_SALARIO_DESCRIPCION AS INGRESO_SALARIO,
  _142_salario                     AS VALOR_SALARIO,

  _143_ind_honorarios_mes_DESCRIPCION AS INGRESO_HONORARIO_MES,
  _144_honorarios_mes              AS VALOR_HONORARIO_MES,

  _145_ind_ingre_cosecha_DESCRIPCION AS INGRESO_COSECHA,
  _146_ingre_cosecha               AS MES_INGRESO_COSECHA,
  _147_valor_cosecha               AS VALOR_COSECHA,

  _148_ind_pension_DESCRIPCION     AS INGRESO_PENSION,
  _149_ingre_pension               AS VALOR_PENSION,

  _150_ind_ingreso_remesa_PAIS_DESCRIPCION AS INGRESO_REMESA,
  _151_ingreso_remesa              AS VALOR_REMESA,

  _152_ind_ingreso_remesa_ext_DESCRIPCION AS INGRESO_REMESA_EXTERIOR,
  _153_ingreso_remesa_ext          AS VALOR_REMESA_EXTERIOR,

  _154_ind_ingreso_arrendo_DESCRIPCION AS INGRESO_ARRIENDO,
  _155_ingreso_arrendo            AS VALOR_INGRESO_ARRIENDO,

  _156_ind_otros_ingresos_DESCRIPCION AS INGRESO_OTROS,
  _157_otros_ingresos            AS VALOR_INGRESO_OTROS,

  _158_ind_subsidios_DESCRIPCION AS INGRESO_SUBCIDIOS,
  _159_fam_accion                AS FAMILIAS_ACCION,
  _160_col_mayor                 AS COLOMBIA_MAYOR,
  _161_otro_subsidio             AS OTRO_SUBCIDIO,

  -- IPM como valores numéricos (1 o 0)
  IPM_H5                          AS IPM,
  IMP_I1                          AS IPM_I1,
  IMP_I2                          AS IPM_I2,
  IMP_I3                          AS IPM_I3,
  IMP_I4                          AS IPM_I4,
  IMP_I5                          AS IPM_I5,
  IMP_I6                          AS IPM_I6,
  IMP_I7                          AS IPM_I7,
  IMP_I8                          AS IPM_I8,
  IMP_I9                          AS IPM_I9,
  IMP_I10                         AS IPM_I10,
  IMP_I11                         AS IPM_I11,
  IMP_I12                         AS IPM_I12,
  IMP_I13                         AS IPM_I13,
  IMP_I14                         AS IPM_I14,
  IMP_I15                         AS IPM_I15

FROM {{ source('silver_ipm_sisben', 'ipm_sisben_transformed_data') }}

