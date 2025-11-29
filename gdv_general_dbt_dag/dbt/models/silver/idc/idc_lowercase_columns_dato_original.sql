{{
  config(
    materialized='table',
    schema='silver_dpt_planeacion_municipal_dev'
  )
}}

-- Modelo: Convierte todos los nombres de columnas a minúsculas
-- Toma del modelo normalize_columns y convierte todas las columnas explícitamente a minúsculas
-- Asegura que todas las columnas estén en formato snake_case con minúsculas (texto_texto)
-- Ejemplo: INS_1_1 -> ins_1_1

SELECT
  -- Columnas base
  departamento AS departamento,
  ano_idc AS ano_idc,
  fecha_lectura AS fecha_lectura,
  
  -- Columnas INS-* (Insuficiencia de Condiciones de Vida)
  INS_1_1 AS ins_1_1,
  INS_1_2 AS ins_1_2,
  INS_1_3 AS ins_1_3,
  INS_2_1 AS ins_2_1,
  INS_2_2 AS ins_2_2,
  INS_2_3 AS ins_2_3,
  INS_3_1 AS ins_3_1,
  INS_3_2 AS ins_3_2,
  INS_3_3 AS ins_3_3,
  INS_4_1 AS ins_4_1,
  INS_4_2 AS ins_4_2,
  INS_4_3 AS ins_4_3,
  INS_4_4 AS ins_4_4,
  INS_4_5 AS ins_4_5,
  INS_4_6 AS ins_4_6,
  
  -- Columnas INF-* (Insuficiencia de Funcionamiento)
  INF_1_1 AS inf_1_1,
  INF_1_2 AS inf_1_2,
  INF_1_3 AS inf_1_3,
  INF_1_4 AS inf_1_4,
  INF_1_5 AS inf_1_5,
  INF_2_1 AS inf_2_1,
  INF_2_2 AS inf_2_2,
  INF_2_3 AS inf_2_3,
  INF_2_4 AS inf_2_4,
  INF_2_5 AS inf_2_5,
  INF_2_6 AS inf_2_6,
  INF_3_1 AS inf_3_1,
  INF_3_2 AS inf_3_2,
  INF_3_3 AS inf_3_3,
  INF_3_4 AS inf_3_4,
  
  -- Columnas TIC-* (Tecnologías de la Información y Comunicación)
  TIC_1_1 AS tic_1_1,
  TIC_1_2 AS tic_1_2,
  TIC_1_3 AS tic_1_3,
  TIC_1_4 AS tic_1_4,
  TIC_2_1 AS tic_2_1,
  TIC_2_2 AS tic_2_2,
  TIC_2_3 AS tic_2_3,
  
  -- Columnas AMB-* (Ambiente)
  AMB_1_1 AS amb_1_1,
  AMB_1_2 AS amb_1_2,
  AMB_1_3 AS amb_1_3,
  AMB_2_1 AS amb_2_1,
  AMB_2_2 AS amb_2_2,
  
  -- Columnas SAL-* (Salud)
  SAL_1_1 AS sal_1_1,
  SAL_1_2 AS sal_1_2,
  SAL_1_3 AS sal_1_3,
  SAL_2_1 AS sal_2_1,
  SAL_2_2 AS sal_2_2,
  SAL_2_3 AS sal_2_3,
  SAL_3_1 AS sal_3_1,
  SAL_3_2 AS sal_3_2,
  SAL_3_3 AS sal_3_3,
  SAL_3_4 AS sal_3_4,
  
  -- Columnas EDU-* (Educación)
  EDU_1_1 AS edu_1_1,
  EDU_1_2 AS edu_1_2,
  EDU_1_3 AS edu_1_3,
  EDU_1_4 AS edu_1_4,
  EDU_1_5 AS edu_1_5,
  EDU_2_1 AS edu_2_1,
  EDU_2_2 AS edu_2_2,
  EDU_2_3 AS edu_2_3,
  EDU_2_4 AS edu_2_4,
  
  -- Columnas EDS-* (Edificaciones y Servicios)
  EDS_1_1 AS eds_1_1,
  EDS_1_2 AS eds_1_2,
  EDS_1_3 AS eds_1_3,
  EDS_2_1 AS eds_2_1,
  EDS_2_2 AS eds_2_2,
  EDS_2_3 AS eds_2_3,
  EDS_2_4 AS eds_2_4,
  EDS_3_1 AS eds_3_1,
  EDS_3_2 AS eds_3_2,
  
  -- Columnas NEG-* (Negocios/Empresas)
  NEG_1_1 AS neg_1_1,
  NEG_1_2 AS neg_1_2,
  NEG_1_3 AS neg_1_3,
  NEG_2_1 AS neg_2_1,
  NEG_2_2 AS neg_2_2,
  NEG_2_3 AS neg_2_3,
  
  -- Columnas LAB-* (Laboral)
  LAB_1_1 AS lab_1_1,
  LAB_1_2 AS lab_1_2,
  LAB_1_3 AS lab_1_3,
  LAB_1_4 AS lab_1_4,
  LAB_1_5 AS lab_1_5,
  
  -- Columnas FIN-* (Financiero)
  FIN_1_1 AS fin_1_1,
  FIN_1_2 AS fin_1_2,
  FIN_1_3 AS fin_1_3,
  FIN_1_4 AS fin_1_4,
  
  -- Columnas TAM-* (Tamaño)
  TAM_1_1 AS tam_1_1,
  TAM_2_1 AS tam_2_1,
  TAM_2_2 AS tam_2_2,
  
  -- Columnas SOF-* (Software)
  SOF_1_1 AS sof_1_1,
  SOF_1_2 AS sof_1_2,
  
  -- Columnas INN-* (Innovación)
  INN_1_1 AS inn_1_1,
  INN_1_2 AS inn_1_2,
  INN_1_3 AS inn_1_3,
  INN_1_4 AS inn_1_4,
  INN_2_1 AS inn_2_1,
  INN_2_2 AS inn_2_2,
  INN_2_3 AS inn_2_3,
  INN_2_4 AS inn_2_4
  
FROM {{ ref('idc_normalize_columns_dato_original') }}

