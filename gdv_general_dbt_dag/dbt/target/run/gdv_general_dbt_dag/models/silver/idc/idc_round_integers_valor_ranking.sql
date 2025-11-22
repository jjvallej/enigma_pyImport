
  
    

    create or replace table `datagov-473122`.`test_idc_silver`.`idc_transformed_data_valor_ranking`
      
    
    

    OPTIONS()
    as (
      

-- Modelo: Convierte todas las columnas numéricas a enteros
-- Toma del modelo fill_nulls y convierte todas las columnas numéricas a enteros
-- Excluye: departamento, ano_idc, fecha_lectura (columnas no numéricas)

SELECT
  -- Columnas base (no numéricas, se mantienen sin cambios)
  departamento,
  ano_idc,
  fecha_lectura,
  
  -- Columnas INS-* (Insuficiencia de Condiciones de Vida) - Convertir a enteros
  SAFE_CAST(ins_1_1 AS INT64) AS ins_1_1,
  SAFE_CAST(ins_1_2 AS INT64) AS ins_1_2,
  SAFE_CAST(ins_1_3 AS INT64) AS ins_1_3,
  SAFE_CAST(ins_2_1 AS INT64) AS ins_2_1,
  SAFE_CAST(ins_2_2 AS INT64) AS ins_2_2,
  SAFE_CAST(ins_2_3 AS INT64) AS ins_2_3,
  SAFE_CAST(ins_3_1 AS INT64) AS ins_3_1,
  SAFE_CAST(ins_3_2 AS INT64) AS ins_3_2,
  SAFE_CAST(ins_3_3 AS INT64) AS ins_3_3,
  SAFE_CAST(ins_4_1 AS INT64) AS ins_4_1,
  SAFE_CAST(ins_4_2 AS INT64) AS ins_4_2,
  SAFE_CAST(ins_4_3 AS INT64) AS ins_4_3,
  SAFE_CAST(ins_4_4 AS INT64) AS ins_4_4,
  SAFE_CAST(ins_4_5 AS INT64) AS ins_4_5,
  SAFE_CAST(ins_4_6 AS INT64) AS ins_4_6,
  
  -- Columnas INF-* (Insuficiencia de Funcionamiento)
  SAFE_CAST(inf_1_1 AS INT64) AS inf_1_1,
  SAFE_CAST(inf_1_2 AS INT64) AS inf_1_2,
  SAFE_CAST(inf_1_3 AS INT64) AS inf_1_3,
  SAFE_CAST(inf_1_4 AS INT64) AS inf_1_4,
  SAFE_CAST(inf_1_5 AS INT64) AS inf_1_5,
  SAFE_CAST(inf_2_1 AS INT64) AS inf_2_1,
  SAFE_CAST(inf_2_2 AS INT64) AS inf_2_2,
  SAFE_CAST(inf_2_3 AS INT64) AS inf_2_3,
  SAFE_CAST(inf_2_4 AS INT64) AS inf_2_4,
  SAFE_CAST(inf_2_5 AS INT64) AS inf_2_5,
  SAFE_CAST(inf_2_6 AS INT64) AS inf_2_6,
  SAFE_CAST(inf_3_1 AS INT64) AS inf_3_1,
  SAFE_CAST(inf_3_2 AS INT64) AS inf_3_2,
  SAFE_CAST(inf_3_3 AS INT64) AS inf_3_3,
  SAFE_CAST(inf_3_4 AS INT64) AS inf_3_4,
  
  -- Columnas TIC-* (Tecnologías de la Información y Comunicación)
  SAFE_CAST(tic_1_1 AS INT64) AS tic_1_1,
  SAFE_CAST(tic_1_2 AS INT64) AS tic_1_2,
  SAFE_CAST(tic_1_3 AS INT64) AS tic_1_3,
  SAFE_CAST(tic_1_4 AS INT64) AS tic_1_4,
  SAFE_CAST(tic_2_1 AS INT64) AS tic_2_1,
  SAFE_CAST(tic_2_2 AS INT64) AS tic_2_2,
  SAFE_CAST(tic_2_3 AS INT64) AS tic_2_3,
  
  -- Columnas AMB-* (Ambiente)
  SAFE_CAST(amb_1_1 AS INT64) AS amb_1_1,
  SAFE_CAST(amb_1_2 AS INT64) AS amb_1_2,
  SAFE_CAST(amb_1_3 AS INT64) AS amb_1_3,
  SAFE_CAST(amb_2_1 AS INT64) AS amb_2_1,
  SAFE_CAST(amb_2_2 AS INT64) AS amb_2_2,
  
  -- Columnas SAL-* (Salud)
  SAFE_CAST(sal_1_1 AS INT64) AS sal_1_1,
  SAFE_CAST(sal_1_2 AS INT64) AS sal_1_2,
  SAFE_CAST(sal_1_3 AS INT64) AS sal_1_3,
  SAFE_CAST(sal_2_1 AS INT64) AS sal_2_1,
  SAFE_CAST(sal_2_2 AS INT64) AS sal_2_2,
  SAFE_CAST(sal_2_3 AS INT64) AS sal_2_3,
  SAFE_CAST(sal_3_1 AS INT64) AS sal_3_1,
  SAFE_CAST(sal_3_2 AS INT64) AS sal_3_2,
  SAFE_CAST(sal_3_3 AS INT64) AS sal_3_3,
  SAFE_CAST(sal_3_4 AS INT64) AS sal_3_4,
  
  -- Columnas EDU-* (Educación)
  SAFE_CAST(edu_1_1 AS INT64) AS edu_1_1,
  SAFE_CAST(edu_1_2 AS INT64) AS edu_1_2,
  SAFE_CAST(edu_1_3 AS INT64) AS edu_1_3,
  SAFE_CAST(edu_1_4 AS INT64) AS edu_1_4,
  SAFE_CAST(edu_1_5 AS INT64) AS edu_1_5,
  SAFE_CAST(edu_2_1 AS INT64) AS edu_2_1,
  SAFE_CAST(edu_2_2 AS INT64) AS edu_2_2,
  SAFE_CAST(edu_2_3 AS INT64) AS edu_2_3,
  SAFE_CAST(edu_2_4 AS INT64) AS edu_2_4,
  
  -- Columnas EDS-* (Edificaciones y Servicios)
  SAFE_CAST(eds_1_1 AS INT64) AS eds_1_1,
  SAFE_CAST(eds_1_2 AS INT64) AS eds_1_2,
  SAFE_CAST(eds_1_3 AS INT64) AS eds_1_3,
  SAFE_CAST(eds_2_1 AS INT64) AS eds_2_1,
  SAFE_CAST(eds_2_2 AS INT64) AS eds_2_2,
  SAFE_CAST(eds_2_3 AS INT64) AS eds_2_3,
  SAFE_CAST(eds_2_4 AS INT64) AS eds_2_4,
  SAFE_CAST(eds_3_1 AS INT64) AS eds_3_1,
  SAFE_CAST(eds_3_2 AS INT64) AS eds_3_2,
  
  -- Columnas NEG-* (Negocios/Empresas)
  SAFE_CAST(neg_1_1 AS INT64) AS neg_1_1,
  SAFE_CAST(neg_1_2 AS INT64) AS neg_1_2,
  SAFE_CAST(neg_1_3 AS INT64) AS neg_1_3,
  SAFE_CAST(neg_2_1 AS INT64) AS neg_2_1,
  SAFE_CAST(neg_2_2 AS INT64) AS neg_2_2,
  SAFE_CAST(neg_2_3 AS INT64) AS neg_2_3,
  
  -- Columnas LAB-* (Laboral)
  SAFE_CAST(lab_1_1 AS INT64) AS lab_1_1,
  SAFE_CAST(lab_1_2 AS INT64) AS lab_1_2,
  SAFE_CAST(lab_1_3 AS INT64) AS lab_1_3,
  SAFE_CAST(lab_1_4 AS INT64) AS lab_1_4,
  SAFE_CAST(lab_1_5 AS INT64) AS lab_1_5,
  
  -- Columnas FIN-* (Financiero)
  SAFE_CAST(fin_1_1 AS INT64) AS fin_1_1,
  SAFE_CAST(fin_1_2 AS INT64) AS fin_1_2,
  SAFE_CAST(fin_1_3 AS INT64) AS fin_1_3,
  SAFE_CAST(fin_1_4 AS INT64) AS fin_1_4,
  
  -- Columnas TAM-* (Tamaño)
  SAFE_CAST(tam_1_1 AS INT64) AS tam_1_1,
  SAFE_CAST(tam_2_1 AS INT64) AS tam_2_1,
  SAFE_CAST(tam_2_2 AS INT64) AS tam_2_2,
  
  -- Columnas SOF-* (Software)
  SAFE_CAST(sof_1_1 AS INT64) AS sof_1_1,
  SAFE_CAST(sof_1_2 AS INT64) AS sof_1_2,
  
  -- Columnas INN-* (Innovación)
  SAFE_CAST(inn_1_1 AS INT64) AS inn_1_1,
  SAFE_CAST(inn_1_2 AS INT64) AS inn_1_2,
  SAFE_CAST(inn_1_3 AS INT64) AS inn_1_3,
  SAFE_CAST(inn_1_4 AS INT64) AS inn_1_4,
  SAFE_CAST(inn_2_1 AS INT64) AS inn_2_1,
  SAFE_CAST(inn_2_2 AS INT64) AS inn_2_2,
  SAFE_CAST(inn_2_3 AS INT64) AS inn_2_3,
  SAFE_CAST(inn_2_4 AS INT64) AS inn_2_4
  
FROM `datagov-473122`.`test_idc_silver`.`idc_fill_nulls_valor_ranking`
    );
  