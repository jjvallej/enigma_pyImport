

-- Modelo: Redondea todas las columnas numéricas a 2 decimales
-- Toma del modelo fill_nulls y redondea todas las columnas numéricas a 2 decimales
-- Excluye: departamento, ano_idc, fecha_lectura (columnas no numéricas)

SELECT
  -- Columnas base (no numéricas, se mantienen sin cambios)
  departamento,
  ano_idc,
  fecha_lectura,
  
  -- Columnas INS-* (Insuficiencia de Condiciones de Vida) - Redondear a 2 decimales
  ROUND(ins_1_1, 2) AS ins_1_1,
  ROUND(ins_1_2, 2) AS ins_1_2,
  ROUND(ins_1_3, 2) AS ins_1_3,
  ROUND(ins_2_1, 2) AS ins_2_1,
  ROUND(ins_2_2, 2) AS ins_2_2,
  ROUND(ins_2_3, 2) AS ins_2_3,
  ROUND(ins_3_1, 2) AS ins_3_1,
  ROUND(ins_3_2, 2) AS ins_3_2,
  ROUND(ins_3_3, 2) AS ins_3_3,
  ROUND(ins_4_1, 2) AS ins_4_1,
  ROUND(ins_4_2, 2) AS ins_4_2,
  ROUND(ins_4_3, 2) AS ins_4_3,
  ROUND(ins_4_4, 2) AS ins_4_4,
  ROUND(ins_4_5, 2) AS ins_4_5,
  ROUND(ins_4_6, 2) AS ins_4_6,
  
  -- Columnas INF-* (Insuficiencia de Funcionamiento)
  ROUND(inf_1_1, 2) AS inf_1_1,
  ROUND(inf_1_2, 2) AS inf_1_2,
  ROUND(inf_1_3, 2) AS inf_1_3,
  ROUND(inf_1_4, 2) AS inf_1_4,
  ROUND(inf_1_5, 2) AS inf_1_5,
  ROUND(inf_2_1, 2) AS inf_2_1,
  ROUND(inf_2_2, 2) AS inf_2_2,
  ROUND(inf_2_3, 2) AS inf_2_3,
  ROUND(inf_2_4, 2) AS inf_2_4,
  ROUND(inf_2_5, 2) AS inf_2_5,
  ROUND(inf_2_6, 2) AS inf_2_6,
  ROUND(inf_3_1, 2) AS inf_3_1,
  ROUND(inf_3_2, 2) AS inf_3_2,
  ROUND(inf_3_3, 2) AS inf_3_3,
  ROUND(inf_3_4, 2) AS inf_3_4,
  
  -- Columnas TIC-* (Tecnologías de la Información y Comunicación)
  ROUND(tic_1_1, 2) AS tic_1_1,
  ROUND(tic_1_2, 2) AS tic_1_2,
  ROUND(tic_1_3, 2) AS tic_1_3,
  ROUND(tic_1_4, 2) AS tic_1_4,
  ROUND(tic_2_1, 2) AS tic_2_1,
  ROUND(tic_2_2, 2) AS tic_2_2,
  ROUND(tic_2_3, 2) AS tic_2_3,
  
  -- Columnas AMB-* (Ambiente)
  ROUND(amb_1_1, 2) AS amb_1_1,
  ROUND(amb_1_2, 2) AS amb_1_2,
  ROUND(amb_1_3, 2) AS amb_1_3,
  ROUND(amb_2_1, 2) AS amb_2_1,
  ROUND(amb_2_2, 2) AS amb_2_2,
  
  -- Columnas SAL-* (Salud)
  ROUND(sal_1_1, 2) AS sal_1_1,
  ROUND(sal_1_2, 2) AS sal_1_2,
  ROUND(sal_1_3, 2) AS sal_1_3,
  ROUND(sal_2_1, 2) AS sal_2_1,
  ROUND(sal_2_2, 2) AS sal_2_2,
  ROUND(sal_2_3, 2) AS sal_2_3,
  ROUND(sal_3_1, 2) AS sal_3_1,
  ROUND(sal_3_2, 2) AS sal_3_2,
  ROUND(sal_3_3, 2) AS sal_3_3,
  ROUND(sal_3_4, 2) AS sal_3_4,
  
  -- Columnas EDU-* (Educación)
  ROUND(edu_1_1, 2) AS edu_1_1,
  ROUND(edu_1_2, 2) AS edu_1_2,
  ROUND(edu_1_3, 2) AS edu_1_3,
  ROUND(edu_1_4, 2) AS edu_1_4,
  ROUND(edu_1_5, 2) AS edu_1_5,
  ROUND(edu_2_1, 2) AS edu_2_1,
  ROUND(edu_2_2, 2) AS edu_2_2,
  ROUND(edu_2_3, 2) AS edu_2_3,
  ROUND(edu_2_4, 2) AS edu_2_4,
  
  -- Columnas EDS-* (Edificaciones y Servicios)
  ROUND(eds_1_1, 2) AS eds_1_1,
  ROUND(eds_1_2, 2) AS eds_1_2,
  ROUND(eds_1_3, 2) AS eds_1_3,
  ROUND(eds_2_1, 2) AS eds_2_1,
  ROUND(eds_2_2, 2) AS eds_2_2,
  ROUND(eds_2_3, 2) AS eds_2_3,
  ROUND(eds_2_4, 2) AS eds_2_4,
  ROUND(eds_3_1, 2) AS eds_3_1,
  ROUND(eds_3_2, 2) AS eds_3_2,
  
  -- Columnas NEG-* (Negocios/Empresas)
  ROUND(neg_1_1, 2) AS neg_1_1,
  ROUND(neg_1_2, 2) AS neg_1_2,
  ROUND(neg_1_3, 2) AS neg_1_3,
  ROUND(neg_2_1, 2) AS neg_2_1,
  ROUND(neg_2_2, 2) AS neg_2_2,
  ROUND(neg_2_3, 2) AS neg_2_3,
  
  -- Columnas LAB-* (Laboral)
  ROUND(lab_1_1, 2) AS lab_1_1,
  ROUND(lab_1_2, 2) AS lab_1_2,
  ROUND(lab_1_3, 2) AS lab_1_3,
  ROUND(lab_1_4, 2) AS lab_1_4,
  ROUND(lab_1_5, 2) AS lab_1_5,
  
  -- Columnas FIN-* (Financiero)
  ROUND(fin_1_1, 2) AS fin_1_1,
  ROUND(fin_1_2, 2) AS fin_1_2,
  ROUND(fin_1_3, 2) AS fin_1_3,
  ROUND(fin_1_4, 2) AS fin_1_4,
  
  -- Columnas TAM-* (Tamaño)
  ROUND(tam_1_1, 2) AS tam_1_1,
  ROUND(tam_2_1, 2) AS tam_2_1,
  ROUND(tam_2_2, 2) AS tam_2_2,
  
  -- Columnas SOF-* (Software)
  ROUND(sof_1_1, 2) AS sof_1_1,
  ROUND(sof_1_2, 2) AS sof_1_2,
  
  -- Columnas INN-* (Innovación)
  ROUND(inn_1_1, 2) AS inn_1_1,
  ROUND(inn_1_2, 2) AS inn_1_2,
  ROUND(inn_1_3, 2) AS inn_1_3,
  ROUND(inn_1_4, 2) AS inn_1_4,
  ROUND(inn_2_1, 2) AS inn_2_1,
  ROUND(inn_2_2, 2) AS inn_2_2,
  ROUND(inn_2_3, 2) AS inn_2_3,
  ROUND(inn_2_4, 2) AS inn_2_4
  
FROM `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`idc_fill_nulls_valor_normalizado`