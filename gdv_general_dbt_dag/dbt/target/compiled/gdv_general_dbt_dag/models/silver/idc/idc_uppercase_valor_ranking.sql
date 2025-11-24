

with __dbt__cte__idc_normalize_columns_valor_ranking as (


-- Modelo: Normaliza nombres de columnas a formato snake_case (texto_texto)
-- Convierte todas las columnas a formato snake_case explícitamente
-- Toma directamente de la fuente bronze y convierte guiones a guiones bajos
-- Ejemplo: INS-1-1 -> INS_1_1, Departamento -> departamento

SELECT
  -- Columnas base
  Departamento AS departamento,
  `Año_IDC` AS ano_idc,
  fecha_lectura,
  
  -- Columnas INS-* (Insuficiencia de Condiciones de Vida)
  `INS-1-1` AS INS_1_1,
  `INS-1-2` AS INS_1_2,
  `INS-1-3` AS INS_1_3,
  `INS-2-1` AS INS_2_1,
  `INS-2-2` AS INS_2_2,
  `INS-2-3` AS INS_2_3,
  `INS-3-1` AS INS_3_1,
  `INS-3-2` AS INS_3_2,
  `INS-3-3` AS INS_3_3,
  `INS-4-1` AS INS_4_1,
  `INS-4-2` AS INS_4_2,
  `INS-4-3` AS INS_4_3,
  `INS-4-4` AS INS_4_4,
  `INS-4-5` AS INS_4_5,
  `INS-4-6` AS INS_4_6,
  
  -- Columnas INF-* (Insuficiencia de Funcionamiento)
  `INF-1-1` AS INF_1_1,
  `INF-1-2` AS INF_1_2,
  `INF-1-3` AS INF_1_3,
  `INF-1-4` AS INF_1_4,
  `INF-1-5` AS INF_1_5,
  `INF-2-1` AS INF_2_1,
  `INF-2-2` AS INF_2_2,
  `INF-2-3` AS INF_2_3,
  `INF-2-4` AS INF_2_4,
  `INF-2-5` AS INF_2_5,
  `INF-2-6` AS INF_2_6,
  `INF-3-1` AS INF_3_1,
  `INF-3-2` AS INF_3_2,
  `INF-3-3` AS INF_3_3,
  `INF-3-4` AS INF_3_4,
  
  -- Columnas TIC-* (Tecnologías de la Información y Comunicación)
  `TIC-1-1` AS TIC_1_1,
  `TIC-1-2` AS TIC_1_2,
  `TIC-1-3` AS TIC_1_3,
  `TIC-1-4` AS TIC_1_4,
  `TIC-2-1` AS TIC_2_1,
  `TIC-2-2` AS TIC_2_2,
  `TIC-2-3` AS TIC_2_3,
  
  -- Columnas AMB-* (Ambiente)
  `AMB-1-1` AS AMB_1_1,
  `AMB-1-2` AS AMB_1_2,
  `AMB-1-3` AS AMB_1_3,
  `AMB-2-1` AS AMB_2_1,
  `AMB-2-2` AS AMB_2_2,
  
  -- Columnas SAL-* (Salud)
  `SAL-1-1` AS SAL_1_1,
  `SAL-1-2` AS SAL_1_2,
  `SAL-1-3` AS SAL_1_3,
  `SAL-2-1` AS SAL_2_1,
  `SAL-2-2` AS SAL_2_2,
  `SAL-2-3` AS SAL_2_3,
  `SAL-3-1` AS SAL_3_1,
  `SAL-3-2` AS SAL_3_2,
  `SAL-3-3` AS SAL_3_3,
  `SAL-3-4` AS SAL_3_4,
  
  -- Columnas EDU-* (Educación)
  `EDU-1-1` AS EDU_1_1,
  `EDU-1-2` AS EDU_1_2,
  `EDU-1-3` AS EDU_1_3,
  `EDU-1-4` AS EDU_1_4,
  `EDU-1-5` AS EDU_1_5,
  `EDU-2-1` AS EDU_2_1,
  `EDU-2-2` AS EDU_2_2,
  `EDU-2-3` AS EDU_2_3,
  `EDU-2-4` AS EDU_2_4,
  
  -- Columnas EDS-* (Edificaciones y Servicios)
  `EDS-1-1` AS EDS_1_1,
  `EDS-1-2` AS EDS_1_2,
  `EDS-1-3` AS EDS_1_3,
  `EDS-2-1` AS EDS_2_1,
  `EDS-2-2` AS EDS_2_2,
  `EDS-2-3` AS EDS_2_3,
  `EDS-2-4` AS EDS_2_4,
  `EDS-3-1` AS EDS_3_1,
  `EDS-3-2` AS EDS_3_2,
  
  -- Columnas NEG-* (Negocios/Empresas)
  `NEG-1-1` AS NEG_1_1,
  `NEG-1-2` AS NEG_1_2,
  `NEG-1-3` AS NEG_1_3,
  `NEG-2-1` AS NEG_2_1,
  `NEG-2-2` AS NEG_2_2,
  `NEG-2-3` AS NEG_2_3,
  
  -- Columnas LAB-* (Laboral)
  `LAB-1-1` AS LAB_1_1,
  `LAB-1-2` AS LAB_1_2,
  `LAB-1-3` AS LAB_1_3,
  `LAB-1-4` AS LAB_1_4,
  `LAB-1-5` AS LAB_1_5,
  
  -- Columnas FIN-* (Financiero)
  `FIN-1-1` AS FIN_1_1,
  `FIN-1-2` AS FIN_1_2,
  `FIN-1-3` AS FIN_1_3,
  `FIN-1-4` AS FIN_1_4,
  
  -- Columnas TAM-* (Tamaño)
  `TAM-1-1` AS TAM_1_1,
  `TAM-2-1` AS TAM_2_1,
  `TAM-2-2` AS TAM_2_2,
  
  -- Columnas SOF-* (Software)
  `SOF-1-1` AS SOF_1_1,
  `SOF-1-2` AS SOF_1_2,
  
  -- Columnas INN-* (Innovación)
  `INN-1-1` AS INN_1_1,
  `INN-1-2` AS INN_1_2,
  `INN-1-3` AS INN_1_3,
  `INN-1-4` AS INN_1_4,
  `INN-2-1` AS INN_2_1,
  `INN-2-2` AS INN_2_2,
  `INN-2-3` AS INN_2_3,
  `INN-2-4` AS INN_2_4
  
FROM `datagov-473122`.`test_idc_bronze`.`idc_raw_data_valor_ranking`
),  __dbt__cte__idc_lowercase_columns_valor_ranking as (


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
  
FROM __dbt__cte__idc_normalize_columns_valor_ranking
) -- Modelo: Normaliza la columna departamento a mayúsculas sin acentos ni caracteres especiales
-- Toma del modelo lowercase_columns y normaliza departamento
-- Ejemplo: "Archipiélago de San Andrés" -> "ARCHIPIELAGO DE SAN ANDRES"

SELECT
  -- Columnas base con normalización de departamento
  REGEXP_REPLACE(
    REPLACE(
      REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                UPPER(departamento),
                'Á', 'A'
              ),
              'É', 'E'
            ),
            'Í', 'I'
          ),
          'Ó', 'O'
        ),
        'Ú', 'U'
      ),
      'Ñ', 'N'
    ),
    r'[^A-Z0-9\s]',
    ''
  ) AS departamento,
  ano_idc AS ano_idc,
  fecha_lectura AS fecha_lectura,
  
  -- Columnas INS-* (Insuficiencia de Condiciones de Vida)
  ins_1_1,
  ins_1_2,
  ins_1_3,
  ins_2_1,
  ins_2_2,
  ins_2_3,
  ins_3_1,
  ins_3_2,
  ins_3_3,
  ins_4_1,
  ins_4_2,
  ins_4_3,
  ins_4_4,
  ins_4_5,
  ins_4_6,
  
  -- Columnas INF-* (Insuficiencia de Funcionamiento)
  inf_1_1,
  inf_1_2,
  inf_1_3,
  inf_1_4,
  inf_1_5,
  inf_2_1,
  inf_2_2,
  inf_2_3,
  inf_2_4,
  inf_2_5,
  inf_2_6,
  inf_3_1,
  inf_3_2,
  inf_3_3,
  inf_3_4,
  
  -- Columnas TIC-* (Tecnologías de la Información y Comunicación)
  tic_1_1,
  tic_1_2,
  tic_1_3,
  tic_1_4,
  tic_2_1,
  tic_2_2,
  tic_2_3,
  
  -- Columnas AMB-* (Ambiente)
  amb_1_1,
  amb_1_2,
  amb_1_3,
  amb_2_1,
  amb_2_2,
  
  -- Columnas SAL-* (Salud)
  sal_1_1,
  sal_1_2,
  sal_1_3,
  sal_2_1,
  sal_2_2,
  sal_2_3,
  sal_3_1,
  sal_3_2,
  sal_3_3,
  sal_3_4,
  
  -- Columnas EDU-* (Educación)
  edu_1_1,
  edu_1_2,
  edu_1_3,
  edu_1_4,
  edu_1_5,
  edu_2_1,
  edu_2_2,
  edu_2_3,
  edu_2_4,
  
  -- Columnas EDS-* (Edificaciones y Servicios)
  eds_1_1,
  eds_1_2,
  eds_1_3,
  eds_2_1,
  eds_2_2,
  eds_2_3,
  eds_2_4,
  eds_3_1,
  eds_3_2,
  
  -- Columnas NEG-* (Negocios/Empresas)
  neg_1_1,
  neg_1_2,
  neg_1_3,
  neg_2_1,
  neg_2_2,
  neg_2_3,
  
  -- Columnas LAB-* (Laboral)
  lab_1_1,
  lab_1_2,
  lab_1_3,
  lab_1_4,
  lab_1_5,
  
  -- Columnas FIN-* (Financiero)
  fin_1_1,
  fin_1_2,
  fin_1_3,
  fin_1_4,
  
  -- Columnas TAM-* (Tamaño)
  tam_1_1,
  tam_2_1,
  tam_2_2,
  
  -- Columnas SOF-* (Software)
  sof_1_1,
  sof_1_2,
  
  -- Columnas INN-* (Innovación)
  inn_1_1,
  inn_1_2,
  inn_1_3,
  inn_1_4,
  inn_2_1,
  inn_2_2,
  inn_2_3,
  inn_2_4
  
FROM __dbt__cte__idc_lowercase_columns_valor_ranking