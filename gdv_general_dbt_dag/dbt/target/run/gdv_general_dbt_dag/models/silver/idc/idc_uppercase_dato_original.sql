
  
    

    create or replace table `datagov-473122`.`test_idc_silver`.`idc_transformed_data_dato_original`
      
    
    

    OPTIONS()
    as (
      

-- Modelo: Normaliza la columna departamento a mayúsculas sin acentos ni caracteres especiales
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
  
FROM `datagov-473122`.`test_idc_silver`.`idc_lowercase_columns_dato_original`
    );
  