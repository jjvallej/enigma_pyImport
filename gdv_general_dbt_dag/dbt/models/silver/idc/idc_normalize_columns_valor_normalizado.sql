{{
  config(
    materialized='table',
    schema=var('silver_dataset')
  )
}}

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
  
FROM {{ source('bronze_idc', 'idc_raw_data_valor_normalizado') }}
