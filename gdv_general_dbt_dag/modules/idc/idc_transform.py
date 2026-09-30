# modules/idc/idc_transform.py
"""
Módulo para transformar datos de IDC desde bronze a silver usando BigQuery directamente.
Reemplaza las transformaciones de dbt con queries SQL directas para mejor rendimiento y control.
"""
from google.cloud import bigquery
from typing import Optional
import logging
from modules.config import PROJECT_ID, DATASET_ID_BRONZE, DATASET_ID_SILVER, DATASET_ID_GOLD, CONF
from modules.gcp_utils import get_bq_client

DEBUG = CONF.global_config.debug
logger = logging.getLogger(__name__)

# Nota: En BigQuery, la estructura es project.dataset.table
# El dataset ya está definido en DATASET_ID_SILVER

def execute_query(query: str, description: str, timeout: int = 1800) -> bigquery.QueryJob:
    """
    Ejecuta una query SQL en BigQuery y espera su completación.
    
    Args:
        query: Query SQL a ejecutar
        description: Descripción de la query para logging
        timeout: Timeout en segundos (default: 1 hora)
    
    Returns:
        QueryJob completado
    """
    client = get_bq_client()
    logger.info(f"[INICIO] {description}")
    if DEBUG:
        logger.debug(f"[DEBUG] Query: {query[:200]}...")
    
    job_config = bigquery.QueryJobConfig(
        use_legacy_sql=False,
        job_timeout_ms=timeout * 1000,
    )
    
    job = client.query(query, job_config=job_config)
    job.result()  # Esperar a que termine
    
    if job.errors:
        error_msg = f"Error en query '{description}': {job.errors}"
        logger.error(error_msg)
        raise RuntimeError(error_msg)
    
    # Obtener información del job
    rows_processed = getattr(job, 'num_dml_affected_rows', None) or getattr(job, 'total_bytes_processed', None)
    if rows_processed:
        logger.info(f"[COMPLETADO] {description} - Procesadas {rows_processed} filas/bytes")
    else:
        logger.info(f"[COMPLETADO] {description}")
    return job


def normalize_columns_step(table_type: str) -> None:
    """
    Paso 1: Normaliza nombres de columnas a formato snake_case y convierte a minúsculas.
    Valida la transformación sin crear tabla (solo verifica que la query funcione).
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.idc_raw_data_{table_type}"
    
    # Query de validación (no crea tabla, solo valida)
    query = f"""
    SELECT COUNT(*) as total_rows
    FROM (
      SELECT
        Departamento AS departamento,
        `Año_IDC` AS ano_idc,
        fecha_lectura,
        `INS-1-1` AS ins_1_1,
        `INS-1-2` AS ins_1_2,
        `INS-1-3` AS ins_1_3,
        `INS-2-1` AS ins_2_1,
        `INS-2-2` AS ins_2_2,
        `INS-2-3` AS ins_2_3,
        `INS-3-1` AS ins_3_1,
        `INS-3-2` AS ins_3_2,
        `INS-3-3` AS ins_3_3,
        `INS-4-1` AS ins_4_1,
        `INS-4-2` AS ins_4_2,
        `INS-4-3` AS ins_4_3,
        `INS-4-4` AS ins_4_4,
        `INS-4-5` AS ins_4_5,
        `INS-4-6` AS ins_4_6,
        `INF-1-1` AS inf_1_1,
        `INF-1-2` AS inf_1_2,
        `INF-1-3` AS inf_1_3,
        `INF-1-4` AS inf_1_4,
        `INF-1-5` AS inf_1_5,
        `INF-2-1` AS inf_2_1,
        `INF-2-2` AS inf_2_2,
        `INF-2-3` AS inf_2_3,
        `INF-2-4` AS inf_2_4,
        `INF-2-5` AS inf_2_5,
        `INF-2-6` AS inf_2_6,
        `INF-3-1` AS inf_3_1,
        `INF-3-2` AS inf_3_2,
        `INF-3-3` AS inf_3_3,
        `INF-3-4` AS inf_3_4,
        `TIC-1-1` AS tic_1_1,
        `TIC-1-2` AS tic_1_2,
        `TIC-1-3` AS tic_1_3,
        `TIC-1-4` AS tic_1_4,
        `TIC-2-1` AS tic_2_1,
        `TIC-2-2` AS tic_2_2,
        `TIC-2-3` AS tic_2_3,
        `AMB-1-1` AS amb_1_1,
        `AMB-1-2` AS amb_1_2,
        `AMB-1-3` AS amb_1_3,
        `AMB-2-1` AS amb_2_1,
        `AMB-2-2` AS amb_2_2,
        `SAL-1-1` AS sal_1_1,
        `SAL-1-2` AS sal_1_2,
        `SAL-1-3` AS sal_1_3,
        `SAL-2-1` AS sal_2_1,
        `SAL-2-2` AS sal_2_2,
        `SAL-2-3` AS sal_2_3,
        `SAL-3-1` AS sal_3_1,
        `SAL-3-2` AS sal_3_2,
        `SAL-3-3` AS sal_3_3,
        `SAL-3-4` AS sal_3_4,
        `EDU-1-1` AS edu_1_1,
        `EDU-1-2` AS edu_1_2,
        `EDU-1-3` AS edu_1_3,
        `EDU-1-4` AS edu_1_4,
        `EDU-1-5` AS edu_1_5,
        `EDU-2-1` AS edu_2_1,
        `EDU-2-2` AS edu_2_2,
        `EDU-2-3` AS edu_2_3,
        `EDU-2-4` AS edu_2_4,
        `EDS-1-1` AS eds_1_1,
        `EDS-1-2` AS eds_1_2,
        `EDS-1-3` AS eds_1_3,
        `EDS-2-1` AS eds_2_1,
        `EDS-2-2` AS eds_2_2,
        `EDS-2-3` AS eds_2_3,
        `EDS-2-4` AS eds_2_4,
        `EDS-3-1` AS eds_3_1,
        `EDS-3-2` AS eds_3_2,
        `NEG-1-1` AS neg_1_1,
        `NEG-1-2` AS neg_1_2,
        `NEG-1-3` AS neg_1_3,
        `NEG-2-1` AS neg_2_1,
        `NEG-2-2` AS neg_2_2,
        `NEG-2-3` AS neg_2_3,
        `LAB-1-1` AS lab_1_1,
        `LAB-1-2` AS lab_1_2,
        `LAB-1-3` AS lab_1_3,
        `LAB-1-4` AS lab_1_4,
        `LAB-1-5` AS lab_1_5,
        `FIN-1-1` AS fin_1_1,
        `FIN-1-2` AS fin_1_2,
        `FIN-1-3` AS fin_1_3,
        `FIN-1-4` AS fin_1_4,
        `TAM-1-1` AS tam_1_1,
        `TAM-2-1` AS tam_2_1,
        `TAM-2-2` AS tam_2_2,
        `SOF-1-1` AS sof_1_1,
        `SOF-1-2` AS sof_1_2,
        `INN-1-1` AS inn_1_1,
        `INN-1-2` AS inn_1_2,
        `INN-1-3` AS inn_1_3,
        `INN-1-4` AS inn_1_4,
        `INN-2-1` AS inn_2_1,
        `INN-2-2` AS inn_2_2,
        `INN-2-3` AS inn_2_3,
        `INN-2-4` AS inn_2_4
      FROM `{source_table}`
    )
    """
    
    execute_query(query, f"Validar normalización de columnas para {table_type}")


def uppercase_departamento_step(table_type: str) -> None:
    """
    Paso 2: Normaliza departamento a mayúsculas sin acentos.
    Valida la transformación sin crear tabla.
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.idc_raw_data_{table_type}"
    
    # Query de validación con normalización de departamento
    query = f"""
    SELECT COUNT(*) as total_rows
    FROM (
      SELECT
        REGEXP_REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(
                      UPPER(Departamento),
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
          r'[^A-Z0-9 ]',
          ''
        ) AS departamento,
        `Año_IDC` AS ano_idc,
        fecha_lectura
      FROM `{source_table}`
    )
    """
    
    execute_query(query, f"Validar normalización de departamento para {table_type}")


def fill_nulls_step(table_type: str) -> None:
    """
    Paso 3: Valida el reemplazo de NULL/NaN por 0 en columnas numéricas.
    Valida la transformación sin crear tabla.
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.idc_raw_data_{table_type}"
    
    # Query de validación con reemplazo de NULLs
    query = f"""
    SELECT COUNT(*) as total_rows
    FROM (
      SELECT
        IF(SAFE_CAST(`INS-1-1` AS FLOAT64) IS NULL OR IS_NAN(SAFE_CAST(`INS-1-1` AS FLOAT64)), 0.0, SAFE_CAST(`INS-1-1` AS FLOAT64)) AS ins_1_1
      FROM `{source_table}`
      LIMIT 1
    )
    """
    
    execute_query(query, f"Validar reemplazo de NULLs para {table_type}")


def transform_table_complete(table_type: str) -> None:
    """
    Transforma una tabla completa desde bronze a silver aplicando todas las transformaciones en una sola query:
    1. Normaliza nombres de columnas (INS-1-1 -> ins_1_1)
    2. Normaliza departamento (mayúsculas sin acentos)
    3. Reemplaza NULL/NaN por 0 en columnas numéricas
    4. Redondea a 2 decimales (para dato_original y valor_normalizado) o convierte a enteros (para valor_ranking)
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.idc_raw_data_{table_type}"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_transformed_data_{table_type}"
    
    # Determinar si es valor_ranking (usa enteros) o los otros (usan decimales)
    is_ranking = table_type == 'valor_ranking'
    
    # Construir la query combinando todas las transformaciones
    # Paso 1: Normalizar columnas y convertir a minúsculas directamente
    # Paso 2: Normalizar departamento
    # Paso 3: Reemplazar NULL/NaN por 0
    # Paso 4: Redondear o convertir a enteros
    
    numeric_transform = "CAST(ROUND(IF(SAFE_CAST({col} AS FLOAT64) IS NULL OR IS_NAN(SAFE_CAST({col} AS FLOAT64)), 0.0, SAFE_CAST({col} AS FLOAT64))) AS INT64)" if is_ranking else "ROUND(IF(SAFE_CAST({col} AS FLOAT64) IS NULL OR IS_NAN(SAFE_CAST({col} AS FLOAT64)), 0.0, SAFE_CAST({col} AS FLOAT64)), 2)"
    
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      -- Normalizar departamento a mayúsculas sin acentos
      REGEXP_REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    UPPER(Departamento),
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
        r'[^A-Z0-9 ]',
        ''
      ) AS departamento,
      `Año_IDC` AS ano_idc,
      fecha_lectura,
      
      -- Columnas INS-* (Insuficiencia de Condiciones de Vida) - Aplicar todas las transformaciones
      {numeric_transform.format(col='`INS-1-1`')} AS ins_1_1,
      {numeric_transform.format(col='`INS-1-2`')} AS ins_1_2,
      {numeric_transform.format(col='`INS-1-3`')} AS ins_1_3,
      {numeric_transform.format(col='`INS-2-1`')} AS ins_2_1,
      {numeric_transform.format(col='`INS-2-2`')} AS ins_2_2,
      {numeric_transform.format(col='`INS-2-3`')} AS ins_2_3,
      {numeric_transform.format(col='`INS-3-1`')} AS ins_3_1,
      {numeric_transform.format(col='`INS-3-2`')} AS ins_3_2,
      {numeric_transform.format(col='`INS-3-3`')} AS ins_3_3,
      {numeric_transform.format(col='`INS-4-1`')} AS ins_4_1,
      {numeric_transform.format(col='`INS-4-2`')} AS ins_4_2,
      {numeric_transform.format(col='`INS-4-3`')} AS ins_4_3,
      {numeric_transform.format(col='`INS-4-4`')} AS ins_4_4,
      {numeric_transform.format(col='`INS-4-5`')} AS ins_4_5,
      {numeric_transform.format(col='`INS-4-6`')} AS ins_4_6,
      
      -- Columnas INF-* (Insuficiencia de Funcionamiento)
      {numeric_transform.format(col='`INF-1-1`')} AS inf_1_1,
      {numeric_transform.format(col='`INF-1-2`')} AS inf_1_2,
      {numeric_transform.format(col='`INF-1-3`')} AS inf_1_3,
      {numeric_transform.format(col='`INF-1-4`')} AS inf_1_4,
      {numeric_transform.format(col='`INF-1-5`')} AS inf_1_5,
      {numeric_transform.format(col='`INF-2-1`')} AS inf_2_1,
      {numeric_transform.format(col='`INF-2-2`')} AS inf_2_2,
      {numeric_transform.format(col='`INF-2-3`')} AS inf_2_3,
      {numeric_transform.format(col='`INF-2-4`')} AS inf_2_4,
      {numeric_transform.format(col='`INF-2-5`')} AS inf_2_5,
      {numeric_transform.format(col='`INF-2-6`')} AS inf_2_6,
      {numeric_transform.format(col='`INF-3-1`')} AS inf_3_1,
      {numeric_transform.format(col='`INF-3-2`')} AS inf_3_2,
      {numeric_transform.format(col='`INF-3-3`')} AS inf_3_3,
      {numeric_transform.format(col='`INF-3-4`')} AS inf_3_4,
      
      -- Columnas TIC-* (Tecnologías de la Información y Comunicación)
      {numeric_transform.format(col='`TIC-1-1`')} AS tic_1_1,
      {numeric_transform.format(col='`TIC-1-2`')} AS tic_1_2,
      {numeric_transform.format(col='`TIC-1-3`')} AS tic_1_3,
      {numeric_transform.format(col='`TIC-1-4`')} AS tic_1_4,
      {numeric_transform.format(col='`TIC-2-1`')} AS tic_2_1,
      {numeric_transform.format(col='`TIC-2-2`')} AS tic_2_2,
      {numeric_transform.format(col='`TIC-2-3`')} AS tic_2_3,
      
      -- Columnas AMB-* (Ambiente)
      {numeric_transform.format(col='`AMB-1-1`')} AS amb_1_1,
      {numeric_transform.format(col='`AMB-1-2`')} AS amb_1_2,
      {numeric_transform.format(col='`AMB-1-3`')} AS amb_1_3,
      {numeric_transform.format(col='`AMB-2-1`')} AS amb_2_1,
      {numeric_transform.format(col='`AMB-2-2`')} AS amb_2_2,
      
      -- Columnas SAL-* (Salud)
      {numeric_transform.format(col='`SAL-1-1`')} AS sal_1_1,
      {numeric_transform.format(col='`SAL-1-2`')} AS sal_1_2,
      {numeric_transform.format(col='`SAL-1-3`')} AS sal_1_3,
      {numeric_transform.format(col='`SAL-2-1`')} AS sal_2_1,
      {numeric_transform.format(col='`SAL-2-2`')} AS sal_2_2,
      {numeric_transform.format(col='`SAL-2-3`')} AS sal_2_3,
      {numeric_transform.format(col='`SAL-3-1`')} AS sal_3_1,
      {numeric_transform.format(col='`SAL-3-2`')} AS sal_3_2,
      {numeric_transform.format(col='`SAL-3-3`')} AS sal_3_3,
      {numeric_transform.format(col='`SAL-3-4`')} AS sal_3_4,
      
      -- Columnas EDU-* (Educación)
      {numeric_transform.format(col='`EDU-1-1`')} AS edu_1_1,
      {numeric_transform.format(col='`EDU-1-2`')} AS edu_1_2,
      {numeric_transform.format(col='`EDU-1-3`')} AS edu_1_3,
      {numeric_transform.format(col='`EDU-1-4`')} AS edu_1_4,
      {numeric_transform.format(col='`EDU-1-5`')} AS edu_1_5,
      {numeric_transform.format(col='`EDU-2-1`')} AS edu_2_1,
      {numeric_transform.format(col='`EDU-2-2`')} AS edu_2_2,
      {numeric_transform.format(col='`EDU-2-3`')} AS edu_2_3,
      {numeric_transform.format(col='`EDU-2-4`')} AS edu_2_4,
      
      -- Columnas EDS-* (Edificaciones y Servicios)
      {numeric_transform.format(col='`EDS-1-1`')} AS eds_1_1,
      {numeric_transform.format(col='`EDS-1-2`')} AS eds_1_2,
      {numeric_transform.format(col='`EDS-1-3`')} AS eds_1_3,
      {numeric_transform.format(col='`EDS-2-1`')} AS eds_2_1,
      {numeric_transform.format(col='`EDS-2-2`')} AS eds_2_2,
      {numeric_transform.format(col='`EDS-2-3`')} AS eds_2_3,
      {numeric_transform.format(col='`EDS-2-4`')} AS eds_2_4,
      {numeric_transform.format(col='`EDS-3-1`')} AS eds_3_1,
      {numeric_transform.format(col='`EDS-3-2`')} AS eds_3_2,
      
      -- Columnas NEG-* (Negocios/Empresas)
      {numeric_transform.format(col='`NEG-1-1`')} AS neg_1_1,
      {numeric_transform.format(col='`NEG-1-2`')} AS neg_1_2,
      {numeric_transform.format(col='`NEG-1-3`')} AS neg_1_3,
      {numeric_transform.format(col='`NEG-2-1`')} AS neg_2_1,
      {numeric_transform.format(col='`NEG-2-2`')} AS neg_2_2,
      {numeric_transform.format(col='`NEG-2-3`')} AS neg_2_3,
      
      -- Columnas LAB-* (Laboral)
      {numeric_transform.format(col='`LAB-1-1`')} AS lab_1_1,
      {numeric_transform.format(col='`LAB-1-2`')} AS lab_1_2,
      {numeric_transform.format(col='`LAB-1-3`')} AS lab_1_3,
      {numeric_transform.format(col='`LAB-1-4`')} AS lab_1_4,
      {numeric_transform.format(col='`LAB-1-5`')} AS lab_1_5,
      
      -- Columnas FIN-* (Financiero)
      {numeric_transform.format(col='`FIN-1-1`')} AS fin_1_1,
      {numeric_transform.format(col='`FIN-1-2`')} AS fin_1_2,
      {numeric_transform.format(col='`FIN-1-3`')} AS fin_1_3,
      {numeric_transform.format(col='`FIN-1-4`')} AS fin_1_4,
      
      -- Columnas TAM-* (Tamaño)
      {numeric_transform.format(col='`TAM-1-1`')} AS tam_1_1,
      {numeric_transform.format(col='`TAM-2-1`')} AS tam_2_1,
      {numeric_transform.format(col='`TAM-2-2`')} AS tam_2_2,
      
      -- Columnas SOF-* (Software)
      {numeric_transform.format(col='`SOF-1-1`')} AS sof_1_1,
      {numeric_transform.format(col='`SOF-1-2`')} AS sof_1_2,
      
      -- Columnas INN-* (Innovación)
      {numeric_transform.format(col='`INN-1-1`')} AS inn_1_1,
      {numeric_transform.format(col='`INN-1-2`')} AS inn_1_2,
      {numeric_transform.format(col='`INN-1-3`')} AS inn_1_3,
      {numeric_transform.format(col='`INN-1-4`')} AS inn_1_4,
      {numeric_transform.format(col='`INN-2-1`')} AS inn_2_1,
      {numeric_transform.format(col='`INN-2-2`')} AS inn_2_2,
      {numeric_transform.format(col='`INN-2-3`')} AS inn_2_3,
      {numeric_transform.format(col='`INN-2-4`')} AS inn_2_4
    FROM `{source_table}`
    """
    
    execute_query(query, f"Transformación completa para {table_type}")


def normalize_columns(table_type: str) -> None:
    """
    Paso 1: Normaliza nombres de columnas a formato snake_case.
    Convierte guiones a guiones bajos (ej: INS-1-1 -> INS_1_1).
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.idc_raw_data_{table_type}"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_normalize_columns_{table_type}"
    
    # Query base - todas las tablas tienen la misma estructura de columnas
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
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
    FROM `{source_table}`
    """
    
    execute_query(query, f"Normalizar columnas para {table_type}")


def lowercase_columns(table_type: str) -> None:
    """
    Paso 2: Convierte todos los nombres de columnas a minúsculas.
    Usa la API de BigQuery para obtener el esquema y generar las columnas dinámicamente.
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_normalize_columns_{table_type}"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_lowercase_columns_{table_type}"
    
    # Obtener el esquema de la tabla fuente
    client = get_bq_client()
    try:
        table_ref = client.get_table(source_table)
    except Exception as e:
        raise RuntimeError(f"No se pudo acceder a la tabla fuente {source_table}: {e}")
    
    # Construir la lista de columnas con conversión a minúsculas
    columns = []
    for field in table_ref.schema:
        col_name = field.name
        if col_name in ['departamento', 'ano_idc', 'fecha_lectura']:
            # Columnas base se mantienen igual
            columns.append(f"{col_name} AS {col_name}")
        else:
            # Columnas numéricas se convierten a minúsculas
            columns.append(f"{col_name} AS {col_name.lower()}")
    
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      {', '.join(columns)}
    FROM `{source_table}`
    """
    
    execute_query(query, f"Convertir a minúsculas para {table_type}")


def uppercase_departamento(table_type: str) -> None:
    """
    Paso 3: Normaliza la columna departamento a mayúsculas sin acentos.
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_lowercase_columns_{table_type}"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_uppercase_{table_type}"
    
    # Obtener todas las columnas de la tabla fuente (excepto departamento)
    client = get_bq_client()
    try:
        table_ref = client.get_table(source_table)
    except Exception as e:
        raise RuntimeError(f"No se pudo acceder a la tabla fuente {source_table}: {e}")
    
    numeric_columns = [field.name for field in table_ref.schema if field.name not in ['departamento', 'ano_idc', 'fecha_lectura']]
    
    if not numeric_columns:
        raise RuntimeError(f"No se encontraron columnas numéricas en {source_table}")
    
    # Construir la query con todas las columnas
    numeric_cols_str = ', '.join(numeric_columns)
    
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      -- Normalizar departamento a mayúsculas sin acentos
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
        '[^A-Z0-9 ]', ''
      ) AS departamento,
      ano_idc,
      fecha_lectura,
      {numeric_cols_str}
    FROM `{source_table}`
    """
    
    execute_query(query, f"Normalizar departamento para {table_type}")


def fill_nulls(table_type: str) -> None:
    """
    Paso 4: Reemplaza valores NULL y NaN por 0 en todas las columnas numéricas.
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_uppercase_{table_type}"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_fill_nulls_{table_type}"
    
    # Obtener todas las columnas de la tabla fuente
    client = get_bq_client()
    try:
        table_ref = client.get_table(source_table)
    except Exception as e:
        raise RuntimeError(f"No se pudo acceder a la tabla fuente {source_table}: {e}")
    
    numeric_columns = [field.name for field in table_ref.schema if field.name not in ['departamento', 'ano_idc', 'fecha_lectura']]
    
    if not numeric_columns:
        raise RuntimeError(f"No se encontraron columnas numéricas en {source_table}")
    
    # Construir la query reemplazando NULL/NaN por 0
    numeric_cols_str = ', '.join([
        f"IF(SAFE_CAST({col} AS FLOAT64) IS NULL OR IS_NAN(SAFE_CAST({col} AS FLOAT64)), 0.0, SAFE_CAST({col} AS FLOAT64)) AS {col}"
        for col in numeric_columns
    ])
    
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      departamento,
      ano_idc,
      fecha_lectura,
      {numeric_cols_str}
    FROM `{source_table}`
    """
    
    execute_query(query, f"Reemplazar NULLs para {table_type}")


def round_decimals(table_type: str) -> None:
    """
    Paso 5: Redondea todas las columnas numéricas a 2 decimales.
    
    Args:
        table_type: 'dato_original' o 'valor_normalizado'
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_fill_nulls_{table_type}"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_transformed_data_{table_type}"
    
    # Obtener todas las columnas de la tabla fuente
    client = get_bq_client()
    try:
        table_ref = client.get_table(source_table)
    except Exception as e:
        raise RuntimeError(f"No se pudo acceder a la tabla fuente {source_table}: {e}")
    
    numeric_columns = [field.name for field in table_ref.schema if field.name not in ['departamento', 'ano_idc', 'fecha_lectura']]
    
    if not numeric_columns:
        raise RuntimeError(f"No se encontraron columnas numéricas en {source_table}")
    
    # Construir la query redondeando a 2 decimales
    numeric_cols_str = ', '.join([
        f"ROUND({col}, 2) AS {col}"
        for col in numeric_columns
    ])
    
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      departamento,
      ano_idc,
      fecha_lectura,
      {numeric_cols_str}
    FROM `{source_table}`
    """
    
    execute_query(query, f"Redondear decimales para {table_type}")


def round_integers() -> None:
    """
    Paso 5 (para valor_ranking): Convierte todas las columnas numéricas a enteros.
    """
    source_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_fill_nulls_valor_ranking"
    target_table = f"{PROJECT_ID}.{DATASET_ID_SILVER}.idc_transformed_data_valor_ranking"
    
    # Obtener todas las columnas de la tabla fuente
    client = get_bq_client()
    try:
        table_ref = client.get_table(source_table)
    except Exception as e:
        raise RuntimeError(f"No se pudo acceder a la tabla fuente {source_table}: {e}")
    
    numeric_columns = [field.name for field in table_ref.schema if field.name not in ['departamento', 'ano_idc', 'fecha_lectura']]
    
    if not numeric_columns:
        raise RuntimeError(f"No se encontraron columnas numéricas en {source_table}")
    
    # Construir la query convirtiendo a enteros
    numeric_cols_str = ', '.join([
        f"CAST(ROUND({col}) AS INT64) AS {col}"
        for col in numeric_columns
    ])
    
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
      departamento,
      ano_idc,
      fecha_lectura,
      {numeric_cols_str}
    FROM `{source_table}`
    """
    
    execute_query(query, "Convertir a enteros para valor_ranking")


def transform_table(table_type: str) -> None:
    """
    Ejecuta todas las transformaciones para una tabla específica.
    
    Args:
        table_type: 'dato_original', 'valor_normalizado', o 'valor_ranking'
    """
    logger.info(f"[INICIO] Transformación completa para {table_type}")
    
    try:
        # Paso 1: Normalizar columnas
        normalize_columns(table_type)
        
        # Paso 2: Convertir a minúsculas
        lowercase_columns(table_type)
        
        # Paso 3: Normalizar departamento
        uppercase_departamento(table_type)
        
        # Paso 4: Reemplazar NULLs
        fill_nulls(table_type)
        
        # Paso 5: Redondear o convertir a enteros
        if table_type == 'valor_ranking':
            round_integers()
        else:
            round_decimals(table_type)
        
        logger.info(f"[COMPLETADO] Transformación completa para {table_type}")
    except Exception as e:
        logger.error(f"[ERROR] Error en transformación de {table_type}: {e}")
        raise

