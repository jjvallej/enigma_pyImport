# modules/evaplan/evaplan_transform.py
"""
Módulo para transformar datos de Evaplan desde la capa bronze a la capa silver.

Lógica:
1. Obtiene los peri_idp únicos de las tablas bronze de la fecha actual
2. Para cada tabla silver, elimina registros con esos peri_idp
3. Copia los datos transformados de bronze a silver
"""
from google.cloud import bigquery
import os
import re
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import List, Set, Optional
from modules.config import PROJECT_ID, DATASET_ID_BRONZE, DATASET_ID_SILVER, DATASET_ID_GOLD, CONF, LOCATION
from modules.gcp_utils import get_bq_client

# === CONFIGURACIÓN ===
DEBUG = CONF.global_config.debug

def get_bronze_table_name(fuente: str) -> str:
    """
    Obtiene el nombre de la tabla bronze para una fuente desde config.yaml.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
    
    Returns:
        Nombre de la tabla bronze
    """
    try:
        table_name = getattr(CONF.evaplan.tables.bronze, fuente, None)
        if table_name:
            return table_name
    except AttributeError:
        pass
    
    raise ValueError(f"No se encontró el nombre de tabla bronze para la fuente '{fuente}' en config.yaml. Verifica evaplan.tables.bronze.{fuente}")

def get_silver_table_name(fuente: str) -> str:
    """
    Obtiene el nombre de la tabla silver para una fuente desde config.yaml.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
    
    Returns:
        Nombre de la tabla silver
    """
    try:
        table_name = getattr(CONF.evaplan.tables.silver, fuente, None)
        if table_name:
            return table_name
    except AttributeError:
        pass
    
    raise ValueError(f"No se encontró el nombre de tabla silver para la fuente '{fuente}' en config.yaml. Verifica evaplan.tables.silver.{fuente}")

# ---------------------------
# Clientes
# ---------------------------
def _bq_client() -> bigquery.Client:
    return get_bq_client()

# ---------------------------
# Helpers
# ---------------------------
def ensure_dataset(dataset_id: str, location: str = None):
    """Crea el dataset si no existe."""
    if location is None:
        location = LOCATION
    client = _bq_client()
    ds_fqn = f"{PROJECT_ID}.{dataset_id}"
    try:
        ds = client.get_dataset(ds_fqn)
        if DEBUG:
            print(f"[OK] Dataset existente: {ds_fqn} ({ds.location})")
    except Exception:
        ds = bigquery.Dataset(ds_fqn)
        ds.location = location
        ds.description = "Silver layer para datos transformados de Evaplan"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

def to_snake_case(name: str) -> str:
    """
    Convierte un nombre de columna a minúsculas y normaliza separadores.
    Solo convierte a minúsculas y reemplaza espacios/guiones con guiones bajos.
    NO inserta guiones bajos adicionales entre letras.
    
    Ejemplos:
    - "TEXTO_TEXTO" -> "texto_texto"
    - "Texto Texto" -> "texto_texto"
    - "texto-texto" -> "texto_texto"
    - "CODIGO_LINEA" -> "codigo_linea"
    - "CodigoLinea" -> "codigolinea" (mantiene sin guiones bajos si no los tiene)
    
    Args:
        name: Nombre de columna a convertir
    
    Returns:
        Nombre en minúsculas con guiones bajos normalizados
    """
    # Reemplazar espacios y guiones con guiones bajos
    name = re.sub(r'[\s\-]+', '_', name)
    
    # Convertir todo a minúsculas
    name = name.lower()
    
    # Limpiar guiones bajos múltiples
    name = re.sub(r'_+', '_', name)
    
    # Eliminar guiones bajos al inicio y final
    name = name.strip('_')
    
    return name

def table_exists(dataset_id: str, table_name: str) -> bool:
    """
    Verifica si una tabla existe en BigQuery.
    
    Args:
        dataset_id: ID del dataset
        table_name: Nombre de la tabla
    
    Returns:
        True si la tabla existe, False en caso contrario
    """
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    try:
        client.get_table(table_fqn)
        return True
    except Exception:
        return False

def get_peri_idps_from_bronze_table(fuente: str) -> Set[int]:
    """
    Obtiene los peri_idp únicos de una tabla bronze de la fecha actual.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
    
    Returns:
        Set de peri_idp únicos (puede estar vacío si no hay peri_idp o si es periodos)
    """
    client = _bq_client()
    try:
        bronze_table = get_bronze_table_name(fuente)
    except ValueError:
        if DEBUG:
            print(f"[WARN] Fuente no reconocida: {fuente}")
        return set()
    
    table_fqn = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.{bronze_table}"
    
    # Verificar que la tabla existe
    if not table_exists(DATASET_ID_BRONZE, bronze_table):
        if DEBUG:
            print(f"[INFO] Tabla bronze {bronze_table} no existe aún")
        return set()
    
    # Obtener fecha actual
    fecha_actual = datetime.now(timezone.utc).date().isoformat()
    
    # Para periodos, no hay peri_idp en los datos (o está dentro del objeto periodo)
    # Necesitamos verificar si la tabla tiene columna peri_idp
    try:
        table = client.get_table(table_fqn)
        has_peri_idp = any(field.name == "peri_idp" for field in table.schema)
        
        if not has_peri_idp:
            if DEBUG:
                print(f"[INFO] Tabla {bronze_table} no tiene columna peri_idp (puede ser periodos)")
            return set()
        
        # Query para obtener peri_idp únicos de la fecha actual
        query = f"""
        SELECT DISTINCT CAST(peri_idp AS INT64) as peri_idp
        FROM `{table_fqn}`
        WHERE DATE(fecha_lectura) = DATE('{fecha_actual}')
          AND peri_idp IS NOT NULL
        """
        
        query_job = client.query(query)
        results = query_job.result()
        
        peri_idps = {row.peri_idp for row in results if row.peri_idp is not None}
        
        if DEBUG:
            print(f"[INFO] Encontrados {len(peri_idps)} peri_idp únicos en {bronze_table} para fecha {fecha_actual}: {sorted(peri_idps)}")
        
        return peri_idps
    
    except Exception as e:
        if DEBUG:
            print(f"[ERROR] Error al obtener peri_idp de {bronze_table}: {e}")
        return set()

def get_all_peri_idps_from_bronze() -> Set[int]:
    """
    Obtiene todos los peri_idp únicos de todas las tablas bronze de la fecha actual.
    
    Returns:
        Set de todos los peri_idp únicos encontrados
    """
    all_peri_idps = set()
    
    # Obtener peri_idp de cada fuente (excepto periodos que puede no tenerlo)
    # Usar todas las fuentes del config, excluyendo periodos
    fuentes = [f for f in CONF.evaplan.fuentes if f != "periodos"]
    for fuente in fuentes:
        peri_idps = get_peri_idps_from_bronze_table(fuente)
        all_peri_idps.update(peri_idps)
    
    if DEBUG:
        print(f"[INFO] Total de peri_idp únicos encontrados en todas las tablas bronze: {sorted(all_peri_idps)}")
    
    return all_peri_idps

def delete_records_by_peri_idp(dataset_id: str, table_name: str, peri_idps: Set[int]):
    """
    Elimina TODOS los registros de una tabla silver que tengan los peri_idp especificados,
    sin importar la fecha de lectura. Esto asegura que solo se mantengan los datos más recientes.
    
    Args:
        dataset_id: ID del dataset
        table_name: Nombre de la tabla
        peri_idps: Set de peri_idp a eliminar (se eliminan TODOS los registros con estos peri_idp)
    """
    if not peri_idps:
        if DEBUG:
            print(f"[INFO] No hay peri_idp para eliminar de {table_name}")
        return
    
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"
    
    # Verificar que la tabla existe antes de intentar eliminar
    if not table_exists(dataset_id, table_name):
        if DEBUG:
            print(f"[INFO] Tabla {table_name} no existe, no hay registros que eliminar")
        return
    
    # Construir query para eliminar registros con los peri_idp especificados
    peri_idps_list = sorted(peri_idps)
    peri_idps_str = ", ".join(str(p) for p in peri_idps_list)
    
    query = f"""
    DELETE FROM `{table_fqn}`
    WHERE CAST(peri_idp AS INT64) IN ({peri_idps_str})
    """
    
    if DEBUG:
        print(f"[INFO] Eliminando registros con peri_idp {peri_idps_list} de la tabla {table_name}")
    
    job = client.query(query)
    job.result()
    
    if DEBUG:
        print(f"[OK] Registros eliminados exitosamente de {table_name}")

def copy_bronze_to_silver(fuente: str, peri_idps: Optional[Set[int]] = None):
    """
    Copia datos de una tabla bronze a su tabla silver correspondiente.
    Transforma los nombres de columnas a snake_case (minúsculas con guiones bajos).
    Si se proporcionan peri_idps, solo copia registros con esos peri_idp.
    Si no se proporcionan, copia todos los registros de la fecha actual.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
        peri_idps: Set opcional de peri_idp a copiar (si None, copia todos de la fecha actual)
    """
    client = _bq_client()
    bronze_table = get_bronze_table_name(fuente)
    silver_table = get_silver_table_name(fuente)
    
    bronze_fqn = f"{PROJECT_ID}.{DATASET_ID_BRONZE}.{bronze_table}"
    silver_fqn = f"{PROJECT_ID}.{DATASET_ID_SILVER}.{silver_table}"
    
    # Verificar que la tabla bronze existe
    if not table_exists(DATASET_ID_BRONZE, bronze_table):
        if DEBUG:
            print(f"[WARN] Tabla bronze {bronze_table} no existe, no hay datos para copiar")
        return
    
    # Obtener fecha actual
    fecha_actual = datetime.now(timezone.utc).date().isoformat()
    
    # Construir query SELECT según si hay peri_idp o no
    if fuente == "periodos":
        # Para periodos, no filtramos por peri_idp
        select_query = f"""
        SELECT *
        FROM `{bronze_fqn}`
        WHERE DATE(fecha_lectura) = DATE('{fecha_actual}')
        """
    elif peri_idps:
        # Filtrar por peri_idp específicos
        peri_idps_list = sorted(peri_idps)
        peri_idps_str = ", ".join(str(p) for p in peri_idps_list)
        select_query = f"""
        SELECT *
        FROM `{bronze_fqn}`
        WHERE DATE(fecha_lectura) = DATE('{fecha_actual}')
          AND CAST(peri_idp AS INT64) IN ({peri_idps_str})
        """
    else:
        # Copiar todos los registros de la fecha actual
        select_query = f"""
        SELECT *
        FROM `{bronze_fqn}`
        WHERE DATE(fecha_lectura) = DATE('{fecha_actual}')
        """
    
    if DEBUG:
        print(f"[INFO] Leyendo datos de {bronze_table}...")
        if peri_idps:
            print(f"[INFO] Filtrando por peri_idp: {sorted(peri_idps)}")
    
    # Leer datos de bronze usando pandas
    df = client.query(select_query).to_dataframe()
    
    if df.empty:
        if DEBUG:
            print(f"[WARN] No hay datos para copiar de {bronze_table}")
        return
    
    if DEBUG:
        print(f"[INFO] Datos leídos: {len(df)} filas, {len(df.columns)} columnas")
        print(f"[DEBUG] Columnas originales: {list(df.columns)}")
    
    # Transformar nombres de columnas a snake_case
    column_mapping = {col: to_snake_case(col) for col in df.columns}
    df = df.rename(columns=column_mapping)
    
    if DEBUG:
        print(f"[INFO] Nombres de columnas transformados a snake_case")
        print(f"[DEBUG] Columnas transformadas: {list(df.columns)}")
        print(f"[DEBUG] Mapeo: {column_mapping}")
    
    # Reemplazar NULL, NaN y valores vacíos
    # Para columnas numéricas: reemplazar con 0
    # Para columnas string: reemplazar con "0"
    # Excluir columnas de fecha/hora y booleanas
    excluded_columns = ['fecha_lectura', 'fecha_cierre', 'fecha_apertura']  # Columnas que no deben modificarse
    
    for col in df.columns:
        # Saltar columnas de fecha/hora y booleanas
        if col.lower() in excluded_columns or 'fecha' in col.lower() or 'date' in col.lower() or 'time' in col.lower():
            continue
        
        dtype = df[col].dtype
        
        # Identificar si es numérico (int, float) o string
        is_numeric = pd.api.types.is_numeric_dtype(dtype)
        is_datetime = pd.api.types.is_datetime64_any_dtype(dtype)
        is_bool = pd.api.types.is_bool_dtype(dtype)
        
        # Saltar columnas de fecha/hora y booleanas
        if is_datetime or is_bool:
            continue
        
        if is_numeric:
            # Para columnas numéricas: reemplazar NaN, None, y valores vacíos con 0
            df[col] = df[col].fillna(0)
            # También manejar casos donde puede haber strings que representan números
            if df[col].dtype == 'object':
                # Convertir a numérico, los que no se puedan convertir se convierten en NaN y luego en 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
        else:
            # Para columnas string: reemplazar NaN, None, y valores vacíos con "0"
            # Primero convertir a string para manejar todos los casos
            df[col] = df[col].astype(str)
            # Reemplazar valores que representan NULL/NaN/vacío
            df[col] = df[col].replace(['nan', 'None', 'NaN', 'null', 'NULL', '<NA>', 'NaT', ''], "0")
            # También reemplazar strings que son solo espacios
            df[col] = df[col].str.strip()
            df[col] = df[col].replace('', "0")
    
    if DEBUG:
        print(f"[INFO] Valores NULL, NaN y vacíos reemplazados: 0 para numéricos, '0' para strings (excluyendo fechas y booleanos)")
    
    # Verificar si la tabla silver existe
    silver_exists = table_exists(DATASET_ID_SILVER, silver_table)
    
    # Obtener esquema de BigQuery para la tabla silver
    if silver_exists:
        # Obtener esquema existente
        silver_table_obj = client.get_table(silver_fqn)
        silver_schema = {field.name: field.field_type for field in silver_table_obj.schema}
        
        # Verificar que las columnas del DataFrame coincidan con el esquema
        df_columns = set(df.columns)
        silver_columns = set(silver_schema.keys())
        
        if df_columns != silver_columns:
            if DEBUG:
                print(f"[WARN] Columnas diferentes. DataFrame: {df_columns}, Silver: {silver_columns}")
                print(f"[INFO] Usando solo columnas comunes...")
            
            # Usar solo columnas comunes
            common_columns = df_columns.intersection(silver_columns)
            if not common_columns:
                raise ValueError(f"No hay columnas comunes entre DataFrame y {silver_table}")
            
            df = df[list(common_columns)]
    
    # Generar esquema de BigQuery a partir del DataFrame
    schema = []
    for col in df.columns:
        dtype = df[col].dtype
        
        if dtype == 'datetime64[ns]' or 'datetime' in str(dtype):
            bq_type = "TIMESTAMP"
        elif dtype == 'Int64' or dtype == 'int64':
            bq_type = "INT64"
        elif dtype == 'float64':
            bq_type = "FLOAT64"
        elif dtype == 'bool':
            bq_type = "BOOL"
        else:
            bq_type = "STRING"
        
        schema.append(bigquery.SchemaField(col, bq_type))
    
    # Configurar job de carga
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND if silver_exists else bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    
    if DEBUG:
        print(f"[INFO] {'Insertando' if silver_exists else 'Creando tabla y cargando'} datos en {silver_table}...")
    
    # Cargar DataFrame a BigQuery
    job = client.load_table_from_dataframe(df, silver_fqn, job_config=job_config)
    job.result()
    
    if DEBUG:
        rows_loaded = job.output_rows if hasattr(job, 'output_rows') else None
        if rows_loaded is not None:
            print(f"[OK] {rows_loaded} filas {'insertadas' if silver_exists else 'cargadas'} en {silver_table}")
        else:
            print(f"[OK] Datos copiados exitosamente a {silver_table}")

def transform_fuente_to_silver(fuente: str, peri_idps: Optional[Set[int]] = None):
    """
    Transforma una fuente completa de bronze a silver:
    1. Elimina TODOS los registros existentes con los peri_idp especificados (sin importar la fecha)
    2. Copia nuevos datos de bronze a silver
    
    IMPORTANTE: Esta función elimina TODOS los registros con los peri_idp especificados,
    sin importar la fecha de lectura. Esto asegura que Silver solo mantenga los datos
    más recientes (última ejecución), mientras que el histórico completo se mantiene en Bronze.
    
    Args:
        fuente: Nombre de la fuente (periodos, avance_mr, etc.)
        peri_idps: Set opcional de peri_idp a procesar (si None, obtiene de bronze)
    """
    silver_table = get_silver_table_name(fuente)
    
    # Si no se proporcionan peri_idps, obtenerlos de bronze
    if peri_idps is None and fuente != "periodos":
        peri_idps = get_peri_idps_from_bronze_table(fuente)
    
    # Eliminar TODOS los registros existentes (sin importar la fecha)
    # Esto asegura que Silver solo tenga los datos más recientes
    if fuente == "periodos":
        # Para periodos, eliminar TODOS los registros (sin importar la fecha)
        # para mantener solo los datos más recientes
        # BigQuery requiere una condición WHERE, usamos WHERE TRUE para eliminar todo
        client = _bq_client()
        table_fqn = f"{PROJECT_ID}.{DATASET_ID_SILVER}.{silver_table}"
        
        if table_exists(DATASET_ID_SILVER, silver_table):
            delete_query = f"""
            DELETE FROM `{table_fqn}`
            WHERE TRUE
            """
            
            if DEBUG:
                print(f"[INFO] Eliminando TODOS los registros de la tabla {silver_table} (sin importar la fecha)")
            
            job = client.query(delete_query)
            job.result()
            
            if DEBUG:
                print(f"[OK] Todos los registros eliminados de {silver_table}")
    elif fuente != "periodos" and peri_idps:
        if DEBUG:
            print(f"[INFO] Eliminando TODOS los registros con peri_idp {sorted(peri_idps)} de {silver_table} (sin importar la fecha)")
        delete_records_by_peri_idp(DATASET_ID_SILVER, silver_table, peri_idps)
    
    # Copiar datos de bronze a silver
    copy_bronze_to_silver(fuente, peri_idps)

