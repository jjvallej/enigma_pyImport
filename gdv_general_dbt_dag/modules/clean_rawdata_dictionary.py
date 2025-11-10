from google.cloud import bigquery
import os

PROJECT_ID = "datagov-473122"

SOURCE_DATASET_ID = "gdv_ipm_sisben_raw"
SOURCE_TABLE = "dictionary_wide"

# ✅ Dataset destino corregido
TARGET_DATASET_ID = "gdv_ipm_sisben_bronze"
TARGET_TABLE = "dictionary_clean"

DATASET_LOCATION = "us-central1"
SA_PATH = "/opt/airflow/include/sa.json"

def _bq_client() -> bigquery.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return bigquery.Client(project=PROJECT_ID)

def _ensure_dataset_exists(client: bigquery.Client, dataset_id: str, location: str = None):
    if location is None:
        location = DATASET_LOCATION
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    try:
        existing = client.get_dataset(ds_ref)
        if existing.location != location:
            print(f"[WARN] {dataset_id} está en {existing.location}, se recreará en {location}")
            client.delete_dataset(ds_ref, delete_contents=True, not_found_ok=True)
            ds_ref.location = location
            ds_ref.description = "Dataset bronze con datos limpios de IPM SISBEN (diccionario)"
            client.create_dataset(ds_ref)
            print(f"[OK] Dataset recreado: {dataset_id} ({location})")
        else:
            print(f"[OK] Dataset ya existe: {dataset_id} ({existing.location})")
    except Exception:
        ds_ref.location = location
        ds_ref.description = "Dataset bronze con datos limpios de IPM SISBEN (diccionario)"
        client.create_dataset(ds_ref)
        print(f"[OK] Dataset creado: {dataset_id} ({location})")

def _strip_diacritics_sql(expr: str) -> str:
    return f"REGEXP_REPLACE(NORMALIZE(TRIM(CAST({expr} AS STRING)), NFD), r'\\p{{M}}', '')"

def _build_clean_sql() -> str:
    src = f"`{PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE}`"
    tgt = f"`{PROJECT_ID}.{TARGET_DATASET_ID}.{TARGET_TABLE}`"
    return f"""
    CREATE OR REPLACE TABLE {tgt} AS
    SELECT
        {_strip_diacritics_sql('Columna')}     AS Columna,
        {_strip_diacritics_sql('Tipo_Valor')}  AS Tipo_Valor,
        {_strip_diacritics_sql('Valor')}       AS Valor,
        {_strip_diacritics_sql('Descripcion')} AS Descripcion
    FROM {src};
    """

def run_clean_dictionary_to_bronze():
    client = _bq_client()
    try:
        source_dataset = client.get_dataset(f"{PROJECT_ID}.{SOURCE_DATASET_ID}")
        print(f"[INFO] Dataset source encontrado: {SOURCE_DATASET_ID} en {source_dataset.location}")
    except Exception as e:
        raise ValueError(
            f"No se pudo acceder al dataset source {SOURCE_DATASET_ID}. "
            f"Verifica existencia y credenciales. Error: {e}"
        )
    
    # Asegurar que el dataset destino existe
    _ensure_dataset_exists(client, TARGET_DATASET_ID, location=DATASET_LOCATION)
    
    # Ejecutar query usando la ubicación del dataset destino
    sql = _build_clean_sql()
    print(f"[INFO] Ejecutando query para limpiar diccionario...")
    job = client.query(sql, location=DATASET_LOCATION)
    job.result()
    print(f"[OK] Bronze creado: {PROJECT_ID}.{TARGET_DATASET_ID}.{TARGET_TABLE} en {DATASET_LOCATION}")

if __name__ == "__main__":
    run_clean_dictionary_to_bronze()
