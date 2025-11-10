# modules/clean.py
from google.cloud import bigquery
import os

# ---------------------------
# Config
# ---------------------------
PROJECT_ID = "datagov-473122"

SOURCE_DATASET_ID = "gdv_ipm_sisben_raw"
SOURCE_TABLE = "ipm_sisben_wide"          # tabla "ancha" creada en RAW

TARGET_DATASET_ID = "gdv_ipm_sisben_bronze"
TARGET_TABLE = "ipm_sisben_clean"         # tabla limpia sin tildes

# Ubicación del dataset (debe coincidir con la ubicación del dataset source)
DATASET_LOCATION = "us-central1"

SA_PATH = "/opt/airflow/include/sa.json"


# ---------------------------
# Utils
# ---------------------------
def _bq_client() -> bigquery.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return bigquery.Client(project=PROJECT_ID)


def _ensure_dataset_exists(client: bigquery.Client, dataset_id: str, location: str = None):
    """Crea el dataset si no existe. Si existe en otra ubicación, lo elimina y recrea."""
    if location is None:
        location = DATASET_LOCATION
    
    ds_ref = bigquery.Dataset(f"{PROJECT_ID}.{dataset_id}")
    try:
        existing_dataset = client.get_dataset(ds_ref)
        existing_location = existing_dataset.location
        
        # Si el dataset existe en una ubicación diferente, eliminarlo y recrearlo
        if existing_location != location:
            print(f"[WARN] Dataset {dataset_id} existe en ubicación {existing_location}, pero se requiere {location}.")
            print(f"[INFO] Eliminando dataset {dataset_id} para recrearlo en la ubicación correcta...")
            client.delete_dataset(ds_ref, delete_contents=True, not_found_ok=True)
            # Recrear en la ubicación correcta
            ds_ref.location = location
            ds_ref.description = "Dataset bronze con datos limpios de IPM SISBEN"
            client.create_dataset(ds_ref)
            print(f"[INFO] Dataset {dataset_id} recreado en ubicación {location}.")
        else:
            print(f"[INFO] Dataset {dataset_id} ya existe en ubicación {existing_location}.")
        
        return location
    except Exception:
        # Dataset no existe, crearlo
        ds_ref.location = location
        ds_ref.description = "Dataset bronze con datos limpios de IPM SISBEN"
        client.create_dataset(ds_ref)
        print(f"[INFO] Dataset {dataset_id} creado en ubicación {location}.")
        return location


def _num_clean(expr: str) -> str:
    """
    Genera SQL para limpiar y castear a INT64:
      - Convierte a STRING
      - Quita todo lo que NO sea dígito (., espacios, %) -> solo números
      - Si queda vacío -> NULL
      - SAFE_CAST a INT64 (evita fallar si algo no se puede convertir)
    """
    return (
        f"SAFE_CAST(NULLIF(REGEXP_REPLACE(CAST({expr} AS STRING), r'[^0-9]', ''), '') AS INT64)"
    )


# ---------------------------
# Build SQL
# ---------------------------
def _build_select_sql() -> str:
    """
    Construye el SELECT con:
      - cod_mpio y Municipio (limpios, sin tildes, mayúsculas)
      - Total, IPM_Pobre, IPM_No_Pobre
      - I1..I15 (CON/SIN)
    """
    base_cols = [
        # cod_mpio - usar cod_mpio directamente (la tabla se crea con este nombre)
        "CAST(TRIM(COALESCE(cod_mpio, '')) AS STRING) AS cod_mpio",
        # Municipio - quitar tildes (normalizar a NFD y eliminar diacríticos) y convertir a mayúsculas
        "UPPER(REGEXP_REPLACE(NORMALIZE(TRIM(COALESCE(Municipio, municipio, '')), NFD), r'\\p{M}', '')) AS Municipio",
        # Totales IPM
        f"{_num_clean('Total')} AS Total",
        f"{_num_clean('IPM_Pobre')} AS IPM_Pobre",
        f"{_num_clean('IPM_No_Pobre')} AS IPM_No_Pobre",
    ]

    # Indicadores I1..I15 (CON/SIN)
    ind_cols = []
    for k in range(1, 16):
        ind_cols.append(f"{_num_clean(f'I{k}_CON_PRIVACION')} AS I{k}_CON_PRIVACION")
        ind_cols.append(f"{_num_clean(f'I{k}_SIN_PRIVACION')} AS I{k}_SIN_PRIVACION")

    return ",\n        ".join(base_cols + ind_cols)


def _build_clean_sql() -> str:
    select_sql = _build_select_sql()
    src = f"`{PROJECT_ID}.{SOURCE_DATASET_ID}.{SOURCE_TABLE}`"
    tgt = f"`{PROJECT_ID}.{TARGET_DATASET_ID}.{TARGET_TABLE}`"

    return f"""
    CREATE OR REPLACE TABLE {tgt} AS
    SELECT
        {select_sql}
    FROM {src}
    WHERE
        TRIM(COALESCE(cod_mpio, '')) != ''
        AND TRIM(COALESCE(Municipio, municipio, '')) != '';
    """


# ---------------------------
# Public entrypoint
# ---------------------------
def run_clean_to_bronze():
    """
    - Garantiza dataset BRONZE en us-central1
    - Crea/Reemplaza la tabla ipm_sisben_clean con datos limpios (sin tildes)
    """
    client = _bq_client()
    
    # Verificar que el dataset source existe
    try:
        source_dataset = client.get_dataset(f"{PROJECT_ID}.{SOURCE_DATASET_ID}")
        source_location = source_dataset.location
        print(f"[INFO] Dataset source encontrado en ubicación: {source_location}")
    except Exception as e:
        raise ValueError(
            f"No se pudo acceder al dataset source {SOURCE_DATASET_ID}. "
            f"Asegúrate de que existe y que las credenciales son correctas. Error: {e}"
        )
    
    # Crear dataset bronze SIEMPRE en us-central1 (independientemente de dónde esté el source)
    bronze_location = DATASET_LOCATION  # us-central1
    print(f"[INFO] Creando dataset bronze en ubicación: {bronze_location}")
    _ensure_dataset_exists(client, TARGET_DATASET_ID, location=bronze_location)

    sql = _build_clean_sql()
    
    # Ejecutar query: usar la ubicación del dataset source para leer los datos
    # (el query se ejecuta donde está el dataset que se está consultando)
    job = client.query(sql, location=source_location)
    job.result()
    print(
        f"[OK] Bronze creado: {PROJECT_ID}.{TARGET_DATASET_ID}.{TARGET_TABLE} en {bronze_location}"
    )


# Permite ejecución directa para pruebas locales en el contenedor del scheduler/webserver
if __name__ == "__main__":
    run_clean_to_bronze()
