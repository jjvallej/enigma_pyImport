# modules/loadDictionary.py
from google.cloud import bigquery, storage
import pandas as pd
import os
import tempfile

PROJECT_ID   = "datagov-473122"
DATASET_ID   = "gdv_ipm_sisben_raw"
TARGET_TABLE = "dictionary_wide"
SA_PATH      = "/opt/airflow/include/sa.json"

# ---------------------------
# Utilidades
# ---------------------------
def _bq_client():
    print("test")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return bigquery.Client(project=PROJECT_ID)

def _storage_client():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return storage.Client(project=PROJECT_ID)

def _table_ref(table):
    return f"{PROJECT_ID}.{DATASET_ID}.{table}"

def _ensure_dataset_exists(dataset_id):
    client = _bq_client()
    ds_id = f"{PROJECT_ID}.{dataset_id}"
    try:
        client.get_dataset(ds_id)
        print(f"[OK] Dataset existente: {ds_id}")
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = "us-central1"  # consistente con otros datasets
        ds.description = "RAW IPM SISBEN"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_id} ({ds.location})")

def _find_header_row_index(df):
    """
    Encuentra la fila de encabezados buscando 'Columna' y 'Tipo Valor' (case-insensitive)
    en las primeras ~15 filas.
    """
    for i in range(min(15, len(df))):
        row_vals = [str(x).strip().lower() for x in df.iloc[i].tolist()]
        if "columna" in row_vals and ("tipo valor" in row_vals or "tipo_valor" in row_vals or "tipo" in row_vals):
            return i
    # fallback si no la encuentra
    return 0

def _load_df_to_bq(df, table_name, schema, write_disposition="WRITE_TRUNCATE"):
    client = _bq_client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, _table_ref(table_name), job_config=job_config)
    job.result()
    print(f"[OK] Cargadas {len(df)} filas en {_table_ref(table_name)}")

# ---------------------------
# Loader principal (wide table)
# ---------------------------
def load_dictionary_from_gcs_excel(gcs_uri, sheet_name="DATOS", table_name=TARGET_TABLE):
    """
    Lee el Excel '1_Estructura de datos IMP.xlsx' desde GCS (hoja por defecto 'DATOS')
    y construye una tabla 'ancha' con columnas:
      Columna (STRING), Tipo_Valor (STRING), Valor (STRING), Descripcion (STRING)
    """
    # asegurar dataset RAW
    _ensure_dataset_exists(DATASET_ID)

    # ---- descarga
    sc = _storage_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    
    print(f"[INFO] Intentando descargar: gs://{bucket_name}/{blob_name}")
    
    # Verificar que el blob existe
    bucket = sc.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    if not blob.exists():
        # Intentar listar archivos en el bucket para ayudar al usuario
        print(f"[ERROR] El archivo no existe en: gs://{bucket_name}/{blob_name}")
        print(f"[INFO] Listando archivos en el bucket '{bucket_name}'...")
        try:
            blobs = list(bucket.list_blobs(prefix="/".join(blob_name.split("/")[:-1]) + "/" if "/" in blob_name else ""))
            if blobs:
                print(f"[INFO] Archivos encontrados en el bucket:")
                for b in blobs[:10]:  # Mostrar solo los primeros 10
                    print(f"  - {b.name}")
            else:
                print(f"[INFO] No se encontraron archivos en el prefijo especificado")
        except Exception as e:
            print(f"[WARN] No se pudo listar archivos del bucket: {e}")
        raise FileNotFoundError(f"El archivo no existe en GCS: gs://{bucket_name}/{blob_name}")

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        local_path = tmp.name
        print(f"[OK] Archivo descargado temporalmente a: {local_path}")

    try:
        # ---- leer y ubicar header real
        df_raw = pd.read_excel(local_path, sheet_name=sheet_name, header=None)
        hdr = _find_header_row_index(df_raw)

        df = df_raw.iloc[hdr+1:].copy()
        headers = [str(c).strip() for c in df_raw.iloc[hdr].tolist()]

        # Construir mapa de nombres (lower) -> índice
        name_map = {h.lower(): idx for idx, h in enumerate(headers)}

        def _pick(name_options):
            """Devuelve la Serie de la primera columna que exista en name_map o None."""
            for opt in name_options:
                if opt in name_map:
                    return df_raw.iloc[hdr+1:, name_map[opt]]
            return None

        s_columna = _pick(["columna"])
        s_tipo    = _pick(["tipo valor", "tipo_valor", "tipo"])
        s_valor   = _pick(["valor"])  # puede no existir en algunos archivos
        s_desc    = _pick(["descripción", "descripcion", "description"])

        # Construimos el DataFrame de salida
        out = pd.DataFrame({
            "Columna":    (s_columna if s_columna is not None else "").astype(str).str.strip(),
            "Tipo_Valor": (s_tipo    if s_tipo    is not None else "").astype(str).str.strip(),
            "Valor":      (s_valor   if s_valor   is not None else pd.Series([None]*len(df))).astype(object),
            "Descripcion":(s_desc    if s_desc    is not None else "").astype(str).str.strip(),
        })

        # limpiar filas totalmente vacías
        mask = out["Columna"].astype(str).str.strip().ne("") | \
               out["Tipo_Valor"].astype(str).str.strip().ne("") | \
               out["Descripcion"].astype(str).str.strip().ne("") | \
               out["Valor"].astype(str).fillna("").str.strip().ne("")
        out = out[mask].copy()

        # ----- esquema BQ
        schema = [
            bigquery.SchemaField("Columna", "STRING"),
            bigquery.SchemaField("Tipo_Valor", "STRING"),
            bigquery.SchemaField("Valor", "STRING"),
            bigquery.SchemaField("Descripcion", "STRING"),
        ]

        # cargar
        _load_df_to_bq(out, table_name, schema, write_disposition="WRITE_TRUNCATE")
        print(f"[OK] Tabla creada/actualizada: {_table_ref(table_name)}")

    finally:
        try:
            os.unlink(local_path)
        except Exception:
            pass
