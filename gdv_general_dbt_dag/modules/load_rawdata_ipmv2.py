# dags/modules/rawdata_ipmv2.py
from google.cloud import bigquery, storage
import pandas as pd
import os, re, tempfile
from datetime import datetime, timezone
from typing import Iterable, Optional

PROJECT_ID = "datagov-473122"
SA_PATH = "/opt/airflow/include/sa.json"
DEBUG = True  # ponlo en False cuando ya no necesites logs

# ---------------------------
# Clientes
# ---------------------------
def _bq_client() -> bigquery.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return bigquery.Client(project=PROJECT_ID)

def _gcs_client() -> storage.Client:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    return storage.Client(project=PROJECT_ID)

# ---------------------------
# Helpers
# ---------------------------
def ensure_dataset(dataset_id: str, location: str = "us-central1"):
    """Crea el dataset si no existe."""
    client = _bq_client()
    ds_fqn = f"{PROJECT_ID}.{dataset_id}"
    try:
        ds = client.get_dataset(ds_fqn)
        if DEBUG:
            print(f"[OK] Dataset existente: {ds_fqn} ({ds.location})")
    except Exception:
        ds = bigquery.Dataset(ds_fqn)
        ds.location = location
        ds.description = "Bronze layer para IPM v2"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

_UNICODE_SPACES_RE = re.compile(r"[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000\uFEFF\u200E\u200F]")


def download_excel_from_gcs(gcs_uri: str) -> str:
    """
    Descarga el archivo Excel desde GCS hacia un archivo temporal
    y devuelve la ruta local generada.
    """
    gcs = _gcs_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = gcs.bucket(bucket_name).blob(blob_name)

    fd, tmp_path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    blob.download_to_filename(tmp_path)
    if DEBUG:
        print(f"[DEBUG] Archivo descargado en {tmp_path}")
    return tmp_path


def transform_excel(local_path: str, sheet_index: int = 0) -> pd.DataFrame:
    """
    Aplica las transformaciones esperadas al Excel de IPM.
    Devuelve un DataFrame listo para cargarse a BigQuery.
    """
    df = pd.read_excel(local_path, sheet_name=sheet_index, header=0)

    # (2) quitar primera fila (suele tener totales o una fila guía)
    if len(df) > 0:
        df = df.iloc[1:, :].reset_index(drop=True)

    # Normaliza nombres actuales para inspección opcional
    if DEBUG:
        print("[DEBUG] Encabezados originales:", list(df.columns))

    # (3) renombrado siguiendo el orden esperado:
    expected_cols = ["cod_mpio", "Municipio", "Total",
                     "IPM_Pobre_Abs", "IPM_No_Pobre_Abs", "IPM_Pobre_Porc", "IPM_No_Pobre_Porc"]
    for k in range(1, 16):
        expected_cols += [
            f"I{k}_Con_Privacion_Abs",
            f"I{k}_Sin_Privacion_Abs",
            f"I{k}_Con_Privacion_Porc",
            f"I{k}_Sin_Privacion_Porc",
        ]

    n_expected = len(expected_cols)
    n_actual = df.shape[1]
    if n_actual < n_expected:
        raise ValueError(
            f"El archivo trae {n_actual} columnas, pero se esperaban al menos {n_expected} "
            f"para mapear todos los indicadores con _Abs/_Porc."
        )
    if n_actual > n_expected and DEBUG:
        print(f"[WARN] El archivo tiene {n_actual} columnas; se tomarán las primeras {n_expected}.")

    df = df.iloc[:, :n_expected].copy()
    df.columns = expected_cols

    # Limpiezas/Tipos
    df["cod_mpio"] = _normalize_cod_mpio(df["cod_mpio"])
    df = df[df["cod_mpio"].notna()].copy()
    df["Municipio"] = df["Municipio"].astype(str).str.strip()

    # Numéricos
    df["Total"] = df["Total"].map(_to_int_safe)
    df["IPM_Pobre_Abs"] = df["IPM_Pobre_Abs"].map(_to_int_safe)
    df["IPM_No_Pobre_Abs"] = df["IPM_No_Pobre_Abs"].map(_to_int_safe)

    # Porcentajes: deja float (no int)
    df["IPM_Pobre_Porc"] = pd.to_numeric(df["IPM_Pobre_Porc"], errors="coerce")
    df["IPM_No_Pobre_Porc"] = pd.to_numeric(df["IPM_No_Pobre_Porc"], errors="coerce")

    for k in range(1, 16):
        df[f"I{k}_Con_Privacion_Abs"] = df[f"I{k}_Con_Privacion_Abs"].map(_to_int_safe)
        df[f"I{k}_Sin_Privacion_Abs"] = df[f"I{k}_Sin_Privacion_Abs"].map(_to_int_safe)
        df[f"I{k}_Con_Privacion_Porc"] = pd.to_numeric(df[f"I{k}_Con_Privacion_Porc"], errors="coerce")
        df[f"I{k}_Sin_Privacion_Porc"] = pd.to_numeric(df[f"I{k}_Sin_Privacion_Porc"], errors="coerce")

    # (4) agregar timestamp de lectura (UTC)
    df["fecha_lectura"] = datetime.now(timezone.utc)
    return df

def _normalize_cod_mpio(series: pd.Series) -> pd.Series:
    """Limpia y deja el código DANE de 5 dígitos."""
    s = series.astype(str)
    s = s.apply(lambda x: _UNICODE_SPACES_RE.sub("", x))  # espacios invisibles
    s = s.str.strip().str.replace(r"\s+", "", regex=True)
    s = s.str.replace("’", "", regex=False).str.replace("'", "", regex=False).str.replace(",", "", regex=False)
    s = s.str.replace(r"^(\d{2})\.(\d{3})$", r"\1\2", regex=True)  # 76.892 -> 76892
    s = s.str.extract(r"(\d{5,})", expand=False).str[:5]
    return s.where(s.str.fullmatch(r"\d{5}", na=False))

def _to_int_safe(v):
    """Convierte '1.178.409', '13.00', '  2 345 ' -> int; vacíos -> None."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = s.replace(" ", "")
    # miles con puntos
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        return int(s.replace(".", ""))
    # 1.234.00 o 13.00
    s2 = s.replace(".", "") if s.count(".") > 1 else s
    try:
        return int(float(s2))
    except Exception:
        only_digits = re.sub(r"[^\d]", "", s)
        return int(only_digits) if only_digits else None

def _load_df_to_bq(df: pd.DataFrame, dataset_id: str, table_name: str):
    client = _bq_client()
    table_fqn = f"{PROJECT_ID}.{dataset_id}.{table_name}"

    # Esquema: 2 identificadores + Total + 4 de IPM + 60 (15*4) + fecha_lectura
    schema = [
        bigquery.SchemaField("cod_mpio", "STRING"),
        bigquery.SchemaField("Municipio", "STRING"),
        bigquery.SchemaField("Total", "INT64"),
        bigquery.SchemaField("IPM_Pobre_Abs", "INT64"),
        bigquery.SchemaField("IPM_No_Pobre_Abs", "INT64"),
        bigquery.SchemaField("IPM_Pobre_Porc", "FLOAT"),
        bigquery.SchemaField("IPM_No_Pobre_Porc", "FLOAT"),
    ]
    for k in range(1, 16):
        schema += [
            bigquery.SchemaField(f"I{k}_Con_Privacion_Abs", "INT64"),
            bigquery.SchemaField(f"I{k}_Sin_Privacion_Abs", "INT64"),
            bigquery.SchemaField(f"I{k}_Con_Privacion_Porc", "FLOAT"),
            bigquery.SchemaField(f"I{k}_Sin_Privacion_Porc", "FLOAT"),
        ]
    schema.append(bigquery.SchemaField("fecha_lectura", "TIMESTAMP"))

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, table_fqn, job_config=job_config)
    job.result()
    print(f"[OK] Cargadas {len(df)} filas en {table_fqn}")


def load_dataframe_to_bq(df: pd.DataFrame, dataset_id: str, table_name: str):
    """Función pública para cargar un DataFrame transformado."""
    _load_df_to_bq(df, dataset_id=dataset_id, table_name=table_name)


def cleanup_temp_paths(paths: Iterable[Optional[str]]):
    """Elimina los archivos temporales indicados (ignora None o paths vacíos)."""
    for path in paths:
        if not path:
            continue
        try:
            os.unlink(path)
            if DEBUG:
                print(f"[DEBUG] Archivo temporal eliminado: {path}")
        except Exception as exc:
            print(f"[WARN] No se pudo eliminar {path}: {exc}")

# ---------------------------
# Core ETL
# ---------------------------
def process_and_load_from_gcs(gcs_uri: str, dataset_id: str, table_name: str, sheet_index: int = 0):
    """
    Paso 0: asegura dataset (se supone ya ejecutado por el DAG).
    Paso 1: lee Excel desde GCS.
    Paso 2: quita la primera fila.
    Paso 3: renombra columnas a *_Abs / *_Porc con patrón fijo.
    Paso 4: agrega columna TIMESTAMP 'fecha_lectura' (UTC).
    Paso 5: guarda en BigQuery (WRITE_TRUNCATE) => dataset.table.
    """
    local_path = download_excel_from_gcs(gcs_uri)
    try:
        df = transform_excel(local_path, sheet_index=sheet_index)
        _load_df_to_bq(df, dataset_id=dataset_id, table_name=table_name)
    finally:
        cleanup_temp_paths([local_path])
