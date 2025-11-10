from google.cloud import bigquery, storage
import pandas as pd
import os
import tempfile
import re

PROJECT_ID = "datagov-473122"
DATASET_ID = "gdv_ipm_sisben_raw"
SA_PATH = "/opt/airflow/include/sa.json"

DEBUG = True  # cambia a False cuando termines de verificar

# ---------------------------
# Utilidades
# ---------------------------
def _bq_client():
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
        existing = client.get_dataset(ds_id)
        if DEBUG:
            print(f"[OK] Dataset existente: {ds_id} ({existing.location})")
    except Exception:
        ds = bigquery.Dataset(ds_id)
        ds.location = "us-central1"
        ds.description = "RAW IPM SISBEN"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_id} ({ds.location})")

def _find_header_row_index(df):
    """Busca fila con 'cod mpio' y 'municipio' (case-insensitive)."""
    for i in range(min(20, len(df))):
        row_vals = [str(x).strip().lower() for x in df.iloc[i].tolist()]
        if any("cod mpio" in v for v in row_vals) and any("municipio" in v for v in row_vals):
            return i
    return 1 if len(df) > 1 else 0

def _to_int_safe(v):
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    s = s.replace(" ", "")
    if re.fullmatch(r"\d{1,3}(\.\d{3})+", s):
        return int(s.replace(".", ""))
    s2 = s.replace(".", "") if s.count(".") > 1 else s
    try:
        return int(float(s2))
    except:
        s3 = re.sub(r"[^\d]", "", s)
        return int(s3) if s3 else None

def _load_df_to_bq(df, table_name, schema, write_disposition="WRITE_TRUNCATE"):
    client = _bq_client()
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
    )
    job = client.load_table_from_dataframe(df, _table_ref(table_name), job_config=job_config)
    job.result()
    print(f"[OK] Subidas {len(df)} filas a {_table_ref(table_name)}")

# ---------------------------
# Helpers de depuración / normalización
# ---------------------------
def _explain_chars(s: str) -> str:
    return " ".join([f"{c}(U+{ord(c):04X})" for c in s])

_UNICODE_SPACES_RE = re.compile(r"[\u00A0\u1680\u2000-\u200B\u202F\u205F\u3000\uFEFF\u200E\u200F]")

def _normalize_cod_mpio(series: pd.Series) -> pd.Series:
    s = series.astype(str)
    s = s.apply(lambda x: _UNICODE_SPACES_RE.sub("", x))       # invisibles
    s = s.str.strip().str.replace(r"\s+", "", regex=True)      # espacios ASCII
    s = s.str.replace("’", "", regex=False).str.replace("'", "", regex=False).str.replace(",", "", regex=False)
    s = s.str.replace(r"^(\d{2})\.(\d{3})$", r"\1\2", regex=True)  # 76.892 -> 76892
    extracted = s.str.extract(r"(\d{5,})", expand=False).str[:5]
    extracted = extracted.where(extracted.str.fullmatch(r"\d{5}", na=False))
    return extracted

# ---------------------------
# Loader principal
# ---------------------------
def load_ipm_wide_from_gcs_excel(gcs_uri, table_name="ipm_sisben_wide"):
    """
    Carga Excel (hoja 0) y construye tabla wide:
      cod_mpio, Municipio, Total, IPM_Pobre, IPM_No_Pobre,
      I1_CON_PRIVACION..I15_SIN_PRIVACION
    """
    _ensure_dataset_exists(DATASET_ID)

    sc = _storage_client()
    bucket_name = gcs_uri.split("/")[2]
    blob_name = "/".join(gcs_uri.split("/")[3:])
    blob = sc.bucket(bucket_name).blob(blob_name)

    with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        local_path = tmp.name

    try:
        df_raw = pd.read_excel(local_path, sheet_name=0, header=None)
        hdr = _find_header_row_index(df_raw)

        # ⚠️ FIX CLAVE: resetear índice para alinear con 'out'
        df = df_raw.iloc[hdr+1:].copy()
        df.columns = [str(c).strip() for c in df_raw.iloc[hdr].tolist()]
        df = df.reset_index(drop=True)

        cols_lower = [c.lower() for c in df.columns]
        try:
            idx_cod = cols_lower.index("cod mpio")
            idx_mun = cols_lower.index("municipio")
        except ValueError:
            raise ValueError("No se encontraron columnas 'cod mpio' y/o 'Municipio'.")

        def _find_after(name_lc, start):
            for j in range(start+1, len(cols_lower)):
                if cols_lower[j] == name_lc:
                    return j
            raise ValueError(f"No se encontró la columna '{name_lc}' después de '{df.columns[start]}'.")

        idx_total    = _find_after("total", idx_mun)
        idx_pobre    = _find_after("pobre", idx_total)
        idx_no_pobre = _find_after("no pobre", idx_pobre)

        start_indicators = idx_no_pobre + 3  # saltar dos %

        if DEBUG:
            print("[DEBUG] Primeros valores crudos de 'cod mpio':")
            print(df.iloc[:8, idx_cod].apply(lambda x: repr(str(x))).to_string(index=False))

        out = pd.DataFrame(index=df.index)  # mismo índice que df
        out["cod_mpio"]  = _normalize_cod_mpio(df.iloc[:, idx_cod])
        out["Municipio"] = df.iloc[:, idx_mun].astype(str).str.strip()

        mask_valid = out["cod_mpio"].str.fullmatch(r"\d{5}", na=False)

        if DEBUG and (~mask_valid).any():
            bad = ~mask_valid
            diag = pd.DataFrame({
                "fila_excel":       [hdr + 1 + i for i in df.index[bad]],
                "cod_crudo":        df.loc[bad, df.columns[idx_cod]].astype(str),
                "cod_crudo_repr":   df.loc[bad, df.columns[idx_cod]].astype(str).map(repr),
                "cod_crudo_chars":  df.loc[bad, df.columns[idx_cod]].astype(str).map(_explain_chars),
                "cod_norm":         out.loc[bad, "cod_mpio"].astype(str),
                "cod_norm_repr":    out.loc[bad, "cod_mpio"].astype(str).map(repr),
                "cod_norm_chars":   out.loc[bad, "cod_mpio"].astype(str).map(_explain_chars),
                "Municipio":        out.loc[bad, "Municipio"],
            })
            print("[WARN] Filas descartadas por cod_mpio inválido (post-normalización):")
            print(diag.to_string(index=False))

        out = out[mask_valid].copy()

        out["Total"]        = df.iloc[:, idx_total].map(_to_int_safe)
        out["IPM_Pobre"]    = df.iloc[:, idx_pobre].map(_to_int_safe)
        out["IPM_No_Pobre"] = df.iloc[:, idx_no_pobre].map(_to_int_safe)

        pos = start_indicators
        for k in range(1, 16):
            out[f"I{k}_CON_PRIVACION"] = df.iloc[:, pos    ].map(_to_int_safe)
            out[f"I{k}_SIN_PRIVACION"] = df.iloc[:, pos + 1].map(_to_int_safe)
            pos += 4

        schema = [
            bigquery.SchemaField("cod_mpio", "STRING"),
            bigquery.SchemaField("Municipio", "STRING"),
            bigquery.SchemaField("Total", "INT64"),
            bigquery.SchemaField("IPM_Pobre", "INT64"),
            bigquery.SchemaField("IPM_No_Pobre", "INT64"),
        ]
        for k in range(1, 16):
            schema.append(bigquery.SchemaField(f"I{k}_CON_PRIVACION", "INT64"))
            schema.append(bigquery.SchemaField(f"I{k}_SIN_PRIVACION", "INT64"))

        _load_df_to_bq(out, table_name, schema, write_disposition="WRITE_TRUNCATE")
        print(f"Tabla creada/actualizada: {_table_ref(table_name)}")

    finally:
        try:
            os.unlink(local_path)
        except Exception:
            pass
