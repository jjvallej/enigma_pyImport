# modules/ipm_transform.py
"""
Módulo para transformar datos del IPM desde bronze a silver y gold.
Contiene funciones auxiliares para la gestión de datasets en BigQuery.
"""
from google.cloud import bigquery
from modules.config import PROJECT_ID, CONF, LOCATION
from modules.gcp_utils import get_bq_client

DEBUG = CONF.global_config.debug

# ---------------------------
# Helpers
# ---------------------------
def ensure_dataset(dataset_id: str, location: str = None):
    """
    Crea el dataset en BigQuery si no existe.
    
    Args:
        dataset_id: ID del dataset (sin el project_id)
        location: Ubicación del dataset (si es None, usa LOCATION de config.yaml)
    """
    if location is None:
        location = LOCATION
    client = get_bq_client()
    ds_fqn = f"{PROJECT_ID}.{dataset_id}"
    try:
        ds = client.get_dataset(ds_fqn)
        if DEBUG:
            print(f"[OK] Dataset existente: {ds_fqn} ({ds.location})")
    except Exception:
        ds = bigquery.Dataset(ds_fqn)
        ds.location = location
        # Actualizar descripción según el dataset
        if "silver" in dataset_id:
            ds.description = "Silver layer para IPM v2"
        elif "gold" in dataset_id:
            ds.description = "Gold layer para IPM v2"
        else:
            ds.description = "Dataset para IPM v2"
        client.create_dataset(ds)
        print(f"[OK] Dataset creado: {ds_fqn} ({location})")

