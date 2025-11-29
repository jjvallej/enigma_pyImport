from google.cloud import storage, bigquery
from .config import PROJECT_ID

def get_gcs_client() -> storage.Client:
    """
    Creates a Google Cloud Storage client.
    Authentication is handled automatically by the environment (Composer SA or GOOGLE_APPLICATION_CREDENTIALS).
    """
    return storage.Client(project=PROJECT_ID)

def get_bq_client() -> bigquery.Client:
    """
    Creates a BigQuery client.
    Authentication is handled automatically by the environment.
    """
    return bigquery.Client(project=PROJECT_ID)
