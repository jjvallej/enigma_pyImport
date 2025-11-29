import os
import sys

# Ensure project root and modules dir are in sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)

if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

try:
    import config_loader
    sources_config = config_loader.config
except ImportError:
    from modules.config_loader import config as sources_config

# Exponer la configuración de fuentes para uso global
CONF = sources_config

# Environment: 'dev', 'prod', or 'local'
# Default to 'dev' for safety
ENV = os.getenv("ENVIRONMENT", "dev")

# GCP Configuration
PROJECT_ID = os.getenv("GCP_PROJECT", "datagov-473122")
LOCATION = os.getenv("GCP_LOCATION", "us-central1")

# Environment Specific Configurations
# Load configurations for each environment from YAML
# CONF.environments is a SimpleNamespace, convert to dict for easier lookup if needed,
# or access directly. Since we need to select based on ENV string, getattr is useful.

try:
    current_config = getattr(CONF.environments, ENV)
except AttributeError:
    # Fallback to dev if ENV not found
    print(f"[WARN] Environment '{ENV}' not found in config. Defaulting to 'dev'.")
    current_config = CONF.environments.dev

# Storage Configuration
DEFAULT_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", current_config.bucket_name)

# BigQuery Configuration
DATASET_ID_BRONZE = os.getenv("BQ_DATASET_BRONZE", current_config.dataset_bronze)
DATASET_ID_SILVER = os.getenv("BQ_DATASET_SILVER", current_config.dataset_silver)
DATASET_ID_GOLD = os.getenv("BQ_DATASET_GOLD", current_config.dataset_gold)

# Paths
# For local dev, you might want to set this env var. In Composer, it's usually /home/airflow/gcs/dags
DAGS_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/home/airflow/gcs/dags")


# Service Account Path (Only for local dev if needed, though env var is preferred)
# SA_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") 
