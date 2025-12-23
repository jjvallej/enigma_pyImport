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
# Default to 'dev' for safety (evita ejecutar en prod por error)
# NOTA: Si no configuras ENVIRONMENT en Composer, cambia este valor por defecto:
# - Para DEV: "dev"
# - Para PROD: "prod"
ENV = os.getenv("ENVIRONMENT", "dev")  # Cambia "dev" a "prod" si quieres usar producción por defecto

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

# GCP Configuration - Leer desde config.yaml
# Si no está en env var ni en config.yaml, lanza error (no usar fallbacks hardcodeados)
project_id_from_config = getattr(current_config, "project_id", None)
location_from_config = getattr(current_config, "location", None)

if not project_id_from_config:
    raise ValueError("project_id no está definido en config.yaml para el ambiente '{}'. Verifica la sección environments.{}".format(ENV, ENV))
if not location_from_config:
    raise ValueError("location no está definido en config.yaml para el ambiente '{}'. Verifica la sección environments.{}".format(ENV, ENV))

PROJECT_ID = os.getenv("GCP_PROJECT", project_id_from_config)
LOCATION = os.getenv("GCP_LOCATION", location_from_config)

# Storage Configuration
DEFAULT_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", current_config.bucket_name)

# BigQuery Configuration
DATASET_ID_BRONZE = os.getenv("BQ_DATASET_BRONZE", current_config.dataset_bronze)
DATASET_ID_SILVER = os.getenv("BQ_DATASET_SILVER", current_config.dataset_silver)
DATASET_ID_GOLD = os.getenv("BQ_DATASET_GOLD", current_config.dataset_gold)

# Paths
# For local prod, you might want to set this env var. In Composer, it's usually /home/airflow/gcs/dags
DAGS_FOLDER = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "/home/airflow/gcs/dags")


# Service Account Path (Only for local prod if needed, though env var is preferred)
# SA_PATH = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Helper function para generar comandos dbt con variables de entorno desde config.yaml
def get_dbt_command(dbt_command: str, dbt_project_dir: str) -> str:
    """
    Genera un comando bash que configura las variables de entorno de dbt desde config.yaml
    y luego ejecuta el comando dbt especificado.
    
    Args:
        dbt_command: Comando dbt a ejecutar (ej: "dbt run --select model_name")
        dbt_project_dir: Directorio del proyecto dbt
        
    Returns:
        Comando bash completo con export de variables de entorno
    """
    # Obtener valores desde config.yaml
    env_vars = {
        "DBT_PROJECT_ID": PROJECT_ID,
        "DBT_DATASET_BRONZE": DATASET_ID_BRONZE,
        "DBT_DATASET_SILVER": DATASET_ID_SILVER,
        "DBT_DATASET_GOLD": DATASET_ID_GOLD,
        "DBT_LOCATION": LOCATION,
    }
    
    # Construir comando con export de variables
    export_vars = " && ".join([f'export {key}="{value}"' for key, value in env_vars.items()])
    
    # Construir argumentos --vars para pasar variables a dbt
    # Esto asegura que las variables estén disponibles incluso si env_var() no funciona dentro de vars:
    import json
    dbt_vars = {
        "project_id": env_vars["DBT_PROJECT_ID"],
        "bronze_dataset": env_vars["DBT_DATASET_BRONZE"],
        "silver_dataset": env_vars["DBT_DATASET_SILVER"],
        "gold_dataset": env_vars["DBT_DATASET_GOLD"],
    }
    vars_json = json.dumps(dbt_vars)
    vars_arg = f"--vars '{vars_json}'"
    
    # Comando completo: exportar variables + ejecutar dbt con --vars
    full_command = f'set -e && {export_vars} && cd {dbt_project_dir} && {dbt_command} {vars_arg} --project-dir {dbt_project_dir} --profiles-dir {dbt_project_dir} 2>&1 || (echo "DBT command failed with exit code:" $? && exit 1)'
    
    return full_command 
