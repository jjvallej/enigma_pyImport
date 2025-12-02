# 📋 Ejemplo Práctico: Migración a un Nuevo Proyecto

## Escenario
Imagina que quieres usar esta plantilla en un nuevo proyecto GCP llamado `mi-empresa-analytics` con los siguientes recursos:

- **Proyecto GCP**: `mi-empresa-analytics-123456`
- **Región**: `us-east1` (diferente a la original)
- **Bucket Dev**: `datalake-mi-empresa-dev`
- **Bucket Prod**: `datalake-mi-empresa-prod`
- **Datasets**: Quieres usar nombres más cortos como `bronze_dev`, `silver_dev`, `gold_dev`

## 📝 Configuración del `config.yaml`

Aquí está cómo se vería tu `config.yaml`:

```yaml
global_config:
  debug: true

environments:
  dev:
    project_id: "mi-empresa-analytics-123456"     # ← Tu proyecto GCP
    location: "us-east1"                          # ← Tu región
    bucket_name: "datalake-mi-empresa-dev"        # ← Tu bucket dev
    dataset_bronze: "bronze_dev"                  # ← Tu dataset bronze
    dataset_silver: "silver_dev"                  # ← Tu dataset silver
    dataset_gold: "gold_dev"                      # ← Tu dataset gold
  prod:
    project_id: "mi-empresa-analytics-123456"     # ← Mismo proyecto
    location: "us-east1"                          # ← Misma región
    bucket_name: "datalake-mi-empresa-prod"       # ← Tu bucket prod
    dataset_bronze: "bronze_prod"                 # ← Tu dataset bronze prod
    dataset_silver: "silver_prod"                 # ← Tu dataset silver prod
    dataset_gold: "gold_prod"                     # ← Tu dataset gold prod
  local:
    project_id: "mi-empresa-analytics-123456"     # ← Para desarrollo local
    location: "us-east1"
    bucket_name: "datalake-mi-empresa-dev"        # ← Usualmente igual a dev
    dataset_bronze: "bronze_dev"
    dataset_silver: "silver_dev"
    dataset_gold: "gold_dev"

# ============================================
# CONFIGURACIÓN DE FUENTES
# ============================================
# Ajusta estas secciones según tus fuentes de datos reales

evaplan:
  auth_endpoint: "http://tu-servidor.com/api/auth/login"  # ← Tu API
  api_base_url: "http://tu-servidor.com/api"              # ← Tu API base
  endpoints:
    periodos: "/datos/periodos"
    avance_mr: "/datos/AvanceMR"
    avance_mp: "/datos/AvanceMP"
    avance_x_subprograma: "/datos/AvanceXSubprograma"
    avance_general: "/datos/AvanceGeneral"
  gcs_base_folder: "data_staging/mi_empresa/api_evaplan"  # ← Tu estructura de carpetas
  gcs_folders:
    periodos: "periodos"
    avance_mr: "avance_mr"
    avance_mp: "avance_mp"
    avance_x_subprograma: "avance_x_subprograma"
    avance_general: "avance_general"
  fuentes:
    - "periodos"
    - "avance_mr"
    - "avance_mp"
    - "avance_x_subprograma"
    - "avance_general"
  tables:
    bronze:
      periodos: "evaplan_api_periodos_raw_data"
      avance_mr: "evaplan_api_avance_mr_raw_data"
      avance_mp: "evaplan_api_avance_mp_raw_data"
      avance_x_subprograma: "evaplan_api_avance_x_subprograma_raw_data"
      avance_general: "evaplan_api_avance_general_raw_data"
    silver:
      periodos: "evaplan_api_periodos_transformed_data"
      avance_mr: "evaplan_api_avance_mr_transformed_data"
      avance_mp: "evaplan_api_avance_mp_transformed_data"
      avance_x_subprograma: "evaplan_api_avance_x_subprograma_transformed_data"
      avance_general: "evaplan_api_avance_general_transformed_data"
  credentials:
    usuario: "mi_usuario"                         # ← Tu usuario
    password: "mi_password_seguro"                # ← Tu password

idc:
  drive_url: "https://docs.google.com/spreadsheets/d/TU_ID/edit"  # ← Tu Google Drive
  gcs_folder: "data_staging/mi_empresa/idc"       # ← Tu carpeta
  tables:
    dictionary: "dim_idc"
    raw_data_mapping:
      Dato_original: "idc_raw_data_dato_original"
      Valor_normalizado: "idc_raw_data_valor_normalizado"
      Valor_ranking: "idc_raw_data_valor_ranking"

ipm:
  drive_url: "https://docs.google.com/spreadsheets/d/TU_ID/edit"  # ← Tu Google Drive
  gcs_folder: "data_staging/mi_empresa/ipm"       # ← Tu carpeta
  tables:
    bronze: "ipm_raw_data"
    silver: "ipm_transformed_data"
    gold: "FACT_DANE"

ipm_sisben:
  drive_url: "https://docs.google.com/spreadsheets/d/TU_ID/edit"  # ← Tu Google Drive
  gcs_folder: "data_staging/mi_empresa/ipm/sisben"
  gcs_temp_folder: "data_staging/mi_empresa/tmp_sisben"
  table_name: "ipm_sisben_raw"
  excel_file_path: "data_staging/mi_empresa/ipm/sisben"
  csv_files:
    - "hoja1_ipm_sisben.csv"
    - "hoja2_ipm_sisben.csv"
    - "hoja3_ipm_sisben.csv"
  tables:
    bronze: "ipm_sisben_raw"
    silver: "ipm_sisben_transformed_data"
    gold: "FACT_SISBEN"

idi:
  config_drive_url: "https://docs.google.com/spreadsheets/d/TU_ID/edit"  # ← Tu Google Drive
  gcs_base_folder: "data_staging/mi_empresa/idi"
  link_name_keywords:
    - "Resultados Territorio"
    - "Resultados consolidados"
  tables:
    bronze_prefix: "idi_raw_data_territorio"
    silver:
      - "idi_transformed_data_2023"
      - "idi_transformed_data_2024"
      - "idi_transformed_data_consolidated"
    gold: "idi_processed_data"
  transform_config:
    skip_rows: 2
    sheet_name: null
    columns_to_drop: null
```

## ✅ Checklist de Cambios

Al migrar, asegúrate de cambiar:

- [ ] `environments.dev.project_id` → Tu proyecto GCP
- [ ] `environments.dev.location` → Tu región (si es diferente)
- [ ] `environments.dev.bucket_name` → Tu bucket de desarrollo
- [ ] `environments.dev.dataset_bronze/silver/gold` → Tus nombres de datasets
- [ ] `environments.prod.*` → Valores de producción
- [ ] `evaplan.auth_endpoint` y `api_base_url` → Tus URLs de API (si usas evaplan)
- [ ] `evaplan.credentials.usuario/password` → Tus credenciales
- [ ] `evaplan.gcs_base_folder` → Tu estructura de carpetas
- [ ] URLs de Google Drive en `idc`, `ipm`, `ipm_sisben`, `idi` → Tus URLs
- [ ] `gcs_folder` y `gcs_base_folder` en cada fuente → Tus carpetas

## 🎯 Lo que NO necesitas cambiar

- ❌ `dbt/dbt_project.yml` → Se configura automáticamente
- ❌ `dbt/profiles.yml` → Se configura automáticamente
- ❌ Modelos dbt → Usan variables automáticamente
- ❌ Código Python → Lee de `config.yaml` automáticamente

## 🚀 Después de cambiar `config.yaml`

1. **Sube el archivo** a tu bucket de Composer
2. **Configura la variable de entorno** `ENVIRONMENT=dev` (o `prod`) en Composer
3. **¡Listo!** Todo funcionará automáticamente

Los DAGs leerán `config.yaml` y configurarán dbt automáticamente.

