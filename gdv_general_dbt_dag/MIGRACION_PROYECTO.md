# Guía de Migración a Otro Proyecto GCP

## 📋 Resumen

Para migrar esta plantilla a otro proyecto de GCP, **SOLO necesitas cambiar UN archivo**: `config/config.yaml`

**¡Todo está configurado automáticamente!** Los DAGs de Airflow configuran las variables de entorno de dbt desde `config.yaml` antes de ejecutar cualquier comando dbt.

## 🔧 Pasos para Migrar

### 1. Actualizar `config/config.yaml` (ÚNICO PASO NECESARIO)

Edita la sección `environments` según tu nuevo proyecto:

```yaml
environments:
  dev:
    project_id: "TU_PROYECTO_ID"           # ← Cambiar aquí
    location: "us-central1"                 # ← Cambiar si es necesario
    bucket_name: "tu_bucket_dev"            # ← Cambiar aquí
    dataset_bronze: "bronze_tu_proyecto_dev"    # ← Cambiar aquí
    dataset_silver: "silver_tu_proyecto_dev"   # ← Cambiar aquí
    dataset_gold: "gold_tu_proyecto_dev"        # ← Cambiar aquí
  prod:
    project_id: "TU_PROYECTO_ID"           # ← Cambiar aquí
    location: "us-central1"                 # ← Cambiar si es necesario
    bucket_name: "tu_bucket_prod"           # ← Cambiar aquí
    dataset_bronze: "bronze_tu_proyecto_prod"   # ← Cambiar aquí
    dataset_silver: "silver_tu_proyecto_prod"  # ← Cambiar aquí
    dataset_gold: "gold_tu_proyecto_prod"       # ← Cambiar aquí
```

### 2. Configurar Variable de Entorno en Composer (Opcional)

Solo necesitas configurar **UNA** variable de entorno en Composer:

- `ENVIRONMENT=dev` (o `prod` o `local`)

**¡Eso es todo!** Los DAGs automáticamente:
- Leen `config.yaml` según el `ENVIRONMENT`
- Configuran las variables de entorno de dbt (`DBT_PROJECT_ID`, `DBT_DATASET_BRONZE`, etc.) desde `config.yaml`
- Ejecutan dbt con esas variables configuradas

## ✅ Verificación

Después de migrar, verifica que:

1. ✅ Los DAGs de Airflow leen correctamente desde `config.yaml`
2. ✅ Los modelos dbt usan las variables correctas (configuradas automáticamente)
3. ✅ Los datasets existen en BigQuery
4. ✅ El bucket de GCS existe y tiene los permisos correctos

## 🔄 Flujo de Configuración

```
config.yaml (FUENTE ÚNICA DE VERDAD)
    ↓
modules/config.py → Lee para DAGs de Airflow
    ↓
get_dbt_command() → Exporta variables de entorno desde config.yaml
    ↓
BashOperator ejecuta dbt → dbt lee variables de entorno
    ↓
dbt_project.yml → Usa variables de entorno (valores por defecto solo para desarrollo local)
```

## 📝 Notas Importantes

- **Los modelos dbt NO tienen valores hardcodeados** - usan `var('silver_dataset')` sin fallbacks
- **Los módulos Python leen de `config.yaml`** - no hay valores hardcodeados
- **Los DAGs configuran automáticamente las variables de entorno de dbt** desde `config.yaml`
- **Solo necesitas cambiar `config.yaml`** - todo lo demás funciona automáticamente
- **Los valores por defecto en `dbt_project.yml`** son solo para desarrollo local si no se configuran variables de entorno

