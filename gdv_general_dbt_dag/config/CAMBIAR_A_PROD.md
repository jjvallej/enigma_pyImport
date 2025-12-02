# 🚀 Cómo Cambiar a Producción (ENVIRONMENT=prod)

## 📋 Pasos para Activar Producción en Composer

### Opción 1: Desde Google Cloud Console (Recomendado)

1. **Ve a Google Cloud Console**
   - URL: https://console.cloud.google.com
   - Selecciona tu proyecto: `datagov-473122`

2. **Navega a Composer**
   - En el menú lateral, busca **"Composer"** o **"Apache Airflow"**
   - O ve directamente a: https://console.cloud.google.com/composer/environments

3. **Selecciona tu ambiente de Composer**
   - Haz clic en el nombre de tu ambiente de Composer

4. **Ve a la pestaña "Environment Variables" o "Airflow Config Overrides"**
   - Busca la sección de **"Environment Variables"** o **"Airflow configuration overrides"**

5. **Agrega la variable de entorno**
   - Haz clic en **"Edit"** o **"Add environment variable"**
   - Agrega:
     ```
     Key: ENVIRONMENT
     Value: prod
     ```
   - Guarda los cambios

6. **Espera a que se actualice el ambiente**
   - Composer reiniciará los workers de Airflow
   - Esto puede tomar 5-10 minutos

### Opción 2: Usando gcloud CLI

```bash
# 1. Configura tu proyecto
gcloud config set project datagov-473122

# 2. Lista tus ambientes de Composer
gcloud composer environments list

# 3. Actualiza la variable de entorno (reemplaza ENVIRONMENT_NAME con el nombre real)
gcloud composer environments update ENVIRONMENT_NAME \
    --location us-central1 \
    --update-env-variables ENVIRONMENT=prod
```

### Opción 3: Desde la UI de Airflow (Temporal)

⚠️ **Nota:** Esta opción es temporal y se pierde al reiniciar. Solo para pruebas.

1. Ve a la UI de Airflow
2. Ve a **Admin** → **Variables**
3. Crea una variable:
   - Key: `ENVIRONMENT`
   - Value: `prod`

## ✅ Verificar que Funciona

### Método 1: Verificar en los Logs de Airflow

1. Ejecuta cualquier DAG
2. Ve a los logs de una tarea
3. Busca mensajes como:
   ```
   [INFO] Environment activo: prod
   [INFO] Project ID: datagov-473122
   [INFO] Bucket: datalake_gdv_prod
   [INFO] Dataset Bronze: bronze_dpt_planeacion_municipal_prod
   ```

### Método 2: Crear un DAG de Prueba

Crea un DAG temporal para verificar:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from modules.config import ENV, PROJECT_ID, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

def print_config(**context):
    print(f"[DEBUG] Environment activo: {ENV}")
    print(f"[DEBUG] Project ID: {PROJECT_ID}")
    print(f"[DEBUG] Bucket: {DEFAULT_BUCKET_NAME}")
    print(f"[DEBUG] Dataset Bronze: {DATASET_ID_BRONZE}")
    
    # Verificar que sea prod
    if ENV == "prod":
        print("[✅] ¡Configuración de PRODUCCIÓN activa!")
    else:
        print(f"[⚠️] Environment actual: {ENV} (esperado: prod)")

with DAG(
    dag_id="verificar_environment",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["debug", "config"],
) as dag:
    check_task = PythonOperator(
        task_id="verificar_config",
        python_callable=print_config
    )
```

### Método 3: Verificar en BigQuery

Después de ejecutar un DAG, verifica que los datos se estén escribiendo en:

- Dataset: `bronze_dpt_planeacion_municipal_prod` (no `_dev`)
- Bucket: `datalake_gdv_prod` (no `_dev`)

## 🔄 Volver a Desarrollo

Si quieres volver a `dev`:

1. Repite los pasos anteriores
2. Cambia el valor a:
   ```
   Key: ENVIRONMENT
   Value: dev
   ```

O simplemente **elimina la variable** `ENVIRONMENT` y usará `dev` por defecto.

## ⚠️ Importante

1. **Después de cambiar la variable**, Composer reiniciará los workers (5-10 minutos)
2. **Verifica siempre** que esté usando `prod` antes de ejecutar DAGs importantes
3. **Los datos de prod son reales** - ten cuidado al hacer cambios
4. **Asegúrate** de que los buckets y datasets de `prod` existan antes de ejecutar

## 📊 Comparación: Dev vs Prod

| Configuración | Dev | Prod |
|---------------|-----|------|
| **Bucket** | `datalake_gdv_dev` | `datalake_gdv_prod` |
| **Dataset Bronze** | `bronze_dpt_planeacion_municipal_dev` | `bronze_dpt_planeacion_municipal_prod` |
| **Dataset Silver** | `silver_dpt_planeacion_municipal_dev` | `silver_dpt_planeacion_municipal_prod` |
| **Dataset Gold** | `gold_dpt_planeacion_municipal_dev` | `gold_dpt_planeacion_municipal_prod` |
| **Project ID** | `datagov-473122` | `datagov-473122` (mismo) |

## 🎯 Resumen Rápido

1. Ve a **Composer** en Google Cloud Console
2. Selecciona tu ambiente
3. Agrega variable: `ENVIRONMENT=prod`
4. Espera 5-10 minutos a que se actualice
5. ¡Listo! Todos los DAGs usarán la configuración de `prod`

