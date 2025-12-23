# Configuración de Documentación de dbt en Google Cloud Composer

Esta guía explica cómo configurar la generación y publicación automática de la documentación de dbt en Google Cloud Composer.

## Resumen

La solución implementada permite:
1. **Generar automáticamente** la documentación de dbt usando `dbt docs generate`
2. **Subir los archivos** generados a un bucket de Google Cloud Storage
3. **Servir la documentación** como sitio web estático accesible 24/7

## Componentes Creados

### 1. Módulo `dbt_docs_upload.py`
Ubicación: `modules/dbt_docs/dbt_docs_upload.py`

Funciones principales:
- `generate_dbt_docs()`: Ejecuta `dbt docs generate` y genera la documentación
- `upload_dbt_docs_to_gcs()`: Sube todos los archivos generados a GCS
- `configure_bucket_for_static_website()`: Configura el bucket para servir contenido estático

### 2. DAG `src_dbt_docs_generate`
Ubicación: `dags_dbt/src_dbt_docs_dag.py`

El DAG ejecuta:
1. **generate_dbt_docs**: Genera la documentación
2. **upload_dbt_docs_to_gcs**: Sube la documentación a GCS

**Frecuencia**: Diaria (se puede cambiar en el `schedule_interval`)

## Configuración Inicial

### Paso 1: Verificar que dbt esté instalado en Composer

En Composer, dbt debe estar instalado en el entorno de Python. Si no lo está, agrega `dbt-bigquery` a los requirements de tu entorno de Composer.

### Paso 2: Configurar el Bucket para Hosting Estático

Para que la documentación sea accesible como sitio web, necesitas configurar el bucket de GCS:

#### Opción A: Configuración Manual (Recomendada)

1. Ve a la [Consola de Google Cloud Storage](https://console.cloud.google.com/storage)
2. Selecciona tu bucket (ej: `datalake_gdv_dev`)
3. Ve a la pestaña **"Configuración"** (Settings)
4. En la sección **"Sitio web"** (Website):
   - Marca **"Habilitar sitio web"** (Enable website)
   - **Página principal**: `dbt_docs/index.html`
   - **Página de error 404** (opcional): `dbt_docs/index.html`
5. Guarda los cambios

#### Opción B: Usando gcloud CLI

```bash
# Configurar el bucket para servir contenido estático
gsutil web set -m dbt_docs/index.html -e dbt_docs/index.html gs://TU_BUCKET_NAME
```

#### Opción C: Configurar Permisos IAM (Opcional - Solo si quieres acceso público)

Si quieres que la documentación sea accesible públicamente (sin autenticación):

```bash
# Hacer el bucket público para lectura
gsutil iam ch allUsers:objectViewer gs://TU_BUCKET_NAME
```

**⚠️ ADVERTENCIA**: Esto hace que todos los archivos en el bucket sean públicos. Considera usar Cloud Load Balancer o Cloud Run para acceso controlado en producción.

### Paso 3: Verificar Permisos del Service Account

El service account usado por Composer debe tener los siguientes permisos en el bucket:
- `storage.objects.create`
- `storage.objects.delete`
- `storage.objects.get`
- `storage.objects.list`
- `storage.objects.update`

Estos permisos normalmente vienen con el rol `roles/storage.objectAdmin`.

## Acceso a la Documentación

Una vez configurado, la documentación estará disponible en:

### URL Pública (si el bucket está configurado para hosting estático)
```
https://storage.googleapis.com/TU_BUCKET_NAME/dbt_docs/index.html
```

### URL con Cloud Load Balancer (Recomendado para Producción)
Si configuras un Load Balancer con un bucket backend, puedes tener una URL personalizada como:
```
https://dbt-docs.tu-dominio.com
```

### Acceso desde la Consola de GCS
1. Ve a [Google Cloud Storage](https://console.cloud.google.com/storage)
2. Navega a tu bucket
3. Abre la carpeta `dbt_docs/`
4. Haz clic en `index.html`
5. Haz clic en "Ver en el navegador" o copia la URL pública

## Personalización

Toda la configuración se realiza desde `config.yaml`, siguiendo el mismo patrón que los otros DAGs del proyecto.

### Cambiar la Frecuencia de Actualización

Edita `config.yaml` en la sección `dbt_docs`:

```yaml
dbt_docs:
  schedule_interval: "@daily"    # Diario
  # schedule_interval: "@weekly"  # Semanal
  # schedule_interval: "null"     # Solo manual
```

### Cambiar el Target de dbt

Edita `config.yaml`:

```yaml
dbt_docs:
  target: "dev"   # Cambiar a "prod" para producción
```

### Cambiar la Ubicación en GCS

Edita `config.yaml`:

```yaml
dbt_docs:
  destination_prefix: "dbt_docs"  # Cambiar a otra ruta si lo deseas
```

### Configuración Completa en config.yaml

```yaml
dbt_docs:
  target: "dev"                    # Target de dbt (dev, prod, local)
  destination_prefix: "dbt_docs"  # Prefijo en GCS
  schedule_interval: "@daily"      # Frecuencia (@daily, @weekly, null para manual)
  bucket_config:
    index_page: "index.html"
    error_page: "index.html"
```

## Solución de Problemas

### Error: "dbt: command not found"
**Solución**: Asegúrate de que `dbt-bigquery` esté instalado en el entorno de Composer. Agrega `dbt-bigquery` a los requirements de Python de tu entorno.

### Error: "Permission denied" al subir a GCS
**Solución**: Verifica que el service account de Composer tenga permisos de escritura en el bucket.

### La URL pública no funciona
**Solución**: 
1. Verifica que el bucket esté configurado para hosting estático (Paso 2)
2. Verifica que el archivo `dbt_docs/index.html` exista en el bucket
3. Si usas IAM, verifica que los permisos estén configurados correctamente

### La documentación no se actualiza
**Solución**: 
1. Verifica que el DAG se esté ejecutando correctamente en Airflow
2. Revisa los logs del DAG para ver si hay errores
3. Verifica que el `schedule_interval` esté configurado correctamente

## Alternativas Avanzadas

### Usar Cloud Run para Servir la Documentación

Para mayor control y seguridad, puedes servir la documentación usando Cloud Run:

1. Crea un contenedor simple que sirva archivos estáticos
2. Sube la documentación a Cloud Storage
3. Monta el bucket como volumen en Cloud Run
4. Expone Cloud Run con una URL personalizada

### Usar Cloud Load Balancer

Para producción, considera usar un Load Balancer con un bucket backend:
- Mayor control de acceso
- URLs personalizadas
- SSL/TLS automático
- Integración con IAM para autenticación

## Referencias

- [dbt Documentation](https://docs.getdbt.com/reference/commands/cmd-docs)
- [Google Cloud Storage Static Website Hosting](https://cloud.google.com/storage/docs/hosting-static-website)
- [Airflow PythonOperator](https://airflow.apache.org/docs/apache-airflow/stable/howto/operator/python.html)

