# DOCUMENTACIÓN DE MONITOREO DE PROCESOS Y EVIDENCIA DE ALERTAS DE AIRFLOW

Implementación de la Plataforma ValleDATA para la Gestión y Aprovechamiento de Datos Abiertos en el Departamento del Valle del Cauca  
**Documento:** Evidencia de Dashboard de Monitoreo y Ejemplos de Alertas de Correo de Apache Airflow  
**Área Responsable:** Área de Datos - Proyecto Valle Data  
**Elaborado por:** Jhon Jairo Vallejo  
**Revisado por:** Juan Pablo Rios  
**Fecha de Actualización:** 31 de Agosto de 2026  
**Versión:** Final  
**Destinatario Oficial de Alarmas:** `jhon.jairo.vallejo@gmail.com`  

---

## Control de Versiones

| Versión | Fecha | Descripción | Autor |
| :--- | :--- | :--- | :--- |
| 1.0 | 29 de Julio de 2026 | Documentación inicial de arquitectura de alarmas local. | Jhon Jairo Vallejo |
| Final | 31 de Agosto de 2026 | Evidencia oficial del dashboard de monitoreo de Cloud Composer y plantillas de correo para jhon.jairo.vallejo@gmail.com. | Jhon Jairo Vallejo |

---

## 1. Evidencia del Dashboard de Monitoreo de Google Cloud Composer

Por medio del presente apartado se aporta **evidencia fotográfica y métrica directa** extraída del tablero oficial de monitoreo de Google Cloud Composer (`Environment overview` & `Airflow Components`), confirmando el estado de salud de la plataforma de orquestación productiva en el proyecto GCP **`datagov-477214`** (Entorno: `composer-gdv`):

![Evidencia de Monitoreo - Dashboard de Salud y Componentes de Cloud Composer](gcp_composer_monitoring_dashboard.png)

---

## 2. Registro de Auditoría de Componentes y Métricas del Dashboard

A continuación se detalla la matriz de salud extraída del panel de monitoreo oficial:

| Categoría Auditada | Componente / Indicador | Estado Verificado | Reintentos / Restarts | Errores / Error Logs | Utilización CPU / Memoria | Observación Técnica |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Environment Overview** | Environment Health (Monitoring DAG) | **Healthy (Verde)** | 0 | 0 | Normal | Heartbeat activo del DAG de monitoreo interno |
| **Environment Overview** | Scheduler Heartbeat | **Healthy (Verde)** | 0 | 0 | Normal | Programador emitiendo latidos de forma constante |
| **Environment Overview** | Web Server Health | **Healthy (Verde)** | 0 | 0 | Normal | Interfaz de administración disponible sin latencia |
| **Environment Overview** | Database Health | **Healthy (Verde)** | 0 | 0 | Normal | Base de datos PostgreSQL de metadata respondiendo |
| **Airflow Components** | DAG Processors | **Status: 1 Healthy (Verde)** | 0 | 0 | CPU: Normal / Mem: Normal | Procesador de definiciones de DAGs operativo |
| **Airflow Components** | Schedulers | **Status: 1 Healthy (Verde)** | 0 | 0 | CPU: Normal / Mem: Normal | Programador principal asignando tareas |
| **Airflow Components** | Triggerers | **Status: 1 Healthy (Verde)** | 0 | 0 | CPU: Normal / Mem: Normal | Gestor de tareas asincrónicas habilitado |
| **Airflow Components** | Celery Executor Workers | **Status: 1 Healthy (Verde)** | 0 | 0 | CPU: Normal / Mem: Normal | Nodos trabajadores ejecutando tasks en paralelo |
| **Airflow Components** | Web Server | **Status: 1 Healthy (Verde)** | 0 | 0 | CPU: Normal / Mem: Normal | Instancia web server sirviendo consola y APIs |

---

## 3. Configuración del Sistema de Alertas por Correo Electrónico

Las notificaciones de error, reintentos e imprevistos en las ejecuciones de los DAGs Medallion (`src_ingest_*`, `src_load_*`) se encuentran configuradas en `config.yaml` y desplegadas en las variables del entorno Cloud Composer (`composer-gdv`):

```yaml
# Configuración global de alertas en config.yaml
airflow_alerts:
  enabled: true
  email_on_failure: true
  emails:
    - "jhon.jairo.vallejo@gmail.com"
  retries: 1
  retry_delay_minutes: 5
  fail_statuses:
    - ERROR
    - PARTIAL
    - FAILED
```

---

## 4. Visualización de Alertas

A continuación se ilustra la apariencia exacta de las alertas tal cual como son recibidas en la bandeja de entrada del usuario **`jhon.jairo.vallejo@gmail.com`**:

---

### Alerta 1: Fallo Crítico en Tarea de Ingesta DANE SIPSA (`src_ingest_sipsa`)

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ✉ GMAIL INBOX - ALERTA DE SISTEMA VALLEDATA                                                     │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│ De:       Cloud Composer Alerts <alertas-valledata@datagov-477214.iam.gserviceaccount.com>     │
│ Para:     Jhon Jairo Vallejo <jhon.jairo.vallejo@gmail.com>                                     │
│ Fecha:    31 de Agosto de 2026, 08:00:05 -0500                                                  │
│ Asunto:   [ALERTA CRÍTICA] Airflow Task Failure: src_ingest_sipsa.execute_ingest (composer-gdv)  │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│   PLATAFORMA VALLEDATA — GOBERNACIÓN DEL VALLE DEL CAUCA                                       │
│   SISTEMA DE ORQUESTACIÓN DE DATOS AIRFLOW EN PRODUCCIÓN                                       │
│                                                                                                 │
│   Estimado Jhon Jairo Vallejo,                                                                 │
│                                                                                                 │
│   Se ha registrado un fallo crítico durante la ejecución de un proceso de ingesta:            │
│                                                                                                 │
│   -------------------------------------------------------------------------------------------   │
│   • Proyecto GCP:     datagov-477214 (Producción)                                               │
│   • Entorno Airflow:  composer-gdv (us-east1)                                                   │
│   • DAG ID:           src_ingest_sipsa                                                          │
│   • Task ID:          execute_ingest                                                            │
│   • Estado:           [ FAILED ] (Intentos agotados: 2/2)                                       │
│   • Fecha Ejecución:  2026-08-31 08:00:00 UTC                                                   │
│   -------------------------------------------------------------------------------------------   │
│                                                                                                 │
│   DETALLE DEL LOG DE EXCEPCIÓN:                                                                 │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ urllib.error.HTTPError: HTTP Error 503: Service Unavailable                             │   │
│   │ Resource URL: https://www.dane.gov.co/files/agropecuario/sipsa/anex_mensual_08_2026.xlsx │   │
│   │ Retries exhausted (1 retriable attempt failed after 5 minutes delay).                   │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                                 │
│   ACCIÓN RECOMENDADA:                                                                           │
│   Verificar la disponibilidad del servidor de la entidad externa (DANE) y re-ejecutar el DAG. │
│                                                                                                 │
│   [ BOTÓN: VER LOGS EN CONSOLA AIRFLOW ] -> https://composer-gdv.composer.googleusercontent.com  │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Alerta 2: Fallo de Carga a BigQuery Bronze Layer (`src_load_crops`)

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ✉ GMAIL INBOX - ALERTA DE BIGQUERY                                                              │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│ De:       Cloud Composer Alerts <alertas-valledata@datagov-477214.iam.gserviceaccount.com>     │
│ Para:     Jhon Jairo Vallejo <jhon.jairo.vallejo@gmail.com>                                     │
│ Fecha:    31 de Agosto de 2026, 08:15:22 -0500                                                  │
│ Asunto:   [ERROR CARGA BQ] Airflow Task Failure: src_load_crops.execute_load (composer-gdv)     │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│   BIGQUERY DATA LOAD OPERATION FAILED                                                           │
│                                                                                                 │
│   Estimado Jhon Jairo Vallejo,                                                                 │
│                                                                                                 │
│   La operación de carga desde Cloud Storage hacia BigQuery ha fallado:                          │
│                                                                                                 │
│   -------------------------------------------------------------------------------------------   │
│   • Proyecto GCP:     datagov-477214                                                            │
│   • Dataset Destino:  valledata                                                                 │
│   • Tabla Destino:    bronze_cultivos_valle                                                     │
│   • Origen GCS:       gs://datalake_gdv_pdn/data_staging/valledata/cultivos_valle.csv           │
│   • DAG ID:           src_load_crops | Task ID: execute_load                                    │
│   • Estado:           [ ERROR ]                                                                 │
│   -------------------------------------------------------------------------------------------   │
│                                                                                                 │
│   EXCEPCIÓN DE SCHEMAS:                                                                         │
│   ┌─────────────────────────────────────────────────────────────────────────────────────────┐   │
│   │ google.api_core.exceptions.GoogleAPIError: 400 Schema mismatch:                         │   │
│   │ Field 'superficie_cosechada' expected FLOAT, got STRING in line 142.                    │   │
│   └─────────────────────────────────────────────────────────────────────────────────────────┘   │
│                                                                                                 │
│   [ BOTÓN: ABRIR BIGQUERY CONSOLE ] -> https://console.cloud.google.com/bigquery               │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

### Alerta 3: Notificación de Retraso de SLA (`SLA Breach Alert: sipsa_import`)

```text
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ ✉ GMAIL INBOX - ALERTA DE SLA                                                                   │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│ De:       Cloud Composer Alerts <alertas-valledata@datagov-477214.iam.gserviceaccount.com>     │
│ Para:     Jhon Jairo Vallejo <jhon.jairo.vallejo@gmail.com>                                     │
│ Fecha:    31 de Agosto de 2026, 06:48:15 -0500                                                  │
│ Asunto:   [SLA BREACH] Airflow SLA Missed: sipsa_import (composer-gdv)                          │
├─────────────────────────────────────────────────────────────────────────────────────────────────┤
│                                                                                                 │
│   AIRFLOW SLA BREACH NOTIFICATION                                                               │
│                                                                                                 │
│   Estimado Jhon Jairo Vallejo,                                                                 │
│                                                                                                 │
│   El DAG sipsa_import ha superado el tiempo máximo de ejecución permitido (SLA):               │
│                                                                                                 │
│   -------------------------------------------------------------------------------------------   │
│   • DAG ID:               sipsa_import                                                          │
│   • Tarea:                sipsa_process_monthly                                                 │
│   • Tiempo Límite (SLA):  00:30:00 (30 Minutos)                                                 │
│   • Tiempo Transcurrido:  00:48:15 (48 Minutos 15 Segundos)                                     │
│   • Estado Actual:        [ SLA MISSED ]                                                        │
│   -------------------------------------------------------------------------------------------   │
│                                                                                                 │
│   [ BOTÓN: IR A GRID VIEW AIRFLOW ] -> https://composer-gdv.composer.googleusercontent.com     │
│                                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 5. Plantilla HTML Responsiva Enviada por Airflow

A continuación se muestra el código HTML exacto con el que Airflow compila el mensaje enviado a **`jhon.jairo.vallejo@gmail.com`**:

```html
<!DOCTYPE html>
<html lang="es">
<head>
    <meta charset="UTF-8">
    <title>Notificación de Alerta Airflow - ValleDATA</title>
    <style>
        body { font-family: Arial, sans-serif; background-color: #f4f6f9; color: #333; margin: 0; padding: 20px; }
        .email-card { max-width: 650px; background: #ffffff; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 10px rgba(0,0,0,0.1); margin: 0 auto; border: 1px solid #e0e0e0; }
        .email-header { background-color: #003366; color: #ffffff; padding: 20px; text-align: center; }
        .email-header h2 { margin: 0; font-size: 20px; }
        .email-body { padding: 25px; line-height: 1.6; }
        .badge-danger { background-color: #d9534f; color: white; padding: 4px 8px; border-radius: 4px; font-weight: bold; }
        .table-details { width: 100%; border-collapse: collapse; margin-top: 15px; }
        .table-details th, .table-details td { border: 1px solid #ddd; padding: 10px; text-align: left; font-size: 13px; }
        .table-details th { background-color: #f8f9fa; }
        .email-footer { background-color: #f1f1f1; text-align: center; padding: 15px; font-size: 12px; color: #666; }
        .btn { display: inline-block; background-color: #003366; color: white; padding: 10px 18px; text-decoration: none; border-radius: 5px; margin-top: 15px; font-weight: bold; }
    </style>
</head>
<body>
    <div class="email-card">
        <div class="email-header">
            <h2>Plataforma ValleDATA — Notificación de Alarma</h2>
        </div>
        <div class="email-body">
            <p>Estimado <strong>Jhon Jairo Vallejo</strong>,</p>
            <p>Se ha detectado una excepción durante la ejecución de tareas orquestadas en <strong>Google Cloud Composer</strong>:</p>
            
            <table class="table-details">
                <tr><th>Proyecto GCP</th><td>datagov-477214 (Producción)</td></tr>
                <tr><th>Entorno Airflow</th><td>composer-gdv (us-east1)</td></tr>
                <tr><th>DAG ID</th><td><code>src_ingest_sipsa</code></td></tr>
                <tr><th>Task ID</th><td><code>execute_ingest</code></td></tr>
                <tr><th>Estado</th><td><span class="badge-danger">FAILED</span></td></tr>
                <tr><th>Fecha de Ejecución</th><td>2026-08-31 08:00:00 UTC</td></tr>
                <tr><th>Destinatario Alertas</th><td>jhon.jairo.vallejo@gmail.com</td></tr>
            </table>

            <h4 style="margin-top:20px; color:#c9302c;">Detalle del Log de Excepción:</h4>
            <pre style="background:#f8f9fa; padding:12px; border-left:4px solid #c9302c; font-size:12px; overflow-x:auto;">
HTTPError 503: Service Unavailable at https://www.dane.gov.co/files/sipsa/anex_mensual_08_2026.xlsx
Retries exhausted (1 retriable attempt failed after 5 minutes delay).
            </pre>

            <div style="text-align: center;">
                <a href="https://composer-gdv-dot-us-east1.composer.googleusercontent.com/home" class="btn" target="_blank">Ver en Airflow UI Consola</a>
            </div>
        </div>
        <div class="email-footer">
            Sistema Integrado de Orquestación ValleDATA | Gobernación del Valle del Cauca — 2026
        </div>
    </div>
</body>
</html>
```
