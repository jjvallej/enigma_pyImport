# Matriz de Eventos, Alarmas y Resiliencia de DAGs de Airflow

**Proyecto:** `pyimport` / `airflow` (Plataforma ValleDATA — Sistema Integrado de Cultivos, Precios SIPSA, Clima ONI y Comentarios Ciudadanos)  
**Versión:** 1.2.0  
**Fecha de Actualización:** 30 de Septiembre de 2026  
**Ubicación:** `documentacion/MATRIZ_DE_EVENTOS_ALARMAS_DAGS.md`

---

## 1. Visión General del Módulo de Alarmas

El módulo `src_common.py` implementa el sistema unificado de gestión de alarmas, notificaciones Webhook (Slack, Microsoft Teams, Google Chat) y políticas de reintento para todos los DAGs de la plataforma ValleDATA.

---

## 2. Matriz Completa de Eventos y Protocolos de Respuesta

| ID DAG | Evento Detectado | Severidad | Acción Automática | Notificación Notificada | Protocolo de Recuperación |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `src_ingest_sipsa` | Fallo de conexión HTTP DANE (404/500/Timeout) | Media | Reintento automático (2 reintentos, 5 min) | Webhook Slack / Teams | Si persiste, usa el último anexo local en `data/raw/sipsa/`. |
| `src_ingest_oni` | Cambio de estructura HTML en NOAA | Alta | Reintento fallido -> Alarma de fallo | Webhook Alerta Crítica | Notifica al equipo de datos para actualizar el selector BeautifulSoup. |
| `src_ingest_crops` | Archivo CSV corrupto o sin cabecera | Alta | Abortar tarea `ingest_crops` | Webhook Alerta Alta | Fallback a la última descarga válida del bucket GCS. |
| `src_load_*` | Error de esquema BigQuery (type mismatch) | Crítica | Abortar DAG | Webhook Alerta Crítica | Revisa logs de `stg_*` y corrige tipos de datos en la tabla staging. |
| `src_transform_consolidado` | Inconsistencia agronómica ($>100\text{ Ton/Ha}$) | Media | Descarte/Filtrado automático de filas | Log warning en Composer | Registra filas anómalas en log para auditoría sin detener el pipeline. |
| `src_transform_spatial` | Geometría GeoJSON nula o inválida | Media | Asignación de geometría por defecto | Log warning | Permite la generación de la tabla Gold agregando polígonos centroides. |
| `src_orchestrator_master` | Timeout en sub-DAG (`TriggerDagRunOperator`) | Alta | Cancelación de downstream tasks | Webhook Alerta Maestro | Permite reiniciar únicamente el sub-DAG fallido. |

---

## 3. Configuración de Parámetros Globales (`src_common.py`)

```python
DEFAULT_DAG_KWARGS = {
    "owner": "valledata_data_team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=300),
    "execution_timeout": timedelta(minutes=60),
    "on_failure_callback": send_airflow_alarm_webhook,
}
```

---

## 4. Guía de Notificaciones por Webhook

Para activar las alertas por Webhook en Cloud Composer / Airflow:
1. Configure la variable de entorno `AIRFLOW_VAR_WEBHOOK_ALERTS_URL` en la consola de Composer o Astronomer.
2. La función `send_airflow_alarm_webhook()` enviará una carga útil JSON con el siguiente formato:

```json
{
  "event": "DAG_TASK_FAILURE",
  "dag_id": "src_ingest_sipsa",
  "task_id": "download_sipsa_excel",
  "execution_date": "2026-09-30T10:00:00Z",
  "log_url": "https://composer.google.com/logs/..."
}
```
