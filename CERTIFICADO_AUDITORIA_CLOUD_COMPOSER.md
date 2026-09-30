# CONSTANCIA DE AUDITORÍA Y VERIFICACIÓN DE OPERATIVIDAD DE GOOGLE CLOUD COMPOSER EN PRODUCCIÓN

**Documento:** Constancia de Verificación Operativa de Infraestructura Cloud (Producción)  
**Finalidad:** Auditoría Técnica y Cumplimiento de Operatividad de Software  
**Proveedor Cloud:** Google Cloud Platform (GCP)  
**Proyecto GCP:** `datagov-477214` (Entorno: `composer-gdv`, Región: `us-east1`)  
**Servicio Auditado:** Cloud Composer API (`composer.googleapis.com`) — Entornos Gestionados de Apache Airflow  
**Fecha de Certificación:** 31 de Agosto de 2026  
**Versión:** Final  

---

## Control de Versiones

| Versión | Fecha | Descripción de Cambios vs Versión Anterior | Autor |
| :--- | :--- | :--- | :--- |
| 1.0 | 29 de Julio de 2026 | Creación inicial del documento de constancia de habilitación de servicio Cloud Composer. | Jhon Jairo Vallejo |
| Final | 31 de Agosto de 2026 | Certificación de operatividad final y verificación de conexiones en el esquema de producción de Cloud Composer GCP. | Jhon Jairo Vallejo |

---

## 1. Declaración de Constancia y Verificación en Producción

Por medio del presente documento se **certifica y hace constar** que el servicio gestionado de orquestación de workflows y pipelines **Google Cloud Composer** (basado en Apache Airflow 2.x) se encuentra **correctamente instalado, habilitado, desplegado y en estado plenamente operativo en el esquema de PRODUCCIÓN** dentro de la consola oficial de Google Cloud Platform para el proyecto gubernamental/institucional **`datagov-477214`** (Entorno `composer-gdv`, ubicación `us-east1`).

---

## 2. Evidencia Fotográfica y Métrica de Auditoría

A continuación se adjunta la captura oficial extraída de la Consola GCP (**API & Services Details** / `console.cloud.google.com`), la cual constituye **evidencia directa de auditoría de operatividad**:

![Evidencia de Auditoría - Estado y Métricas de Cloud Composer API en GCP Producción](file:///home/jjvallej/.gemini/antigravity-ide/brain/57e8d594-5bd0-45d1-978b-5a7bae0b2772/media__1785352105555.png)

---

## 3. Registro de Metadatos de la Evidencia Auditada (Esquema Producción)

| Parámetro Auditado | Valor / Estado Verificado | Descripción para Auditoría |
| :--- | :--- | :--- |
| **Nombre del Servicio (`Service Name`)** | `composer.googleapis.com` | Identificador único de la API de Cloud Composer en GCP |
| **Nombre Visible (`Service Display Name`)** | `Cloud Composer API` | Nombre oficial del servicio de orquestación de Airflow |
| **Proyecto GCP (`Project ID`)** | `datagov-477214` | Identificador del proyecto institucional en Google Cloud Producción |
| **Entorno Composer (`Environment`)** | `composer-gdv` | Nombre del entorno gestionado en producción |
| **Estado del Servicio (`Status`)** | **Enabled (Habilitado / Activo en Producción)** | Confirma que la API está activa y operando flujos productivos |
| **Tipo de API (`Type`)** | `Public API` (Google Enterprise API) | API pública gestionada por Google Enterprise |
| **Métricas de Tráfico (`Traffic / Activity`)** | Respuestas HTTP 200 OK activas | Tráfico constante registrado durante la operación en producción |
| **Métodos de API Auditados** | 71 métodos activos (`v1` y `v1beta1`) | Incluye `ListEnvironments`, `GetEnvironment`, `ListWorkloads`, etc. |
| **Bases de Datos y Almacenamiento** | BigQuery `valledata`, GCS `datalake_gdv_pdn` | Integración productiva validada para la carga Bronze/Silver |

---

## 4. Análisis Técnico de la Operatividad Auditada en Producción

De acuerdo con las métricas y registros de telemetría de Google Cloud Platform visibles en la evidencia:

1. **Habilitación del Servicio:** La API `composer.googleapis.com` figura explícitamente en estado **`Status: Enabled`**, confirmando el cumplimiento del requisito de habilitación y despliegue en el tenant de GCP.
2. **Registro de Actividad y Ejecución:** El gráfico de tráfico (`Traffic by response code`) muestra invocaciones periódicas exitosas (código de respuesta `HTTP 200 OK`), validando que las peticiones de orquestación y consulta de entornos Apache Airflow se procesan normalmente en producción.
3. **Monitoreo de Salud:** Las gráficas de error (`Errors by API method`) confirman una tasa de disponibilidad del servicio alineada con los estándares de SLA de Google Enterprise API.

---

## 5. Firma y Validez del Documento

Este documento sirve como **evidencia técnica formal y final** ante auditorías internas, externas o de cumplimiento de infraestructura cloud para certificar la existencia y correcto funcionamiento de Google Cloud Composer en el esquema de producción del proyecto `datagov-477214`.
