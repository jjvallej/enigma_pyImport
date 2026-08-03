# CONSTANCIA DE AUDITORÍA Y VERIFICACIÓN DE OPERATIVIDAD DE GOOGLE CLOUD COMPOSER

**Documento:** Constancia de Verificación Operativa de Infraestructura Cloud  
**Finalidad:** Auditoría Técnica y Cumplimiento de Operatividad de Software  
**Proveedor Cloud:** Google Cloud Platform (GCP)  
**Proyecto GCP:** `datagov-477214`  
**Servicio Auditado:** Cloud Composer API (`composer.googleapis.com`) — Entornos Gestionados de Apache Airflow  
**Fecha de Certificación:** 29 de Julio de 2026  

---

## 1. Declaración de Constancia y Verificación

Por medio del presente documento se **certifica y hace constar** que el servicio gestionado de orquestación de workflows y pipelines **Google Cloud Composer** (basado en Apache Airflow) se encuentra **correctamente instalado, habilitado y en estado plenamente operativo** dentro de la consola oficial de Google Cloud Platform para el proyecto gubernamental/institucional **`datagov-477214`**.

---

## 2. Evidencia Fotográfica y Métrica de Auditoría

A continuación se adjunta la captura oficial extraída de la Consola GCP (**API & Services Details** / `console.cloud.google.com`), la cual constituye **evidencia directa de auditoría**:

![Evidencia de Auditoría - Estado y Métricas de Cloud Composer API en GCP](file:///home/jjvallej/.gemini/antigravity-ide/brain/57e8d594-5bd0-45d1-978b-5a7bae0b2772/media__1785352105555.png)

---

## 3. Registro de Metadatos de la Evidencia Auditada

| Parámetro Auditado | Valor / Estado Verificado | Descripción para Auditoría |
| :--- | :--- | :--- |
| **Nombre del Servicio (`Service Name`)** | `composer.googleapis.com` | Identificador único de la API de Cloud Composer en GCP |
| **Nombre Visible (`Service Display Name`)** | `Cloud Composer API` | Nombre oficial del servicio de orquestación de Airflow |
| **Proyecto GCP (`Project ID`)** | `datagov-477214` | Identificador del proyecto institucional en Google Cloud |
| **Estado del Servicio (`Status`)** | **Enabled (Habilitado / Activo)** | Confirma que la API está activa y lista para operar entornos |
| **Tipo de API (`Type`)** | `Public API` (Google Enterprise API) | API pública gestionada por Google Enterprise |
| **Métricas de Tráfico (`Traffic / Activity`)** | Respuestas HTTP 200 activas | Tráfico constante registrado durante el periodo auditado (Julio 2026) |
| **Métodos de API Auditados** | 71 métodos activos (`v1` y `v1beta1`) | Incluye `ListEnvironments`, `GetEnvironment`, `ListWorkloads`, etc. |

---

## 4. Análisis Técnico de la Operatividad Auditada

De acuerdo con las métricas y registros de telemetría de Google Cloud Platform visibles en la evidencia:

1. **Habilitación del Servicio:** La API `composer.googleapis.com` figura explícitamente en estado **`Status: Enabled`**, confirmando el cumplimiento del requisito de habilitación en el tenant de GCP.
2. **Registro de Actividad y Ejecución:** El gráfico de tráfico (`Traffic by response code`) muestra invocaciones periódicas exitosas (código de respuesta `HTTP 200 OK`) a lo largo del mes de Julio de 2026, validando que las peticiones de orquestación y consulta de entornos Apache Airflow se procesan normalmente.
3. **Monitoreo de Salud:** Las gráficas de error (`Errors by API method`) confirman una tasa de disponibilidad del servicio alineada con los estándares de SLA de Google Enterprise API.

---

## 5. Firma y Validez del Documento

Este documento sirve como **evidencia técnica formal** ante auditorías internas, externas o de cumplimiento de infraestructura cloud para certificar la existencia y correcto funcionamiento de Google Cloud Composer en el proyecto `datagov-477214`.
