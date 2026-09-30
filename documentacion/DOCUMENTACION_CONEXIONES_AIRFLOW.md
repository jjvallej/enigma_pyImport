# Documentación Técnica de Conexiones e Integraciones de Airflow

**Proyecto:** `pyimport` / `airflow` (Plataforma ValleDATA — Sistema Integrado de Cultivos, Precios SIPSA, Clima ONI y Comentarios Ciudadanos)  
**Versión:** 1.2.0  
**Fecha de Actualización:** 30 de Septiembre de 2026  
**Ubicación:** `documentacion/DOCUMENTACION_CONEXIONES_AIRFLOW.md`

---

## 1. Visión General de Conexiones

Airflow abstrae el acceso a fuentes externas públicas e institucionales mediante el sistema de **Conexiones (`Connections`)**. Las conexiones se pueden configurar mediante la interfaz de Airflow (`Admin -> Connections`), variables de entorno (`AIRFLOW_CONN_*`), o los archivos de configuración local `connections.yaml` y `config/config.yaml`.

---

## 2. Detalle de Conexiones Registradas

### 1. `sipsa_dane`
- **Tipo:** `HTTP`
- **Host:** `https://www.dane.gov.co`
- **Propósito:** Ingestión de archivos Excel con el Índice de Precios del Sector Agropecuario (SIPSA DANE).
- **Uso en Código:** Utilizado por `src_ingest_sipsa.py` para construir dinámicamente la URL de descarga de anexos mensuales de la Central Mayorista Cavasa en Cali.

### 2. `noaa_oni`
- **Tipo:** `HTTP`
- **Host:** `https://www.cpc.ncep.noaa.gov`
- **Propósito:** Scraping de la tabla del Oceanic Niño Index (ONI v5) para la clasificación de anomalías de temperatura marina.
- **Uso en Código:** Utilizado por `src_ingest_oni.py` para la extracción mensual de la tabla climática de la NOAA.

### 3. `gobernacion_valle`
- **Tipo:** `HTTP`
- **Host:** `https://datosabiertos.valledelcauca.gov.co`
- **Propósito:** Descarga de microdatos en formato CSV de la evaluación agropecuaria departamental (cultivos permanentes y transitorios).
- **Uso en Código:** Utilizado por `src_ingest_crops.py` y `cultivos.py`.

### 4. `datos_gov`
- **Tipo:** `HTTP`
- **Host:** `https://www.datos.gov.co`
- **Propósito:** Extracción del maestro oficial de municipios de Colombia con sus códigos DIVIPOLA.
- **Uso en Código:** Utilizado por `src_ingest_municipios.py`.

### 5. `ckan_valle`
- **Tipo:** `HTTP`
- **Host:** `https://datosabiertos.valledelcauca.gov.co/api/3`
- **Propósito:** API REST CKAN para extracción de opiniones y comentarios de la ciudadanía sobre el sector agrícola.
- **Uso en Código:** Utilizado por `src_ingest_ckan_comentarios.py`.

### 6. `google_cloud_default`
- **Tipo:** `Google Cloud`
- **Propósito:** Autenticación con Google Cloud Project (BigQuery, GCS, BigQuery ML).
- **Uso en Código:** Operadores de BigQuery en la carga de tablas Bronze, Silver y Gold Espacial.

---

## 3. Configuración en Archivos Locales (`connections.yaml`)

Para entornos de desarrollo local con Astronomer CLI o execution script, las conexiones están declaradas en `connections.yaml`:

```yaml
sipsa_dane:
  conn_type: http
  host: https://www.dane.gov.co

noaa_oni:
  conn_type: http
  host: https://www.cpc.ncep.noaa.gov

gobernacion_valle:
  conn_type: http
  host: https://datosabiertos.valledelcauca.gov.co

datos_gov:
  conn_type: http
  host: https://www.datos.gov.co

ckan_valle:
  conn_type: http
  host: https://datosabiertos.valledelcauca.gov.co/api/3
```
