# GDV Datastack Template

Plantilla para desplegar una canalización de datos de IPM (Índice de Pobreza Multidimensional) usando **Apache Airflow**, **dbt** y **BigQuery**. El repositorio incluye todo lo necesario para:

- Levantar un entorno local de Airflow (vía Docker Compose) con las dependencias correctas.
- Ingerir archivos Excel alojados en Google Cloud Storage (capa **raw**).
- Estandarizar y limpiar los datos en BigQuery (capa **bronze**).
- Generar dimensiones y la tabla de hechos con dbt (capas **dims** y **gold**).

---

## Arquitectura de Alto Nivel

```
GCS (Excel IPM) ──► DAGs de carga                              ──► BigQuery RAW
                    (`gdv_load_*`)                                (`gdv_ipm_sisben_raw`)

BigQuery RAW ──► DAGs de limpieza                            ──► BigQuery BRONZE
                 (`gdv_clean_*`)                                (`gdv_ipm_sisben_bronze`)

BigQuery BRONZE ──► DAGs dbt                                   ──► BigQuery DIMS / GOLD
                    (`gdv_build_dim_*`, `gdv_build_fact_*`)        (`gdv_ipm_sisben_dims`, `gdv_ipm_sisben_gold`)
```

Airflow orquesta todas las etapas; los scripts Python bajo `gdv_general_dbt_dag/modules` encapsulan la lógica de manipulación de datos, y dbt se encarga de modelar las tablas analíticas.

---

## Requisitos Previos

- Docker y Docker Compose.
- Google Cloud Service Account con acceso a BigQuery y al bucket GCS que contiene los archivos fuente.
- Variable de entorno `SA_GCP_PATH` apuntando al archivo `sa.json` local (montada por Docker Compose).
- Python 3.10+ si se requiere usar Poetry o tooling local (opcional).

---

## Puesta en Marcha Rápida

1. **Configurar credenciales**
   - Colocar el JSON de la service account en una ruta local.
   - Exportar `SA_GCP_PATH=/ruta/absoluta/sa.json`.

2. **Levantar Airflow**
   ```bash
   make run       # usa astro_runtime/docker-compose.yml
   ```
   Airflow queda disponible en `http://localhost:8081` (usuario/contraseña: `admin` / `admin`).

3. **Cargar y ejecutar DAGs manualmente**
   - Activar los DAGs desde la UI de Airflow y ejecutarlos en el orden indicado en el *Flujo Operativo sugerido*.

4. **Detener servicios al finalizar**
   ```bash
   make stop
   ```

---

## Componentes Principales

### 1. Entorno y orquestación

| Archivo / Carpeta | Rol |
| --- | --- |
| `astro_runtime/docker-compose.yml` | Define servicios `webserver`, `scheduler`, `triggerer` y `postgres` para Airflow. Monta los DAGs, credenciales y logs. |
| `astro_runtime/Dockerfile` | Imagen base `apache/airflow:2.10.5` con compiladores y dependencias para BigQuery/dbt. |
| `astro_runtime/requirements.txt` | Paquetes Python instalados en los contenedores (google-cloud-*, dbt, pandas, etc.). |
| `astro_runtime/include/dbt/profiles.yml` | Configuración de dbt apuntando a BigQuery (`datagov-473122`). |
| `astro_runtime/include/sa.json` | **No versionado**. Se monta desde `SA_GCP_PATH` para credenciales GCP. |
| `Makefile` | Comandos `run`, `stop`, `restart` que envuelven Docker Compose. |

### 2. DAGs de Airflow (`gdv_general_dbt_dag/*.py`)

Los DAGs están agrupados por etapa. Todos usan `start_date=2024-01-01`, `catchup=False` y se ejecutan bajo demanda (sin `schedule_interval`).

#### Ingesta a RAW

| DAG | Archivo | Descripción | Tasks principales |
| --- | --- | --- | --- |
| `gdv_load_ipm_raw_data_dag` | `load_rawdata_ipm_dag.py` | Descarga el Excel de IPM desde GCS y lo carga en la tabla `ipm_sisben_wide` de `gdv_ipm_sisben_raw`. | `PythonOperator(load_data_from_gcs)` → `load_ipm_wide_from_gcs_excel`. |
| `gdv_load_idictionary_raw_data_dag` | `load_rawdata_dictionary_dag.py` | Descarga el diccionario de datos desde GCS y genera `dictionary_wide` en `gdv_ipm_sisben_raw`. Incluye limpieza de cachés `.pyc`. | `PythonOperator(load_data_from_gcs)` → `load_dictionary_from_gcs_excel`. |

#### Limpieza a BRONZE

| DAG | Archivo | Descripción | Tasks |
| --- | --- | --- | --- |
| `gdv_clean_ipm_bronze_dag` | `clean_rawdata_ipm_dag.py` | Estandariza códigos y columnas numéricas de IPM; crea `ipm_sisben_clean` en `gdv_ipm_sisben_bronze`. | `PythonOperator(clean_data_to_bronze)` → `run_clean_to_bronze`. |
| `gdv_clean_dictionary_bronze_dag` | `clean_rawdata_dictionary_dag.py` | Normaliza textos del diccionario (sin tildes) hacia `dictionary_clean`. | `PythonOperator(clean_dictionary_to_bronze)` → `run_clean_dictionary_to_bronze`. |

#### Construcción de Dimensiones (dbt)

Todos verifican que `gdv_ipm_sisben_dims` exista en `us-central1` antes de correr dbt.

| DAG | Archivo | Modelo dbt | Notas |
| --- | --- | --- | --- |
| `gdv_build_dim_municipio_dag` | `build_dim_municipio_dag.py` | `dim_municipio` | `ensure_dims_dataset` → `dbt run` → `dbt test`. |
| `gdv_build_dim_nivel_pobreza_dag` | `build_dim_nivel_pobreza_dag.py` | `dim_nivel_pobreza` | Igual estructura. |
| `gdv_build_dim_categorias_ipm_dag` | `build_dim_categorias_ipm_dag.py` | `dim_categoria_ipm` | Ensambla indicadores I1...I15 por municipio. |
| `gdv_build_dim_tiempo_dag` | `build_dim_tiempo_dag.py` | `dim_tiempo` | Toma variables de Airflow (`ipm_dim_tiempo_anio`, `ipm_dim_tiempo_periodo`) o usa defaults (`2025`, `2025-10`). |

#### Construcción de la tabla de hechos (dbt)

| DAG | Archivo | Descripción |
| --- | --- | --- |
| `gdv_build_fact_ipm_dag` | `build_fact_ipm_dag.py` | Garantiza el dataset `gdv_ipm_sisben_gold`, luego ejecuta `dbt run/test --select gdv_ipm_sisben_fact`. Combina dimensiones para recrear la tabla wide final. |

### 3. Módulos Python de soporte (`gdv_general_dbt_dag/modules`)

| Archivo | Propósito | Destino |
| --- | --- | --- |
| `load_rawdata_ipm.py` | Descarga Excel IPM desde GCS, detecta encabezados, normaliza columnas y sube a BigQuery RAW (`ipm_sisben_wide`). Incluye normalización de códigos municipales y manejo de valores numéricos. | RAW |
| `load_rawdata_dictionary.py` | Descarga el Excel del diccionario, identifica encabezados flexibles y construye una tabla ancha (`dictionary_wide`). | RAW |
| `clean_rawdata_ipm.py` | Genera SQL para limpiar/estandarizar `ipm_sisben_wide` en `ipm_sisben_clean`. Quita caracteres no numéricos, normaliza `Municipio`, asegura datasets en `us-central1`. | BRONZE |
| `clean_rawdata_dictionary.py` | Limpia tildes y espacios del diccionario (`dictionary_clean`). Verifica/crea dataset bronze. | BRONZE |
| `__init__.py` | Marca el directorio como paquete Python. | — |

### 4. Proyecto dbt (`gdv_general_dbt_dag/dbt`)

| Archivo / Carpeta | Contenido |
| --- | --- |
| `dbt_project.yml` | Configuración del proyecto; fija materialización por defecto `view` para `staging`, base de datos `datagov-473122`. |
| `models/sources.yml` | Registra fuentes `bronze` y `dims` para ref/relaciones. |
| `models/staging/dim_municipio.sql` | Tabla en `gdv_ipm_sisben_dims` con nombre y población por municipio. |
| `models/staging/dim_nivel_pobreza.sql` | Métricas IPM pobre/no pobre por municipio. |
| `models/staging/dim_categoria_ipm.sql` | Despivotado de indicadores I1-I15 (con/sin privación). |
| `models/staging/dim_tiempo.sql` | Dimensión mínima de tiempo basada en variables. |
| `models/fact/gdv_ipm_sisben_fact.sql` | Recompone la tabla wide final en `gdv_ipm_sisben_gold`. |
| `macros/generate_schema_name.sql` | Macro para mantener el esquema objetivo configurado por modelo. |
| `target/` | Salida generada por dbt (artefactos). |

### 5. Configuración de proyecto y tooling

| Archivo | Rol |
| --- | --- |
| `pyproject.toml` | Metadatos y dependencias Poetry. Permite instalar Airflow, dbt y utilidades localmente. |
| `poetry.lock` | Versiones fijadas (si se trabaja con Poetry). |

---

## Flujo Operativo Sugerido

1. **Ingesta a RAW**
   - Ejecutar `gdv_load_ipm_raw_data_dag`.
   - Ejecutar `gdv_load_idictionary_raw_data_dag` (opcional según necesidades).

2. **Limpieza a BRONZE**
   - Ejecutar `gdv_clean_ipm_bronze_dag`.
   - Ejecutar `gdv_clean_dictionary_bronze_dag`.

3. **Construcción de Dimensiones**
   - Ejecutar los DAGs `gdv_build_dim_*` según la dimensión requerida.
   - Verificar los tests de dbt (se ejecutan automáticamente en cada DAG).

4. **Tabla de hechos**
   - Ejecutar `gdv_build_fact_ipm_dag` para poblar `gdv_ipm_sisben_gold.gdv_ipm_sisben_fact`.

La secuencia puede automatizarse encadenando DAGs desde Airflow o usando `TriggerDagRunOperator` para obtener orquestación end-to-end.

---

## Configuraciones Importantes

- **Credenciales GCP:** Todos los módulos definen `SA_PATH = "/opt/airflow/include/sa.json"`. Docker Compose debe montar la ruta correcta mediante la variable `SA_GCP_PATH`.
- **Ubicaciones BigQuery:** Los scripts crean datasets en `us-central1`. Si la región difiere, se deben actualizar las constantes `LOCATION` / `DATASET_LOCATION`.
- **URIs de GCS:** Las constantes `GCS_URI` en `load_rawdata_ipm_dag.py` y `load_rawdata_dictionary_dag.py` deben apuntar a los archivos reales.
- **Variables de Airflow:** Para `dim_tiempo` es posible configurar `ipm_dim_tiempo_anio` (int) y `ipm_dim_tiempo_periodo` (string) en la UI (Admin → Variables).
- **Parámetro DEBUG:** `modules/load_rawdata_ipm.py` define `DEBUG = True` para diagnósticos; conviene desactivarlo en entornos productivos.

---

## Buenas Prácticas y Extensiones

- **Pruebas:** Se pueden añadir tests de dbt adicionales (unique/not null) en `models/tests`.
- **Observabilidad:** Airflow guarda logs en `astro_runtime/logs`. Es posible montarlos externamente para persistencia.
- **CI/CD:** El `Makefile` sirve como punto de integración en pipelines (por ejemplo, `make run` en jobs de integración).
- **Customización dbt:** Nuevos modelos pueden ubicarse en `models/staging` o `models/marts`, actualizando los DAGs correspondientes para incluirlos.

---

## Referencia Rápida

- **Airflow UI:** `http://localhost:8081`
- **Usuarios por defecto:** `admin` / `admin`
- **Datasets BigQuery:**
  - RAW: `gdv_ipm_sisben_raw`
  - BRONZE: `gdv_ipm_sisben_bronze`
  - DIMS: `gdv_ipm_sisben_dims`
  - GOLD: `gdv_ipm_sisben_gold`
- **Tablas claves:**
  - `ipm_sisben_wide`, `dictionary_wide`
  - `ipm_sisben_clean`, `dictionary_clean`
  - `dim_municipio`, `dim_nivel_pobreza`, `dim_categoria_ipm`, `dim_tiempo`
  - `gdv_ipm_sisben_fact`

Esta documentación cubre el paso a paso del proyecto y el rol de cada archivo. Los parámetros marcados (URIs, IDs de proyecto, ubicaciones) deben adaptarse al entorno concreto donde se despliegue.
