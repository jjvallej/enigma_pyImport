# Astro Runtime

Esta plantilla encapsula un entorno de orquestación local basado en Apache Airflow, dbt y BigQuery, dirigido al procesamiento de datos del Índice de Pobreza Multidimensional (IPM). Contiene la definición de servicios Docker que recrean la infraestructura recomendada por Astronomer, una imagen personalizada con dependencias de BigQuery/GCS/dbt, credenciales y perfiles para ejecutar transformaciones, además de los DAGs que cargan, limpian y modelan la información. Con ella se construye y prueba localmente toda la canalización: ingestión desde Google Cloud Storage, normalización de tablas en BigQuery y modelado analítico por medio de dbt.

Documentación detallada de la carpeta `astro_runtime`, la cual contiene la infraestructura local basada en Astro Runtime para ejecutar Apache Airflow junto con dbt y los DAGs del proyecto.

---

## Propósito General

`astro_runtime` encapsula todo lo necesario para levantar el stack local de Airflow recomendado por Astronomer. Proporciona:

- Definición de servicios en Docker (webserver, scheduler, triggerer y base de datos Postgres).
- Imagen base personalizada con las dependencias requeridas (BigQuery, Google Cloud Storage, dbt, pandas, etc.).
- Montaje de DAGs, credenciales y archivos de configuración dentro de los contenedores.
- Gestión de logs y plugins.

De esta forma, la carpeta garantiza un entorno reproducible y alineado con las mejores prácticas de Astronomer para desarrollar y probar la solución de datos.

---

## Estructura y Detalle de Archivos

### 1. `docker-compose.yml`

Archivo principal que describe los servicios Docker necesarios:

- **Servicio `postgres`**
  - Imagen oficial `postgres:13`.
  - Variables de entorno para credenciales básicas (`airflow`).
  - Volumen persistente `postgres_data` que conserva el estado de la base de datos (metadatos de Airflow) entre reinicios.
  - `healthcheck` que garantiza que otros servicios arranquen solo cuando Postgres está listo.

- **Servicio `webserver`**
  - Se construye con el `Dockerfile` de la carpeta (contexto `.`).
  - Depende de `postgres` para asegurar la disponibilidad de la base de datos.
  - Variables de entorno clave:
    - `AIRFLOW__CORE__EXECUTOR=LocalExecutor` para permitir ejecución paralela.
    - `AIRFLOW__CORE__LOAD_EXAMPLES="False"` para evitar los DAGs de ejemplo.
    - `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` con la URL de conexión a Postgres.
    - `DBT_PROFILES_DIR=/opt/airflow/include/dbt` para que dbt encuentre `profiles.yml`.
    - `GOOGLE_APPLICATION_CREDENTIALS=/opt/airflow/include/sa.json` para las credenciales GCP.
    - Variables `_AIRFLOW_WWW_USER_*` para crear el usuario administrador en la UI.
  - Puertos: expone `8080` del contenedor en `8081` local.
  - Volúmenes montados:
    - `../gdv_general_dbt_dag` con los DAGs y el proyecto dbt.
    - `./include` para credenciales y perfiles.
    - `${SA_GCP_PATH}` montado como `sa.json` de solo lectura.
    - Carpetas locales `./logs` y `./plugins` para persistir logs y extensiones.
  - Comando de arranque:
    1. `airflow db upgrade` para migrar la base de datos.
    2. Creación del usuario admin (se ignora error si ya existe).
    3. Ejecución del proceso `airflow webserver`.

- **Servicio `scheduler`**
  - Comparte la misma imagen (`Dockerfile`).
  - Misma configuración de entorno (concentrada en el scheduler).
  - Montajes idénticos para DAGs, include, credenciales, logs y plugins.
  - Comando: `airflow db upgrade` seguido de `airflow scheduler`.

- **Servicio `triggerer`**
  - Encargado de los triggers asíncronos en Airflow 2.
  - Misma imagen, credenciales y montajes.
  - Comando simple: `airflow triggerer`.

- **Volumen `postgres_data`**
  - Declarado al final del archivo para persistir la base de datos de Postgres.

Este `docker-compose.yml` es indispensable: sin él no existiría la definición de servicios que levantan Airflow y su base de datos, ni los montajes de DAGs, credenciales y configuraciones externas.

### 2. `Dockerfile`

Define la imagen personalizada utilizada por `webserver`, `scheduler` y `triggerer`:

1. Imagen base `apache/airflow:2.10.5` sobre arquitectura `linux/amd64`.
2. Cambia a usuario `root` para instalar dependencias del sistema:
   - Actualiza mirrors Debian.
   - Instala `gcc`, `g++`, `libpq-dev` y `git`, necesarios para compilar paquetes Python y conectar con Postgres.
3. Regresa al usuario `airflow`.
4. Copia `requirements.txt` al contenedor y ejecuta `pip install` con cada dependencia listada.

Este `Dockerfile` asegura que la imagen contenga todas las librerías que los DAGs y dbt requieren, evitando errores por falta de paquetes en tiempo de ejecución.

### 3. `requirements.txt`

Lista exhaustiva de dependencias Python que se instalan dentro de la imagen:

- `google-cloud-bigquery[pandas,pyarrow]`, `google-cloud-storage`: clientes de BigQuery y GCS con soporte de pandas y Parquet.
- `requests`, `pandas`, `openpyxl`: lectura/manipulación de datos, especialmente Excel.
- `psycopg2-binary`: conector Postgres usado por Airflow.
- `astronomer-cosmos`: integración entre Airflow y dbt.
- `hydra-core`: configuración avanzada si se utilizara en el proyecto.
- `dbt-bigquery`, `dbt-core`: motor dbt dirigido a BigQuery.

Sin este archivo, el contenedor carecería de bibliotecas críticas y los DAGs fallarían al acceder a BigQuery, GCS o ejecutar dbt.

### 4. Carpeta `include/`

Contiene recursos compartidos montados en los contenedores Airflow.

- `include/dbt/profiles.yml`
  - Perfil de dbt (nombre `gdv_general_dbt_dag`) con target `dev`.
  - Configura el método `service-account`, la ruta del keyfile (`/opt/airflow/include/sa.json`), el proyecto `datagov-473122`, el dataset por defecto `gdv_ipm_sisben_raw` y la región `us-central1`.
  - Permite que los comandos dbt ejecutados dentro de los contenedores accedan a BigQuery usando la cuenta de servicio.

- `include/sa.json`
  - Archivo de credenciales de la service account de Google Cloud. No se versiona; se monta desde la máquina host mediante la variable `SA_GCP_PATH`.
  - Requisito para autenticar todos los accesos a BigQuery y GCS.

### 5. Carpeta `logs/`

Directorio montado en los contenedores `webserver`, `scheduler` y `triggerer` para persistir los logs generados por Airflow. Está subdividido por `dag_id` y `run_id`, lo que facilita auditoría y depuración. Al montarse desde el host:

- Los logs permanecen aunque los contenedores se detengan o reinicien.
- Permite revisar ejecuciones pasadas sin ingresar al contenedor.

Es una pieza clave para monitoreo y explotación de Airflow en local.

### 6. Carpeta `plugins/`

Directorio reservado para plugins de Airflow. Aunque actualmente puede estar vacío, mantiene la convención de Astronomer y permite agregar operadores, ganchos o macros personalizados en el futuro. Se monta en `webserver`, `scheduler` y `triggerer` para que cualquier extensión esté disponible en todos los procesos.

---

## Integración con el resto del proyecto

- Los DAGs y el proyecto dbt residen en `../gdv_general_dbt_dag`. El `docker-compose.yml` monta este directorio dentro de los contenedores en `/opt/airflow/dags/gdv_general_dbt_dag`, lo que permite que Airflow los detecte automáticamente.
- El `Makefile` a nivel de repositorio ejecuta comandos sobre esta carpeta (`make run`, `make stop`) para levantar o detener el stack completo. El Makefile depende directamente de la estructura aquí documentada.
- Las rutas y variables de entorno están alineadas con los módulos Python y DAGs; por ejemplo, `SA_PATH = "/opt/airflow/include/sa.json"` en los módulos corresponde al montaje del archivo de credenciales descrito en `docker-compose.yml`.

---

## Razones por las que cada elemento es necesario

| Elemento | Motivo |
| --- | --- |
| `docker-compose.yml` | Orquesta todos los servicios de Airflow y Postgres; sin él no habría infraestructura local coherente. |
| `Dockerfile` | Permite añadir dependencias específicas; la imagen oficial de Airflow no incluye dbt ni librerías GCS/BigQuery. |
| `requirements.txt` | Define exactamente qué paquetes se instalan; garantiza reproducibilidad y evita fallos por ausencia de librerías. |
| `include/dbt/profiles.yml` | dbt necesita un perfil configurado para conectarse a BigQuery con la service account. |
| `include/sa.json` | Credenciales GCP indispensables para acceder a datasets, buckets y ejecutar jobs en BigQuery. |
| `logs/` | Persistencia de registros de ejecución para auditoría, depuración y referencia histórica. |
| `plugins/` | Soporte a extensiones personalizadas de Airflow; estructura estándar para futuras necesidades. |

---

## Flujo de Arranque Resumido

1. Se exporta `SA_GCP_PATH` apuntando al JSON local de la cuenta de servicio.
2. `make run` ejecuta `docker compose up -d` dentro de `astro_runtime/`.
3. Docker Compose construye la imagen personalizada (usando `Dockerfile` y `requirements.txt`).
4. Se levantan `postgres`, `webserver`, `scheduler` y `triggerer`, con montajes a DAGs, include, logs y plugins.
5. Airflow queda disponible en `http://localhost:8081`; el usuario admin se crea automáticamente.

---

## Mantenimiento y Buenas Prácticas

- **Actualización de dependencias:** cualquier cambio en `requirements.txt` requiere reconstruir la imagen (`docker compose build` o `make restart`).
- **Rotación de credenciales:** reemplazar `sa.json` cuando cambien las credenciales de GCP; asegurar que `SA_GCP_PATH` apunte al archivo correcto.
- **Limpieza de logs:** revisar y limpiar `logs/` periódicamente si se ejecutan muchos DAGs o se comparten con otros miembros.
- **Extensiones futuras:** cualquier plugin personalizado debe ubicarse en `plugins/` para que Airflow lo cargue.

---

Esta documentación proporciona la visión completa de `astro_runtime`, la función de cada archivo y la razón por la que resulta esencial para ejecutar el proyecto.


