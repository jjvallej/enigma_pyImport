# GDV General DBT DAG

Este repositorio contiene los flujos de trabajo (DAGs) de Airflow y modelos dbt para los pipelines de datos de la Secretaría de Planeación. El código está diseñado para funcionar tanto en un entorno de desarrollo local como en producción (Google Cloud Composer).

## Estructura del Repositorio

El repositorio está organizado para separar la lógica de negocio (módulos), la orquestación (DAGs) y las transformaciones (dbt).

```
gdv_general_dbt_dag/
├── dags_evaplan/           # DAGs específicos para la fuente Evaplan
├── dags_idc/               # DAGs específicos para la fuente IDC
├── dags_ipm/               # DAGs específicos para la fuente IPM
├── dbt/                    # Proyecto dbt (modelos SQL, tests, seeds)
│   ├── profiles.yml        # Configuración de conexión a BigQuery (Local y Prod)
│   └── ...
├── modules/                # Código Python reutilizable (Lógica de extracción y carga)
│   ├── evaplan/            # Funciones para Evaplan
│   ├── idc/                # Funciones para IDC
│   └── ipm/                # Funciones para IPM
└── requirements.txt        # Dependencias de Python necesarias
```

## Gestión de Ambientes (Local vs. Producción)

El código está preparado para detectar automáticamente el entorno o configurarse mediante variables de entorno.

### 1. Ambiente Productivo (Cloud Composer)
En Cloud Composer, **no es necesario configurar nada manualmente**.
-   **Python**: Los clientes de BigQuery y Storage (`_bq_client`, `_gcs_client`) detectan automáticamente las credenciales de la cuenta de servicio del entorno.
-   **dbt**: El perfil `dev` en `dbt/profiles.yml` usa el método `oauth`, que aprovecha la identidad de Composer.

### 2. Ambiente de Desarrollo Local
Para ejecutar el código en tu máquina local, necesitas una llave de cuenta de servicio (JSON) con
## 4. Configuración y Ambientes

El proyecto ha sido refactorizado para manejar la configuración a través de variables de entorno, facilitando el despliegue en diferentes ambientes (Desarrollo, QA, Producción) sin cambiar el código.

### 4.1. Módulo de Configuración (`modules/config.py`)
Todas las constantes y configuraciones clave están centralizadas en `modules/config.py`. Este archivo lee variables de entorno y provee valores por defecto seguros para desarrollo.

### 4.2. Variables de Entorno
Para cambiar el comportamiento entre ambientes, puedes configurar las siguientes variables de entorno en Cloud Composer o en tu entorno local:

| Variable | Descripción | Valor por Defecto |
|----------|-------------|-------------------|
| `ENVIRONMENT` | Ambiente actual (`dev`, `prod`, `local`) | `dev` |
| `GCP_PROJECT` | ID del proyecto de Google Cloud | `datagov-473122` |
| `GCS_BUCKET_NAME` | Nombre del bucket para Data Lake | `datalake_gdv_dev` |
| `BQ_DATASET_BRONZE` | Dataset para capa Bronze | `bronze_dpt_planeacion_municipal_dev` |
| `BQ_DATASET_SILVER` | Dataset para capa Silver | `silver_dpt_planeacion_municipal_dev` |
| `BQ_DATASET_GOLD` | Dataset para capa Gold | `gold_dpt_planeacion_municipal_dev` |

### 4.3. Carpeta `dbt/`
La carpeta `dbt/` contiene el proyecto de **Data Build Tool (dbt)**.
*   **¿Qué es?**: Es la herramienta estándar para realizar transformaciones de datos (T) dentro del Data Warehouse (BigQuery).
*   **¿Para qué sirve?**: En lugar de escribir SQL complejo dentro de los DAGs de Python, dbt permite escribir modelos SQL modulares, documentados y testeables.
*   **Estructura**:
    *   `models/`: Contiene los archivos `.sql` con la lógica de transformación (Bronze -> Silver -> Gold).
    *   `profiles.yml`: Configura la conexión a BigQuery.

## 5. Guía de Desarrollo (Developer Journey)

Hemos estandarizado el flujo de trabajo utilizando un `Makefile` para facilitar las tareas comunes.

### 5.1. Setup Inicial
1.  Clona el repositorio.
2.  Configura tu entorno local:
    ```bash
    make setup
    ```
    Esto instalará las dependencias de Python y dbt.

### 5.2. Ciclo de Desarrollo (Local)
1.  **Crea una rama**: `git checkout -b feature/mi-nueva-feature`
2.  **Desarrolla**: Modifica tus DAGs en `dags_*/` o modelos dbt en `dbt/`.
3.  **Prueba**:
    *   **Linting**: Verifica que tu código cumpla con los estándares de estilo.
        ```bash
        make lint
        ```
    *   **Tests**: Ejecuta pruebas de integridad para asegurar que no rompiste nada.
        ```bash
        make test
        ```
    *   **Ejecución Manual**:
        *   Python: `export GOOGLE_APPLICATION_CREDENTIALS="path/to/key.json"`
        *   dbt: `dbt run --target local`

### 5.3. Despliegue a Producción (CI/CD)
El despliegue está automatizado con GitHub Actions.

1.  **Pull Request**: Sube tus cambios y abre un PR hacia la rama `develop` (para ambiente de pruebas) o `main` (para producción).
2.  **CI Checks**: GitHub ejecutará automáticamente `make lint` y `make test`. Si fallan, no podrás hacer merge.
3.  **Merge & Deploy**:
    *   Al hacer merge a `develop` -> Despliega automáticamente a **Dev**.
    *   Al hacer merge a `main` -> Despliega automáticamente a **Prod**.

**Nota**: El despliegue usa `gcloud storage rsync` para sincronizar tu código con el bucket de Cloud Composer.

## 6. Estructura del Código Refactorizado

*   `dags_*/`: Contienen la definición de los workflows de Airflow.
*   `modules/`: Contiene la lógica de negocio en Python puro.
    *   `config.py`: Configuración centralizada (selecciona automáticamente Dev/Prod).
    *   `gcp_utils.py`: Funciones reutilizables para conectar a GCS y BigQuery.
*   `dbt/`: Proyecto de transformación de datos.
*   `tests/`: Tests unitarios y de integridad.
*   `Makefile`: Comandos de utilidad.
*   `.github/workflows/`: Configuración de CI/CD.
## Manejo de Módulos e Imports

Para mantener el código limpio y modular, la lógica pesada (extracciones de API, lectura de Excel, carga a BigQuery) se encuentra en la carpeta `modules/`. Los DAGs en `dags_*/` solo se encargan de orquestar estas funciones.

**¿Cómo funcionan los imports?**
Dado que Airflow a veces tiene dificultades para importar módulos desde carpetas personalizadas, en cada DAG se incluye un bloque de código para agregar la raíz del proyecto al `PYTHONPATH` dinámicamente:

```python
import sys
import os
# Agregar el directorio raíz del proyecto al path para importar módulos
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

from modules.fuente.modulo import funcion
```

Esto asegura que `from modules...` funcione correctamente tanto localmente como en Composer, sin importar dónde se ubique la carpeta de DAGs.

## Despliegue en Cloud Composer

Para desplegar en producción:

1.  **Copiar Archivos**: Sube todo el contenido de la carpeta `gdv_general_dbt_dag` dentro de la carpeta `dags/` en el bucket de Cloud Storage de tu entorno Composer.
    *   Ruta final: `gs://<bucket>/dags/gdv_general_dbt_dag/...`

2.  **Instalar Dependencias**: Asegúrate de que los paquetes en `requirements.txt` estén instalados en el entorno de Composer (PyPI Packages).
    *   `apache-airflow`
    *   `dbt-bigquery`
    *   `pandas`
    *   `google-cloud-bigquery`
    *   `google-cloud-storage`
    *   `openpyxl`

3.  **Verificar**: Revisa la interfaz de Airflow. Los DAGs deberían aparecer sin errores de importación.
