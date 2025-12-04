# Descripción General de la Plantilla

## 1. Descripción de la Plantilla

Esta plantilla encapsula un entorno de orquestación basado en **Apache Airflow**, **dbt (Data Build Tool)** y **Google BigQuery**, dirigido al procesamiento de datos y que se puede ajustar o manipular para cualquier fuente interesada en ingresar al proyecto de datastack.

La arquitectura está diseñada para ser **altamente flexible y extensible**, permitiendo que nuevas fuentes de datos se integren siguiendo el patrón establecido sin necesidad de modificar el código base. La configuración centralizada permite agregar, modificar o desactivar fuentes simplemente editando archivos de configuración, manteniendo el código fuente intacto y garantizando la portabilidad entre diferentes proyectos y ambientes.

## 2. Componente Principal: `gdv_general_dbt_dag/`

La carpeta `gdv_general_dbt_dag` es el **núcleo de la plantilla** y contiene todo el código que se despliega tanto en el entorno de QA como de Producción en **Google Cloud Composer**, garantizando consistencia entre ambientes. Este mismo código también se ejecuta en el entorno local de desarrollo, asegurando que lo que funciona localmente funcionará en producción.

### 2.1 Propósito General

Esta carpeta encapsula un **sistema completo de procesamiento de datos** que automatiza el flujo desde la extracción de múltiples fuentes heterogéneas hasta la generación de modelos analíticos listos para consumo. El sistema implementa un flujo de trabajo ETL/ELT robusto siguiendo la arquitectura de **Medallion (Bronze-Silver-Gold)**, garantizando trazabilidad, calidad y disponibilidad de los datos.

### 2.2 Funcionalidad Principal

La carpeta `gdv_general_dbt_dag` se encarga de:

- **Orquestación de pipelines de datos**: Define y ejecuta workflows completos que coordinan la extracción, carga y transformación de datos desde múltiples fuentes (APIs REST, Google Drive, archivos Excel/CSV) hacia BigQuery.

- **Extracción y carga de datos**: Implementa la lógica necesaria para conectarse a diferentes fuentes de datos, extraer información y cargarla a Google Cloud Storage o directamente a BigQuery en la capa Bronze, preservando los datos en su forma original.

- **Transformación y modelado de datos**: Ejecuta transformaciones SQL modulares y versionadas que limpian, normalizan, validan y enriquecen los datos, transformándolos desde la capa Bronze (raw) hacia Silver (cleaned) y finalmente hacia Gold (curated), donde se generan tablas de hechos y dimensiones optimizadas para análisis.

- **Configuración centralizada**: Proporciona un sistema de configuración unificado que permite ajustar todos los parámetros del sistema (proyectos GCP, buckets, datasets, URLs de fuentes, nombres de tablas) sin necesidad de modificar código fuente, facilitando la portabilidad entre diferentes proyectos y ambientes.

### 2.3 Arquitectura y Organización

La carpeta está organizada de manera modular, separando claramente las responsabilidades:

- **Orquestación**: Los DAGs de Airflow definen el flujo de trabajo y coordinan la ejecución de las diferentes etapas del pipeline, pero no contienen lógica de negocio.

- **Lógica de negocio**: Los módulos Python encapsulan toda la lógica específica de cada fuente de datos (extracción, procesamiento, validación, carga), permitiendo reutilización y facilitando el mantenimiento.

- **Transformaciones**: El proyecto dbt contiene todas las transformaciones SQL organizadas por capas, permitiendo versionado, documentación, testing y reutilización de lógica de transformación compleja.

- **Configuración**: Un sistema centralizado de configuración permite ajustar el comportamiento del sistema sin modificar código, garantizando que el mismo código funcione en diferentes ambientes (desarrollo, QA, producción) y proyectos GCP.

### 2.4 Características Clave

- **Modularidad**: Cada fuente de datos es completamente independiente, permitiendo agregar nuevas fuentes sin afectar las existentes. El sistema está diseñado para escalar horizontalmente agregando nuevas fuentes siguiendo el patrón establecido.

- **Separación de responsabilidades**: La orquestación, la lógica de negocio y las transformaciones están claramente separadas, facilitando el mantenimiento, el testing y la colaboración entre diferentes roles (desarrolladores, analistas de datos).

- **Parametrización completa**: Todo el sistema está parametrizado mediante configuración centralizada, eliminando valores hardcodeados y permitiendo ajustar el comportamiento sin modificar código fuente.

- **Consistencia entre ambientes**: El mismo código se ejecuta en desarrollo local, QA y producción, garantizando que lo que funciona localmente funcionará en producción y reduciendo sorpresas en despliegues.

- **Orquestación granular**: Cada fuente tiene workflows especializados para cada etapa del pipeline (ingest → load → transform), permitiendo reprocesamiento independiente y mejor manejo de errores. Si una transformación falla, se puede reprocesar sin necesidad de re-ejecutar toda la cadena.

### 2.5 Flujo de Trabajo

El sistema procesa datos siguiendo un flujo estándar de tres etapas:

1. **Ingestión**: Extrae datos desde las fuentes (APIs, Google Drive, archivos) y los almacena en Google Cloud Storage o los carga directamente a BigQuery en la capa Bronze.

2. **Carga**: Procesa los datos extraídos y los carga a BigQuery en la capa Bronze, o crea tablas externas que apuntan directamente a los archivos en GCS, preservando los datos en su forma original.

3. **Transformación**: Ejecuta modelos dbt que transforman los datos desde Bronze (raw) hacia Silver (cleaned) aplicando limpieza, normalización y validación, y finalmente hacia Gold (curated) generando tablas de hechos y dimensiones optimizadas para consumo analítico.

Este flujo está completamente automatizado y orquestado mediante DAGs de Airflow, que coordinan la ejecución de cada etapa, manejan dependencias, gestionan reintentos y proporcionan monitoreo y trazabilidad completa.

### 2.6 Extensibilidad

El sistema está diseñado para facilitar la incorporación de nuevas fuentes de datos. Para agregar una nueva fuente, solo se requiere:

- Crear módulos Python con la lógica específica de extracción, procesamiento y carga.
- Definir DAGs de Airflow que orquesten el flujo de trabajo de la nueva fuente.
- Crear modelos dbt para las transformaciones específicas (si aplica).
- Agregar la configuración de la nueva fuente al archivo de configuración centralizado.

**No se requiere modificar código existente**, solo agregar nuevos componentes siguiendo el patrón establecido, garantizando que las nuevas fuentes se integren sin afectar las existentes.

## 3. Arquitectura de Datos

La plantilla implementa la arquitectura de **Medallion (Bronze-Silver-Gold)** con tres capas bien definidas:

- **Bronze (Raw)**: Almacena los datos en su forma original, sin transformaciones. Sirve como respaldo histórico y punto de partida para reprocesamiento. Los datos se cargan tal como vienen de las fuentes, preservando la información original para auditoría y recuperación.

- **Silver (Cleaned)**: Datos limpiados, validados y normalizados. Se aplican transformaciones para corregir errores, estandarizar formatos, validar integridad referencial y enriquecer con metadatos. Los datos están listos para análisis pero aún no agregados.

- **Gold (Curated)**: Datos agregados, enriquecidos y optimizados para consumo final. Se crean tablas de hechos y dimensiones siguiendo modelos dimensionales, optimizados para consultas analíticas y herramientas de BI. Los datos están listos para dashboards, reportes y análisis avanzados.

## 4. Stack Tecnológico

| Componente | Tecnología | Versión | Propósito |
|------------|-----------|---------|-----------|
| **Orquestación** | Apache Airflow | 2.10.5+ | Orquestación y programación de workflows, manejo de dependencias, reintentos y monitoreo |
| **Transformación** | dbt (Data Build Tool) | 1.9.0+ | Transformaciones SQL modulares, versionadas, documentadas y testeables |
| **Data Warehouse** | Google BigQuery | - | Almacenamiento y procesamiento de datos a escala, con capacidades de SQL y análisis avanzado |
| **Data Lake** | Google Cloud Storage | - | Almacenamiento de archivos raw (CSV, Excel, JSON) con versionado y lifecycle policies |
| **Lenguaje** | Python | 3.10+ | Lógica de extracción y carga, integración con APIs y procesamiento de datos |
| **Configuración** | YAML | - | Configuración centralizada y parametrizada, fácil de leer y modificar |

### Integraciones

- **Google Cloud Composer**: Entorno de Airflow gestionado en GCP, proporciona escalabilidad automática, alta disponibilidad y integración nativa con otros servicios de GCP.
- **Google Drive API**: Extracción de archivos desde Google Drive, permitiendo automatizar la descarga de archivos compartidos en Google Workspace.
- **REST APIs**: Consumo de APIs REST para extracción de datos, con soporte para autenticación, paginación y manejo de errores.
- **BigQuery**: Data Warehouse para almacenamiento y análisis, con capacidades de procesamiento distribuido y consultas SQL estándar.

---

**Última actualización**: 2025-01-XX  
**Versión de la plantilla**: 3.0  
**Mantenedor**: Secretaría de Planeación Municipal

