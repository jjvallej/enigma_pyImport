# Documentación: dbt docs

## 1. Información General

dbt docs es una funcionalidad nativa de dbt que genera documentación interactiva y automática del proyecto. Esta herramienta crea un sitio web que incluye información sobre modelos, fuentes, columnas, tests, dependencias y gráficos de lineage (linaje de datos).

## 2. Qué es dbt docs

dbt docs genera un sitio web estático que muestra:

- Documentación de todos los modelos dbt
- Información sobre fuentes (sources) definidas
- Descripción de columnas y sus tipos de datos
- Gráficos de lineage que muestran las dependencias entre modelos
- Resultados de tests ejecutados
- Documentación de macros y snapshots
- Búsqueda de modelos y columnas

## 3. Cómo Funciona

dbt docs funciona en dos pasos:

1. Generación: El comando `dbt docs generate` crea archivos JSON (manifest.json y catalog.json) que contienen toda la información del proyecto
2. Visualización: El comando `dbt docs serve` inicia un servidor web local para visualizar la documentación

## 4. Generar la Documentación

Para generar la documentación, ejecuta el siguiente comando desde el directorio del proyecto dbt:

```bash
cd gdv_general_dbt_dag/dbt
dbt docs generate
```

Este comando:
- Compila todos los modelos
- Genera el manifest.json con información sobre modelos, fuentes, tests, etc.
- Genera el catalog.json con información sobre columnas y tipos de datos desde BigQuery
- Crea archivos HTML estáticos en el directorio target/

## 5. Visualizar la Documentación Localmente

Para ver la documentación en tu navegador, ejecuta:

```bash
dbt docs serve
```

Esto iniciará un servidor web local (generalmente en http://localhost:8080) donde podrás navegar por toda la documentación.

## 6. Documentar Modelos

Para agregar documentación a los modelos, crea archivos YAML (schema.yml) en las carpetas de modelos. Estos archivos pueden contener:

- Descripción del modelo
- Descripción de cada columna
- Tests para validar datos
- Tags para organizar modelos

Ejemplo de estructura:

```yaml
version: 2

models:
  - name: nombre_del_modelo
    description: |
      Descripción detallada del modelo.
      Puede incluir múltiples líneas.
    columns:
      - name: nombre_columna
        description: Descripción de la columna
        data_type: STRING
      - name: otra_columna
        description: Otra descripción
        data_type: INT64
```

## 7. Ubicación de Archivos de Documentación

Los archivos de documentación (schema.yml) pueden ubicarse en cualquier carpeta dentro de models/. dbt los encontrará automáticamente. Es recomendable organizarlos por capa o fuente:

- models/gold/evaplan/schema.yml
- models/silver/idc/schema.yml
- models/bronze/schema.yml

## 8. Documentar Fuentes

Las fuentes ya están documentadas en models/sources.yml. Puedes agregar más información:

```yaml
sources:
  - name: bronze_evaplan
    description: Fuentes de datos raw de Evaplan en la capa bronze
    tables:
      - name: evaplan_api_sector_mp_raw_data
        description: Datos raw del endpoint SectorMP de Evaplan
        columns:
          - name: peri_idp
            description: Identificador del periodo
```

## 9. Generar Documentación desde Airflow

Para generar la documentación automáticamente desde Airflow, puedes agregar una tarea al final del DAG de transformación:

```python
generate_docs = BashOperator(
    task_id='generate_dbt_docs',
    bash_command='cd /opt/airflow/dags/gdv_general_dbt_dag/dbt && dbt docs generate',
    dag=dag
)
```

## 10. Compartir la Documentación

La documentación generada puede compartirse de varias formas:

- Servir desde un servidor web: Los archivos HTML en target/ pueden subirse a cualquier servidor web
- Integrar con CI/CD: Generar la documentación en cada deploy y publicarla automáticamente
- Compartir el manifest.json: Otros desarrolladores pueden usar `dbt docs serve --manifest target/manifest.json` para ver la documentación

## 11. Actualizar la Documentación

Cada vez que hagas cambios en los modelos o agregues nueva documentación, debes regenerar la documentación:

```bash
dbt docs generate
```

Si el servidor está corriendo, se actualizará automáticamente.

## 12. Gráficos de Lineage

Los gráficos de lineage muestran visualmente:
- De dónde vienen los datos (fuentes)
- Cómo se transforman (modelos)
- Hacia dónde van (dependencias)

Esto es muy útil para entender el flujo completo de datos en el proyecto.

## 13. Búsqueda y Navegación

dbt docs incluye una función de búsqueda que permite encontrar rápidamente:
- Modelos por nombre
- Columnas por nombre
- Fuentes por nombre
- Tests por nombre

## 14. Ventajas de Usar dbt docs

- Documentación siempre actualizada: Se genera automáticamente desde el código
- Visualización de dependencias: Los gráficos de lineage muestran claramente las relaciones
- Búsqueda rápida: Encuentra modelos y columnas fácilmente
- Compartible: Puede compartirse con todo el equipo
- Integración con tests: Muestra resultados de tests directamente en la documentación

## 15. Notas Importantes

- La documentación se genera después de compilar los modelos, por lo que requiere conexión a BigQuery para obtener información de columnas
- El comando `dbt docs generate` debe ejecutarse desde el directorio del proyecto dbt
- Los archivos YAML de documentación deben seguir el formato correcto (version: 2)
- La documentación incluye información del último run de dbt, incluyendo resultados de tests
- Para ver información actualizada de columnas, es necesario que los modelos existan en BigQuery o ejecutar `dbt run` primero

