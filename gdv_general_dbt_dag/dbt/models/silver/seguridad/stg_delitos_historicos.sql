{{
  config(
    materialized='view',
    schema=var('silver_dataset'),
    description='Vista staging sobre la tabla bronze de delitos históricos (2016-2025). Origen: observatorio-de-seguridad, cargado por Airflow.'
  )
}}

SELECT *
FROM {{ source('bronze_seguridad', 'delitos_historicos_raw_data') }}
