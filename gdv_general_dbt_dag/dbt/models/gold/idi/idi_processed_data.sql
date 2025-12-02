{{
  config(
    materialized='view',
    schema='gold_dpt_planeacion_municipal_dev',
    alias='idi_processed_data'
  )
}}

-- Vista: Datos consolidados de IDI en capa Gold
-- Apunta directamente a la tabla consolidada de Silver

SELECT *
FROM {{ ref('idi_transformed_data_consolidated') }}