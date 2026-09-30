{{
  config(
    materialized='view',
    schema=var('gold_dataset'),
    alias='IDI_PROCESSED_DATA'
  )
}}

-- Vista: Datos consolidados de IDI en capa Gold
-- Apunta directamente a la tabla consolidada de Silver

SELECT *
FROM {{ ref('idi_transformed_data_consolidated') }}