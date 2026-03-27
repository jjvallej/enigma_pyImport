{{
  config(
    materialized='table',
    schema=var('gold_dataset'),
    alias='sap_api_evaplan_final_data'
  )
}}

select *
from {{ ref('sap_api_evaplan_transformed_data') }}
