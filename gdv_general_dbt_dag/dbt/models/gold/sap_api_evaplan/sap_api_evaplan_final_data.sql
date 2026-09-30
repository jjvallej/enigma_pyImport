{{
  config(
    materialized='table',
    schema=var('gold_dataset'),
    alias='sap_api_evaplan_final_data'
  )
}}

{# Silver = solo última corrida (transformada). Gold = conserva bloques de periodos anteriores
   y sustituye únicamente el bloque (periodo_ini, periodo_fin) de la última corrida. #}

{% set bronze_db = var('project_id') %}
{% set bronze_schema = var('bronze_dataset') %}
{% set bronze_table = 'sap_api_evaplan_raw_data' %}
{% set gold_db = var('project_id') %}
{% set gold_schema = var('gold_dataset') %}
{% set gold_identifier = 'sap_api_evaplan_final_data' %}

{% if execute %}
  {% set gold_relation = adapter.get_relation(
        database=gold_db,
        schema=gold_schema,
        identifier=gold_identifier
  ) %}
{% else %}
  {% set gold_relation = none %}
{% endif %}

{% if gold_relation is not none %}
with latest_bounds as (
  select
    any_value(periodo_ini) as p_ini,
    any_value(periodo_fin) as p_fin
  from `{{ bronze_db }}.{{ bronze_schema }}.{{ bronze_table }}`
  where run_ts = (
    select max(run_ts) from `{{ bronze_db }}.{{ bronze_schema }}.{{ bronze_table }}`
  )
),
silver_new as (
  select * from {{ ref('sap_api_evaplan_transformed_data') }}
),
gold_kept as (
  select g.*
  from `{{ gold_db }}.{{ gold_schema }}.{{ gold_identifier }}` as g
  cross join latest_bounds as lb
  where not (
    coalesce(cast(g.periodo_ini as string), '') = coalesce(cast(lb.p_ini as string), '')
    and coalesce(cast(g.periodo_fin as string), '') = coalesce(cast(lb.p_fin as string), '')
  )
)
select * from gold_kept
union all
select * from silver_new
{% else %}
select * from {{ ref('sap_api_evaplan_transformed_data') }}
{% endif %}
