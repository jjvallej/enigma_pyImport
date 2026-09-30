{{
  config(
    materialized='table',
    schema=var('silver_dataset'),
    alias='sap_api_evaplan_transformed_data'
  )
}}

{% set bronze_db = var('project_id') %}
{% set bronze_schema = var('bronze_dataset') %}
{% set bronze_table = 'sap_api_evaplan_raw_data' %}
{% set relation = adapter.get_relation(database=bronze_db, schema=bronze_schema, identifier=bronze_table) %}

with latest_run as (
  select max(run_ts) as run_ts
  from `{{ bronze_db }}.{{ bronze_schema }}.{{ bronze_table }}`
)
select
{% if execute and relation is not none %}
  {% set cols = adapter.get_columns_in_relation(relation) %}
  {%- for col in cols %}
    {%- if col.data_type is string and col.data_type | upper == 'STRING' -%}
  upper({{ adapter.quote(col.name) }}) as {{ adapter.quote(col.name) }}
    {%- else -%}
  {{ adapter.quote(col.name) }}
    {%- endif -%}
    {%- if not loop.last %},{% endif %}
  {%- endfor %}
{% else %}
  *
{% endif %}
from `{{ bronze_db }}.{{ bronze_schema }}.{{ bronze_table }}`
where run_ts = (select run_ts from latest_run)
