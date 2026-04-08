# Indice de Flujos de DAGs

Este indice permite ubicar rapido el documento de cada fuente y sus DAGs principales.

## Fuentes de datos

- `SAP API Evaplan`: `documentacion/sap_api_evaplan_flujo_dags.md`  
  DAGs: `src_sap_api_evaplan_ingest_dag`, `src_sap_api_evaplan_load_dag`, `src_sap_api_evaplan_transform_dag`

- `Evaplan (no SAP)`: `documentacion/evaplan_flujo_dags.md`  
  DAGs: `src_planeacion_ingest_evaplan`, `src_planeacion_load_evaplan`, `src_planeacion_transform_evaplan`

- `IDC`: `documentacion/idc_flujo_dags.md`  
  DAGs: `src_planeacion_ingest_idc_dictionary`, `src_planeacion_load_idc_dictionary`, `src_planeacion_ingest_idc`, `src_planeacion_load_idc`, `src_planeacion_transf_idc`

- `IDI`: `documentacion/idi_flujo_dags.md`  
  DAGs: `src_planeacion_ingest_idi`, `src_planeacion_load_idi`, `src_planeacion_transf_idi`

- `IPM`: `documentacion/ipm_flujo_dags.md`  
  DAGs: `src_planeacion_ingest_ipm`, `src_planeacion_load_ipm`, `src_planeacion_transf_ipm`

- `IPM SISBEN`: `documentacion/ipm_sisben_flujo_dags.md`  
  DAGs: `src_planeacion_ingest_ipm_sisben`, `src_planeacion_load_ipm_sisben`, `src_planeacion_transform_ipm_sisben`

- `database_sc_stackdb`: `documentacion/database_sc_stackdb_flujo_dags.md`  
  DAGs: `src_database_sc_stackdb_ingest_dag`, `src_database_sc_stackdb_load_dag`

- `Seguridad (delitos_historicos)`: `documentacion/seguridad_flujo_dags.md`  
  DAGs: `src_delitos_historicos_ingest_dag`, `src_delitos_historicos_load_dag`

## Documentacion dbt docs

- `documentacion/dbt_docs_setup.md`
- `documentacion/dbt_docs_hosting_gcs.md`

## Uso recomendado en un chat nuevo

1. Abrir este indice.
2. Ir al documento de la fuente objetivo.
3. Validar que el documento y el codigo desplegado en Composer coincidan (DAGs, rutas y modelos dbt).
