test_end_to_end_flow_ipm

Objetivo: Verificar el flujo completo end-to-end de los DAGs de IPM desde ingesta hasta transformación.
Validaciones:
- El DAG de ingesta (src_planeacion_ingest_ipm) dispara correctamente el DAG de carga (src_planeacion_load_ipm)
- El DAG de carga (src_planeacion_load_ipm) dispara correctamente el DAG de transformación (src_planeacion_transf_ipm)
- Los TriggerDagRunOperator tienen wait_for_completion=False para no bloquear el flujo
- El flujo completo: ingest -> load -> transform se ejecuta en secuencia
- Cada DAG puede ejecutarse independientemente si es necesario
- Los datos fluyen correctamente: GCS (ingest) -> BigQuery Bronze (load) -> BigQuery Silver/Gold (transform)
- No hay dependencias circulares entre los DAGs
- Cada DAG tiene su propósito específico y bien definido
  Estado esperado: ✅ PASS

