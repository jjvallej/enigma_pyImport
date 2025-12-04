test_end_to_end_flow_ipm_sisben

Objetivo: Verificar el flujo completo end-to-end de los DAGs de IPM SISBEN desde ingesta hasta transformación.
Validaciones:
- El DAG de ingesta (src_planeacion_ingest_ipm_sisben) mueve archivos dentro de GCS
- El DAG de carga (src_planeacion_load_ipm_sisben) crea la tabla externa y dispara el DAG de transformación
- El DAG de transformación (src_planeacion_transform_ipm_sisben) transforma los datos usando dbt
- El TriggerDagRunOperator en load tiene wait_for_completion=False para no bloquear el flujo
- El flujo completo: ingest -> load -> transform se ejecuta en secuencia
- Cada DAG puede ejecutarse independientemente si es necesario
- Los datos fluyen correctamente: GCS temporal (ingest) -> GCS destino -> BigQuery Bronze External Table (load) -> BigQuery Silver/Gold (transform)
- No hay dependencias circulares entre los DAGs
- Cada DAG tiene su propósito específico y bien definido
- El DAG de ingesta no dispara automáticamente el DAG de carga (a diferencia de IPM regular)
  Estado esperado: ✅ PASS

