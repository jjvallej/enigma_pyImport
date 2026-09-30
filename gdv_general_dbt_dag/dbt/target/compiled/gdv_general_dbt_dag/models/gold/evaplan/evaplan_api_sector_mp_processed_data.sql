

-- Modelo gold: Une la tabla de periodos con la tabla de sector_mp
-- JOIN por peri_idp para tener toda la información del periodo junto con los datos de sector_mp

SELECT
  p.*,
  a.* EXCEPT(peri_idp, fecha_lectura)  -- Excluir peri_idp y fecha_lectura duplicados (ya están en p.*)
FROM `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`evaplan_api_periodos_transformed_data` p
INNER JOIN `datagov-473122`.`silver_dpt_planeacion_municipal_dev`.`evaplan_api_sector_mp_transformed_data` a
  ON CAST(p.peri_idp AS INT64) = CAST(a.peri_idp AS INT64)