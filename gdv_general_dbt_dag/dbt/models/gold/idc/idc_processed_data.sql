{{
  config(
    materialized='table',
    schema='test_idc_gold',
    alias='fact_idc'
  )
}}

-- Modelo gold: Une las 3 tablas (dato_original, valor_normalizado, valor_ranking) con el diccionario (dim_idc)
-- Estructura final: DEPARTAMENTO, ANIO, ID_FACTOR, ID_PILAR, ID_INDICADOR, ID_SUBINDICADOR, VALOR_ORIGINAL, VALOR_NORMALIZADO, VALOR_RANKING
-- Usa UNPIVOT para convertir las columnas de indicadores en filas y hace JOIN con dim_idc usando ID_SUBINDICADOR

WITH dato_original_unpivot AS (
  SELECT
    departamento,
    ano_idc AS ano,
    id_indicador,
    valor AS valor_original
  FROM {{ ref('idc_round_decimals_dato_original') }}
  UNPIVOT INCLUDE NULLS (
    valor FOR id_indicador IN (
      ins_1_1, ins_1_2, ins_1_3, ins_2_1, ins_2_2, ins_2_3, ins_3_1, ins_3_2, ins_3_3,
      ins_4_1, ins_4_2, ins_4_3, ins_4_4, ins_4_5, ins_4_6,
      inf_1_1, inf_1_2, inf_1_3, inf_1_4, inf_1_5, inf_2_1, inf_2_2, inf_2_3, inf_2_4, inf_2_5, inf_2_6,
      inf_3_1, inf_3_2, inf_3_3, inf_3_4,
      tic_1_1, tic_1_2, tic_1_3, tic_1_4, tic_2_1, tic_2_2, tic_2_3,
      amb_1_1, amb_1_2, amb_1_3, amb_2_1, amb_2_2,
      sal_1_1, sal_1_2, sal_1_3, sal_2_1, sal_2_2, sal_2_3, sal_3_1, sal_3_2, sal_3_3, sal_3_4,
      edu_1_1, edu_1_2, edu_1_3, edu_1_4, edu_1_5, edu_2_1, edu_2_2, edu_2_3, edu_2_4,
      eds_1_1, eds_1_2, eds_1_3, eds_2_1, eds_2_2, eds_2_3, eds_2_4, eds_3_1, eds_3_2,
      neg_1_1, neg_1_2, neg_1_3, neg_2_1, neg_2_2, neg_2_3,
      lab_1_1, lab_1_2, lab_1_3, lab_1_4, lab_1_5,
      fin_1_1, fin_1_2, fin_1_3, fin_1_4,
      tam_1_1, tam_2_1, tam_2_2,
      sof_1_1, sof_1_2,
      inn_1_1, inn_1_2, inn_1_3, inn_1_4, inn_2_1, inn_2_2, inn_2_3, inn_2_4
    )
  )
),

valor_normalizado_unpivot AS (
  SELECT
    departamento,
    ano_idc AS ano,
    id_indicador,
    valor AS valor_normalizado
  FROM {{ ref('idc_round_decimals_valor_normalizado') }}
  UNPIVOT INCLUDE NULLS (
    valor FOR id_indicador IN (
      ins_1_1, ins_1_2, ins_1_3, ins_2_1, ins_2_2, ins_2_3, ins_3_1, ins_3_2, ins_3_3,
      ins_4_1, ins_4_2, ins_4_3, ins_4_4, ins_4_5, ins_4_6,
      inf_1_1, inf_1_2, inf_1_3, inf_1_4, inf_1_5, inf_2_1, inf_2_2, inf_2_3, inf_2_4, inf_2_5, inf_2_6,
      inf_3_1, inf_3_2, inf_3_3, inf_3_4,
      tic_1_1, tic_1_2, tic_1_3, tic_1_4, tic_2_1, tic_2_2, tic_2_3,
      amb_1_1, amb_1_2, amb_1_3, amb_2_1, amb_2_2,
      sal_1_1, sal_1_2, sal_1_3, sal_2_1, sal_2_2, sal_2_3, sal_3_1, sal_3_2, sal_3_3, sal_3_4,
      edu_1_1, edu_1_2, edu_1_3, edu_1_4, edu_1_5, edu_2_1, edu_2_2, edu_2_3, edu_2_4,
      eds_1_1, eds_1_2, eds_1_3, eds_2_1, eds_2_2, eds_2_3, eds_2_4, eds_3_1, eds_3_2,
      neg_1_1, neg_1_2, neg_1_3, neg_2_1, neg_2_2, neg_2_3,
      lab_1_1, lab_1_2, lab_1_3, lab_1_4, lab_1_5,
      fin_1_1, fin_1_2, fin_1_3, fin_1_4,
      tam_1_1, tam_2_1, tam_2_2,
      sof_1_1, sof_1_2,
      inn_1_1, inn_1_2, inn_1_3, inn_1_4, inn_2_1, inn_2_2, inn_2_3, inn_2_4
    )
  )
),

valor_ranking_unpivot AS (
  SELECT
    departamento,
    ano_idc AS ano,
    id_indicador,
    valor AS ranking
  FROM {{ ref('idc_round_integers_valor_ranking') }}
  UNPIVOT INCLUDE NULLS (
    valor FOR id_indicador IN (
      ins_1_1, ins_1_2, ins_1_3, ins_2_1, ins_2_2, ins_2_3, ins_3_1, ins_3_2, ins_3_3,
      ins_4_1, ins_4_2, ins_4_3, ins_4_4, ins_4_5, ins_4_6,
      inf_1_1, inf_1_2, inf_1_3, inf_1_4, inf_1_5, inf_2_1, inf_2_2, inf_2_3, inf_2_4, inf_2_5, inf_2_6,
      inf_3_1, inf_3_2, inf_3_3, inf_3_4,
      tic_1_1, tic_1_2, tic_1_3, tic_1_4, tic_2_1, tic_2_2, tic_2_3,
      amb_1_1, amb_1_2, amb_1_3, amb_2_1, amb_2_2,
      sal_1_1, sal_1_2, sal_1_3, sal_2_1, sal_2_2, sal_2_3, sal_3_1, sal_3_2, sal_3_3, sal_3_4,
      edu_1_1, edu_1_2, edu_1_3, edu_1_4, edu_1_5, edu_2_1, edu_2_2, edu_2_3, edu_2_4,
      eds_1_1, eds_1_2, eds_1_3, eds_2_1, eds_2_2, eds_2_3, eds_2_4, eds_3_1, eds_3_2,
      neg_1_1, neg_1_2, neg_1_3, neg_2_1, neg_2_2, neg_2_3,
      lab_1_1, lab_1_2, lab_1_3, lab_1_4, lab_1_5,
      fin_1_1, fin_1_2, fin_1_3, fin_1_4,
      tam_1_1, tam_2_1, tam_2_2,
      sof_1_1, sof_1_2,
      inn_1_1, inn_1_2, inn_1_3, inn_1_4, inn_2_1, inn_2_2, inn_2_3, inn_2_4
    )
  )
),

-- Unir las 3 tablas de valores
unified_values AS (
  SELECT
    COALESCE(do.departamento, vn.departamento, vr.departamento) AS departamento,
    COALESCE(do.ano, vn.ano, vr.ano) AS ano,
    COALESCE(do.id_indicador, vn.id_indicador, vr.id_indicador) AS id_subindicador,
    do.valor_original,
    vn.valor_normalizado,
    vr.ranking
  FROM dato_original_unpivot do
  FULL OUTER JOIN valor_normalizado_unpivot vn
    ON do.departamento = vn.departamento
    AND do.ano = vn.ano
    AND do.id_indicador = vn.id_indicador
  FULL OUTER JOIN valor_ranking_unpivot vr
    ON COALESCE(do.departamento, vn.departamento) = vr.departamento
    AND COALESCE(do.ano, vn.ano) = vr.ano
    AND COALESCE(do.id_indicador, vn.id_indicador) = vr.id_indicador
)

SELECT
  uv.departamento AS DEPARTAMENTO,
  uv.ano AS ANIO,
  CAST(d.ID_FACTOR AS INT64) AS ID_FACTOR,
  CAST(d.ID_PILAR AS INT64) AS ID_PILAR,
  d.ID_INDICADOR,
  d.ID_SUBINDICADOR,
  CAST(uv.valor_original AS FLOAT64) AS VALOR_ORIGINAL,
  CAST(uv.valor_normalizado AS FLOAT64) AS VALOR_NORMALIZADO,
  CAST(uv.ranking AS INT64) AS VALOR_RANKING
FROM unified_values uv
INNER JOIN {{ source('gold_idc', 'dim_idc') }} d
  ON UPPER(REPLACE(uv.id_subindicador, '_', '-')) = UPPER(d.ID_SUBINDICADOR)
ORDER BY
  uv.departamento,
  uv.ano,
  CAST(d.ID_FACTOR AS INT64),
  CAST(d.ID_PILAR AS INT64),
  d.ID_INDICADOR,
  uv.id_subindicador

