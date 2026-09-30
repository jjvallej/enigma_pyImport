# dags/src_ipm_sisben_load_dag.py
"""
DAG para crear la EXTERNAL TABLE de IPM SISBEN en BigQuery.
La tabla externa apunta a los archivos CSV en GCS.
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
import os
import sys

def add_project_root_to_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    while current_dir != "/":
        if os.path.exists(os.path.join(current_dir, "modules")):
            if current_dir not in sys.path:
                sys.path.insert(0, current_dir)
            return
        current_dir = os.path.dirname(current_dir)
    
    parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

add_project_root_to_path()

from modules.config import CONF, PROJECT_ID, LOCATION, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

# === CONFIGURACIÓN ===
TABLE_NAME = CONF.ipm_sisben.tables.bronze
GCS_BUCKET_NAME = DEFAULT_BUCKET_NAME
GCS_PATH = CONF.ipm_sisben.external_table.gcs_path
SKIP_LEADING_ROWS = CONF.ipm_sisben.external_table.skip_leading_rows
FIELD_DELIMITER = CONF.ipm_sisben.external_table.field_delimiter
ALLOW_QUOTED_NEWLINES = CONF.ipm_sisben.external_table.allow_quoted_newlines

# Construir el SQL para crear la EXTERNAL TABLE
def get_create_external_table_sql():
    """Genera el SQL para crear la EXTERNAL TABLE usando los parámetros de config.yaml"""
    gcs_uri = f"gs://{GCS_BUCKET_NAME}/{GCS_PATH}"
    
    sql = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS `{PROJECT_ID}.{DATASET_ID_BRONZE}.{TABLE_NAME}`
    (
      `_2_num_ficha` STRING,
      `_3_cod_departamento` STRING,
      `departamento` STRING,
      `_4_cod_municipio` STRING,
      `municipio` STRING,
      `_6_cod_clase` STRING,
      `_25_tipo_vivienda` STRING,
      `_26_tipo_material_paredes` STRING,
      `_90_vendaval` STRING,
      `_52_cocina` STRING,
      `_102_tip_documento` STRING,
      `_53_alimentos` STRING,
      `_51_basura` STRING,
      `_50_agua_potable` STRING,
      `_28_energia` STRING,
      `_29_estrato_energia` STRING,
      `_30_alcantarillado` STRING,
      `_31_gas` STRING,
      `_32_recoleccion_basura` STRING,
      `_33_acueducto` STRING,
      `_34_estrato_energia` STRING,
      `_35_cuartos` STRING,
      `_36_hogares` STRING,
      `_37_ide_hogar` STRING,
      `_38_tip_ocupacion` STRING,
      `_39_cuartos_excl` STRING,
      `_40_cuartos_dorm` STRING,
      `_41_cuartos_dorm_uni` STRING,
      `_42_sanitario` STRING,
      `_43_tip_sanitario` STRING,
      `_44_uso_sanitario` STRING,
      `_45_orig_agua` STRING,
      `_46_agua_7_dias` STRING,
      `_47_num_dias_agua` STRING,
      `_48_agua_llega` STRING,
      `_49_horas_agua` STRING,
      `_50_agua_beber` STRING,
      `_51_elimina_basura` STRING,
      `_52_tiene_cocina` STRING,
      `_53_prepara_alimentos` STRING,
      `_54_tipo_cocina` STRING,
      `_55_tip_ener_coc` STRING,
      `_56_nevera` STRING,
      `_57_lavadora` STRING,
      `_58_pc` STRING,
      `_59_internet` STRING,
      `_60_moto` STRING,
      `_61_tractor` STRING,
      `_62_carro` STRING,
      `_63_bien_raiz` STRING,
      `_64_tiene_gasto_ali` STRING,
      `_65_gasto` STRING,
      `_66_tiene_gasto_trans` STRING,
      `_67_gasto_transporte` STRING,
      `_68_tiene_gasto_educ` STRING,
      `_69_gasto_educ` STRING,
      `_70_tiene_gasto_salud` STRING,
      `_71_gasto_salud` STRING,
      `_72_tiene_gasto_servpub` STRING,
      `_73_gasto_servpub` STRING,
      `_74_tiene_gasto_cel` STRING,
      `_75_gasto_cel` STRING,
      `_76_tiene_gasto_arren` STRING,
      `_77_gasto_arrendo` STRING,
      `_78_tiene_gastos_otros` STRING,
      `_79_gastos_otros` STRING,
      `_79_gastos_otros_1` STRING,
      `_80_num_hab` STRING,
      `_81_inundacion` STRING,
      `_82_num_inundacion` STRING,
      `_83_avalancha` STRING,
      `_84_num_avalancha` STRING,
      `_85_terremoto` STRING,
      `_86_num_terremoto` STRING,
      `_87_incendio` STRING,
      `_88_num_incendio` STRING,
      `_89_vendaval` STRING,
      `_90_num_vendaval` STRING,
      `_91_hundimiento` STRING,
      `_92_num_hundimiento` STRING,
      `_93_num_pers_hogar` STRING,
      `_95_ord_persona` STRING,
      `_101_sexo` STRING,
      `xxx` STRING,
      `_102_tip_doc` STRING,
      `_105_edad` STRING,
      `_109_tip_parentesci` STRING,
      `_110_estado_civil` STRING,
      `_111_conyuge_vive_hogar` STRING,
      `_112_ide_conyuge` STRING,
      `_113_padre_vive_hogar` STRING,
      `_114_ide_padre` STRING,
      `_115_pariente` STRING,
      `_116_serv_domestico` STRING,
      `_117_disca_ver` STRING,
      `_118_disca_oir` STRING,
      `_119_disca_hablar` STRING,
      `_120_disca_moverse` STRING,
      `_121_disca_banarse` STRING,
      `_122_disca_salir` STRING,
      `_123_disca_entender` STRING,
      `_124_disca_ninguna` STRING,
      `_125_seg_social` STRING,
      `_126_enfermo` STRING,
      `_127_acudio_salud` STRING,
      `_128_fue_atendido` STRING,
      `_129_estaba_embarazada` STRING,
      `_130_tuvo_hijos` STRING,
      `_131_cuidado_hijos` STRING,
      `_132_recibe_comida` STRING,
      `_133_sabe_leer` STRING,
      `_134_estudia` STRING,
      `_135_nivel_educativo` STRING,
      `_136_grado` STRING,
      `_137_fondo_pensiones` STRING,
      `_138_actividad_mes` STRING,
      `_139_sem_buscando` STRING,
      `_140_empleado` STRING,
      `_141_ingreso` STRING,
      `_142_salario` STRING,
      `_143_ind_honorarios_mes` STRING,
      `_144_honorarios_mes` STRING,
      `_145_ind_ingre_cosecha` STRING,
      `_146_ingre_cosecha` STRING,
      `_147_valor_cosecha` STRING,
      `_148_ind_pension` STRING,
      `_149_ingre_pension` STRING,
      `_150_ind_ingreso_remesa` STRING,
      `_151_ingreso_remesa` STRING,
      `_152_ind_ingreso_remesa_ext` STRING,
      `_153_ingreso_remesa_ext` STRING,
      `_154_ind_ingreso_arrendo` STRING,
      `_155_ingreso_arrendo` STRING,
      `_156_ind_otros_ingresos` STRING,
      `_157_otros_ingresos` STRING,
      `_158_ind_subsidios` STRING,
      `_159_fam_accion` STRING,
      `_160_col_mayor` STRING,
      `_161_otro_subsidio` STRING,
      `_175` STRING,
      `_176` STRING,
      `_177` STRING,
      `_178` STRING,
      `_179` STRING,
      `_180` STRING,
      `_181` STRING,
      `_182` STRING,
      `_183` STRING,
      `_184` STRING,
      `_185` STRING,
      `_186` STRING,
      `_187` STRING,
      `_188` STRING,
      `_189` STRING,
      `_190` STRING,
      `_192_grupo` STRING,
      `_193_nivel` STRING
    )
    OPTIONS (
      format = 'CSV',
      uris = ['{gcs_uri}'],
      skip_leading_rows = {SKIP_LEADING_ROWS},
      field_delimiter = '{FIELD_DELIMITER}',
      allow_quoted_newlines = {str(ALLOW_QUOTED_NEWLINES).upper()}
    );
    """
    return sql.strip()

with DAG(
    dag_id="src_planeacion_load_ipm_sisben",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["secretaria:planeacion", "actividad:carga", "fuente:ipm_sisben", "ejecución:manual"],
    description="Crea la EXTERNAL TABLE de IPM SISBEN en BigQuery apuntando a los CSVs en GCS.",
) as dag:

    # Tarea inicial
    start = EmptyOperator(
        task_id="start",
    )

    # Asegurar que el dataset exista
    ensure_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="ensure_dataset",
        dataset_id=DATASET_ID_BRONZE,
        project_id=PROJECT_ID,
        location=LOCATION,
        exists_ok=True,  # No falla si el dataset ya existe
    )

    # Crear la EXTERNAL TABLE
    create_external_table = BigQueryInsertJobOperator(
        task_id="create_external_table",
        configuration={
            "query": {
                "query": get_create_external_table_sql(),
                "useLegacySql": False,
            }
        },
        location=LOCATION,
        project_id=PROJECT_ID,
    )

    # Trigger del DAG de transform (sin esperar su finalización)
    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="src_planeacion_transform_ipm_sisben",
        wait_for_completion=False,  # No espera a que el DAG llamado termine
        reset_dag_run=True,  # Permite re-ejecutar el DAG si ya está en ejecución
    )

    # Tarea final
    end = EmptyOperator(
        task_id="end",
    )

    # Dependencias: start -> ensure_dataset -> create_external_table -> trigger_transform -> end
    start >> ensure_dataset >> create_external_table >> trigger_transform >> end
