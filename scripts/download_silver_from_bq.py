"""Script para descargar la tabla silver_agri_consolidado desde BigQuery
y actualizar los datos locales en data/dataset_consolidado_valle.csv.
"""

import sys
from pathlib import Path
import pandas as pd
from google.cloud import bigquery

PROJECT_ID = "datagov-477214"
DATASET_ID = "valledata"
TABLE_NAME = "silver_agri_consolidado"
FULL_TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

OUTPUT_CSV = Path("data/dataset_consolidado_valle.csv")
OUTPUT_SILVER_CSV = Path("data/silver_agri_consolidado.csv")

def descargar_tabla_silver():
    print(f"--> Conectando a BigQuery ({FULL_TABLE_ID})...")
    client = bigquery.Client(project=PROJECT_ID)

    query = f"SELECT * FROM `{FULL_TABLE_ID}`"
    print(f"--> Ejecutando consulta: {query}")
    df = client.query(query).to_dataframe()
    print(f"✅ Se descargaron {len(df):,} filas desde BigQuery ({FULL_TABLE_ID}).")
    print(f"📋 Columnas obtenidas ({len(df.columns)}): {list(df.columns)}")

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    df.to_csv(OUTPUT_SILVER_CSV, index=False)
    print(f"💾 Guardado localmente en: {OUTPUT_CSV} y {OUTPUT_SILVER_CSV}")
    return df

if __name__ == "__main__":
    descargar_tabla_silver()
