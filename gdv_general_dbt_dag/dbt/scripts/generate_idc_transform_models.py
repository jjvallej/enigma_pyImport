#!/usr/bin/env python3
"""
Script para generar los modelos de transformación numérica de IDC.
Consulta las columnas en BigQuery y genera el SQL con las transformaciones aplicadas.
"""
import os
import sys
from google.cloud import bigquery

PROJECT_ID = "datagov-473122"
BRONZE_DATASET_ID = "test_idc_bronze"  # Consultar las tablas bronze para obtener la estructura
SA_PATH = "/opt/airflow/include/sa.json"

# Columnas de texto que NO deben transformarse
TEXT_COLUMNS = {'departamento', 'ano_idc', 'fecha_lectura', 'nombre_hoja'}

def get_columns_from_bq(dataset_id: str, table_name: str) -> list:
    """Obtiene las columnas de una tabla en BigQuery."""
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_PATH
    client = bigquery.Client(project=PROJECT_ID)
    
    query = f"""
    SELECT column_name
    FROM `{PROJECT_ID}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position
    """
    
    try:
        results = client.query(query).result()
        return [row.column_name for row in results]
    except Exception as e:
        print(f"Error consultando columnas: {e}", file=sys.stderr)
        return []

def generate_decimal_transform_sql(table_name: str, source_ref: str, numeric_columns: list) -> str:
    """Genera el SQL para transformar columnas numéricas a decimales con 2 decimales."""
    text_cols = ', '.join([col for col in TEXT_COLUMNS if col != 'nombre_hoja'])  # nombre_hoja puede no existir
    except_cols = ', '.join([f"'{col}'" for col in TEXT_COLUMNS])
    
    sql_parts = [
        "{{",
        "  config(",
        "    materialized='table',",
        "    schema='test_idc_silver'",
        "  )",
        "}}",
        "",
        f"-- Modelo 3: Transforma números a decimales con 2 decimales (vacíos = 0)",
        f"-- Transforma automáticamente todas las columnas excepto: {', '.join(TEXT_COLUMNS)}",
        "",
        "SELECT",
        f"  -- Mantener columnas de texto sin cambios",
    ]
    
    # Agregar columnas de texto
    for col in TEXT_COLUMNS:
        if col in numeric_columns or col == 'departamento' or col == 'ano_idc' or col == 'fecha_lectura':
            sql_parts.append(f"  {col},")
    
    sql_parts.append("")
    sql_parts.append("  -- Transformar columnas numéricas a decimales con 2 decimales")
    
    # Agregar transformaciones numéricas usando la macro clean_decimal
    for col in numeric_columns:
        if col not in TEXT_COLUMNS:
            sql_parts.append(
                f"  {{{{ clean_decimal('{col}') }}}} AS {col},"
            )
    
    # Remover la última coma
    if sql_parts[-1].endswith(','):
        sql_parts[-1] = sql_parts[-1][:-1]
    
    sql_parts.append("")
    sql_parts.append(f"FROM {{{{ ref('{source_ref}') }}}}")
    
    return "\n".join(sql_parts)

def generate_integer_transform_sql(table_name: str, source_ref: str, numeric_columns: list) -> str:
    """Genera el SQL para transformar columnas numéricas a enteros."""
    sql_parts = [
        "{{",
        "  config(",
        "    materialized='table',",
        "    schema='test_idc_silver'",
        "  )",
        "}}",
        "",
        f"-- Modelo 3: Transforma números a enteros (vacíos = 0)",
        f"-- Transforma automáticamente todas las columnas excepto: {', '.join(TEXT_COLUMNS)}",
        "",
        "SELECT",
        f"  -- Mantener columnas de texto sin cambios",
    ]
    
    # Agregar columnas de texto
    for col in TEXT_COLUMNS:
        if col in numeric_columns or col == 'departamento' or col == 'ano_idc' or col == 'fecha_lectura':
            sql_parts.append(f"  {col},")
    
    sql_parts.append("")
    sql_parts.append("  -- Transformar columnas numéricas a enteros")
    
    # Agregar transformaciones numéricas usando la macro clean_integer
    for col in numeric_columns:
        if col not in TEXT_COLUMNS:
            sql_parts.append(
                f"  {{{{ clean_integer('{col}') }}}} AS {col},"
            )
    
    # Remover la última coma
    if sql_parts[-1].endswith(','):
        sql_parts[-1] = sql_parts[-1][:-1]
    
    sql_parts.append("")
    sql_parts.append(f"FROM {{{{ ref('{source_ref}') }}}}")
    
    return "\n".join(sql_parts)

if __name__ == "__main__":
    # Mapeo de tablas bronze a modelos de transformación
    models = [
        ('idc_raw_data_dato_original', 'idc_transform_numbers_dato_original', 'dato_original', generate_decimal_transform_sql),
        ('idc_raw_data_valor_normalizado', 'idc_transform_numbers_valor_normalizado', 'valor_normalizado', generate_decimal_transform_sql),
        ('idc_raw_data_valor_ranking', 'idc_transform_numbers_valor_ranking', 'valor_ranking', generate_integer_transform_sql),
    ]
    
    for bronze_table, target_file, sheet_name, generator_func in models:
        try:
            print(f"Obteniendo columnas de {bronze_table}...")
            columns = get_columns_from_bq(BRONZE_DATASET_ID, bronze_table)
            
            if not columns:
                print(f"⚠ No se encontraron columnas para {bronze_table}. Saltando...")
                continue
            
            print(f"Columnas encontradas: {len(columns)}")
            print(f"  Texto: {[c for c in columns if c in TEXT_COLUMNS]}")
            print(f"  Numéricas: {[c for c in columns if c not in TEXT_COLUMNS]}")
            
            # Generar SQL (pasar todas las columnas, la función filtrará las de texto)
            sql = generator_func(bronze_table, f'idc_normalize_text_{sheet_name}', columns)
            
            # Escribir archivo
            output_path = f"models/silver/idc/{target_file}.sql"
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(sql)
            
            print(f"✓ Generado: {output_path}")
        except Exception as e:
            print(f"✗ Error procesando {bronze_table}: {e}", file=sys.stderr)
            import traceback
            traceback.print_exc()

