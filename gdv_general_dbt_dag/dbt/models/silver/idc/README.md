# Modelos de Transformación IDC - Silver Layer

## Estructura

Los modelos de transformación para IDC están organizados en 3 etapas:

1. **Normalizar columnas** (`idc_normalize_columns_*.sql`): Convierte nombres de columnas a snake_case
2. **Normalizar texto** (`idc_normalize_text_*.sql`): Convierte texto a mayúsculas sin acentos ni caracteres especiales
3. **Transformar números** (`idc_transform_numbers_*.sql`): Convierte números a decimales (2 decimales) o enteros

## Generación Automática de Modelos de Transformación Numérica

Los modelos `idc_transform_numbers_*.sql` requieren listar explícitamente todas las columnas numéricas. Para generarlos automáticamente:

```bash
cd gdv_general_dbt_dag/dbt
python3 scripts/generate_idc_transform_models.py
```

Este script:
1. Consulta las columnas reales en las tablas bronze de BigQuery
2. Genera automáticamente los modelos SQL con todas las transformaciones aplicadas
3. Usa las macros `clean_decimal` y `clean_integer` para mantener el código limpio

## Macros Disponibles

- `clean_decimal(column_name)`: Transforma una columna a decimal con 2 decimales, reemplazando vacíos con 0
- `clean_integer(column_name)`: Transforma una columna a entero, reemplazando vacíos con 0

## Columnas de Texto

Las siguientes columnas se mantienen como texto (no se transforman numéricamente):
- `departamento`
- `ano_idc`
- `fecha_lectura`
- `nombre_hoja`

Todas las demás columnas se tratan como numéricas.

