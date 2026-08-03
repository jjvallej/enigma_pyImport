# pyimport

Importa anexos mensuales SIPSA (DANE) y genera un CSV consolidado con
`anio`, `mes`, `alimento` (columna A) y `valor` (columna J, precio Cali).

## Setup

```bash
cd /home/jjvallej/work/enigma/pyimport
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev,airflow]"
```

## Run CLI

Descarga el rango de anexos y escribe un solo CSV:

```bash
pyimport --start 2015-02 --end 2026-06 --download-dir data/raw --output data/sipsa_precios.csv
```

Para cada mes se prueban, en orden, estos esquemas de URL:

1. `.../sipsa/anex_mensual_{mes}_{anio}.xls`
2. `.../sipsa/anex_mensual_{mes}_{anio}.xlsx`
3. `.../sipsa/anexo_mensual_SIPSA_mayoristas_{mes}_{anio}.xlsx`
4. `.../operaciones/SIPSA/anex-SIPSAMensual-{mes}{anio}.xlsx`

donde `{mes}` son las tres primeras letras (`ene`…`dic`). El mes se convierte a número
(`ene` → `01`, …, `dic` → `12`) y se antepone a cada registro junto con el año.

## Apache Airflow

Para ejecutar la importación con Apache Airflow:

1. El DAG `sipsa_import` se encuentra en `dags/sipsa_airflow_dag.py`.
2. Probar el DAG en local:
   ```bash
   airflow dags test sipsa_import 2026-01-01
   ```
3. Disparar el DAG con parámetros personalizados desde Airflow CLI:
   ```bash
   airflow dags trigger sipsa_import --conf '{"start": "2024-01", "end": "2024-12"}'
   ```

## Tests

```bash
pytest
```
