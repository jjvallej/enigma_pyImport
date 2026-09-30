"""Script para actualizar y reconstruir el notebook notebooks/eda_y_prediccion_arima_plus.ipynb.
- Asegura la resolución robusta de rutas de datos y figuras (soporta ejecuciones desde raíz y notebooks/).
- Presenta las 6 secciones de EDA y modelado predictivo ARIMA_PLUS con gráficos e iteraciones claras.
"""

import json
import time
from pathlib import Path

t0 = time.time()

def make_code_cell(source: str) -> dict:
    return {
        "cell_type": "code",
        "execution_count": 1,
        "metadata": {},
        "outputs": [],
        "source": source.strip().splitlines(keepends=True)
    }

def make_markdown_cell(source: str) -> dict:
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": source.strip().splitlines(keepends=True)
    }

cells = []

# Celda 0: Título e Introducción
cells.append(make_markdown_cell(r"""
# Análisis Exploratorio de Datos (EDA) y Modelo de Predicción ARIMA_PLUS
## Sector Agrícola del Valle del Cauca (2000 - 2027)

Este notebook presenta la metodología completa de análisis agrícola y ciencia de datos sobre la base consolidada `silver_agri_modelo_base`:
1. **EDA Nivel 1: Por Cultivo**: Volumen de producción acumulada, hectáreas sembradas, rendimientos y variabilidad.
2. **EDA Nivel 2: Por Municipio**: Producción agrícola total, diversidad de cultivos y densidad productiva.
3. **EDA Nivel 3: Por Municipio y Cultivo con Análisis de Correlación**: Interrelaciones intervariables (Hectáreas, Producción, Rendimiento, Precios SIPSA e Índice Climático ONI).
4. **Esquema de Predicción ARIMA_PLUS**: Selección de hiperparámetros $(p, d, q)$, estacionariedad y tendencia por serie temporal.
5. **Backtesting & Evaluación de Desempeño**: Medición del coeficiente de determinación $R^2$, RMSE, MAE y MAPE en muestras fuera de tiempo (2022-2024), identificando las series con $R^2 \ge 0.7$.
6. **Pronóstico a 3 Años (2025 - 2027)**: Proyecciones futuras de producción con intervalos de confianza al 80%.
"""))

# Celda 1: Configuración y Carga de Datos
cells.append(make_code_cell("""
import os
from pathlib import Path
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import Image, display

# Función auxiliar para resolver rutas de datos e imágenes en cualquier entorno de ejecución
def get_path(rel_path):
    p = Path(rel_path)
    if p.exists():
        return p
    p_sub = Path('../') / rel_path
    if p_sub.exists():
        return p_sub
    return p

# Cargar archivos de datos de resumen del modelo agrícola
df_cultivo = pd.read_csv(get_path('data/eda_resumen_por_cultivo.csv'))
df_mun = pd.read_csv(get_path('data/eda_resumen_por_municipio.csv'))
df_corr_pearson = pd.read_csv(get_path('data/matriz_correlacion_pearson.csv'), index_col=0)
df_eval = pd.read_csv(get_path('data/evaluacion_modelos_arima.csv'))
df_fc = pd.read_csv(get_path('data/predicciones_3_anos_r2_optimo.csv'))

print("--- Resumen de Datasets de Análisis y Pronóstico Cargados ---")
print(f"Cultivos analizados: {len(df_cultivo)} | Municipios analizados: {len(df_mun)}")
print(f"Modelos ARIMA evaluados: {len(df_eval)} | Series con R² >= 0.7: {(df_eval['cumple_r2_07'] == True).sum()}")
"""))

# Celda 2: Sección 1 Markdown
cells.append(make_markdown_cell("""
---
## 1. EDA por Cultivo

Análisis agrupado por tipo de cultivo en el departamento del Valle del Cauca, identificando los rubros de mayor producción y rendimiento agrícola.
"""))

# Celda 3: Sección 1 Code
cells.append(make_code_cell("""
print("=== TODOS LOS CULTIVOS POR PRODUCCIÓN ACUMULADA (TONELADAS) ===")
display(df_cultivo[['cultivo', 'total_produccion', 'rendimiento_promedio', 'total_ha_sembradas', 'coef_variacion_prod_pct']])

img1 = get_path('notebooks/figures/eda_cultivo_top15_produccion.png')
if not img1.exists():
    img1 = get_path('figures/eda_cultivo_top15_produccion.png')
if img1.exists():
    display(Image(filename=str(img1)))

img2 = get_path('notebooks/figures/eda_cultivo_rendimiento_por_grupo.png')
if not img2.exists():
    img2 = get_path('figures/eda_cultivo_rendimiento_por_grupo.png')
if img2.exists():
    display(Image(filename=str(img2)))
"""))

# Celda 4: Sección 2 Markdown
cells.append(make_markdown_cell("""
---
## 2. EDA por Municipio

Análisis territorial de producción agrícola total, diversidad de cultivos producidos y hectáreas cultivadas por municipio.
"""))

# Celda 5: Sección 2 Code
cells.append(make_code_cell("""
print("=== TODOS LOS MUNICIPIOS POR PRODUCCIÓN AGRÍCOLA TOTAL ===")
display(df_mun[['municipio', 'total_produccion', 'diversidad_cultivos', 'total_ha_sembradas']])

img1 = get_path('notebooks/figures/eda_municipio_top15_produccion.png')
if not img1.exists():
    img1 = get_path('figures/eda_municipio_top15_produccion.png')
if img1.exists():
    display(Image(filename=str(img1)))

img2 = get_path('notebooks/figures/eda_municipio_diversidad_vs_produccion.png')
if not img2.exists():
    img2 = get_path('figures/eda_municipio_diversidad_vs_produccion.png')
if img2.exists():
    display(Image(filename=str(img2)))
"""))

# Celda 6: Sección 3 Markdown
cells.append(make_markdown_cell("""
---
## 3. EDA por Municipio, Cultivo y Matriz de Correlación

Análisis de correlación multivariable entre hectáreas sembradas, cosechadas, volumen de producción, rendimientos (Ton/Ha), precios SIPSA e índice climático ONI.
"""))

# Celda 7: Sección 3 Code
cells.append(make_code_cell("""
print("=== MATRIZ DE CORRELACIÓN DE PEARSON (VARIABLES CLAVE) ===")
display(df_corr_pearson.round(3))

img1 = get_path('notebooks/figures/eda_correlacion_pearson_heatmap.png')
if not img1.exists():
    img1 = get_path('figures/eda_correlacion_pearson_heatmap.png')
if img1.exists():
    display(Image(filename=str(img1)))

img2 = get_path('notebooks/figures/eda_distribucion_correlacion_ha_vs_prod.png')
if not img2.exists():
    img2 = get_path('figures/eda_distribucion_correlacion_ha_vs_prod.png')
if img2.exists():
    display(Image(filename=str(img2)))
"""))

# Celda 8: Sección 4 & 5 Markdown
cells.append(make_markdown_cell(r"""
---
## 4 & 5. Modelo ARIMA_PLUS, Backtesting y Evaluación ($R^2 \ge 0.7$)

Resultados de la validación cruzada out-of-sample (entrenamiento en período histórico y prueba en 2022-2024), evaluando precisión mediante $R^2$, RMSE, MAE y MAPE.
"""))

# Celda 9: Sección 4 & 5 Code
cells.append(make_code_cell("""
df_optimos = df_eval[df_eval['cumple_r2_07'] == True].sort_values('r2_score', ascending=False)
print(f"Total de series temporales que cumplen criterio R² >= 0.7: {len(df_optimos)} de {len(df_eval)} evaluadas ({len(df_optimos)/len(df_eval)*100:.1f}%)")

if len(df_optimos) > 0:
    print("=== SERIES QUE CUMPLEN CRITERIO R² >= 0.7 ===")
    display(df_optimos[['municipio', 'cultivo', 'arima_order', 'r2_score', 'rmse', 'mape_pct']])
else:
    print("=== TODAS LAS SERIES EVALUADAS ORDENADAS POR R² ===")
    display(df_eval[['municipio', 'cultivo', 'arima_order', 'r2_score', 'rmse', 'mape_pct']])
"""))

# Celda 10: Sección 6 Markdown
cells.append(make_markdown_cell("""
---
## 6. Predicción a 3 Años (2025 - 2027)

Proyección de producción agrícola futura a 3 años para las combinaciones (Municipio - Cultivo) con mayor ajuste y capacidad predictiva, incluyendo intervalos de confianza al 80%.
"""))

# Celda 11: Sección 6 Code
cells.append(make_code_cell("""
img1 = get_path('notebooks/figures/arima_plus_top_pronosticos_3_anos.png')
if not img1.exists():
    img1 = get_path('figures/arima_plus_top_pronosticos_3_anos.png')
if img1.exists():
    display(Image(filename=str(img1)))

img2 = get_path('notebooks/figures/arima_plus_grid_pronosticos_municipios.png')
if not img2.exists():
    img2 = get_path('figures/arima_plus_grid_pronosticos_municipios.png')
if img2.exists():
    display(Image(filename=str(img2)))

print("=== TABLA COMPLETA DE PRONÓSTICOS A 3 AÑOS (2025 - 2027) PARA TODAS LAS PAREJAS MUNICIPIO - CULTIVO ===")
display(df_fc)
"""))

notebook_content = {
    "cells": cells,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3 (.venv)",
            "language": "python",
            "name": "python3"
        },
        "language_info": {
            "name": "python"
        }
    },
    "nbformat": 4,
    "nbformat_minor": 2
}

nb_path = Path("notebooks/eda_y_prediccion_arima_plus.ipynb")
with open(nb_path, "w", encoding="utf-8") as f:
    json.dump(notebook_content, f, indent=2, ensure_ascii=False)

print(f"Notebook ARIMA_PLUS actualizado exitosamente en: {nb_path}")
print(f"Tiempo transcurrido: {time.time()-t0:.2f}s.")
