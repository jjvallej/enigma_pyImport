"""Script integral para realizar:
1. EDA por Cultivo
2. EDA por Municipio
3. EDA por Municipio y Cultivo con Análisis de Correlación
4. Modelado Predictivo ARIMA_PLUS (AutoARIMA)
5. Evaluación Out-of-Sample (Backtesting) y filtro R² >= 0.7
6. Pronóstico a 3 Años (2025-2027) para combinaciones óptimas
"""

import sys
import os
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import r2_score, mean_squared_error, mean_absolute_error
from pmdarima import auto_arima
import warnings
warnings.filterwarnings('ignore')

# Configuración visual
sns.set_theme(style="whitegrid", palette="muted")
plt.rcParams["font.sans-serif"] = "DejaVu Sans"
plt.rcParams["font.size"] = 10

# Rutas de entrada y salida
DATA_PATH = Path("data/dataset_consolidado_valle.csv")
FIG_DIR = Path("notebooks/figures")
FIG_DIR.mkdir(parents=True, exist_ok=True)
OUT_DIR = Path("data")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def cargar_y_preparar_datos():
    """Carga y limpia el dataset agrícola (silver_agri_modelo_base / dataset_consolidado_valle)."""
    print("--> Cargando dataset agrícola...")
    data_sources = [
        Path("data/silver_agri_modelo_base.csv"),
        Path("data/dataset_prediccion_rentabilidad.csv"),
        DATA_PATH,
        Path("data/cultivos_valle.csv")
    ]
    
    target_path = next((p for p in data_sources if p.exists()), DATA_PATH)
    print(f"--> Fuente seleccionada: {target_path}")
    df = pd.read_csv(target_path)
    
    renames = {
        "nombre_cultivo": "cultivo",
        "precio": "precio_promedio_anual_sipsa",
        "indice_oni": "promedio_oni",
        "oni": "promedio_oni",
        "rendimiento_t_ha": "rendimiento_toneladas_ha",
        "id_municipio": "codigo_municipio",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})

    if "cultivo" in df.columns:
        df["cultivo"] = df["cultivo"].astype(str).str.strip().str.title()

    # Mapear nombre de municipio si solo está codigo_municipio
    if ("municipio" not in df.columns or df["municipio"].isnull().all()) and "codigo_municipio" in df.columns:
        if Path("data/municipios_valle_clean.csv").exists():
            df_mun_map = pd.read_csv("data/municipios_valle_clean.csv")[["codigo_municipio", "municipio"]]
            df = pd.merge(df, df_mun_map, on="codigo_municipio", how="left")
        elif Path("data/dataset_consolidado_valle.csv").exists():
            df_cons = pd.read_csv("data/dataset_consolidado_valle.csv")[["codigo_municipio", "municipio"]].drop_duplicates()
            df = pd.merge(df, df_cons, on="codigo_municipio", how="left")

    if "municipio" not in df.columns:
        df["municipio"] = "Valle del Cauca"

    if "produccion_toneladas" not in df.columns and Path("data/cultivos_valle.csv").exists():
        df_crops = pd.read_csv("data/cultivos_valle.csv")
        cols_to_add = [c for c in ["produccion_toneladas", "rendimiento_toneladas_ha"] if c in df_crops.columns and c not in df.columns]
        if cols_to_add and "cultivo" in df.columns and "municipio" in df.columns:
            df_crops["municipio_tmp"] = df_crops["municipio"].astype(str).str.strip().str.lower()
            df_crops["cultivo_tmp"] = df_crops["cultivo"].astype(str).str.strip().str.lower()
            df["municipio_tmp"] = df["municipio"].astype(str).str.strip().str.lower()
            df["cultivo_tmp"] = df["cultivo"].astype(str).str.strip().str.lower()
            df_sub = df_crops[["anio", "municipio_tmp", "cultivo_tmp"] + cols_to_add].drop_duplicates(subset=["anio", "municipio_tmp", "cultivo_tmp"])
            df = pd.merge(df, df_sub, on=["anio", "municipio_tmp", "cultivo_tmp"], how="left").drop(columns=["municipio_tmp", "cultivo_tmp"])

    if "produccion_toneladas" not in df.columns:
        if "hectareas_cosechadas" in df.columns and "rendimiento_toneladas_ha" in df.columns:
            df["produccion_toneladas"] = df["hectareas_cosechadas"] * df["rendimiento_toneladas_ha"]
        else:
            df["produccion_toneladas"] = 0.0

    if "precio_promedio_anual_sipsa" not in df.columns and Path("data/dataset_consolidado_valle.csv").exists():
        df_prices = pd.read_csv("data/dataset_consolidado_valle.csv")
        p_col = "precio_promedio_anual_sipsa" if "precio_promedio_anual_sipsa" in df_prices.columns else ("precio" if "precio" in df_prices.columns else None)
        c_col = "cultivo" if "cultivo" in df_prices.columns else ("nombre_cultivo" if "nombre_cultivo" in df_prices.columns else None)
        if p_col and c_col and "anio" in df_prices.columns:
            df_prices["cultivo_clean"] = df_prices[c_col].astype(str).str.strip().str.title()
            df_sub_price = df_prices[["anio", "cultivo_clean", p_col]].rename(columns={"cultivo_clean": "cultivo", p_col: "precio_promedio_anual_sipsa"}).dropna().groupby(["anio", "cultivo"])["precio_promedio_anual_sipsa"].mean().reset_index()
            df = pd.merge(df, df_sub_price, on=["anio", "cultivo"], how="left")

    if "precio_promedio_anual_sipsa" not in df.columns:
        df["precio_promedio_anual_sipsa"] = np.nan

    num_cols = ['hectareas_sembradas', 'hectareas_cosechadas', 'produccion_toneladas', 
                'rendimiento_toneladas_ha', 'precio_promedio_anual_sipsa', 'promedio_oni']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '.'), errors='coerce')

    df['municipio'] = df['municipio'].astype(str).str.strip()
    df['cultivo'] = df['cultivo'].astype(str).str.strip()
    if 'tipo_cultivo' in df.columns:
        df['tipo_cultivo'] = df['tipo_cultivo'].astype(str).str.strip().str.title()
    else:
        df['tipo_cultivo'] = "General"
    
    # Calcular valor estimado de venta (Millones COP) donde haya precio SIPSA
    if 'precio_promedio_anual_sipsa' in df.columns:
        df['kilos_cosechados'] = df['produccion_toneladas'] * 1000.0
        df['valor_venta_millones'] = (df['kilos_cosechados'] * df['precio_promedio_anual_sipsa']) / 1e6
    else:
        df['valor_venta_millones'] = np.nan
    
    print(f"Dataset cargado con {len(df):,} filas. Años: {df['anio'].min()} - {df['anio'].max()}")
    return df

def ejecutar_eda_cultivo(df):
    """1. EDA por Cultivo"""
    print("\n==========================================")
    print("1. EJECUTANDO EDA POR CULTIVO")
    print("==========================================")
    
    resumen_cultivo = df.groupby('cultivo').agg(
        total_produccion=('produccion_toneladas', 'sum'),
        total_ha_sembradas=('hectareas_sembradas', 'sum'),
        total_ha_cosechadas=('hectareas_cosechadas', 'sum'),
        rendimiento_promedio=('rendimiento_toneladas_ha', 'mean'),
        precio_prom_sipsa=('precio_promedio_anual_sipsa', 'mean'),
        total_valor_venta_millones=('valor_venta_millones', 'sum'),
        anios_presencia=('anio', 'nunique'),
        municipios_presencia=('municipio', 'nunique')
    ).reset_index()
    
    # Coeficiente de variación de producción por cultivo
    cv_cultivo = df.groupby('cultivo')['produccion_toneladas'].apply(
        lambda x: (x.std() / x.mean()) * 100 if x.mean() > 0 else 0
    ).reset_index(name='coef_variacion_prod_pct')
    
    resumen_cultivo = resumen_cultivo.merge(cv_cultivo, on='cultivo')
    resumen_cultivo.sort_values('total_produccion', ascending=False, inplace=True)
    resumen_cultivo.to_csv(OUT_DIR / "eda_resumen_por_cultivo.csv", index=False)
    
    print(f"Total cultivos analizados: {len(resumen_cultivo)}")
    print("Cultivos analizados por producción total (Ton):")
    print(resumen_cultivo[['cultivo', 'total_produccion', 'rendimiento_promedio', 'total_ha_sembradas']])
    
    # Gráficos EDA Cultivo
    # Fig C1: Todos los Cultivos por Producción Total
    fig, ax = plt.subplots(figsize=(12, 16))
    top15_prod = resumen_cultivo.copy()
    sns.barplot(data=top15_prod, x='total_produccion', y='cultivo', hue='cultivo', palette='viridis', ax=ax, legend=False)
    ax.set_title('Todos los Cultivos por Producción Acumulada (Toneladas, 2000-2024)', fontweight='bold', fontsize=12)
    ax.set_xlabel('Producción Total (Toneladas)')
    ax.set_ylabel('Cultivo')
    for p in ax.patches:
        width = p.get_width()
        if width > 0:
            ax.annotate(f'{width:,.0f}', (width, p.get_y() + p.get_height() / 2.),
                        ha='left', va='center', xytext=(5, 0), textcoords='offset points', fontsize=7)
    plt.tight_layout()
    plt.savefig(FIG_DIR / "eda_cultivo_top15_produccion.png", dpi=150)
    plt.close()
    
    df_box = df[(df['rendimiento_toneladas_ha'] > 0) & (df['rendimiento_toneladas_ha'] <= 100)].dropna(subset=['tipo_cultivo', 'rendimiento_toneladas_ha'])
    if len(df_box) > 0:
        fig, ax = plt.subplots(figsize=(12, 6))
        df_box.boxplot(column='rendimiento_toneladas_ha', by='tipo_cultivo', ax=ax)
        ax.set_yscale('log')
        ax.set_title('Distribución del Rendimiento (Ton/Ha) por Grupo de Cultivo (Escala Log)', fontweight='bold', fontsize=12)
        ax.set_xlabel('Tipo de Cultivo')
        ax.set_ylabel('Rendimiento (Ton/Ha, Log)')
        plt.suptitle('')
        plt.tight_layout()
        plt.savefig(FIG_DIR / "eda_cultivo_rendimiento_por_grupo.png", dpi=150)
        plt.close()
    
    return resumen_cultivo

def ejecutar_eda_municipio(df):
    """2. EDA por Municipio"""
    print("\n==========================================")
    print("2. EJECUTANDO EDA POR MUNICIPIO")
    print("==========================================")
    
    resumen_mun = df.groupby('municipio').agg(
        total_produccion=('produccion_toneladas', 'sum'),
        total_ha_sembradas=('hectareas_sembradas', 'sum'),
        total_ha_cosechadas=('hectareas_cosechadas', 'sum'),
        rendimiento_promedias=('rendimiento_toneladas_ha', 'mean'),
        diversidad_cultivos=('cultivo', 'nunique'),
        anios_cobertura=('anio', 'nunique'),
        total_valor_venta_millones=('valor_venta_millones', 'sum')
    ).reset_index()
    
    resumen_mun.sort_values('total_produccion', ascending=False, inplace=True)
    resumen_mun.to_csv(OUT_DIR / "eda_resumen_por_municipio.csv", index=False)
    
    print(f"Total municipios analizados: {len(resumen_mun)}")
    print("Todos los municipios por producción agrícola total:")
    print(resumen_mun[['municipio', 'total_produccion', 'diversidad_cultivos', 'total_ha_sembradas']])
    
    # Fig M1: Todos los Municipios por Producción Agrícola Total
    fig, ax = plt.subplots(figsize=(12, 10))
    top15_mun = resumen_mun.copy()
    sns.barplot(data=top15_mun, x='total_produccion', y='municipio', hue='municipio', palette='magma', ax=ax, legend=False)
    ax.set_title('Todos los Municipios por Producción Agrícola Total (Toneladas, 2000-2024)', fontweight='bold', fontsize=12)
    ax.set_xlabel('Producción Total (Toneladas)')
    ax.set_ylabel('Municipio')
    for p in ax.patches:
        width = p.get_width()
        if width > 0:
            ax.annotate(f'{width:,.0f}', (width, p.get_y() + p.get_height() / 2.),
                        ha='left', va='center', xytext=(5, 0), textcoords='offset points', fontsize=8)
    plt.tight_layout()
    plt.savefig(FIG_DIR / "eda_municipio_top15_produccion.png", dpi=150)
    plt.close()
    
    # Fig M2: Diversidad Agrícola vs Producción por Municipio
    fig, ax = plt.subplots(figsize=(10, 6))
    sns.scatterplot(data=resumen_mun, x='diversidad_cultivos', y='total_produccion', 
                    size='total_ha_sembradas', sizes=(40, 400), hue='rendimiento_promedias', palette='coolwarm', ax=ax)
    ax.set_yscale('log')
    ax.set_title('Diversidad Agrícola (N° Cultivos) vs Producción Total por Municipio (Escala Log)', fontweight='bold', fontsize=12)
    ax.set_xlabel('Número de Cultivos Distintos Registrados')
    ax.set_ylabel('Producción Agrícola Acumulada (Ton, Log)')
    
    # Etiquetar algunos municipios clave
    for _, row in resumen_mun.head(8).iterrows():
        ax.annotate(row['municipio'], (row['diversidad_cultivos'], row['total_produccion']),
                    xytext=(5, 5), textcoords='offset points', fontsize=9, fontweight='semibold')
    
    plt.tight_layout()
    plt.savefig(FIG_DIR / "eda_municipio_diversidad_vs_produccion.png", dpi=150)
    plt.close()
    
    return resumen_mun

def ejecutar_eda_municipio_cultivo_correlacion(df):
    """3. EDA por Municipio y Cultivo con Análisis de Correlación"""
    print("\n==========================================")
    print("3. EJECUTANDO EDA POR MUNICIPIO, CULTIVO Y MATRIZ DE CORRELACIÓN")
    print("==========================================")
    
    # Agrupar datos anuales por municipio y cultivo
    df_pair_year = df.groupby(['municipio', 'cultivo', 'anio']).agg(
        hectareas_sembradas=('hectareas_sembradas', 'sum'),
        hectareas_cosechadas=('hectareas_cosechadas', 'sum'),
        produccion_toneladas=('produccion_toneladas', 'sum'),
        rendimiento_toneladas_ha=('rendimiento_toneladas_ha', 'mean'),
        precio_promedio_anual_sipsa=('precio_promedio_anual_sipsa', 'mean'),
        promedio_oni=('promedio_oni', 'mean')
    ).reset_index()
    
    # Calcular matriz de correlación global entre variables numéricas
    vars_corr = ['hectareas_sembradas', 'hectareas_cosechadas', 'produccion_toneladas', 
                 'rendimiento_toneladas_ha', 'precio_promedio_anual_sipsa', 'promedio_oni']
    
    corr_pearson = df_pair_year[vars_corr].corr(method='pearson')
    corr_spearman = df_pair_year[vars_corr].corr(method='spearman')
    
    print("\nMatriz de Correlación de Pearson (Variables Clave):")
    print(corr_pearson.round(3))
    
    corr_pearson.to_csv(OUT_DIR / "matriz_correlacion_pearson.csv")
    corr_spearman.to_csv(OUT_DIR / "matriz_correlacion_spearman.csv")
    
    # Fig MC1: Heatmap de Correlación Pearson
    fig, ax = plt.subplots(figsize=(8, 6))
    sns.heatmap(corr_pearson, annot=True, fmt=".2f", cmap='RdBu_r', vmin=-1, vmax=1, ax=ax, square=True,
                linewidths=0.5, cbar_kws={"shrink": .8})
    ax.set_title('Matriz de Correlación de Pearson entre Variables Agrícolas', fontweight='bold', fontsize=12)
    plt.tight_layout()
    plt.savefig(FIG_DIR / "eda_correlacion_pearson_heatmap.png", dpi=150)
    plt.close()
    
    # Identificar parejas (municipio, cultivo) con suficiencia de datos (>= 15 años)
    pair_counts = df_pair_year.groupby(['municipio', 'cultivo'])['anio'].nunique().reset_index()
    candidate_pairs = pair_counts[pair_counts['anio'] >= 15].copy()
    print(f"\nParejas (Municipio - Cultivo) candidatas con >= 15 años de datos: {len(candidate_pairs)}")
    
    # Calcular correlación interna por pareja (Hectáreas cosechadas vs Producción, Rendimiento vs ONI)
    pair_corrs = []
    for _, row in candidate_pairs.iterrows():
        mun, cul = row['municipio'], row['cultivo']
        sub = df_pair_year[(df_pair_year['municipio'] == mun) & (df_pair_year['cultivo'] == cul)].sort_values('anio')
        
        corr_ha_prod = sub['hectareas_cosechadas'].corr(sub['produccion_toneladas'])
        corr_oni_rend = sub['promedio_oni'].corr(sub['rendimiento_toneladas_ha'])
        
        mean_prod = sub['produccion_toneladas'].mean()
        std_prod = sub['produccion_toneladas'].std()
        cv_prod = (std_prod / mean_prod * 100) if mean_prod > 0 else np.nan
        
        pair_corrs.append({
            'municipio': mun,
            'cultivo': cul,
            'anios_disponibles': len(sub),
            'prod_promedio_anual': mean_prod,
            'cv_produccion_pct': cv_prod,
            'corr_ha_vs_prod': corr_ha_prod,
            'corr_oni_vs_rendimiento': corr_oni_rend
        })
    
    df_pair_corrs = pd.DataFrame(pair_corrs)
    df_pair_corrs.sort_values('prod_promedio_anual', ascending=False, inplace=True)
    df_pair_corrs.to_csv(OUT_DIR / "eda_parejas_municipio_cultivo_correlaciones.csv", index=False)
    
    # Fig MC2: Dispersión Correlación Hectáreas vs Producción por Pareja
    fig, ax = plt.subplots(figsize=(10, 5))
    sns.histplot(df_pair_corrs['corr_ha_vs_prod'].dropna(), bins=20, kde=True, color='teal', ax=ax)
    ax.set_title('Distribución de Correlaciones (Hectáreas Cosechadas vs Producción) en Parejas Candidatas', fontweight='bold', fontsize=12)
    ax.set_xlabel('Coeficiente de Correlación de Pearson (r)')
    ax.set_ylabel('Frecuencia (Cantidad de Parejas)')
    plt.tight_layout()
    plt.savefig(FIG_DIR / "eda_distribucion_correlacion_ha_vs_prod.png", dpi=150)
    plt.close()
    
    return df_pair_year, df_pair_corrs

def modelar_arima_plus_y_evaluar(df_pair_year, df_pair_corrs):
    """4. Modelado ARIMA_PLUS, Backtesting out-of-sample, Filtro R² >= 0.7 y Pronóstico 3 Años (2025-2027)"""
    print("\n==========================================")
    print("4. ENTRENANDO Y EVALUANDO MODELOS ARIMA_PLUS (BACKTESTING & PRONÓSTICO)")
    print("==========================================")
    
    TEST_YEARS = 3  # Evaluar en los últimos 3 años (2022, 2023, 2024)
    results_eval = []
    forecasts_3y = []
    
    # Tomar las top parejas candidatas (con suficientes datos y volumen representativo)
    candidatos = df_pair_corrs[df_pair_corrs['anios_disponibles'] >= 15].copy()
    print(f"Evaluando {len(candidatos)} series temporales candidatas para ARIMA_PLUS...")
    
    for idx, row in candidatos.iterrows():
        mun, cul = row['municipio'], row['cultivo']
        ts_data = df_pair_year[(df_pair_year['municipio'] == mun) & (df_pair_year['cultivo'] == cul)].sort_values('anio').copy()
        
        # Rellenar la secuencia de años de min_year a max_year si hay vacíos
        min_y, max_y = ts_data['anio'].min(), ts_data['anio'].max()
        full_years = pd.DataFrame({'anio': range(min_y, max_y + 1)})
        ts_full = pd.merge(full_years, ts_data, on='anio', how='left')
        
        # Interpolación lineal para faltantes intermedios leves
        ts_full['produccion_toneladas'] = ts_full['produccion_toneladas'].interpolate(method='linear').bfill().ffill()
        ts_full['hectareas_cosechadas'] = ts_full['hectareas_cosechadas'].interpolate(method='linear').bfill().ffill()
        ts_full['promedio_oni'] = ts_full['promedio_oni'].interpolate(method='linear').bfill().ffill()
        
        n_obs = len(ts_full)
        if n_obs < 12:
            continue  # Requerir al menos 12 observaciones continuas
            
        y = ts_full['produccion_toneladas'].values
        years = ts_full['anio'].values
        
        x_exog = ts_full[['hectareas_cosechadas']].values
        
        train_y = y[:-TEST_YEARS]
        test_y = y[-TEST_YEARS:]
        train_x = x_exog[:-TEST_YEARS]
        test_x = x_exog[-TEST_YEARS:]
        
        if len(train_y) < 8:
            continue
            
        best_model = None
        best_r2 = -999.0
        best_rmse = 9999999.0
        best_mae = 9999999.0
        best_mape = 9999999.0
        best_order_str = ""
        best_pred_cv = None
        used_exog = False
        
        # 1. Probar ARIMA univariado
        try:
            m_uni = auto_arima(
                train_y,
                start_p=0, max_p=3,
                start_q=0, max_q=3,
                d=None, seasonal=False, stepwise=True,
                suppress_warnings=True, error_action='ignore'
            )
            p_uni = m_uni.predict(n_periods=TEST_YEARS)
            r2_uni = r2_score(test_y, p_uni)
            if r2_uni > best_r2:
                best_r2 = r2_uni
                best_model = m_uni
                best_pred_cv = p_uni
                best_order_str = f"ARIMA{m_uni.order}"
                used_exog = False
        except Exception:
            pass
            
        # 2. Probar ARIMAX multivariado con Hectáreas Cosechadas como covariable exógena
        try:
            m_exog = auto_arima(
                train_y, X=train_x,
                start_p=0, max_p=3,
                start_q=0, max_q=3,
                d=None, seasonal=False, stepwise=True,
                suppress_warnings=True, error_action='ignore'
            )
            p_exog = m_exog.predict(n_periods=TEST_YEARS, X=test_x)
            r2_exog = r2_score(test_y, p_exog)
            if r2_exog > best_r2:
                best_r2 = r2_exog
                best_model = m_exog
                best_pred_cv = p_exog
                best_order_str = f"ARIMAX{m_exog.order}+Hectáreas"
                used_exog = True
        except Exception:
            pass
            
        if best_model is None or best_pred_cv is None:
            continue
            
        best_rmse = np.sqrt(mean_squared_error(test_y, best_pred_cv))
        best_mae = mean_absolute_error(test_y, best_pred_cv)
        best_mape = np.mean(np.abs((test_y - best_pred_cv) / np.maximum(test_y, 1.0))) * 100
        
        cumple = (best_r2 >= 0.7)
        
        results_eval.append({
            'municipio': mun,
            'cultivo': cul,
            'anios_historicos': n_obs,
            'prod_promedio_historica': np.mean(y),
            'arima_order': best_order_str,
            'r2_score': best_r2,
            'rmse': best_rmse,
            'mae': best_mae,
            'mape_pct': best_mape,
            'cumple_r2_07': cumple
        })
        
        # Si el modelo cumple estrictamente R² >= 0.7, entrenar con el 100% de datos y proyectar a 3 años (2025-2027)
        if cumple:
            try:
                if used_exog:
                    # Para proyección a 3 años con ARIMAX, asumir área cosechada promedio reciente
                    recent_ha = np.mean(x_exog[-3:], axis=0).reshape(1, -1)
                    future_x = np.tile(recent_ha, (3, 1))
                    m_full = auto_arima(y, X=x_exog, start_p=0, max_p=3, start_q=0, max_q=3, d=None, seasonal=False, stepwise=True, suppress_warnings=True, error_action='ignore')
                    fc_3y, conf_int = m_full.predict(n_periods=3, X=future_x, return_conf_int=True, alpha=0.20)
                    ord_str = f"ARIMAX{m_full.order}+Hectáreas"
                else:
                    m_full = auto_arima(y, start_p=0, max_p=3, start_q=0, max_q=3, d=None, seasonal=False, stepwise=True, suppress_warnings=True, error_action='ignore')
                    fc_3y, conf_int = m_full.predict(n_periods=3, return_conf_int=True, alpha=0.20)
                    ord_str = f"ARIMA{m_full.order}"
                    
                for i, target_year in enumerate([2025, 2026, 2027]):
                    forecasts_3y.append({
                        'municipio': mun,
                        'cultivo': cul,
                        'arima_order': ord_str,
                        'r2_backtesting': best_r2,
                        'anio_pronostico': target_year,
                        'produccion_predicha_ton': max(0.0, fc_3y[i]),
                        'intervalo_inferior_80': max(0.0, conf_int[i][0]),
                        'intervalo_superior_80': max(0.0, conf_int[i][1])
                    })
            except Exception:
                pass
            continue

    df_eval = pd.DataFrame(results_eval)
    df_eval.sort_values('r2_score', ascending=False, inplace=True)
    df_eval.to_csv(OUT_DIR / "evaluacion_modelos_arima.csv", index=False)
    
    df_fc = pd.DataFrame(forecasts_3y)
    df_fc.to_csv(OUT_DIR / "predicciones_3_anos_r2_optimo.csv", index=False)
    
    # Resumen de Resultados
    cumplen_r2 = df_eval[df_eval['cumple_r2_07'] == True]
    print(f"\n==========================================")
    print(f"RESULTADOS MODELADO ARIMA_PLUS:")
    print(f"Total series evaluadas: {len(df_eval)}")
    print(f"Series que CUMPLEN R² >= 0.7: {len(cumplen_r2)} ({len(cumplen_r2)/len(df_eval)*100:.1f}%)")
    print("==========================================")
    
    if len(cumplen_r2) > 0:
        print("\nTop Parejas (Municipio - Cultivo) con R² >= 0.7:")
        print(cumplen_r2[['municipio', 'cultivo', 'arima_order', 'r2_score', 'rmse', 'mape_pct']].head(10))
    else:
        print("\nTop 10 Parejas ordenadas por mayor R² alcanzado:")
        print(df_eval[['municipio', 'cultivo', 'arima_order', 'r2_score', 'rmse', 'mape_pct']].head(10))

    # Graficar TODAS las predicciones por Municipio / Cultivo
    if len(df_fc) > 0:
        df_fc['mun_cultivo'] = df_fc['municipio'] + ' - ' + df_fc['cultivo']
        tot_fc = df_fc.groupby(['municipio', 'cultivo', 'mun_cultivo']).agg(
            prod_predicha_promedio=('produccion_predicha_ton', 'mean'),
            r2_backtesting=('r2_backtesting', 'first')
        ).reset_index().sort_values('prod_predicha_promedio', ascending=False)
        
        # Figura 1: Diagrama completo de Barras con TODAS las parejas (Municipio - Cultivo) predichas
        fig, ax = plt.subplots(figsize=(16, max(12, len(tot_fc) * 0.22)))
        sns.barplot(data=tot_fc, x='prod_predicha_promedio', y='mun_cultivo', hue='municipio', dodge=False, palette='tab20', ax=ax, legend=False)
        ax.set_title(f'Pronóstico Promedio de Producción (2025-2027) para TODAS las Parejas (Municipio - Cultivo) Evaluadas ({len(tot_fc)} Parejas)', fontweight='bold', fontsize=14, pad=12)
        ax.set_xlabel('Producción Predicha Promedio (Toneladas / Año)', fontsize=12)
        ax.set_ylabel('Pareja (Municipio - Cultivo)', fontsize=12)
        
        for p in ax.patches:
            w = p.get_width()
            if w > 0:
                ax.annotate(f'{w:,.0f} Ton', (w, p.get_y() + p.get_height() / 2.),
                            ha='left', va='center', xytext=(5, 0), textcoords='offset points', fontsize=6)
                            
        plt.tight_layout()
        plt.savefig(FIG_DIR / "arima_plus_top_pronosticos_3_anos.png", dpi=150, bbox_inches='tight')
        plt.close()
        
        # Figura 2: Grilla de subplots con series de tiempo de las mejores parejas por municipio
        muns_fc = sorted(df_fc['municipio'].unique().tolist())
        n_muns = len(muns_fc)
        n_cols = 4
        n_rows = (n_muns + n_cols - 1) // n_cols
        
        fig, axes = plt.subplots(n_rows, n_cols, figsize=(22, 4.0 * n_rows))
        axes = axes.flatten()
        
        for idx, mun in enumerate(muns_fc):
            ax = axes[idx]
            sub_fc_mun = df_fc[df_fc['municipio'] == mun]
            crops_mun = sub_fc_mun['cultivo'].unique()
            
            for cul in crops_mun:
                sub_h = df_pair_year[(df_pair_year['municipio'] == mun) & (df_pair_year['cultivo'] == cul)].sort_values('anio')
                sub_f = sub_fc_mun[sub_fc_mun['cultivo'] == cul].sort_values('anio_pronostico')
                
                if len(sub_h) > 0:
                    ax.plot(sub_h['anio'], sub_h['produccion_toneladas'], marker='o', label=f'{cul} (Histórico)', alpha=0.8)
                if len(sub_f) > 0:
                    ax.plot(sub_f['anio_pronostico'], sub_f['produccion_predicha_ton'], marker='s', linestyle='--', label=f'{cul} (Pronóstico 2025-27)')
                    
            ax.set_title(f'Pronósticos ARIMA: {mun}', fontweight='bold', fontsize=11)
            ax.set_xlabel('Año', fontsize=8)
            ax.set_ylabel('Producción (Ton)', fontsize=8)
            h_leg, l_leg = ax.get_legend_handles_labels()
            if h_leg:
                ax.legend(h_leg, l_leg, bbox_to_anchor=(1.01, 1), loc='upper left', fontsize=5)
                
        for j in range(idx + 1, len(axes)):
            axes[j].axis('off')
            
        plt.suptitle('Pronósticos Agrícolas 2025-2027 por Municipio y Todos sus Cultivos Predichos', fontweight='bold', fontsize=16, y=1.01)
        plt.tight_layout()
        plt.savefig(FIG_DIR / "arima_plus_grid_pronosticos_municipios.png", dpi=130, bbox_inches='tight')
        plt.close()
        
    return df_eval, df_fc

def main():
    print("Iniciando Pipeline Completo: EDA Multinivel + ARIMA_PLUS + Pronóstico 3 Años...")
    df = cargar_y_preparar_datos()
    resumen_cultivo = ejecutar_eda_cultivo(df)
    resumen_mun = ejecutar_eda_municipio(df)
    df_pair_year, df_pair_corrs = ejecutar_eda_municipio_cultivo_correlacion(df)
    df_eval, df_fc = modelar_arima_plus_y_evaluar(df_pair_year, df_pair_corrs)
    print("\nProcess finished successfully! High-resolution figures saved to notebooks/figures/ and CSV data saved to data/")

if __name__ == "__main__":
    main()
