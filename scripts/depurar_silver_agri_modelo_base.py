"""Script de depuración de datos atípicos para datagov-477214.valledata.silver_agri_modelo_base.
Aplica criterios agronómicos y estadísticos para remover errores de digitación, rendimientos imposibles y picos anómalos.
"""

from pathlib import Path
import pandas as pd
import numpy as np

def depurar_silver_agri_modelo_base(csv_path: Path) -> pd.DataFrame:
    print(f"--> Cargando {csv_path} para depuración de datos atípicos...")
    df = pd.read_csv(csv_path)
    initial_rows = len(df)
    
    # Estandarizar nombres
    renames = {
        "nombre_cultivo": "cultivo",
        "rendimiento_t_ha": "rendimiento_toneladas_ha",
        "id_municipio": "codigo_municipio",
        "oni": "promedio_oni",
        "indice_oni": "promedio_oni",
    }
    df = df.rename(columns={k: v for k, v in renames.items() if k in df.columns})
    
    # 1. Filtros de validez básica (Valores no nulos y mayores a 0 en variables productivas)
    if 'ratio_cosecha' not in df.columns and 'hectareas_sembradas' in df.columns and 'hectareas_cosechadas' in df.columns:
        df['ratio_cosecha'] = np.where(df['hectareas_sembradas'] > 0, df['hectareas_cosechadas'] / df['hectareas_sembradas'], 1.0)

    # Llenar la columna semestre si es nula a partir de ciclo
    if 'ciclo' in df.columns:
        df['semestre'] = df.apply(
            lambda r: 1 if '1' in str(r.get('ciclo','')) else (2 if '2' in str(r.get('ciclo','')) else 0),
            axis=1
        )
    elif 'semestre' in df.columns:
        df['semestre'] = df['semestre'].fillna(0).astype(int)
    else:
        df['semestre'] = 0

    # 2. Atípicos agronómicos de Rendimiento (Ton/Ha)
    def es_rendimiento_atipico(row):
        crop = str(row.get('cultivo', '')).upper()
        rend = row.get('rendimiento_t_ha', row.get('rendimiento_toneladas_ha', np.nan))
        if pd.isnull(rend):
            return True
        if 'AZÚCAR' in crop or 'PANELERA' in crop:
            return rend > 180.0 or rend < 2.0
        elif 'TOMATE' in crop or 'PIÑA' in crop or 'SÁBILA' in crop or 'YUCA' in crop or 'PAPAYA' in crop:
            return rend > 120.0 or rend < 0.1
        else:
            return rend > 85.0 or rend < 0.05

    mask_yield = df.apply(es_rendimiento_atipico, axis=1)
    
    # 3. Atípicos de ratio de cosecha (hectáreas cosechadas / hectáreas sembradas > 1.25 o < 0.01)
    mask_ratio = (df['ratio_cosecha'] > 1.25) | (df['ratio_cosecha'] < 0.01)
    
    # 4. Atípicos locales por serie temporal (municipio, cultivo) usando distorsión IQR de producción
    def es_pico_serie(group):
        if len(group) < 4:
            return pd.Series(False, index=group.index)
        med_prod = group['produccion_toneladas'].median()
        iqr_prod = group['produccion_toneladas'].quantile(0.75) - group['produccion_toneladas'].quantile(0.25)
        
        rend_col = 'rendimiento_t_ha' if 'rendimiento_t_ha' in group.columns else 'rendimiento_toneladas_ha'
        med_rend = group[rend_col].median()
        iqr_rend = group[rend_col].quantile(0.75) - group[rend_col].quantile(0.25)
        
        spike_prod = (group['produccion_toneladas'] > med_prod + 4.0 * max(iqr_prod, 1.0)) & (group['produccion_toneladas'] > med_prod * 6.0)
        spike_rend = (group[rend_col] > med_rend + 4.0 * max(iqr_rend, 0.5)) & (group[rend_col] > med_rend * 5.0)
        return spike_prod | spike_rend

    group_col_mun = 'id_municipio' if 'id_municipio' in df.columns else 'codigo_municipio'
    mask_spikes = df.groupby([group_col_mun, 'cultivo'], group_keys=False).apply(es_pico_serie)

    mask_total = mask_yield | mask_ratio | mask_spikes
    
    print(f"--> Atípicos identificados y removidos:")
    print(f"    - Rendimientos inviables / excesivos: {mask_yield.sum():,}")
    print(f"    - Ratios de cosecha anómalos: {mask_ratio.sum():,}")
    print(f"    - Picos locales severos en series: {mask_spikes.sum():,}")
    print(f"    - TOTAL registros atípicos eliminados: {mask_total.sum():,} ({(mask_total.sum()/initial_rows)*100:.2f}%)")

    df_clean = df[~mask_total].copy()
    print(f"--> Filas finales depuradas en silver_agri_modelo_base: {len(df_clean):,}")
    
    return df_clean

if __name__ == "__main__":
    csv_path = Path("data/silver_agri_modelo_base.csv")
    df_clean = depurar_silver_agri_modelo_base(csv_path)
    df_clean.to_csv(csv_path, index=False)
    print(f"✅ Tabla `data/silver_agri_modelo_base.csv` depurada y actualizada exitosamente.")
