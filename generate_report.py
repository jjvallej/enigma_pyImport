import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

df = pd.read_csv('data/ckan_comentarios.csv')
df['created_at'] = pd.to_datetime(df['created_at'])
df['created_year_month'] = df['created_at'].dt.to_period('M').astype(str)
df['municipio_label'] = df['municipio'].str.capitalize()
sentiment_map = {'POS': 'Positivo', 'NEG': 'Negativo', 'NEU': 'Neutral'}
df['sentiment_name'] = df['fixture_sentiment_hint'].map(sentiment_map)

COLOR_POS = '#10b981'
COLOR_NEG = '#ef4444'
COLOR_NEU = '#3b82f6'
COLOR_MAP = {'Positivo': COLOR_POS, 'Negativo': COLOR_NEG, 'Neutral': COLOR_NEU}

# 1. Timeline
ts_df = df.groupby(['created_year_month', 'sentiment_name']).size().reset_index(name='count')
fig_ts = px.line(
    ts_df, 
    x='created_year_month', 
    y='count', 
    color='sentiment_name', 
    color_discrete_map=COLOR_MAP, 
    markers=True, 
    title='📈 Evolución Temporal del Sentimiento (2022-2025)'
)
fig_ts.update_layout(paper_bgcolor='#1e293b', plot_bgcolor='#1e293b', font=dict(color='#f8fafc'))

# 2. Ranking NSS
muni_calc = df.groupby(['municipio_label', 'fixture_sentiment_hint']).size().unstack(fill_value=0)
muni_calc['Total'] = muni_calc.sum(axis=1)
muni_calc['NSS'] = ((muni_calc['POS'] - muni_calc['NEG']) / muni_calc['Total']) * 100
muni_calc = muni_calc.reset_index().sort_values('NSS', ascending=True)
colors_nss = ['#ef4444' if val < 0 else '#10b981' for val in muni_calc['NSS']]

fig_muni = go.Figure(go.Bar(
    x=muni_calc['NSS'], 
    y=muni_calc['municipio_label'], 
    orientation='h', 
    marker_color=colors_nss, 
    text=[f'{val:+.1f}%' for val in muni_calc['NSS']], 
    textposition='auto'
))
fig_muni.update_layout(
    title='🏆 Net Sentiment Score por Municipio (% NSS)', 
    paper_bgcolor='#1e293b', 
    plot_bgcolor='#1e293b', 
    font=dict(color='#f8fafc')
)

# 3. Heatmap
heatmap_df = df[df['fixture_sentiment_hint'] == 'NEG'].groupby(['municipio_label', 'subject']).size().unstack(fill_value=0)
fig_heat = px.imshow(
    heatmap_df, 
    color_continuous_scale='Reds', 
    title='🔥 Matriz de Calor: Comentarios Negativos por Municipio y Tema'
)
fig_heat.update_layout(paper_bgcolor='#1e293b', plot_bgcolor='#1e293b', font=dict(color='#f8fafc'))

# Export to single HTML file
html_content = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Reporte Interactivo - Dashboard de Sentimiento Datos Abiertos</title>
    <style>
        body {{ background-color: #0f172a; color: #f8fafc; font-family: system-ui, -apple-system, sans-serif; padding: 2rem; margin: 0; }}
        .header {{ background: #1e293b; padding: 1.5rem 2rem; border-radius: 12px; border: 1px solid #334155; margin-bottom: 2rem; }}
        .kpi-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
        .kpi-card {{ background: #1e293b; border: 1px solid #334155; border-radius: 10px; padding: 1.2rem; text-align: center; }}
        .kpi-title {{ color: #94a3b8; font-size: 0.85rem; text-transform: uppercase; font-weight: 600; margin-bottom: 0.4rem; }}
        .kpi-value {{ font-size: 1.8rem; font-weight: 700; color: #f8fafc; }}
        .chart-container {{ background: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 1rem; margin-bottom: 2rem; }}
    </style>
</head>
<body>
    <div class="header">
        <h1 style="margin:0;font-size:1.8rem;">📊 Reporte de Sentimiento - Datos Abiertos del Valle</h1>
        <p style="color:#94a3b8;margin-top:0.5rem;">Análisis de 1.200 comentarios y clasificaciones en 8 municipios (2022-2025)</p>
    </div>
    <div class="kpi-grid">
        <div class="kpi-card"><div class="kpi-title">Total Comentarios</div><div class="kpi-value">1,200</div></div>
        <div class="kpi-card"><div class="kpi-title">Net Sentiment Score</div><div class="kpi-value" style="color:#10b981;">+6.5%</div></div>
        <div class="kpi-card"><div class="kpi-title">Positivos / Negativos</div><div class="kpi-value"><span style="color:#10b981;">38.2%</span> / <span style="color:#ef4444;">31.7%</span></div></div>
        <div class="kpi-card"><div class="kpi-title">Moderación Pendiente</div><div class="kpi-value" style="color:#f59e0b;">292 (24.3%)</div></div>
    </div>
    <div class="chart-container">{fig_ts.to_html(full_html=False, include_plotlyjs='cdn')}</div>
    <div class="chart-container">{fig_muni.to_html(full_html=False, include_plotlyjs='cdn')}</div>
    <div class="chart-container">{fig_heat.to_html(full_html=False, include_plotlyjs='cdn')}</div>
</body>
</html>
"""

with open('dashboard_sentimiento_report.html', 'w', encoding='utf-8') as f:
    f.write(html_content)

print("Generated dashboard_sentimiento_report.html successfully.")
