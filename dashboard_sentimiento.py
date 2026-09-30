import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import re

# -----------------------------------------------------------------------------
# 1. Configuración de la Página y Estilos CSS
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Dashboard Sentimiento - Datos Abiertos Valle",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS con estética moderna tipo zinc / dark-friendly
st.markdown("""
<style>
    /* Estilos globales */
    .main {
        background-color: #0f172a;
        color: #f8fafc;
    }
    
    /* Encabezado principal */
    .header-container {
        background: linear-gradient(135deg, #1e293b 0%, #0f172a 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        border: 1px solid #334155;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
    }
    .header-title {
        color: #f8fafc;
        font-size: 1.8rem;
        font-weight: 700;
        margin: 0;
    }
    .header-subtitle {
        color: #94a3b8;
        font-size: 0.95rem;
        margin-top: 0.25rem;
    }

    /* Cards de KPI */
    .kpi-card {
        background: #1e293b;
        border: 1px solid #334155;
        border-radius: 10px;
        padding: 1.2rem;
        text-align: center;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .kpi-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 12px -2px rgba(0, 0, 0, 0.4);
    }
    .kpi-title {
        color: #94a3b8;
        font-size: 0.85rem;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-weight: 600;
        margin-bottom: 0.4rem;
    }
    .kpi-value {
        color: #f8fafc;
        font-size: 1.8rem;
        font-weight: 700;
    }
    .kpi-badge-pos {
        color: #10b981;
        background: rgba(16, 185, 129, 0.15);
        padding: 0.2rem 0.6rem;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .kpi-badge-neg {
        color: #ef4444;
        background: rgba(239, 68, 68, 0.15);
        padding: 0.2rem 0.6rem;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
    }
    .kpi-badge-neu {
        color: #3b82f6;
        background: rgba(59, 130, 246, 0.15);
        padding: 0.2rem 0.6rem;
        border-radius: 9999px;
        font-size: 0.8rem;
        font-weight: 600;
    }

    /* Badges de Estado y Sentimiento */
    .badge-pos {
        background-color: #064e3b;
        color: #6ee7b7;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-neg {
        background-color: #7f1d1d;
        color: #fca5a5;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }
    .badge-neu {
        background-color: #1e3a8a;
        color: #93c5fd;
        padding: 2px 8px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.8rem;
    }

    /* Tarjetas de municipio */
    .muni-card {
        background: #1e293b;
        border-left: 4px solid #3b82f6;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    .muni-card-pos { border-left-color: #10b981; }
    .muni-card-neg { border-left-color: #ef4444; }
    .muni-card-neu { border-left-color: #f59e0b; }
</style>
""", unsafe_allow_html=True)


# -----------------------------------------------------------------------------
# 2. Carga y Procesamiento de Datos
# -----------------------------------------------------------------------------
@st.cache_data
def load_data():
    from pathlib import Path
    path = Path('data/gold_comentarios_sentimiento.csv')
    if not path.exists():
        path = Path('data/silver_comentarios.csv')
    if not path.exists():
        path = Path('data/ckan_comentarios.csv')
    df = pd.read_csv(path)
    if 'id_municipio' in df.columns:
        df = df.drop(columns=['id_municipio'])
    df['created_at'] = pd.to_datetime(df['created_at'])
    df['modified_at'] = pd.to_datetime(df['modified_at'])
    df['created_date'] = df['created_at'].dt.date
    df['created_year_month'] = df['created_at'].dt.to_period('M').astype(str)
    df['municipio_label'] = df['municipio'].str.capitalize()
    
    # Mapeo legible de sentimiento
    sentiment_map = {'POS': 'Positivo', 'NEG': 'Negativo', 'NEU': 'Neutral'}
    hint_col = 'fixture_sentiment_hint' if 'fixture_sentiment_hint' in df.columns else 'sentimiento_codigo'
    df['sentiment_name'] = df[hint_col].map(sentiment_map)
    if 'fixture_sentiment_hint' not in df.columns and 'sentimiento_codigo' in df.columns:
        df['fixture_sentiment_hint'] = df['sentimiento_codigo']
    return df

df_raw = load_data()

# -----------------------------------------------------------------------------
# 3. Sidebar y Filtros Dinámicos
# -----------------------------------------------------------------------------
st.sidebar.image("https://img.icons8.com/color/96/analytics.png", width=64)
st.sidebar.title("Filtros del Dashboard")
st.sidebar.markdown("---")

# Filtro Rango de Fechas
min_date = df_raw['created_at'].min().date()
max_date = df_raw['created_at'].max().date()

date_range = st.sidebar.date_input(
    "📅 Rango de Fechas",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

# Filtro Municipio
all_municipios = sorted(df_raw['municipio_label'].unique())
selected_municipios = st.sidebar.multiselect(
    "🏙️ Municipio",
    options=all_municipios,
    default=all_municipios
)

# Filtro Categoria Temática
all_subjects = sorted(df_raw['subject'].unique())
selected_subjects = st.sidebar.multiselect(
    "🏷️ Categoría Temática",
    options=all_subjects,
    default=all_subjects
)

# Filtro Sentimiento
all_sentiments = ['Positivo', 'Negativo', 'Neutral']
selected_sentiments = st.sidebar.multiselect(
    "🎭 Sentimiento",
    options=all_sentiments,
    default=all_sentiments
)

# Filtro Estado Moderación
all_states = sorted(df_raw['state'].unique())
selected_states = st.sidebar.multiselect(
    "🛡️ Estado Moderación",
    options=all_states,
    default=all_states
)

# Aplicar Filtros
df = df_raw[
    (df_raw['created_at'].dt.date >= start_date) &
    (df_raw['created_at'].dt.date <= end_date) &
    (df_raw['municipio_label'].isin(selected_municipios)) &
    (df_raw['subject'].isin(selected_subjects)) &
    (df_raw['sentiment_name'].isin(selected_sentiments)) &
    (df_raw['state'].isin(selected_states))
]

st.sidebar.markdown("---")
st.sidebar.info(f"📊 **Registros Filtrados**: {len(df):,} de {len(df_raw):,}")

# -----------------------------------------------------------------------------
# 4. Encabezado del Dashboard
# -----------------------------------------------------------------------------
st.markdown("""
<div class="header-container">
    <div class="header-title">📊 Dashboard de Análisis de Sentimiento - Datos Abiertos Valle</div>
    <div class="header-subtitle">Monitoreo, moderación y análisis de percepción ciudadana en 8 municipios del Valle del Cauca</div>
</div>
""", unsafe_allow_html=True)

if len(df) == 0:
    st.warning("⚠️ No se encontraron registros con los filtros seleccionados. Ajusta los filtros en el menú lateral.")
    st.stop()

# Paleta de colores para gráficos
COLOR_POS = "#10b981"
COLOR_NEG = "#ef4444"
COLOR_NEU = "#3b82f6"
COLOR_MAP = {'Positivo': COLOR_POS, 'Negativo': COLOR_NEG, 'Neutral': COLOR_NEU}

# -----------------------------------------------------------------------------
# 5. Pestañas Principales
# -----------------------------------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs([
    "📈 Vista Ejecutiva",
    "🛠️ Operativo & Moderación",
    "🗺️ Análisis Geográfico & Temático",
    "🤖 Asistente de Consulta IA"
])

# =============================================================================
# TAB 1: VISTA EJECUTIVA
# =============================================================================
with tab1:
    # Cálculo de Métricas Clave
    total_comments = len(df)
    pos_cnt = (df['fixture_sentiment_hint'] == 'POS').sum()
    neg_cnt = (df['fixture_sentiment_hint'] == 'NEG').sum()
    neu_cnt = (df['fixture_sentiment_hint'] == 'NEU').sum()
    
    pct_pos = (pos_cnt / total_comments * 100) if total_comments > 0 else 0
    pct_neg = (neg_cnt / total_comments * 100) if total_comments > 0 else 0
    pct_neu = (neu_cnt / total_comments * 100) if total_comments > 0 else 0
    nss = pct_pos - pct_neg
    
    pending_cnt = (df['state'] == 'pending').sum()
    pct_pending = (pending_cnt / total_comments * 100) if total_comments > 0 else 0

    # Grid de KPIs
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Total Comentarios</div>
            <div class="kpi-value">{total_comments:,}</div>
            <div class="kpi-badge-neu">Base Filtrada</div>
        </div>
        """, unsafe_allow_html=True)
    with c2:
        badge_class = "kpi-badge-pos" if nss >= 0 else "kpi-badge-neg"
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Net Sentiment Score (NSS)</div>
            <div class="kpi-value">{nss:+.1f}%</div>
            <div class="{badge_class}">% POS - % NEG</div>
        </div>
        """, unsafe_allow_html=True)
    with c3:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Proporción Sentimiento</div>
            <div class="kpi-value">{pct_pos:.1f}% <span style="font-size:1rem;color:#10b981;">POS</span> / {pct_neg:.1f}% <span style="font-size:1rem;color:#ef4444;">NEG</span></div>
            <div class="kpi-badge-neu">{pct_neu:.1f}% Neutrales</div>
        </div>
        """, unsafe_allow_html=True)
    with c4:
        st.markdown(f"""
        <div class="kpi-card">
            <div class="kpi-title">Moderación Pendiente</div>
            <div class="kpi-value">{pending_cnt} <span style="font-size:1rem;color:#94a3b8;">({pct_pending:.1f}%)</span></div>
            <div class="kpi-badge-neg">Requiere Revisión</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Fila 1 de Gráficos: Tendencia Temporal + NSS por Municipio
    col_g1, col_g2 = st.columns([1.2, 1])

    with col_g1:
        st.subheader("📈 Evolución Temporal del Sentimiento (2022 - 2025)")
        # Agrupación mensual
        ts_df = df.groupby(['created_year_month', 'sentiment_name']).size().reset_index(name='count')
        
        fig_ts = px.line(
            ts_df,
            x='created_year_month',
            y='count',
            color='sentiment_name',
            color_discrete_map=COLOR_MAP,
            markers=True,
            labels={'created_year_month': 'Mes', 'count': 'N° Comentarios', 'sentiment_name': 'Sentimiento'}
        )
        fig_ts.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#f8fafc'),
            xaxis=dict(showgrid=True, gridcolor='#334155'),
            yaxis=dict(showgrid=True, gridcolor='#334155'),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    with col_g2:
        st.subheader("🏆 Ranking NSS por Municipio (% Net Sentiment)")
        muni_calc = df.groupby(['municipio_label', 'fixture_sentiment_hint']).size().unstack(fill_value=0)
        for s in ['POS', 'NEG', 'NEU']:
            if s not in muni_calc.columns:
                muni_calc[s] = 0
        muni_calc['Total'] = muni_calc.sum(axis=1)
        muni_calc['NSS'] = ((muni_calc['POS'] - muni_calc['NEG']) / muni_calc['Total']) * 100
        muni_calc = muni_calc.reset_index().sort_values('NSS', ascending=True)

        colors_nss = ['#ef4444' if val < 0 else '#10b981' for val in muni_calc['NSS']]

        fig_muni = go.Figure(go.Bar(
            x=muni_calc['NSS'],
            y=muni_calc['municipio_label'],
            orientation='h',
            marker_color=colors_nss,
            text=[f"{val:+.1f}%" for val in muni_calc['NSS']],
            textposition='auto'
        ))
        fig_muni.update_layout(
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#f8fafc'),
            xaxis=dict(title="Net Sentiment Score (%)", showgrid=True, gridcolor='#334155'),
            yaxis=dict(title="")
        )
        st.plotly_chart(fig_muni, use_container_width=True)

    # Fila 2: Categorías Temáticas con mayor negatividad
    st.subheader("🏷️ Sentimiento por Categoría Temática")
    subj_calc = df.groupby(['subject', 'sentiment_name']).size().reset_index(name='count')
    fig_subj = px.bar(
        subj_calc,
        x='subject',
        y='count',
        color='sentiment_name',
        color_discrete_map=COLOR_MAP,
        barmode='stack',
        labels={'subject': 'Categoría Temática', 'count': 'Comentarios', 'sentiment_name': 'Sentimiento'}
    )
    fig_subj.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#f8fafc'),
        xaxis=dict(tickangle=-30, showgrid=False),
        yaxis=dict(showgrid=True, gridcolor='#334155'),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    st.plotly_chart(fig_subj, use_container_width=True)

    # Resumen Analítico Generado Automáticamente
    st.info(f"""
    💡 **Insights Clave del Análisis**:
    - **Municipio con mayor satisfacción**: **{muni_calc.iloc[-1]['municipio_label']}** (NSS: {muni_calc.iloc[-1]['NSS']:+.1f}%).
    - **Municipio con mayor insatisfacción**: **{muni_calc.iloc[0]['municipio_label']}** (NSS: {muni_calc.iloc[0]['NSS']:+.1f}%).
    - **Categorías que requieren atención inmediata**: *Documentación*, *Actualización de datos* y *Problemas de descarga* concentran la mayor densidad de reclamos negativos por parte de los usuarios.
    """)


# =============================================================================
# TAB 2: OPERATIVO & MODERACIÓN
# =============================================================================
with tab2:
    st.subheader("🛡️ Panel de Moderación de Comentarios e Incidencias")

    m1, m2, m3 = st.columns(3)
    pending_total = (df['state'] == 'pending').sum()
    pending_neg = len(df[(df['state'] == 'pending') & (df['fixture_sentiment_hint'] == 'NEG')])
    approved_total = (df['state'] == 'approved').sum()

    with m1:
        st.metric("Comentarios Pendientes", pending_total, delta=f"{pending_total/len(df)*100:.1f}% del total")
    with m2:
        st.metric("🔴 Negativos Críticos Pendientes", pending_neg, delta="Requieren Atención Pronta", delta_color="inverse")
    with m3:
        st.metric("Comentarios Aprobados", approved_total, delta=f"{approved_total/len(df)*100:.1f}%")

    st.markdown("---")

    # Matriz de Calor (Heatmap)
    st.subheader("🔥 Matriz de Calor: Densidad de Comentarios Negativos por Municipio y Tema")
    
    heatmap_df = df[df['fixture_sentiment_hint'] == 'NEG'].groupby(['municipio_label', 'subject']).size().unstack(fill_value=0)
    
    fig_heat = px.imshow(
        heatmap_df,
        labels=dict(x="Categoría Temática", y="Municipio", color="N° Comentarios Negativos"),
        color_continuous_scale="Reds",
        aspect="auto"
    )
    fig_heat.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#f8fafc'),
        xaxis=dict(tickangle=-30)
    )
    st.plotly_chart(fig_heat, use_container_width=True)

    # Tabla Interactiva de Moderación
    st.subheader("🔍 Explorador & Tabla de Moderación de Comentarios")
    
    search_query = st.text_input("🔎 Búsqueda de Texto en Comentarios:", placeholder="Ej. descarga, error, excelente, nulos...")
    
    df_table = df.copy()
    if search_query:
        df_table = df_table[df_table['content'].str.contains(search_query, case=False, na=False)]
    
    st.write(f"Mostrando **{len(df_table)}** comentarios:")
    
    # Formatear columnas para visualización
    display_cols = ['created_at', 'municipio_label', 'subject', 'sentiment_name', 'state', 'content', 'author_id']
    st.dataframe(
        df_table[display_cols].rename(columns={
            'created_at': 'Fecha',
            'municipio_label': 'Municipio',
            'subject': 'Categoría',
            'sentiment_name': 'Sentimiento',
            'state': 'Estado',
            'content': 'Comentario',
            'author_id': 'Usuario'
        }),
        use_container_width=True,
        height=400
    )

    # Botón para descargar CSV filtrado
    csv = df_table.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="📥 Descargar Datos Filtrados en CSV",
        data=csv,
        file_name="comentarios_sentimiento_filtrados.csv",
        mime="text/csv"
    )


# =============================================================================
# TAB 3: ANÁLISIS GEOGRÁFICO & TEMÁTICO
# =============================================================================
with tab3:
    st.subheader("🗺️ Diagnóstico Geográfico por Municipio")

    muni_list = sorted(df['municipio_label'].unique())
    cols = st.columns(4)

    for idx, muni in enumerate(muni_list):
        col = cols[idx % 4]
        muni_sub = df[df['municipio_label'] == muni]
        m_tot = len(muni_sub)
        m_pos = (muni_sub['fixture_sentiment_hint'] == 'POS').sum()
        m_neg = (muni_sub['fixture_sentiment_hint'] == 'NEG').sum()
        m_neu = (muni_sub['fixture_sentiment_hint'] == 'NEU').sum()
        m_nss = ((m_pos - m_neg) / m_tot * 100) if m_tot > 0 else 0

        card_class = "muni-card-pos" if m_nss > 5 else ("muni-card-neg" if m_nss < 0 else "muni-card-neu")
        
        top_issue = muni_sub[muni_sub['fixture_sentiment_hint'] == 'NEG']['subject'].mode()
        top_issue_str = top_issue.iloc[0] if not top_issue.empty else "N/A"

        with col:
            st.markdown(f"""
            <div class="muni-card {card_class}">
                <div style="font-size:1.1rem;font-weight:700;color:#f8fafc;">{muni}</div>
                <div style="font-size:0.85rem;color:#94a3b8;margin-bottom:0.5rem;">NSS: <b>{m_nss:+.1f}%</b> ({m_tot} comentarios)</div>
                <div style="font-size:0.8rem;color:#e2e8f0;">
                    🟢 {m_pos} | 🔴 {m_neg} | ⚪ {m_neu}
                </div>
                <div style="font-size:0.75rem;color:#94a3b8;margin-top:0.4rem;">
                    Top Queja: <i>{top_issue_str}</i>
                </div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Treemap Jerárquico
    st.subheader("🌳 Estructura Jerárquica: Municipio ➔ Categoría ➔ Sentimiento")
    fig_tree = px.treemap(
        df,
        path=['municipio_label', 'subject', 'sentiment_name'],
        color='sentiment_name',
        color_discrete_map=COLOR_MAP
    )
    fig_tree.update_layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#f8fafc')
    )
    st.plotly_chart(fig_tree, use_container_width=True)

    # Análisis de Frecuencia de Palabras
    st.subheader("🔤 Palabras Clave Recurrentes por Sentimiento")
    
    def extract_keywords(text_series):
        words = []
        stopwords = set(['de', 'la', 'que', 'el', 'en', 'y', 'a', 'los', 'del', 'se', 'por', 'un', 'para', 'con', 'no', 'una', 'su', 'al', 'lo', 'como', 'más', 'pero', 'sus', 'le', 'ya', 'o', 'este', 'sí', 'porque', 'esta', 'son', 'entre', 'está', 'cuando', 'muy', 'sin', 'sobre', 'también', 'me', 'hasta', 'hay', 'donde', 'quien', 'desde', 'todo', 'nos', 'durante', 'todos', 'uno', 'les', 'ni', 'contra', 'otros', 'ese', 'eso', 'ante', 'ellos', 'e', 'esto', 'mí', 'antes', 'algunos', 'qué', 'unos', 'yo', 'otro', 'otras', 'otra', 'él', 'tanto', 'esa', 'estos', 'mucho', 'quienes', 'nada', 'muchos', 'cual', 'poco', 'ella', 'estar', 'estas', 'algunas', 'algo', 'nosotros', 'mi', 'mis', 'tú', 'te', 'ti', 'tu', 'tus', 'ellas', 'nosotras', 'vosotros', 'vosotras', 'os', 'mío', 'mía', 'míos', 'mías', 'tuyo', 'tuya', 'tuyos', 'tuyas', 'suyo', 'suya', 'suyos', 'suyas', 'nuestro', 'nuestra', 'nuestros', 'nuestras', 'vuestro', 'vuestra', 'vuestros', 'vuestras', 'esos', 'esas', 'gracias', 'buen', 'día', 'comentario', 'hola', 'ref', 'ticket', 'caso'])
        for text in text_series.dropna():
            cleaned = re.sub(r'[^\w\s]', '', str(text).lower())
            tokens = [w for w in cleaned.split() if w not in stopwords and len(w) > 3]
            words.extend(tokens)
        return pd.Series(words).value_counts().head(10)

    kw_pos = extract_keywords(df[df['fixture_sentiment_hint'] == 'POS']['content'])
    kw_neg = extract_keywords(df[df['fixture_sentiment_hint'] == 'NEG']['content'])

    col_w1, col_w2 = st.columns(2)
    with col_w1:
        st.write("🟢 **Top Términos en Comentarios Positivos**")
        if not kw_pos.empty:
            fig_w_pos = px.bar(x=kw_pos.values, y=kw_pos.index, orientation='h', color_discrete_sequence=[COLOR_POS])
            fig_w_pos.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#f8fafc'), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_w_pos, use_container_width=True)
    with col_w2:
        st.write("🔴 **Top Términos en Comentarios Negativos**")
        if not kw_neg.empty:
            fig_w_neg = px.bar(x=kw_neg.values, y=kw_neg.index, orientation='h', color_discrete_sequence=[COLOR_NEG])
            fig_w_neg.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='#f8fafc'), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_w_neg, use_container_width=True)


# =============================================================================
# TAB 4: ASISTENTE IA DE CONSULTA
# =============================================================================
with tab4:
    st.subheader("🤖 Asistente de Consulta de Sentimiento en Lenguaje Natural")
    st.markdown("Escribe preguntas o temas de interés para analizar el dataset de forma conversacional:")

    query = st.text_input("💬 Pregunta al dataset (Ej: '¿Cuáles son los principales reclamos en Yumbo?' o 'comentarios sobre descargas'):")

    if query:
        st.markdown("---")
        # Filtrar comentarios relevantes por coincidencia semántica/texto
        keywords = [w.lower() for w in query.split() if len(w) > 3]
        matches = df[df['content'].apply(lambda c: any(k in str(c).lower() for k in keywords) if keywords else True)]
        
        st.write(f"🔍 **Resultados encontrados**: {len(matches)} comentarios relacionados con '{query}'.")
        
        if len(matches) > 0:
            m_pos = (matches['fixture_sentiment_hint'] == 'POS').sum()
            m_neg = (matches['fixture_sentiment_hint'] == 'NEG').sum()
            m_neu = (matches['fixture_sentiment_hint'] == 'NEU').sum()

            c_a, c_b, c_c = st.columns(3)
            c_a.metric("Positivos", m_pos)
            c_b.metric("Negativos", m_neg)
            c_c.metric("Neutrales", m_neu)

            st.write("📝 **Ejemplos de Comentarios Encontrados**:")
            for _, row in matches.head(5).iterrows():
                badge = f"<span class='badge-pos'>POS</span>" if row['fixture_sentiment_hint'] == 'POS' else (
                    f"<span class='badge-neg'>NEG</span>" if row['fixture_sentiment_hint'] == 'NEG' else f"<span class='badge-neu'>NEU</span>"
                )
                st.markdown(f"- **[{row['municipio_label']} - {row['subject']}]** {badge}: *\"{row['content']}\"*", unsafe_allow_html=True)
        else:
            st.info("No se encontraron coincidencias exactas. Intenta con otras palabras clave como 'descarga', 'documentación', 'lento', 'excelente'.")
