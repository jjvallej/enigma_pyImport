"""Script generador del Prototipo Web Interactivo de Looker Studio (Data Studio)
para ValleDATA - Análisis de Sentimientos de Comentarios CKAN.

Genera el archivo HTML standalone `data/mockup_looker_studio_sentimiento.html`
con todas las 6 páginas, 9 KPIs, filtros de informe, parámetros y gráficos interactivos.
"""

from __future__ import annotations

import json
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"
GOLD_CSV = DATA_DIR / "gold_comentarios_sentimiento.csv"
OUTPUT_HTML = DATA_DIR / "mockup_looker_studio_sentimiento.html"


def main():
    if not GOLD_CSV.exists():
        raise FileNotFoundError(f"No se encontró el archivo de datos Gold: {GOLD_CSV}")

    df = pd.read_csv(GOLD_CSV)
    
    # Asegurar tipos de datos
    df["total_comentarios"] = pd.to_numeric(df["total_comentarios"], errors="coerce").fillna(0).astype(int)
    df["positivos"] = pd.to_numeric(df["positivos"], errors="coerce").fillna(0).astype(int)
    df["negativos"] = pd.to_numeric(df["negativos"], errors="coerce").fillna(0).astype(int)
    df["neutros"] = pd.to_numeric(df["neutros"], errors="coerce").fillna(0).astype(int)
    df["confianza_promedio"] = pd.to_numeric(df["confianza_promedio"], errors="coerce").fillna(0.0)

    # Convertir a lista de dicts para JS
    gold_data_json = json.dumps(df.to_dict(orient="records"), ensure_ascii=False)

    html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ValleDATA — Prototipo Looker Studio: Sentimiento Analítico (9 KPIs)</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
  <style>
    :root {{
      --bg-dark: #0f172a;
      --bg-card: #1e293b;
      --bg-card-hover: #334155;
      --border-color: #334155;
      --text-main: #f8fafc;
      --text-muted: #94a3b8;
      --primary: #38bdf8;
      --pos-green: #34A853;
      --neg-red: #EA4335;
      --neu-gray: #9AA0A6;
      --warn-yellow: #FBBC04;
      --header-bg: #0b1329;
      --accent: #6366f1;
    }}

    * {{
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }}

    body {{
      font-family: 'Inter', sans-serif;
      background-color: var(--bg-dark);
      color: var(--text-main);
      min-height: 100vh;
      display: flex;
      flex-direction: column;
    }}

    /* Header Looker Studio canvas */
    header {{
      height: 72px;
      background: var(--header-bg);
      border-bottom: 1px solid var(--border-color);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 24px;
      box-shadow: 0 4px 20px rgba(0,0,0,0.3);
      position: sticky;
      top: 0;
      z-index: 100;
    }}

    .logo-container {{
      display: flex;
      align-items: center;
      gap: 12px;
    }}

    .logo-badge {{
      background: linear-gradient(135deg, #0284c7, #6366f1);
      color: #fff;
      font-weight: 800;
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 14px;
      letter-spacing: 0.5px;
    }}

    .header-titles h1 {{
      font-size: 16px;
      font-weight: 700;
      color: #fff;
    }}

    .header-titles p {{
      font-size: 11px;
      color: var(--text-muted);
    }}

    .header-actions {{
      display: flex;
      align-items: center;
      gap: 16px;
    }}

    .refresh-tag {{
      background: rgba(56, 189, 248, 0.1);
      color: var(--primary);
      border: 1px solid rgba(56, 189, 248, 0.3);
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 11px;
      font-weight: 500;
    }}

    /* Controls Bar (Report Level Filters & Parameters) */
    .controls-bar {{
      background: #151e33;
      border-bottom: 1px solid var(--border-color);
      padding: 12px 24px;
      display: flex;
      flex-wrap: wrap;
      gap: 16px;
      align-items: center;
      justify-content: space-between;
    }}

    .filter-group {{
      display: flex;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }}

    .filter-item {{
      display: flex;
      flex-direction: column;
      gap: 4px;
    }}

    .filter-item label {{
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      color: var(--text-muted);
      letter-spacing: 0.5px;
    }}

    select, input[type="number"], input[type="text"] {{
      background: var(--bg-card);
      color: var(--text-main);
      border: 1px solid var(--border-color);
      padding: 6px 12px;
      border-radius: 6px;
      font-size: 12px;
      font-family: inherit;
      outline: none;
      transition: all 0.2s;
    }}

    select:focus, input:focus {{
      border-color: var(--primary);
      box-shadow: 0 0 0 2px rgba(56, 189, 248, 0.2);
    }}

    .checkbox-item {{
      display: flex;
      align-items: center;
      gap: 8px;
      margin-top: 14px;
      font-size: 12px;
      color: var(--text-muted);
      cursor: pointer;
    }}

    .checkbox-item input {{
      cursor: pointer;
      accent-color: var(--primary);
    }}

    /* Navigation Tabs */
    .tabs-bar {{
      background: #0f172a;
      display: flex;
      gap: 4px;
      padding: 0 24px;
      border-bottom: 1px solid var(--border-color);
      overflow-x: auto;
    }}

    .tab-btn {{
      background: transparent;
      border: none;
      color: var(--text-muted);
      padding: 12px 18px;
      font-size: 13px;
      font-weight: 600;
      cursor: pointer;
      border-bottom: 2px solid transparent;
      transition: all 0.2s;
      white-space: nowrap;
    }}

    .tab-btn:hover {{
      color: var(--text-main);
      background: rgba(255,255,255,0.03);
    }}

    .tab-btn.active {{
      color: var(--primary);
      border-bottom-color: var(--primary);
      background: rgba(56, 189, 248, 0.05);
    }}

    /* Dashboard Canvas Grid */
    .canvas-container {{
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
      width: 100%;
      flex-grow: 1;
    }}

    .tab-content {{
      display: none;
    }}

    .tab-content.active {{
      display: block;
    }}

    /* Scorecards Grid */
    .kpi-grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}

    .kpi-card {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 16px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      position: relative;
      overflow: hidden;
      transition: transform 0.2s, box-shadow 0.2s;
    }}

    .kpi-card:hover {{
      transform: translateY(-2px);
      box-shadow: 0 8px 24px rgba(0,0,0,0.2);
    }}

    .kpi-title {{
      font-size: 11px;
      font-weight: 600;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.5px;
      margin-bottom: 8px;
    }}

    .kpi-value {{
      font-size: 26px;
      font-weight: 800;
      color: var(--text-main);
      margin-bottom: 4px;
    }}

    .kpi-subtext {{
      font-size: 11px;
      color: var(--text-muted);
    }}

    .kpi-badge {{
      display: inline-block;
      padding: 3px 8px;
      border-radius: 12px;
      font-size: 10px;
      font-weight: 700;
      margin-top: 6px;
    }}

    .badge-normal {{ background: rgba(52, 168, 83, 0.2); color: var(--pos-green); }}
    .badge-alerta {{ background: rgba(251, 188, 4, 0.2); color: var(--warn-yellow); }}
    .badge-critico {{ background: rgba(234, 67, 53, 0.2); color: var(--neg-red); }}

    /* Layout Charts Rows */
    .charts-row {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(450px, 1fr));
      gap: 20px;
      margin-bottom: 24px;
    }}

    .chart-card {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 20px;
      display: flex;
      flex-direction: column;
    }}

    .chart-header {{
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 16px;
    }}

    .chart-title {{
      font-size: 14px;
      font-weight: 700;
      color: var(--text-main);
    }}

    .chart-body {{
      position: relative;
      width: 100%;
      height: 320px;
    }}

    /* Table styles */
    .table-container {{
      background: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      padding: 20px;
      overflow-x: auto;
    }}

    table {{
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 12px;
    }}

    th {{
      background: #151e33;
      color: var(--text-muted);
      font-weight: 600;
      padding: 10px 14px;
      border-bottom: 1px solid var(--border-color);
      text-transform: uppercase;
      font-size: 10px;
      letter-spacing: 0.5px;
    }}

    td {{
      padding: 10px 14px;
      border-bottom: 1px solid rgba(255,255,255,0.05);
      color: var(--text-main);
    }}

    tr:hover td {{
      background: rgba(255,255,255,0.02);
    }}

    .search-bar {{
      margin-bottom: 16px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}

    footer {{
      margin-top: auto;
      background: var(--header-bg);
      border-top: 1px solid var(--border-color);
      padding: 16px 24px;
      font-size: 11px;
      color: var(--text-muted);
      text-align: center;
    }}
  </style>
</head>
<body>

  <!-- Header Looker Studio -->
  <header>
    <div class="logo-container">
      <div class="logo-badge">ValleDATA</div>
      <div class="header-titles">
        <h1>Tablero de Sentimiento Analítico — CKAN Valle del Cauca</h1>
        <p>Google Looker Studio (Data Studio) Blueprint • Fuente: valledata.gold.gold_comentarios_sentimiento</p>
      </div>
    </div>
    <div class="header-actions">
      <span class="refresh-tag">● En vivo (BigQuery)</span>
      <span style="font-size: 11px; color: var(--text-muted);">Corte T+1 Día</span>
    </div>
  </header>

  <!-- Controls Bar -->
  <div class="controls-bar">
    <div class="filter-group">
      <div class="filter-item">
        <label for="filter-municipio">Municipio</label>
        <select id="filter-municipio">
          <option value="ALL">Todos los municipios</option>
        </select>
      </div>
      <div class="filter-item">
        <label for="filter-dataset">Dataset / Temática</label>
        <select id="filter-dataset">
          <option value="ALL">Todas las temáticas</option>
        </select>
      </div>
      <div class="filter-item">
        <label for="filter-emocion">KPI-09: Emoción Predominante</label>
        <select id="filter-emocion">
          <option value="ALL">Todas</option>
          <option value="POS">Positivo (POS)</option>
          <option value="NEU">Neutro (NEU)</option>
          <option value="NEG">Negativo (NEG)</option>
        </select>
      </div>
      <label class="checkbox-item">
        <input type="checkbox" id="filter-baja-confianza">
        Solo baja confianza (&lt; P_UMBRAL_CONF)
      </label>
    </div>

    <!-- Parameter Config -->
    <div class="filter-group" style="border-left: 1px solid var(--border-color); padding-left: 16px;">
      <div class="filter-item">
        <label for="param-umbral">P_UMBRAL_CONF (IA)</label>
        <input type="number" id="param-umbral" value="0.70" step="0.05" min="0.5" max="0.95" style="width: 80px;">
      </div>
      <div class="filter-item">
        <label for="param-critico">P_CRIT_CRITICO (%)</label>
        <input type="number" id="param-critico" value="50" step="5" min="10" max="90" style="width: 70px;">
      </div>
    </div>
  </div>

  <!-- Tabs Bar -->
  <div class="tabs-bar">
    <button class="tab-btn active" onclick="switchTab('tab-01')">Página 01: Tablero Ejecutivo</button>
    <button class="tab-btn" onclick="switchTab('tab-02')">Página 02: Ranking Criticidad (KPI-07)</button>
    <button class="tab-btn" onclick="switchTab('tab-03')">Página 03: Polaridad por Dataset</button>
    <button class="tab-btn" onclick="switchTab('tab-04')">Página 04: Auditoría Confianza IA (KPI-08)</button>
    <button class="tab-btn" onclick="switchTab('tab-05')">Página 05: Emociones (KPI-09)</button>
    <button class="tab-btn" onclick="switchTab('tab-06')">Página 06: Explorador Gold (Tabla)</button>
  </div>

  <!-- Canvas Container -->
  <div class="canvas-container">

    <!-- PAGE 01: TABLERO EJECUTIVO -->
    <div id="tab-01" class="tab-content active">
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-title">KPI-01: Volumen Comentarios</div>
          <div class="kpi-value" id="kpi-1-val">0</div>
          <div class="kpi-subtext">Total comentarios procesados</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">KPI-02: Net Sentiment Score (NSS)</div>
          <div class="kpi-value" id="kpi-2-val">0%</div>
          <div class="kpi-subtext">% Positivos - % Negativos</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">KPI-03: Tasa de Criticidad</div>
          <div class="kpi-value" id="kpi-3-val">0%</div>
          <div class="kpi-subtext">% Comentarios Negativos</div>
          <div id="kpi-3-badge" class="kpi-badge badge-normal">NORMAL</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">KPI-04: Confianza IA Ponderada</div>
          <div class="kpi-value" id="kpi-4-val">0.00</div>
          <div class="kpi-subtext">Certeza del modelo pysentimiento</div>
          <div id="kpi-4-badge" class="kpi-badge badge-normal">ÓPTIMO</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">KPI-07: Municipios Críticos</div>
          <div class="kpi-value" id="kpi-7-val" style="color: var(--neg-red);">0</div>
          <div class="kpi-subtext">Municipios con NSS &lt; 0</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-title">KPI-08: Datasets Bajo Umbral</div>
          <div class="kpi-value" id="kpi-8-val" style="color: var(--warn-yellow);">0</div>
          <div class="kpi-subtext">Confianza &lt; 0.70 o Criticidad &gt; 25%</div>
        </div>
      </div>

      <div class="charts-row">
        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">Distribución General de Polaridad (DS_POLARIDAD)</div>
          </div>
          <div class="chart-body">
            <canvas id="chart-polaridad-donut"></canvas>
          </div>
        </div>

        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">Resumen por Municipio Principal</div>
          </div>
          <div class="chart-body">
            <canvas id="chart-resumen-barras"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- PAGE 02: RANKING TERRITORIAL -->
    <div id="tab-02" class="tab-content">
      <div class="charts-row" style="grid-template-columns: 1fr;">
        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">KPI-07: Ranking Territorial de Tasa de Criticidad (%) por Municipio</div>
          </div>
          <div class="chart-body" style="height: 420px;">
            <canvas id="chart-ranking-criticidad"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- PAGE 03: POLARIDAD POR DATASET -->
    <div id="tab-03" class="tab-content">
      <div class="charts-row" style="grid-template-columns: 1fr;">
        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">Composición de Polaridad (Positivo, Neutro, Negativo) por Municipio</div>
          </div>
          <div class="chart-body" style="height: 420px;">
            <canvas id="chart-polaridad-stacked"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- PAGE 04: AUDITORÍA CONFIANZA IA -->
    <div id="tab-04" class="tab-content">
      <div class="charts-row" style="grid-template-columns: 1fr;">
        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">KPI-08: Auditoría de Confianza Probabilística IA por Municipio (Línea de Corte = P_UMBRAL_CONF)</div>
          </div>
          <div class="chart-body" style="height: 420px;">
            <canvas id="chart-confianza-ia"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- PAGE 05: EMOCIONES -->
    <div id="tab-05" class="tab-content">
      <div class="charts-row">
        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">KPI-09: Distribución de Emoción Predominante en la Muestra</div>
          </div>
          <div class="chart-body">
            <canvas id="chart-emociones-pie"></canvas>
          </div>
        </div>
        <div class="chart-card">
          <div class="chart-header">
            <div class="chart-title">Conteo de Datasets por Emoción Predominante</div>
          </div>
          <div class="chart-body">
            <canvas id="chart-emociones-bar"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- PAGE 06: EXPLORADOR GOLD -->
    <div id="tab-06" class="tab-content">
      <div class="table-container">
        <div class="search-bar">
          <input type="text" id="table-search" placeholder="🔍 Buscar municipio o dataset..." style="width: 300px;">
          <button onclick="exportTableToCSV()" style="background: var(--accent); color: #fff; border: none; padding: 6px 14px; border-radius: 6px; cursor: pointer; font-size: 12px; font-weight: 600;">📥 Exportar CSV</button>
        </div>
        <table>
          <thead>
            <tr>
              <th>Municipio</th>
              <th>ID Dataset</th>
              <th>Volumen</th>
              <th>Positivos</th>
              <th>Negativos</th>
              <th>Neutros</th>
              <th>NSS (%)</th>
              <th>Criticidad (%)</th>
              <th>Confianza IA</th>
              <th>KPI-09 Emoción</th>
            </tr>
          </thead>
          <tbody id="gold-table-body">
          </tbody>
        </table>
      </div>
    </div>

  </div>

  <footer>
    ValleDATA — Gobernación del Valle del Cauca • Especificación de Entregables Wadua Looker Studio v2.4
  </footer>

  <script>
    const rawData = {gold_data_json};

    let chartDonut, chartResumen, chartRanking, chartStacked, chartConfianza, chartEmocionesPie, chartEmocionesBar;

    function initFilters() {{
      const municipios = [...new Set(rawData.map(d => d.municipio))].sort();
      const datasets = [...new Set(rawData.map(d => d.id_dataset))].sort();

      const selMuni = document.getElementById('filter-municipio');
      municipios.forEach(m => {{
        const opt = document.createElement('option');
        opt.value = m;
        opt.textContent = m.charAt(0).toUpperCase() + m.slice(1);
        selMuni.appendChild(opt);
      }});

      const selDs = document.getElementById('filter-dataset');
      datasets.forEach(d => {{
        const opt = document.createElement('option');
        opt.value = d;
        opt.textContent = d.length > 25 ? d.substring(0,25) + '...' : d;
        selDs.appendChild(opt);
      }});

      document.getElementById('filter-municipio').addEventListener('change', updateDashboard);
      document.getElementById('filter-dataset').addEventListener('change', updateDashboard);
      document.getElementById('filter-emocion').addEventListener('change', updateDashboard);
      document.getElementById('filter-baja-confianza').addEventListener('change', updateDashboard);
      document.getElementById('param-umbral').addEventListener('input', updateDashboard);
      document.getElementById('param-critico').addEventListener('input', updateDashboard);
      document.getElementById('table-search').addEventListener('input', filterTable);
    }}

    function getFilteredData() {{
      const muni = document.getElementById('filter-municipio').value;
      const ds = document.getElementById('filter-dataset').value;
      const emo = document.getElementById('filter-emocion').value;
      const bajaConf = document.getElementById('filter-baja-confianza').checked;
      const umbralConf = parseFloat(document.getElementById('param-umbral').value) || 0.70;

      return rawData.filter(d => {{
        if (muni !== 'ALL' && d.municipio !== muni) return false;
        if (ds !== 'ALL' && d.id_dataset !== ds) return false;
        if (emo !== 'ALL' && d.emocion_predominante !== emo) return false;
        if (bajaConf && d.confianza_promedio >= umbralConf) return false;
        return true;
      }});
    }}

    function updateDashboard() {{
      const data = getFilteredData();
      const umbralConf = parseFloat(document.getElementById('param-umbral').value) || 0.70;
      const umbralCritico = parseFloat(document.getElementById('param-critico').value) || 50;

      // Aggregations
      const totalVol = data.reduce((acc, r) => acc + r.total_comentarios, 0);
      const totalPos = data.reduce((acc, r) => acc + r.positivos, 0);
      const totalNeg = data.reduce((acc, r) => acc + r.negativos, 0);
      const totalNeu = data.reduce((acc, r) => acc + r.neutros, 0);

      const nss = totalVol > 0 ? ((totalPos - totalNeg) / totalVol) * 100 : 0;
      const criticidad = totalVol > 0 ? (totalNeg / totalVol) * 100 : 0;

      let sumWeightedConf = 0;
      data.forEach(r => sumWeightedConf += r.confianza_promedio * r.total_comentarios);
      const confianzaPond = totalVol > 0 ? sumWeightedConf / totalVol : 0;

      // KPI-07: Municipios Críticos (con NSS < 0)
      const muniStats = {{}};
      data.forEach(r => {{
        if (!muniStats[r.municipio]) muniStats[r.municipio] = {{ pos: 0, neg: 0, total: 0 }};
        muniStats[r.municipio].pos += r.positivos;
        muniStats[r.municipio].neg += r.negativos;
        muniStats[r.municipio].total += r.total_comentarios;
      }});

      let criticosCount = 0;
      Object.keys(muniStats).forEach(m => {{
        if (muniStats[m].pos < muniStats[m].neg) criticosCount++;
      }});

      // KPI-08: Datasets Bajo Umbral
      const dsBajoUmbral = new Set();
      data.forEach(r => {{
        const critFila = r.total_comentarios > 0 ? r.negativos / r.total_comentarios : 0;
        if (r.confianza_promedio < umbralConf || critFila > 0.25) {{
          dsBajoUmbral.add(r.id_dataset);
        }}
      }});

      // Update Cards UI
      document.getElementById('kpi-1-val').innerText = totalVol.toLocaleString();
      document.getElementById('kpi-2-val').innerText = (nss > 0 ? '+' : '') + nss.toFixed(1) + '%';
      document.getElementById('kpi-3-val').innerText = criticidad.toFixed(1) + '%';
      document.getElementById('kpi-4-val').innerText = confianzaPond.toFixed(2);
      document.getElementById('kpi-7-val').innerText = criticosCount;
      document.getElementById('kpi-8-val').innerText = dsBajoUmbral.size;

      // Update Badges
      const badgeCrit = document.getElementById('kpi-3-badge');
      if (criticidad > umbralCritico) {{
        badgeCrit.innerText = 'CRÍTICO';
        badgeCrit.className = 'kpi-badge badge-critico';
      }} else if (criticidad >= 30) {{
        badgeCrit.innerText = 'ALERTA';
        badgeCrit.className = 'kpi-badge badge-alerta';
      }} else {{
        badgeCrit.innerText = 'NORMAL';
        badgeCrit.className = 'kpi-badge badge-normal';
      }}

      const badgeConf = document.getElementById('kpi-4-badge');
      if (confianzaPond >= 0.80) {{
        badgeConf.innerText = 'ÓPTIMO';
        badgeConf.className = 'kpi-badge badge-normal';
      }} else if (confianzaPond >= umbralConf) {{
        badgeConf.innerText = 'ACEPTABLE';
        badgeConf.className = 'kpi-badge badge-alerta';
      }} else {{
        badgeConf.innerText = 'REQUIERE REVISIÓN';
        badgeConf.className = 'kpi-badge badge-critico';
      }}

      renderCharts(data, totalPos, totalNeu, totalNeg, umbralConf);
      renderTable(data);
    }}

    function renderCharts(data, pos, neu, neg, umbralConf) {{
      // Donut Chart
      if (chartDonut) chartDonut.destroy();
      chartDonut = new Chart(document.getElementById('chart-polaridad-donut'), {{
        type: 'doughnut',
        data: {{
          labels: ['Positivos', 'Neutros', 'Negativos'],
          datasets: [{{
            data: [pos, neu, neg],
            backgroundColor: ['#34A853', '#9AA0A6', '#EA4335']
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#f8fafc' }} }} }}
        }}
      }});

      // Resumen por municipio
      const muniGroup = {{}};
      data.forEach(r => {{
        if (!muniGroup[r.municipio]) muniGroup[r.municipio] = 0;
        muniGroup[r.municipio] += r.total_comentarios;
      }});

      const munis = Object.keys(muniGroup).sort((a,b) => muniGroup[b] - muniGroup[a]);
      const volMunis = munis.map(m => muniGroup[m]);

      if (chartResumen) chartResumen.destroy();
      chartResumen = new Chart(document.getElementById('chart-resumen-barras'), {{
        type: 'bar',
        data: {{
          labels: munis.map(m => m.toUpperCase()),
          datasets: [{{
            label: 'Volumen Comentarios',
            data: volMunis,
            backgroundColor: '#38bdf8'
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{ y: {{ ticks: {{ color: '#94a3b8' }} }}, x: {{ ticks: {{ color: '#94a3b8' }} }} }}
        }}
      }});

      // Page 2: Ranking Criticidad
      const muniCrit = {{}};
      data.forEach(r => {{
        if (!muniCrit[r.municipio]) muniCrit[r.municipio] = {{ neg: 0, total: 0 }};
        muniCrit[r.municipio].neg += r.negativos;
        muniCrit[r.municipio].total += r.total_comentarios;
      }});

      const rankingData = Object.keys(muniCrit).map(m => ({{
        muni: m.toUpperCase(),
        pct: muniCrit[m].total > 0 ? (muniCrit[m].neg / muniCrit[m].total) * 100 : 0
      }})).sort((a,b) => b.pct - a.pct);

      if (chartRanking) chartRanking.destroy();
      chartRanking = new Chart(document.getElementById('chart-ranking-criticidad'), {{
        type: 'bar',
        data: {{
          labels: rankingData.map(d => d.muni),
          datasets: [{{
            label: 'Tasa de Criticidad (%)',
            data: rankingData.map(d => d.pct.toFixed(1)),
            backgroundColor: rankingData.map(d => d.pct > 35 ? '#EA4335' : '#38bdf8')
          }}]
        }},
        options: {{
          indexAxis: 'y',
          responsive: true,
          maintainAspectRatio: false,
          scales: {{ x: {{ ticks: {{ color: '#94a3b8' }} }}, y: {{ ticks: {{ color: '#94a3b8' }} }} }}
        }}
      }});

      // Page 3: Stacked Bar Chart
      const muniStack = {{}};
      data.forEach(r => {{
        if (!muniStack[r.municipio]) muniStack[r.municipio] = {{ pos: 0, neu: 0, neg: 0 }};
        muniStack[r.municipio].pos += r.positivos;
        muniStack[r.municipio].neu += r.neutros;
        muniStack[r.municipio].neg += r.negativos;
      }});

      const stackMunis = Object.keys(muniStack).sort();

      if (chartStacked) chartStacked.destroy();
      chartStacked = new Chart(document.getElementById('chart-polaridad-stacked'), {{
        type: 'bar',
        data: {{
          labels: stackMunis.map(m => m.toUpperCase()),
          datasets: [
            {{ label: 'Positivos', data: stackMunis.map(m => muniStack[m].pos), backgroundColor: '#34A853' }},
            {{ label: 'Neutros', data: stackMunis.map(m => muniStack[m].neu), backgroundColor: '#9AA0A6' }},
            {{ label: 'Negativos', data: stackMunis.map(m => muniStack[m].neg), backgroundColor: '#EA4335' }}
          ]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{ x: {{ stacked: true, ticks: {{ color: '#94a3b8' }} }}, y: {{ stacked: true, ticks: {{ color: '#94a3b8' }} }} }}
        }}
      }});

      // Page 4: Confidence Audit with line reference
      const muniConf = {{}};
      data.forEach(r => {{
        if (!muniConf[r.municipio]) muniConf[r.municipio] = {{ sumConf: 0, total: 0 }};
        muniConf[r.municipio].sumConf += r.confianza_promedio * r.total_comentarios;
        muniConf[r.municipio].total += r.total_comentarios;
      }});

      const confMunis = Object.keys(muniConf).sort();
      const confVals = confMunis.map(m => muniConf[m].total > 0 ? muniConf[m].sumConf / muniConf[m].total : 0);

      if (chartConfianza) chartConfianza.destroy();
      chartConfianza = new Chart(document.getElementById('chart-confianza-ia'), {{
        type: 'bar',
        data: {{
          labels: confMunis.map(m => m.toUpperCase()),
          datasets: [
            {{
              label: 'Confianza Promedio IA',
              data: confVals.map(v => v.toFixed(3)),
              backgroundColor: confVals.map(v => v < umbralConf ? '#EA4335' : '#34A853')
            }}
          ]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{ y: {{ min: 0.5, max: 1.0, ticks: {{ color: '#94a3b8' }} }}, x: {{ ticks: {{ color: '#94a3b8' }} }} }}
        }}
      }});

      // Page 5: Emociones
      const emoCounts = {{ POS: 0, NEU: 0, NEG: 0 }};
      data.forEach(r => {{
        if (emoCounts[r.emocion_predominante] !== undefined) emoCounts[r.emocion_predominante]++;
      }});

      if (chartEmocionesPie) chartEmocionesPie.destroy();
      chartEmocionesPie = new Chart(document.getElementById('chart-emociones-pie'), {{
        type: 'pie',
        data: {{
          labels: ['Positivo', 'Neutro', 'Negativo'],
          datasets: [{{
            data: [emoCounts.POS, emoCounts.NEU, emoCounts.NEG],
            backgroundColor: ['#34A853', '#9AA0A6', '#EA4335']
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{ legend: {{ position: 'bottom', labels: {{ color: '#f8fafc' }} }} }}
        }}
      }});

      if (chartEmocionesBar) chartEmocionesBar.destroy();
      chartEmocionesBar = new Chart(document.getElementById('chart-emociones-bar'), {{
        type: 'bar',
        data: {{
          labels: ['Positivo (POS)', 'Neutro (NEU)', 'Negativo (NEG)'],
          datasets: [{{
            label: 'Cantidad Datasets',
            data: [emoCounts.POS, emoCounts.NEU, emoCounts.NEG],
            backgroundColor: ['#34A853', '#9AA0A6', '#EA4335']
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          scales: {{ y: {{ ticks: {{ color: '#94a3b8' }} }}, x: {{ ticks: {{ color: '#94a3b8' }} }} }}
        }}
      }});
    }}

    function renderTable(data) {{
      const tbody = document.getElementById('gold-table-body');
      tbody.innerHTML = '';

      data.forEach(r => {{
        const nss = r.total_comentarios > 0 ? ((r.positivos - r.negativos) / r.total_comentarios) * 100 : 0;
        const crit = r.total_comentarios > 0 ? (r.negativos / r.total_comentarios) * 100 : 0;
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td><strong>${{r.municipio.toUpperCase()}}</strong></td>
          <td><code>${{r.id_dataset}}</code></td>
          <td>${{r.total_comentarios}}</td>
          <td style="color: var(--pos-green); font-weight:600;">${{r.positivos}}</td>
          <td style="color: var(--neg-red); font-weight:600;">${{r.negativos}}</td>
          <td style="color: var(--neu-gray);">${{r.neutros}}</td>
          <td style="color: ${{nss >= 0 ? 'var(--pos-green)' : 'var(--neg-red)'}};">${{nss.toFixed(1)}}%</td>
          <td style="color: ${{crit > 30 ? 'var(--neg-red)' : 'var(--text-main)'}};">${{crit.toFixed(1)}}%</td>
          <td>${{r.confianza_promedio.toFixed(4)}}</td>
          <td><span class="kpi-badge ${{r.emocion_predominante === 'POS' ? 'badge-normal' : (r.emocion_predominante === 'NEG' ? 'badge-critico' : 'badge-alerta')}}">${{r.emocion_predominante}}</span></td>
        `;
        tbody.appendChild(tr);
      }});
    }}

    function filterTable() {{
      const query = document.getElementById('table-search').value.toLowerCase();
      const rows = document.querySelectorAll('#gold-table-body tr');
      rows.forEach(tr => {{
        const text = tr.innerText.toLowerCase();
        tr.style.display = text.includes(query) ? '' : 'none';
      }});
    }}

    function exportTableToCSV() {{
      const data = getFilteredData();
      let csv = 'municipio,id_dataset,total_comentarios,positivos,negativos,neutros,confianza_promedio,emocion_predominante\\n';
      data.forEach(r => {{
        csv += `${{r.municipio}},${{r.id_dataset}},${{r.total_comentarios}},${{r.positivos}},${{r.negativos}},${{r.neutros}},${{r.confianza_promedio}},${{r.emocion_predominante}}\\n`;
      }});

      const blob = new Blob([csv], {{ type: 'text/csv' }});
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = 'gold_comentarios_sentimiento_filtrado.csv';
      a.click();
    }}

    function switchTab(tabId) {{
      document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
      document.querySelectorAll('.tab-btn').forEach(el => el.classList.remove('active'));

      document.getElementById(tabId).classList.add('active');
      event.target.classList.add('active');
    }}

    // Initial load
    window.onload = function() {{
      initFilters();
      updateDashboard();
    }};
  </script>
</body>
</html>
"""

    OUTPUT_HTML.write_text(html_content, encoding="utf-8")
    print(f"✅ Prototipo HTML interactivo generado en: {OUTPUT_HTML}")


if __name__ == "__main__":
    main()
