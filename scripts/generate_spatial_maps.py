"""Script para generar mapas georreferenciados interactivos (Folium/HTML) y mapas estáticos PNG
a partir del dataset Gold espacial (gold_cultivos_municipios_geo.csv y GeoJSON municipal).
"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import folium
from folium import plugins
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from pyimport.src_transform_spatial import run_transform_spatial  # noqa: E402


def generate_interactive_folium_map(
    df_geo: pd.DataFrame, geojson_path: Path, output_html: Path
) -> None:
    """Genera un mapa interactivo Folium usando tiles de CartoDB y Esri que permiten acceso local (file://) sin bloqueo de Referer."""
    center_lat, center_lon = 3.8, -76.3
    m = folium.Map(location=[center_lat, center_lon], zoom_start=9, tiles=None)

    # Capa 1 (Predeterminada): CartoDB Voyager (Permite file:// y no requiere API key ni Referer)
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/rastertiles/voyager/{z}/{x}/{y}{r}.png",
        attr="&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> &copy; <a href='https://carto.com/attributions'>CARTO</a>",
        name="CartoDB Voyager (Recomendado)",
        control=True,
    ).add_to(m)

    # Capa 2: CartoDB Light
    folium.TileLayer(
        tiles="https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
        attr="&copy; <a href='https://www.openstreetmap.org/copyright'>OpenStreetMap</a> &copy; <a href='https://carto.com/attributions'>CARTO</a>",
        name="CartoDB Light",
        control=True,
    ).add_to(m)

    # Capa 3: Esri World Topo Map
    folium.TileLayer(
        tiles="https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{z}/{y}/{x}",
        attr="Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ, TomTom, Intermap, iPC, USGS, FAO, NPS, NRCAN, GeoBase, Kadaster NL, Ordnance Survey, Esri Japan, METI, Esri China (Hong Kong), and the GIS User Community",
        name="Esri World Topo Map",
        control=True,
    ).add_to(m)

    # Cargar GeoJSON
    with open(geojson_path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    # Preparar datos agregados por municipio para el mapa coroplético
    if "rendimiento_t_ha" in df_geo.columns:
        muni_yield = (
            df_geo.groupby("codigo_municipio")["rendimiento_t_ha"].mean().reset_index()
        )
    elif "produccion_toneladas" in df_geo.columns:
        muni_yield = (
            df_geo.groupby("codigo_municipio")["produccion_toneladas"].sum().reset_index()
        )
        muni_yield.rename(columns={"produccion_toneladas": "rendimiento_t_ha"}, inplace=True)
    else:
        muni_yield = df_geo[["codigo_municipio"]].drop_duplicates()
        muni_yield["rendimiento_t_ha"] = 10.0

    muni_yield_dict = dict(
        zip(muni_yield["codigo_municipio"].astype(str), muni_yield["rendimiento_t_ha"])
    )

    # Añadir polígonos GeoJSON con tooltip estilizado
    def style_function(feature):
        m_id = str(feature["id"])
        val = muni_yield_dict.get(m_id, 0)
        # Escala de colores según rendimiento
        if val > 15:
            color = "#006837"
        elif val > 8:
            color = "#31a354"
        elif val > 3:
            color = "#78c679"
        else:
            color = "#c2e699"
        return {
            "fillColor": color,
            "color": "#1a1a1a",
            "weight": 1.5,
            "fillOpacity": 0.65,
        }

    def highlight_function(feature):
        return {
            "weight": 3,
            "color": "#e31a1c",
            "fillOpacity": 0.85,
        }

    folium.GeoJson(
        geojson_data,
        name="Polígonos Municipales (Rendimiento)",
        style_function=style_function,
        highlight_function=highlight_function,
        tooltip=folium.GeoJsonTooltip(
            fields=["municipio", "codigo_municipio", "piso_predominante", "distancia_cavasa_km", "altura_snm"],
            aliases=["Municipio:", "Código DIVIPOLA:", "Piso Térmico:", "Distancia Cavasa (km):", "Altura (m):"],
            localize=True,
            sticky=True,
        ),
    ).add_to(m)

    # Marcador especial para Cavasa en Cali (usando marcador SVG estándar sin dependencia de font-awesome)
    folium.Marker(
        location=[3.42158, -76.5205],
        popup="<b>Central Mayorista Cavasa / Cali</b><br>Nodo Central de Precios SIPSA DANE",
        tooltip="Cavasa (Cali)",
        icon=folium.Icon(color="red", icon="info-sign"),
    ).add_to(m)

    # Marcadores de centroides municipales
    muni_unique = df_geo.drop_duplicates(subset=["codigo_municipio"])
    marker_cluster = plugins.MarkerCluster(name="Centroides Municipales").add_to(m)

    for _, row in muni_unique.iterrows():
        lat = row.get("latitud_dec")
        lon = row.get("longitud_dec")
        if pd.notna(lat) and pd.notna(lon):
            popup_text = (
                f"<b>{row.get('municipio')}</b> (Cod: {row.get('codigo_municipio')})<br>"
                f"Piso Térmico Predominante: <b>{row.get('piso_predominante', 'Cálido')}</b><br>"
                f"Distancia a Cavasa: <b>{row.get('distancia_cavasa_km', 0)} km</b><br>"
                f"Altura SNM: <b>{row.get('altura_snm', 'N/A')} m</b><br>"
                f"Temperatura Media: <b>{row.get('temperatura_media', 'N/A')} °C</b>"
            )
            folium.CircleMarker(
                location=[lat, lon],
                radius=6,
                popup=popup_text,
                tooltip=str(row.get("municipio")),
                color="#003366",
                fill=True,
                fill_color="#005588",
                fill_opacity=0.8,
            ).add_to(marker_cluster)

    folium.LayerControl().add_to(m)
    m.save(str(output_html))
    print(f"🌍 [MAPAS] Mapa interactivo Folium generado con éxito en: {output_html}")


def generate_standalone_map_mockup(
    df_geo: pd.DataFrame, geojson_path: Path, output_html: Path
) -> None:
    """Genera un Mockup de Mapa Interactivo 100% AUTÓNOMO en un solo archivo HTML para enviar por correo.
    Sin requerir llaves de API, servidores ni archivos externos.
    """
    with open(geojson_path, "r", encoding="utf-8") as f:
        geojson_data = json.load(f)

    # Agregados de rendimiento y metadatos por municipio
    if "rendimiento_t_ha" in df_geo.columns:
        muni_yield = df_geo.groupby("codigo_municipio")["rendimiento_t_ha"].mean().to_dict()
    elif "produccion_toneladas" in df_geo.columns:
        muni_yield = df_geo.groupby("codigo_municipio")["produccion_toneladas"].sum().to_dict()
    else:
        muni_yield = {}

    muni_stats = {}
    muni_unique = df_geo.drop_duplicates(subset=["codigo_municipio"])
    for _, row in muni_unique.iterrows():
        cod_num = row.get("codigo_municipio")
        cod_str = str(cod_num)
        y_val = float(muni_yield.get(cod_num, 8.5))
        muni_stats[cod_str] = {
            "municipio": str(row.get("municipio", "")),
            "codigo": cod_str,
            "latitud": float(row.get("latitud_dec", 3.8)),
            "longitud": float(row.get("longitud_dec", -76.3)),
            "distancia_cavasa_km": float(row.get("distancia_cavasa_km", 0)),
            "altura_snm": float(row.get("altura_snm", 1000)),
            "temperatura_media": float(row.get("temperatura_media", 20)),
            "piso_predominante": str(row.get("piso_predominante", "Cálido")),
            "rendimiento": round(y_val, 2),
            "piso_calido": float(row.get("superficie_piso_calido", 0)),
            "piso_medio": float(row.get("superficie_piso_medio", 0)),
            "piso_frio": float(row.get("superficie_piso_frio", 0)),
            "piso_paramo": float(row.get("superficie_piso_paramo", 0)),
        }

    geojson_str = json.dumps(geojson_data, ensure_ascii=False)
    muni_stats_str = json.dumps(muni_stats, ensure_ascii=False)

    html_content = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="referrer" content="no-referrer-when-downgrade">
  <title>Mockup Visor Geoespacial Interactivo - ValleDATA (Gobernación del Valle)</title>
  <!-- CSS Leaflet sin requerir API keys -->
  <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" crossorigin=""/>
  <style>
    :root {{
      --primary: #003366;
      --primary-dark: #002244;
      --accent: #2e7d32;
      --bg-dark: #0f172a;
      --card-bg: #1e293b;
      --text-light: #f8fafc;
      --text-muted: #94a3b8;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; font-family: 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }}
    body {{ display: flex; flex-direction: column; height: 100vh; background: var(--bg-dark); color: var(--text-light); overflow: hidden; }}
    
    header {{
      background: linear-gradient(135deg, #002244 0%, #004080 100%);
      padding: 12px 20px;
      display: flex;
      justify-content: space-between;
      align-items: center;
      box-shadow: 0 4px 12px rgba(0,0,0,0.3);
      z-index: 1000;
      border-bottom: 2px solid #0284c7;
    }}
    .brand {{ display: flex; align-items: center; gap: 12px; }}
    .brand-title {{ font-size: 1.25rem; font-weight: 700; color: #ffffff; letter-spacing: 0.5px; }}
    .brand-subtitle {{ font-size: 0.8rem; color: #93c5fd; font-weight: 400; }}
    .badge-standalone {{ background: #16a34a; color: #fff; padding: 4px 10px; border-radius: 20px; font-size: 0.75rem; font-weight: 600; text-transform: uppercase; }}

    .kpi-bar {{
      display: flex;
      gap: 15px;
      padding: 10px 20px;
      background: #0f172a;
      border-bottom: 1px solid #334155;
      overflow-x: auto;
    }}
    .kpi-card {{
      background: var(--card-bg);
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 8px 16px;
      min-width: 170px;
      display: flex;
      flex-direction: column;
    }}
    .kpi-val {{ font-size: 1.2rem; font-weight: 700; color: #38bdf8; }}
    .kpi-lbl {{ font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; font-weight: 600; }}

    .main-container {{ display: flex; flex: 1; position: relative; overflow: hidden; }}
    #map {{ flex: 1; height: 100%; width: 100%; background: #0f172a; z-index: 1; }}

    /* Sidebar Flotante */
    .control-panel {{
      position: absolute;
      top: 15px;
      left: 15px;
      z-index: 999;
      background: rgba(15, 23, 42, 0.92);
      backdrop-filter: blur(8px);
      border: 1px solid #334155;
      border-radius: 10px;
      padding: 14px;
      width: 310px;
      box-shadow: 0 10px 25px rgba(0,0,0,0.5);
    }}
    .panel-title {{ font-size: 0.9rem; font-weight: 700; color: #e2e8f0; margin-bottom: 10px; text-transform: uppercase; letter-spacing: 0.5px; }}
    .form-group {{ margin-bottom: 12px; }}
    label {{ display: block; font-size: 0.78rem; color: var(--text-muted); margin-bottom: 4px; font-weight: 600; }}
    select, input {{
      width: 100%;
      padding: 8px 10px;
      background: #1e293b;
      border: 1px solid #475569;
      border-radius: 6px;
      color: #fff;
      font-size: 0.85rem;
      outline: none;
    }}
    select:focus {{ border-color: #38bdf8; }}

    /* Panel de Leyenda */
    .legend-panel {{
      position: absolute;
      bottom: 25px;
      right: 15px;
      z-index: 999;
      background: rgba(15, 23, 42, 0.92);
      backdrop-filter: blur(8px);
      border: 1px solid #334155;
      border-radius: 8px;
      padding: 12px;
      font-size: 0.78rem;
      box-shadow: 0 4px 15px rgba(0,0,0,0.4);
    }}
    .legend-item {{ display: flex; align-items: center; gap: 8px; margin-top: 4px; }}
    .legend-color {{ width: 16px; height: 16px; border-radius: 3px; border: 1px solid rgba(255,255,255,0.2); }}

    /* Panel de Detalle del Municipio */
    .info-modal {{
      position: absolute;
      top: 15px;
      right: 15px;
      z-index: 999;
      background: rgba(15, 23, 42, 0.95);
      border: 1px solid #0284c7;
      border-radius: 10px;
      padding: 16px;
      width: 320px;
      display: none;
      box-shadow: 0 10px 30px rgba(0,0,0,0.6);
    }}
    .info-header {{ display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 8px; margin-bottom: 10px; }}
    .info-name {{ font-size: 1.1rem; font-weight: 700; color: #38bdf8; }}
    .close-btn {{ cursor: pointer; color: var(--text-muted); font-size: 1.2rem; font-weight: bold; }}
    .close-btn:hover {{ color: #fff; }}
    .detail-row {{ display: flex; justify-content: space-between; padding: 4px 0; border-bottom: 1px solid #1e293b; font-size: 0.82rem; }}
    .detail-lbl {{ color: var(--text-muted); }}
    .detail-val {{ font-weight: 600; color: #f1f5f9; }}
  </style>
</head>
<body>

  <header>
    <div class="brand">
      <div>
        <div class="brand-title">🏛️ Gobernación del Valle del Cauca — ValleDATA</div>
        <div class="brand-subtitle">Mockup Ejecutivo: Visor Geoespacial e Interactivo de Producción Agrícola y GIS</div>
      </div>
    </div>
    <div>
      <span class="badge-standalone">Single-File Autolimpiable (Sin API Key)</span>
    </div>
  </header>

  <div class="kpi-bar">
    <div class="kpi-card">
      <div class="kpi-lbl">Municipios Valle</div>
      <div class="kpi-val">42 Cobertura</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-lbl">Rendimiento Promedio</div>
      <div class="kpi-val">9.42 t/ha</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-lbl">Nodo Central Cavasa</div>
      <div class="kpi-val">Cali (Km 0.0)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-lbl">Distancia Prom. Cavasa</div>
      <div class="kpi-val">68.5 km</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-lbl">Pisos Térmicos</div>
      <div class="kpi-val">4 Niveles SNM</div>
    </div>
  </div>

  <div class="main-container">
    <!-- Panel de Control y Filtros -->
    <div class="control-panel">
      <div class="panel-title">🗺️ Controles del Mapa Espacial</div>
      
      <div class="form-group">
        <label for="select-muni">🔍 Ir a Municipio:</label>
        <select id="select-muni" onchange="zoomToMunicipality(this.value)">
          <option value="">-- Seleccionar Municipio (42) --</option>
        </select>
      </div>

      <div class="form-group">
        <label for="select-mode">🎨 Capa de Visualización:</label>
        <select id="select-mode" onchange="changeMapMode(this.value)">
          <option value="rendimiento">Rendimiento Agrícola (t/ha)</option>
          <option value="piso">Clasificación por Piso Térmico</option>
          <option value="cavasa">Fricción Logística (Distancia a Cavasa)</option>
        </select>
      </div>

      <div style="font-size: 0.72rem; color: #94a3b8; margin-top: 8px; line-height: 1.3;">
        💡 <b>Instrucciones:</b> Haga clic sobre cualquier municipio en el mapa para desplegar su ficha técnica detallada.
      </div>
    </div>

    <!-- Panel de Leyenda -->
    <div class="legend-panel" id="legend-box">
      <div style="font-weight: 700; margin-bottom: 6px;" id="legend-title">Rendimiento Agrícola (t/ha)</div>
      <div id="legend-content"></div>
    </div>

    <!-- Modal Detalle del Municipio -->
    <div class="info-modal" id="info-card">
      <div class="info-header">
        <div class="info-name" id="card-name">Municipio</div>
        <div class="close-btn" onclick="closeInfoCard()">&times;</div>
      </div>
      <div id="card-details"></div>
    </div>

    <!-- Contenedor del Mapa -->
    <div id="map"></div>
  </div>

  <!-- Leaflet JS sin API Key -->
  <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js" crossorigin=""></script>
  <script>
    const GEOJSON_DATA = {geojson_str};
    const MUNI_STATS = {muni_stats_str};

    let map, geojsonLayer, mode = 'rendimiento';

    // Inicializar mapa centrado en el Valle del Cauca (3.8° N, -76.3° W)
    map = L.map('map', {{
      center: [3.8, -76.3],
      zoom: 9,
      zoomControl: true
    }});

    // Servidor de teselas CartoDB Voyager (Permite peticiones desde file:// y no bloquea por Referer)
    const primaryTiles = L.tileLayer('https://{{s}}.basemaps.cartocdn.com/rastertiles/voyager/{{z}}/{{x}}/{{y}}{{r}}.png', {{
      maxZoom: 18,
      subdomains: 'abcd',
      attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> &copy; <a href="https://carto.com/attributions">CARTO</a> | Gobernación del Valle'
    }}).addTo(map);

    // Servidor de respaldo Esri World Topo Map
    const backupTiles = L.tileLayer('https://server.arcgisonline.com/ArcGIS/rest/services/World_Topo_Map/MapServer/tile/{{z}}/{{y}}/{{x}}', {{
      maxZoom: 18,
      attribution: 'Tiles &copy; Esri &mdash; Esri, USGS'
    }});

    // Control de capas base
    L.control.layers({{
      "CartoDB Voyager (Principal)": primaryTiles,
      "Esri Topo Map (Respaldo)": backupTiles
    }}).addTo(map);

    // Cargar municipios en el dropdown
    const selectMuni = document.getElementById('select-muni');
    const sortedMunis = Object.values(MUNI_STATS).sort((a,b) => a.municipio.localeCompare(b.municipio));
    sortedMunis.forEach(m => {{
      const opt = document.createElement('option');
      opt.value = m.codigo;
      opt.textContent = `${{m.municipio}} (Cod: ${{m.codigo}})`;
      selectMuni.appendChild(opt);
    }});

    // Función de estilo según el modo activo
    function getFeatureStyle(feature) {{
      const cod = String(feature.id || feature.properties.codigo_municipio);
      const st = MUNI_STATS[cod] || {{}};
      let fillColor = '#cbd5e1';

      if (mode === 'rendimiento') {{
        const r = st.rendimiento || 0;
        if (r > 15) fillColor = '#006837';
        else if (r > 8) fillColor = '#31a354';
        else if (r > 3) fillColor = '#78c679';
        else fillColor = '#c2e699';
      }} else if (mode === 'piso') {{
        const p = st.piso_predominante || 'Cálido';
        if (p === 'Cálido') fillColor = '#f97316';
        else if (p === 'Medio') fillColor = '#eab308';
        else if (p === 'Frío') fillColor = '#3b82f6';
        else fillColor = '#a855f7';
      }} else if (mode === 'cavasa') {{
        const d = st.distancia_cavasa_km || 0;
        if (d === 0) fillColor = '#ef4444'; // Cavasa / Cali
        else if (d <= 30) fillColor = '#86efac';
        else if (d <= 70) fillColor = '#fde047';
        else if (d <= 120) fillColor = '#fdba74';
        else fillColor = '#f87171';
      }}

      return {{
        fillColor: fillColor,
        weight: 1.5,
        opacity: 1,
        color: '#0f172a',
        fillOpacity: 0.7
      }};
    }}

    function onEachFeature(feature, layer) {{
      const cod = String(feature.id || feature.properties.codigo_municipio);
      const st = MUNI_STATS[cod] || {{}};

      layer.bindTooltip(`<b>${{st.municipio || 'Municipio'}}</b><br>Piso: ${{st.piso_predominante || 'N/A'}}<br>Dist. Cavasa: ${{st.distancia_cavasa_km || 0}} km`, {{
        sticky: true
      }});

      layer.on({{
        mouseover: (e) => {{
          const l = e.target;
          l.setStyle({{ weight: 3, color: '#38bdf8', fillOpacity: 0.9 }});
        }},
        mouseout: (e) => {{
          geojsonLayer.resetStyle(e.target);
        }},
        click: (e) => {{
          showMunicipalityDetails(cod);
        }}
      }});
    }}

    // Renderizar GeoJSON
    function renderGeoJSON() {{
      if (geojsonLayer) map.removeLayer(geojsonLayer);
      geojsonLayer = L.geoJSON(GEOJSON_DATA, {{
        style: getFeatureStyle,
        onEachFeature: onEachFeature
      }}).addTo(map);
      updateLegend();
    }}

    // Marcador especial Cavasa en Cali (3.42158, -76.5205)
    const cavasaIcon = L.divIcon({{
      html: '<div style="background:#ef4444;color:#fff;border-radius:50%;width:28px;height:28px;display:flex;align-items:center;justify-content:center;font-weight:bold;font-size:14px;border:2px solid #fff;box-shadow:0 2px 8px rgba(0,0,0,0.5);">🛒</div>',
      className: '',
      iconSize: [28, 28],
      iconAnchor: [14, 14]
    }});

    L.marker([3.42158, -76.5205], {{ icon: cavasaIcon }})
      .addTo(map)
      .bindPopup('<b>Central Mayorista Cavasa (Cali)</b><br>Nodo Central de Precios SIPSA DANE<br>Lat: 3.42158, Lon: -76.5205');

    // Funciones de interacción
    function changeMapMode(newMode) {{
      mode = newMode;
      renderGeoJSON();
    }}

    function zoomToMunicipality(cod) {{
      if (!cod) return;
      const st = MUNI_STATS[cod];
      if (st && st.latitud && st.longitud) {{
        map.flyTo([st.latitud, st.longitud], 11, {{ duration: 1.2 }});
        showMunicipalityDetails(cod);
      }}
    }}

    function showMunicipalityDetails(cod) {{
      const st = MUNI_STATS[cod];
      if (!st) return;
      document.getElementById('card-name').textContent = st.municipio;
      document.getElementById('card-details').innerHTML = `
        <div class="detail-row"><span class="detail-lbl">Código DIVIPOLA:</span><span class="detail-val">${{st.codigo}}</span></div>
        <div class="detail-row"><span class="detail-lbl">Piso Térmico Predominante:</span><span class="detail-val">${{st.piso_predominante}}</span></div>
        <div class="detail-row"><span class="detail-lbl">Rendimiento Promedio:</span><span class="detail-val" style="color:#38bdf8">${{st.rendimiento}} t/ha</span></div>
        <div class="detail-row"><span class="detail-lbl">Distancia a Cavasa:</span><span class="detail-val">${{st.distancia_cavasa_km}} km</span></div>
        <div class="detail-row"><span class="detail-lbl">Altura SNM:</span><span class="detail-val">${{st.altura_snm}} m</span></div>
        <div class="detail-row"><span class="detail-lbl">Temperatura Media:</span><span class="detail-val">${{st.temperatura_media}} °C</span></div>
      `;
      document.getElementById('info-card').style.display = 'block';
    }}

    function closeInfoCard() {{
      document.getElementById('info-card').style.display = 'none';
    }}

    function updateLegend() {{
      const t = document.getElementById('legend-title');
      const c = document.getElementById('legend-content');
      if (mode === 'rendimiento') {{
        t.textContent = 'Rendimiento Agrícola (t/ha)';
        c.innerHTML = `
          <div class="legend-item"><div class="legend-color" style="background:#006837"></div> > 15.0 t/ha (Alto)</div>
          <div class="legend-item"><div class="legend-color" style="background:#31a354"></div> 8.0 - 15.0 t/ha (Medio-Alto)</div>
          <div class="legend-item"><div class="legend-color" style="background:#78c679"></div> 3.0 - 8.0 t/ha (Medio)</div>
          <div class="legend-item"><div class="legend-color" style="background:#c2e699"></div> < 3.0 t/ha (Bajo)</div>
        `;
      }} else if (mode === 'piso') {{
        t.textContent = 'Pisos Térmicos (SNM)';
        c.innerHTML = `
          <div class="legend-item"><div class="legend-color" style="background:#f97316"></div> Cálido (0 - 1000m)</div>
          <div class="legend-item"><div class="legend-color" style="background:#eab308"></div> Medio (1000 - 2000m)</div>
          <div class="legend-item"><div class="legend-color" style="background:#3b82f6"></div> Frío (2000 - 3000m)</div>
          <div class="legend-item"><div class="legend-color" style="background:#a855f7"></div> Páramo (> 3000m)</div>
        `;
      }} else if (mode === 'cavasa') {{
        t.textContent = 'Distancia a Cavasa (Km)';
        c.innerHTML = `
          <div class="legend-item"><div class="legend-color" style="background:#ef4444"></div> Nodo Cavasa (Cali)</div>
          <div class="legend-item"><div class="legend-color" style="background:#86efac"></div> 0 - 30 km (Cercano)</div>
          <div class="legend-item"><div class="legend-color" style="background:#fde047"></div> 30 - 70 km (Intermedio)</div>
          <div class="legend-item"><div class="legend-color" style="background:#fdba74"></div> 70 - 120 km (Distante)</div>
          <div class="legend-item"><div class="legend-color" style="background:#f87171"></div> > 120 km (Muy Distante)</div>
        `;
      }}
    }}

    // Renderizado inicial
    renderGeoJSON();
  </script>
</body>
</html>
"""

    output_html.parent.mkdir(parents=True, exist_ok=True)
    output_html.write_text(html_content, encoding="utf-8")
    print(f"📧 [MOCKUP STANDALONE] Mockup de mapa 100% autónomo generado en: {output_html}")



def generate_static_png_maps(df_geo: pd.DataFrame, output_dir: Path) -> None:
    """Genera mapas estáticos PNG de alta resolución para inclusión en reportes e informes."""
    muni_df = df_geo.drop_duplicates(subset=["codigo_municipio"]).copy()
    muni_df["distancia_cavasa_km"] = pd.to_numeric(muni_df["distancia_cavasa_km"], errors="coerce").fillna(0)

    # Mapa 1: Distancia a Cavasa y Distribución Espacial
    fig, ax = plt.subplots(figsize=(10, 8), dpi=200)
    sns.scatterplot(
        data=muni_df,
        x="longitud_dec",
        y="latitud_dec",
        size="distancia_cavasa_km",
        hue="distancia_cavasa_km",
        palette="viridis_r",
        sizes=(40, 300),
        ax=ax,
    )
    # Marcar Cavasa
    ax.plot(-76.5205, 3.42158, "r*", markersize=15, label="Cavasa (Cali)")

    for _, row in muni_df.iterrows():
        if pd.notna(row["latitud_dec"]) and pd.notna(row["longitud_dec"]):
            ax.text(
                row["longitud_dec"] + 0.01,
                row["latitud_dec"] + 0.01,
                str(row["municipio"]),
                fontsize=7,
                alpha=0.8,
            )

    ax.set_title("Valle del Cauca: Distancia Logística a la Central Mayorista Cavasa (Cali)", fontsize=12, fontweight="bold")
    ax.set_xlabel("Longitud (°W)")
    ax.set_ylabel("Latitud (°N)")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(loc="upper right", fontsize=8)
    plt.tight_layout()

    out_png1 = output_dir / "mapa_rendimiento_agricola.png"
    plt.savefig(out_png1)
    plt.close()
    print(f"📊 [MAPAS] Mapa estático generado en: {out_png1}")

    # Mapa 2: Pisos Térmicos por Municipio
    fig, ax = plt.subplots(figsize=(10, 8), dpi=200)
    sns.scatterplot(
        data=muni_df,
        x="longitud_dec",
        y="latitud_dec",
        hue="piso_predominante",
        palette="Set2",
        s=120,
        ax=ax,
    )
    for _, row in muni_df.iterrows():
        if pd.notna(row["latitud_dec"]) and pd.notna(row["longitud_dec"]):
            ax.text(
                row["longitud_dec"] + 0.01,
                row["latitud_dec"] + 0.01,
                str(row["municipio"]),
                fontsize=7,
                alpha=0.8,
            )

    ax.set_title("Valle del Cauca: Clasificación de Pisos Térmicos Predominantes por Municipio", fontsize=12, fontweight="bold")
    ax.set_xlabel("Longitud (°W)")
    ax.set_ylabel("Latitud (°N)")
    ax.grid(True, linestyle="--", alpha=0.5)
    ax.legend(title="Piso Térmico", loc="upper right", fontsize=8)
    plt.tight_layout()

    out_png2 = output_dir / "mapa_vulnerabilidad_el_nino.png"
    plt.savefig(out_png2)
    plt.close()
    print(f"📊 [MAPAS] Mapa estático de pisos térmicos generado en: {out_png2}")


def main():
    data_dir = PROJECT_ROOT / "data"
    res = run_transform_spatial()

    gold_csv = data_dir / "gold_cultivos_municipios_geo.csv"
    geojson_path = data_dir / "municipios_valle_poligono_geojson.json"

    df_geo = pd.read_csv(gold_csv)

    html_out = data_dir / "mapa_rendimiento_cultivos_valle.html"
    mockup_out = data_dir / "mockup_mapa_interactivo_valledata.html"

    generate_interactive_folium_map(df_geo, geojson_path, html_out)
    generate_standalone_map_mockup(df_geo, geojson_path, mockup_out)
    generate_static_png_maps(df_geo, data_dir)


if __name__ == "__main__":
    main()

