# Manual de Configuración del Tablero en Looker Studio (Data Studio) — Los 9 KPIs de ValleDATA

Este documento detalla **exactamente cómo ejecutar y configurar el tablero de sentimientos directamente DENTRO de Google Looker Studio (Data Studio)** para consumir la tabla de BigQuery `valledata.gold.gold_comentarios_sentimiento`, con **énfasis especial en los KPIs 7, 8 y 9**.

---

## 🛠️ Método 1: Conexión Directa a BigQuery con Fórmulas Copiar/Pegar en Looker Studio (Recomendado)

Al conectar Looker Studio a la tabla `datagov-477214.valledata.gold_comentarios_sentimiento`, abre la opción **"Añadir un campo" (Add a field)** en la fuente de datos y copia/pega exactamente las siguientes expresiones:

### 🌟 Los 3 KPIs Principales Solicitados (KPI-07, KPI-08 y KPI-09)

| ID | Nombre del Campo en Looker Studio | Tipo | Expresión Exacta para Copiar y Pegar en Looker Studio | Descripción y Significado |
| :---: | :--- | :---: | :--- | :--- |
| **KPI-07** ⭐ | **Municipios en Estado CRÍTICO** | Número | `COUNT_DISTINCT(CASE WHEN (positivos - negativos) < 0 THEN municipio END)` | **Conteo de municipios cuyo Net Sentiment Score es negativo** (donde los comentarios insatisfechos superan a los positivos). |
| **KPI-08** ⭐ | **Datasets bajo Umbral de Revisión** | Número | `COUNT_DISTINCT(CASE WHEN confianza_promedio < 0.70 OR (negativos / NULLIF(total_comentarios, 0)) > 0.25 THEN id_dataset END)` | **Conteo de datasets/temáticas con baja certeza de IA (< 0.70) o alta tasa de insatisfacción (> 25%)**. |
| **KPI-09** ⭐ | **Emoción Predominante** | Texto | `CASE WHEN SUM(positivos) >= SUM(negativos) AND SUM(positivos) >= SUM(neutros) THEN "Positivo" WHEN SUM(negativos) >= SUM(positivos) AND SUM(negativos) >= SUM(neutros) THEN "Negativo" ELSE "Neutro" END` | **Etiqueta dinámica de la emoción o sentimiento imperante** en el conjunto de datos activo tras aplicar filtros. |

---

### 📋 Los Otros 6 KPIs Complementarios (KPI-01 al KPI-06)

| ID | Nombre del Campo en Looker Studio | Tipo | Expresión Exacta para Copiar y Pegar en Looker Studio |
| :---: | :--- | :---: | :--- |
| **KPI-01** | **Volumen de Comentarios** | Número | `SUM(total_comentarios)` |
| **KPI-02** | **Net Sentiment Score (NSS %)** | Porcentaje | `((SUM(positivos) - SUM(negativos)) / NULLIF(SUM(total_comentarios), 0))` |
| **KPI-03** | **Tasa de Criticidad (%)** | Porcentaje | `(SUM(negativos) / NULLIF(SUM(total_comentarios), 0))` |
| **KPI-04** | **Confianza IA Ponderada** | Número | `SUM(confianza_promedio * total_comentarios) / NULLIF(SUM(total_comentarios), 0)` |
| **KPI-05** | **Participación Positiva (%)** | Porcentaje | `(SUM(positivos) / NULLIF(SUM(total_comentarios), 0))` |
| **KPI-06** | **Participación Neutra (%)** | Porcentaje | `(SUM(neutros) / NULLIF(SUM(total_comentarios), 0))` |

---

## 🏛️ Método 2: Vista BigQuery Precalculada (`gold_vw_datastudio_sentimiento_9kpis`)

Si prefieres no configurar fórmulas manualmente dentro de Looker Studio, hemos creado la vista de BigQuery [`scripts/bigquery_vw_datastudio_sentimientos_9kpis.sql`](file:///home/jjvallej/work/enigma/pyimport/scripts/bigquery_vw_datastudio_sentimientos_9kpis.sql) que expone todos los 9 KPIs directamente como columnas:

Para crear la vista en BigQuery:
```sql
CREATE OR REPLACE VIEW `datagov-477214.valledata.gold_vw_datastudio_sentimiento_9kpis` AS
SELECT
    municipio,
    id_municipio,
    id_dataset,
    total_comentarios,
    positivos,
    negativos,
    neutros,
    confianza_promedio,
    emocion_predominante,
    -- KPI-07
    CASE WHEN (positivos - negativos) < 0 THEN 1 ELSE 0 END AS es_municipio_critico,
    -- KPI-08
    CASE WHEN confianza_promedio < 0.70 OR SAFE_DIVIDE(negativos, total_comentarios) > 0.25 THEN 1 ELSE 0 END AS es_dataset_bajo_umbral,
    -- KPI-09
    CASE 
        WHEN UPPER(emocion_predominante) IN ('POS', 'POSITIVO') THEN 'Positivo'
        WHEN UPPER(emocion_predominante) IN ('NEG', 'NEGATIVO') THEN 'Negativo'
        ELSE 'Neutro'
    END AS etiqueta_emocion_predominante
FROM `datagov-477214.valledata.gold_comentarios_sentimiento`;
```

---

## ⚡ Método 3: Conector Comunitario en Google Apps Script (`looker_studio_community_connector.gs`)

También tienes disponible el script [`scripts/looker_studio_community_connector.gs`](file:///home/jjvallej/work/enigma/pyimport/scripts/looker_studio_community_connector.gs) ejecutable directamente desde el entorno de desarrollador de Google Apps Script para desplegar un Conector Nativo de Looker Studio.

### Pasos de Despliegue del Conector:
1. Entra a [script.google.com](https://script.google.com/) y crea un nuevo proyecto de Apps Script.
2. Copia todo el código contenido en [`scripts/looker_studio_community_connector.gs`](file:///home/jjvallej/work/enigma/pyimport/scripts/looker_studio_community_connector.gs).
3. Habilita el servicio de **BigQuery API** en Servicios (`+`).
4. Haz clic en **Desplegar -> Nuevo despliegue -> Conector de Looker Studio**.
5. Copia la URL del conector generado y págala en Looker Studio. ¡Todos los 9 KPIs (incluyendo 7, 8 y 9) se cargarán automáticamente!

---

## 🌐 Método 4: Prototipo Interactivo Ejecutable en el Navegador

Puedes abrir y probar interactivamente la representación completa del tablero con los 9 KPIs en:
- **[dashboard_sentimiento_datastudio_ejemplo.html](file:///home/jjvallej/work/enigma/pyimport/dashboard_sentimiento_datastudio_ejemplo.html)**

Este tablero simula exactamente el lienzo de 1200px de Looker Studio y te permite interactuar con los scorecards del **KPI-07**, **KPI-08** y **KPI-09** en vivo.
