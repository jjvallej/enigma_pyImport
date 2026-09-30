# DOCUMENTO DE ENTREGA A WADUA
## Propuesta integral de reportes de sentimiento analítico

Especificación funcional y técnica para implementar los tableros de percepción ciudadana en Google Looker Studio (antes Data Studio), consumiendo únicamente la tabla `valledata.gold.gold_comentarios_sentimiento`.

---

| Campo | Valor |
| :--- | :--- |
| **Proyecto** | ValleDATA — Gestión y aprovechamiento de datos abiertos del Valle del Cauca |
| **Caso de uso** | Análisis de sentimientos sobre comentarios de portales CKAN municipales |
| **Proveedor de reportes** | Wadua |
| **Plataforma de reportes** | Google Looker Studio (Data Studio) |
| **Fuente de datos** | Google BigQuery — `valledata.gold.gold_comentarios_sentimiento` (región us-east1) |
| **Orquestación de datos** | Cloud Composer / Airflow — ingesta diaria (fuera del alcance de Wadua) |
| **Audiencia de este documento** | Wadua — implementación de reportes y analítica visual |
| **Usuarios finales** | Administradores ValleDATA, analistas departamentales y enlaces municipales |
| **Idiomas de la interfaz** | Español (Colombia) |
| **Zona horaria** | America/Bogota (UTC−5) |
| **Fecha de este documento** | 16 de Septiembre de 2026 |
| **Versión** | 2.4 — especificación de implementación ajustada (proveedor: Wadua) |
| **Elaborado por** | Área de Datos — Proyecto ValleDATA |

---

## 1. Propósito y alcance

### 1.1 Propósito
Este documento es el insumo contractual y técnico para que Wadua construya, configure y entregue los reportes de sentimiento en Looker Studio. Reemplaza la propuesta conceptual previa e incorpora todos los parámetros que Wadua necesita para implementar sin ambigüedad: fuentes, grano de dato, diccionario, KPIs, umbrales, filtros, páginas, campos calculados, paleta, seguridad y criterios de aceptación.

### 1.2 Dentro del alcance de Wadua
- Conectar Looker Studio a las tablas/vistas de BigQuery indicadas (solo lectura).
- Crear las fuentes de datos, campos calculados, parámetros, filtros y controles.
- Diseñar e implementar las páginas de reporte descritas en la sección 8, con la paleta y umbrales de la sección 7.
- Configurar formato condicional, líneas de referencia y semáforos.
- Publicar el informe, configurar el refresco y transferir la propiedad a ValleDATA.
- Entregar guía de uso (2–4 páginas) y sesión de empalme.

### 1.3 Fuera del alcance de Wadua
- Ingesta desde CKAN, DAGs de Airflow, `pysentimiento` y reverse-ETL (el proceso de ingesta es diario y lo opera ValleDATA).
- Crear o modificar tablas Bronze/Silver/Gold. No se creará ninguna vista diaria adicional.
- Conectar Looker Studio a Silver ni a Bronze. La única fuente de los reportes es Gold.
- Entrenar o recalibrar el modelo de IA.
- Desarrollar un sistema de alertas transaccional (correo/SMS). El tablero solo visualiza el estado; las alertas operativas las consume ValleDATA desde BigQuery.

---

## 2. Contexto funcional

Cada municipio del Valle del Cauca opera un portal CKAN donde los ciudadanos comentan datasets (movilidad, seguridad, servicios, educación, cultura, entre otros). Un pipeline único lee esas fuentes, unifica el texto en BigQuery y clasifica cada comentario con `pysentimiento` (sentimiento POS/NEG/NEU). Los reportes deben permitir al administrador de ValleDATA: (1) ver el balance de percepción, (2) ubicar territorios y temáticas críticas, (3) auditar la confianza del modelo antes de actuar, y (4) explorar el consolidado municipio × dataset bajo filtros.

El universo esperado es de hasta 14 municipios (nodos CKAN hijos) más la consolidación departamental. Agregar un municipio nuevo no debe exigir rediseñar el tablero: aparece solo al entrar en `gold_comentarios_sentimiento`.

---

## 3. Arquitectura de consumo para Looker Studio

Looker Studio no consulta Bronze, Silver ni Cloud Storage. La única fuente de los reportes es la tabla Gold `gold_comentarios_sentimiento`. El pipeline de ingesta corre a diario y materializa esta tabla; el tablero lee el consolidado ya calculado. No hay eje de fecha en Gold: las series del informe son categóricas (municipio, dataset), no temporales.

| Capa | Objeto BigQuery | Grano | Uso en Looker Studio |
| :--- | :--- | :--- | :--- |
| **Gold** | `valledata.gold.gold_comentarios_sentimiento` | 1 fila = municipio + dataset | Única fuente (DS_GOLD): KPIs, series por municipio/dataset, polaridad, semáforo, confianza IA, emociones y explorador agregado |

### 3.1 Conector y modo de conexión

| Parámetro Looker Studio | Valor requerido |
| :--- | :--- |
| **Conector** | BigQuery nativo (Google) |
| **Tipo de conexión** | Live / en vivo (no Extraer datos) |
| **Tabla** | `valledata.gold.gold_comentarios_sentimiento` |
| **Autenticación** | Cuenta de servicio o usuario institucional con solo lectura |
| **SQL personalizado** | Solo para despivotar polaridad (DS_POLARIDAD). Ver 3.2. No apuntar a Silver |
| **Caché de datos** | Activada; invalidación automática tras la corrida diaria de Gold |
| **Frescura objetivo** | T+1 día calendario: la ingesta es diaria; Gold refleja el corte del día anterior |
| **Moneda / locale** | `es-CO`; separador de miles punto; decimal coma en etiquetas de UI |

> [!IMPORTANT]
> **Regla de costo**: Wadua no debe crear una segunda fuente sobre Silver ni Bronze. Toda consulta de Looker Studio sale de `gold_comentarios_sentimiento` (o de un SQL que solo lea esa tabla).

### 3.2 Series y polaridad sobre Gold (sin eje temporal)

La tabla no tiene campo `fecha`. Queda prohibido construir series de tiempo (línea por día) y no se creará `gold.comentarios_diario`. Las “series” del tablero son gráficos de barras o columnas por `municipio` o `id_dataset`, usando las métricas ya materializadas (`total_comentarios`, `positivos`, `negativos`, `neutros`, `confianza_promedio`).

La dona de polaridad necesita una dimensión de categoría (Positivo / Neutro / Negativo). Como Gold trae esas cantidades en columnas, Wadua crea una fuente `DS_POLARIDAD` con SQL personalizado que solo lee esta tabla (unpivot):

```sql
SELECT municipio, id_municipio, id_dataset, 'POS' AS sentimiento, positivos AS n, total_comentarios, confianza_promedio, emocion_predominante
FROM `valledata.gold.gold_comentarios_sentimiento`
UNION ALL
SELECT municipio, id_municipio, id_dataset, 'NEG', negativos, total_comentarios, confianza_promedio, emocion_predominante
FROM `valledata.gold.gold_comentarios_sentimiento`
UNION ALL
SELECT municipio, id_municipio, id_dataset, 'NEU', neutros, total_comentarios, confianza_promedio, emocion_predominante
FROM `valledata.gold.gold_comentarios_sentimiento`;
```

---

## 4. Diccionario de datos

### 4.1 Tabla Gold — `valledata.gold.gold_comentarios_sentimiento`

Única tabla de consumo consolidada. Grano: una fila por municipio × dataset. Esquema oficial de 9 columnas (`field name`, `mode`, `type`, `description`):

| Field Name | Mode | Type | Description / Uso en el reporte |
| :--- | :--- | :--- | :--- |
| `municipio` | NULLABLE | STRING | Nombre del municipio. Dimensión y filtro |
| `id_municipio` | NULLABLE | INTEGER | Código DIVIPOLA. Cruce y orden estable |
| `id_dataset` | NULLABLE | STRING | Identificador del dataset (ej. `ds_movilidad`). Dimensión y filtro |
| `total_comentarios` | NULLABLE | INTEGER | Volumen / denominador de NSS y criticidad |
| `positivos` | NULLABLE | INTEGER | Conteo POS. Numerador de NSS y dona |
| `negativos` | NULLABLE | INTEGER | Conteo NEG. Numerador de criticidad y NSS |
| `neutros` | NULLABLE | INTEGER | Conteo NEU. Dona de polaridad |
| `confianza_promedio` | NULLABLE | FLOAT | Promedio de certeza del modelo, rango [0, 1] |
| `emocion_predominante` | NULLABLE | STRING | Código de sentimiento o emoción predominante del grupo (`POS`, `NEG`, `NEU`) |

### 4.2 Catálogo de dimensiones de negocio

#### Homologación de etiquetas de sentimiento y emoción de la tabla Gold:

| Campo crudo | Valor crudo | Etiqueta UI | Color Hex |
| :--- | :--- | :--- | :--- |
| `sentimiento` / `emocion_predominante` | POS / positivo | Positivo | `#34A853` |
| `sentimiento` / `emocion_predominante` | NEG / negativo | Negativo | `#EA4335` |
| `sentimiento` / `emocion_predominante` | NEU / neutro | Neutro | `#9AA0A6` |

---

## 5. Catálogo de KPIs y Fórmulas Ajustadas (KPIs 1 a 9)

Todas las métricas operan exclusivamente sobre la tabla consolidada `gold_comentarios_sentimiento`.

### 5.1 Matriz General de KPIs

| ID | KPI | Fórmula BigQuery SQL / Lógica | Expresión Looker Studio | Unidad | Agregación |
| :---: | :--- | :--- | :--- | :---: | :--- |
| **KPI-01** | **Volumen de comentarios** | `SUM(total_comentarios)` | `SUM(total_comentarios)` | Entero | Suma total |
| **KPI-02** | **Net Sentiment Score (NSS)** | `((SUM(positivos) - SUM(negativos)) / SUM(total_comentarios)) * 100` | `((SUM(positivos) - SUM(negativos)) / NULLIF(SUM(total_comentarios), 0)) * 100` | % (1 dec) | Recalcular sobre el filtro |
| **KPI-03** | **Tasa de criticidad** | `(SUM(negativos) / SUM(total_comentarios)) * 100` | `(SUM(negativos) / NULLIF(SUM(total_comentarios), 0)) * 100` | % (1 dec) | Recalcular sobre el filtro |
| **KPI-04** | **Índice de confianza IA** | `SUM(confianza_promedio * total_comentarios) / SUM(total_comentarios)` | `SUM(confianza_promedio * total_comentarios) / NULLIF(SUM(total_comentarios), 0)` | 0 a 1 (2 dec) | Promedio ponderado por volumen |
| **KPI-05** | **Participación positiva** | `(SUM(positivos) / SUM(total_comentarios)) * 100` | `(SUM(positivos) / NULLIF(SUM(total_comentarios), 0)) * 100` | % (1 dec) | Recalcular |
| **KPI-06** | **Participación neutra** | `(SUM(neutros) / SUM(total_comentarios)) * 100` | `(SUM(neutros) / NULLIF(SUM(total_comentarios), 0)) * 100` | % (1 dec) | Recalcular |
| **KPI-07** | **Municipios en estado CRÍTICO** | `COUNT(DISTINCT CASE WHEN (SUM(positivos) - SUM(negativos)) < 0 THEN municipio END)` | `COUNT_DISTINCT(CASE WHEN (positivos - negativos) < 0 THEN municipio END)` | Entero | Conteo de municipios con NSS negativo |
| **KPI-08** | **Datasets bajo umbral de revisión** | `COUNT(DISTINCT CASE WHEN confianza_promedio < 0.70 OR SAFE_DIVIDE(SUM(negativos), SUM(total_comentarios)) > 0.25 THEN id_dataset END)` | `COUNT_DISTINCT(CASE WHEN confianza_promedio < 0.70 OR (negativos / NULLIF(total_comentarios, 0)) > 0.25 THEN id_dataset END)` | Entero | Conteo de datasets bajo umbral (< 0.70) o alta insatisfacción |
| **KPI-09** | **Emoción predominante** | `CASE WHEN SUM(positivos) >= SUM(negativos) AND SUM(positivos) >= SUM(neutros) THEN 'Positivo' WHEN SUM(negativos) >= SUM(positivos) AND SUM(negativos) >= SUM(neutros) THEN 'Negativo' ELSE 'Neutro' END` | `CASE WHEN SUM(positivos) >= SUM(negativos) AND SUM(positivos) >= SUM(neutros) THEN "Positivo" WHEN SUM(negativos) >= SUM(positivos) AND SUM(negativos) >= SUM(neutros) THEN "Negativo" ELSE "Neutro" END` | Categoría | Evaluación dinámica sobre el consolidado Gold |

### 5.2 Fórmulas Looker Studio (Campos Calculados Oficiales)

| Nombre del Campo | Tipo | Expresión Looker Studio |
| :--- | :---: | :--- |
| **NSS** | Número | `((SUM(positivos) - SUM(negativos)) / NULLIF(SUM(total_comentarios), 0)) * 100` |
| **Tasa criticidad** | Número | `(SUM(negativos) / NULLIF(SUM(total_comentarios), 0)) * 100` |
| **Confianza IA ponderada** | Número | `SUM(confianza_promedio * total_comentarios) / NULLIF(SUM(total_comentarios), 0)` |
| **KPI-07: Municipios en estado CRÍTICO** | Número | `COUNT_DISTINCT(CASE WHEN (positivos - negativos) < 0 THEN municipio END)` |
| **KPI-08: Datasets bajo umbral de revisión** | Número | `COUNT_DISTINCT(CASE WHEN confianza_promedio < 0.70 OR (negativos / NULLIF(total_comentarios, 0)) > 0.25 THEN id_dataset END)` |
| **KPI-09: Etiqueta emoción / Sentimiento Predominante** | Texto | `CASE WHEN SUM(positivos) >= SUM(negativos) AND SUM(positivos) >= SUM(neutros) THEN "Positivo" WHEN SUM(negativos) >= SUM(positivos) AND SUM(negativos) >= SUM(neutros) THEN "Negativo" ELSE "Neutro" END` |
| **Etiqueta sentimiento** | Texto | `CASE WHEN emocion_predominante IN ("POS", "positivo") THEN "Positivo" WHEN emocion_predominante IN ("NEG", "negativo") THEN "Negativo" WHEN emocion_predominante IN ("NEU", "neutro") THEN "Neutro" ELSE "Sin dato" END` |
| **Estado semáforo** | Texto | `CASE WHEN Tasa criticidad > 50 THEN "CRÍTICO" WHEN Tasa criticidad >= 30 THEN "ALERTA" ELSE "NORMAL" END` |
| **Estado confianza IA** | Texto | `CASE WHEN Confianza IA ponderada >= 0.80 THEN "ÓPTIMO" WHEN Confianza IA ponderada >= 0.70 THEN "ACEPTABLE" ELSE "REQUIERE REVISIÓN" END` |

---

## 6. Parámetros y controles de Looker Studio

Los parámetros permiten que el administrador ajuste umbrales sin reeditar el informe. Los controles de filtro son de página y deben persistir entre páginas mediante un grupo de filtros a nivel de informe.

### 6.1 Parámetros configurables (Looker Studio Parameters)

| ID Parámetro | Nombre UI | Tipo | Valor Por Defecto | Rango / Lista | Afecta |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `P_UMBRAL_CONF` | **Umbral de revisión IA** | Número | `0.70` | 0.50 – 0.95, paso 0.05 | Semáforo IA, línea de referencia, **KPI-08** |
| `P_CONF_OPTIMO` | **Umbral óptimo IA** | Número | `0.80` | 0.70 – 0.99, paso 0.05 | Color verde de confianza |
| `P_CRIT_ALERTA` | **Umbral alerta criticidad** | Número | `30` | 10 – 50 | Semáforo (amarillo) |
| `P_CRIT_CRITICO` | **Umbral crítico criticidad** | Número | `50` | 30 – 90 | Semáforo (rojo), **KPI-07** |
| `P_TOP_N` | **Top N municipios** | Número | `10` | 5 / 10 / 15 | Ranking horizontal |
| `P_MIN_VOLUMEN` | **Volumen mínimo** | Número | `20` | 1 – 500 | Oculta municipios/datasets con muestra insuficiente |

### 6.2 Controles de filtro (Report-level)

| Control | Campo | Tipo de Control | Valor Por Defecto | Comportamiento |
| :--- | :--- | :--- | :---: | :--- |
| **Municipio** | `municipio` | Drop-down (multi) | Todos | Lista ordenada A–Z; tooltip con `id_municipio` |
| **Dataset / Temática** | `id_dataset` | Drop-down (multi) o búsqueda | Todos | No hardcodear la lista |
| **Emoción / Sentimiento** | `emocion_predominante` | Drop-down (multi) | Todos | Página de emociones y explorador (**KPI-09**) |
| **Solo baja confianza** | `confianza_promedio` | Checkbox + filtro `< P_UMBRAL_CONF` | Off | Auditoría IA |

### 6.3 Comparaciones y drill-down
- No hay comparación contra periodo anterior: Gold no expone fecha.
- Clic en una barra de municipio filtra el resto del informe (*cross-filtering ON*).
- Clic en un dataset filtra la dona de polaridad y el explorador agregado.
- El explorador lista filas Gold (municipio × dataset), no comentarios individuales ni PII.

---

## 7. Umbrales, semáforos y paleta

### 7.1 Semáforo de criticidad (Administrador)
Diagnóstico instantáneo enlazado a los umbrales. Se calcula sobre la Tasa de criticidad (KPI-03) del universo filtrado.
- **NORMAL**: Criticidad $< 30\%$ (Verde)
- **ALERTA**: $30\% \le \text{Criticidad} \le 50\%$ (Amarillo)
- **CRÍTICO**: Criticidad $> 50\%$ (Rojo, **KPI-07**)

### 7.2 Semáforo de confianza del modelo IA
- **ÓPTIMO**: Confianza $\ge 0.80$ (Verde)
- **ACEPTABLE**: $0.70 \le \text{Confianza} < 0.80$ (Ámbar)
- **REQUIERE REVISIÓN**: Confianza $< 0.70$ (Rojo, **KPI-08**)

---

## 8. Especificación de informes y páginas

Se entrega un único informe Looker Studio con navegación por páginas (pestañas), lienzo estándar 1200 × 900 px y encabezado fijo de 72 px con título, logo ValleDATA y controles de filtro.

- **8.1 Página 01 — Tablero ejecutivo**: Diagnóstico global en 5 segundos. Scorecards KPI-01 a KPI-08, semáforo de criticidad y polaridad.
- **8.2 Página 02 — Ranking territorial de criticidad**: Identificación de territorios con mayor descontento (**KPI-07**).
- **8.3 Página 03 — Polaridad por dataset y municipio**: Desglose de distribución POS/NEU/NEG.
- **8.4 Página 04 — Auditoría de confianza IA**: Identificación de fuentes y datasets con baja confianza probabilística (**KPI-08**).
- **8.5 Página 05 — Emociones**: Distribución de `emocion_predominante` (`POS`, `NEG`, `NEU`) por territorio y dataset (**KPI-09**).
- **8.6 Página 06 — Explorador Gold (municipio × dataset)**: Inspección tabular del consolidado.

---

## 9. Criterios de aceptación y entregables

### Criterios de Aceptación (CA-01 a CA-13)
- **CA-01**: Las 6 páginas existen, con encabezado y filtros de informe.
- **CA-02**: Scorecards calculan las fórmulas sobre el filtro activo.
- **CA-05**: Barras de auditoría IA tienen línea de referencia en `0.70` (`P_UMBRAL_CONF`).
- **CA-11**: Ninguna fuente Looker apunta a Silver o Bronze; solo `gold_comentarios_sentimiento`.

### Entregables de Wadua (E-01 a E-06)
- **E-01**: Informe Looker Studio en el dominio institucional con propiedad transferida.
- **E-02**: Fuentes `DS_GOLD` y `DS_POLARIDAD` documentadas.
- **E-03**: Listado de campos calculados y parámetros.
- **E-04**: Guía de uso para administrador (PDF 2–4 páginas).
- **E-05**: Guía técnica de mantenimiento.
