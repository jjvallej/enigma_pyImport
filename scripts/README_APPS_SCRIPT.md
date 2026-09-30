# Guía de Despliegue y Uso: Google Apps Script para Tablas Gold de BigQuery

Esta guía explica cómo desplegar y usar el script de Google Apps Script ([`apps_script_valledata_gold.gs`](file:///home/jjvallej/work/enigma/pyimport/scripts/apps_script_valledata_gold.gs)) para conectar **Google Sheets** directamente con el dataset `datagov-477214.valledata` en BigQuery y consumir las tablas de la capa **Gold**.

---

## 📌 Tablas Gold Sincronizadas

1. `gold_comentarios_sentimiento`: Resumen consolidado por `municipio`, `id_dataset` y `nombre_dataset` (total comentarios, positivos, negativos, neutros, confianza promedio, emoción predominante).
2. `gold_modelo_pronostico_produccion`: Predicciones del modelo ARIMA a 3 años para cultivos.
3. `gold_modelo_rendimiento`: Dataset de predicción de rendimiento y rentabilidad agrícola.
4. `gold_cultivos_valle_geo`: Dataset georreferenciado con coordenadas, polígonos WKT y pisos térmicos.

---

## 🚀 Pasos de Configuración en Google Sheets

### 1. Crear o Abrir la Hoja de Cálculo
- Abre una nueva hoja de cálculo en [Google Sheets](https://sheets.google.com).
- Asigna un nombre, por ejemplo: `ValleData - Dashboards & Tablas Gold`.

### 2. Abrir el Editor de Apps Script
- En la barra de menú superior de Google Sheets, ve a **Extensiones** > **Apps Script**.

### 3. Habilitar el Servicio de BigQuery (Avanzado)
- En el panel izquierdo de Apps Script, busca la sección **Servicios** (junto a los archivos de código) y haz clic en el botón `+`.
- Selecciona **BigQuery API** de la lista de servicios.
- Asegúrate de que el identificador sea `BigQuery` y haz clic en **Añadir**.

### 4. Copiar el Código
- Copia todo el contenido de [`scripts/apps_script_valledata_gold.gs`](file:///home/jjvallej/work/enigma/pyimport/scripts/apps_script_valledata_gold.gs).
- En el editor de Apps Script, reemplaza el contenido del archivo `Code.gs` con el código copiado.
- Guarda el proyecto haciendo clic en el icono de disco o con `Ctrl + S`.

---

## 💻 Uso e Interacción

### Menú Personalizado en Google Sheets
Al recargar tu Google Sheet, aparecerá un nuevo menú en la barra superior llamado **📊 ValleData Gold**:
- **🔄 Sincronizar Todas las Tablas Gold**: Ejecuta la importación masiva y crea automáticamente una pestaña para cada tabla.
- **💬 Sincronizar Comentarios y Sentimiento**: Importa la vista `gold_comentarios_sentimiento`.
- **📈 Sincronizar Pronóstico Producción**: Importa `gold_modelo_pronostico_produccion`.
- **🌾 Sincronizar Rendimiento y Rentabilidad**: Importa `gold_modelo_rendimiento`.
- **🗺️ Sincronizar Cultivos Georreferenciados**: Importa `gold_cultivos_valle_geo`.

---

## ⏰ Sincronización Automática (Activadores / Triggers)

Para actualizar las hojas automáticamente todos los días:
1. En Apps Script, haz clic en el icono de reloj **Activadores** (Triggers) en la barra lateral izquierda.
2. Haz clic en **+ Añadir activador** (esquina inferior derecha).
3. Selecciona la función a ejecutar: `importarTodasLasTablasGold`.
4. Elige el tipo de fuente de eventos: **Según el tiempo (Time-driven)**.
5. Elige el tipo de activador: **Temporizador por días (Day timer)**.
6. Selecciona la hora preferida (ej. *de 6:00 a 7:00 a. m.*) y haz clic en **Guardar**.

---

## 🌐 Endpoint Web App API (JSON)

Si deseas consumir las tablas Gold desde otras aplicaciones web o dashboards:
1. En Apps Script, haz clic en **Desplegar** > **Nuevo despliegue**.
2. Selecciona **Aplicación web**.
3. Configurar:
   - **Ejecutar como**: Tu usuario (con acceso a BigQuery).
   - **Quién tiene acceso**: Cualquier usuario con el enlace / Cualquier usuario dentro de la organización.
4. Al desplegar, obtendrás una URL como:
   `https://script.google.com/macros/s/XXXXX/exec?table=gold_comentarios_sentimiento`
