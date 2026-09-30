import docx
from docx.shared import Pt, Inches, RGBColor
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.oxml import parse_xml
from docx.oxml.ns import nsdecls
from pathlib import Path

def set_cell(cell, text, font_name="Arial Narrow", font_size=Pt(10.5), bold=False, italic=False, bg_hex=None, text_color=None):
    cell.text = ""
    p = cell.paragraphs[0]
    run = p.add_run(text)
    run.font.name = font_name
    if font_size:
        run.font.size = font_size
    run.bold = bold
    run.italic = italic
    if text_color:
        run.font.color.rgb = text_color
    if bg_hex:
        tcPr = cell._element.get_or_add_tcPr()
        shd = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{bg_hex}"/>')
        tcPr.append(shd)

def main():
    base_vf = Path("/home/jjvallej/work/enigma/pyimport/12. VF - DOCUMENTACION_CONEXIONES_AIRFLOW.docx")
    doc = docx.Document(str(base_vf))

    # 1. Update Table 0 (Metadata Table)
    t0 = doc.tables[0]
    set_cell(t0.cell(1, 1), "Constancia de Auditoría y Verificación de Operatividad de Google Cloud Composer")
    set_cell(t0.cell(2, 1), "Final")
    set_cell(t0.cell(5, 1), "31 de Agosto de 2026")

    # 2. Update Table 1 (Control de Versiones)
    t1 = doc.tables[1]
    set_cell(t1.cell(1, 2), "Creación inicial del documento de constancia de habilitación de servicio Cloud Composer.")
    set_cell(t1.cell(2, 0), "Final")
    set_cell(t1.cell(2, 1), "31 de Agosto de 2026")
    set_cell(t1.cell(2, 2), "Certificación de operatividad final y verificación de conexiones en el esquema de producción de Cloud Composer GCP.")
    set_cell(t1.cell(2, 3), "Jhon Jairo Vallejo")

    # 3. Title P19 & P24
    if len(doc.paragraphs) > 19:
        p19 = doc.paragraphs[19]
        p19.text = ""
        r = p19.add_run("CONSTANCIA DE AUDITORÍA Y VERIFICACIÓN DE OPERATIVIDAD DE GOOGLE CLOUD COMPOSER EN PRODUCCIÓN")
        r.font.name = "Arial"
        r.font.size = Pt(16)
        r.bold = True

    # 4. Table 2 - Audit Metadata Table
    t2 = doc.tables[2]

    # Adjust t2 to 11 rows and 3 columns
    metadata_data = [
        ["Parámetro Auditado", "Valor / Estado Verificado", "Explicación y Descripción para Auditoría"],
        ["Nombre del Servicio (Service Name)", "composer.googleapis.com", "Identificador único del servicio Cloud Composer en Google Cloud"],
        ["Nombre Visible (Service Display Name)", "Cloud Composer API", "Nombre comercial del servicio orquestador de flujos Apache Airflow"],
        ["Proyecto GCP (Project ID)", "datagov-477214", "Proyecto oficial e institucional en la nube GCP Producción"],
        ["Entorno Composer (Environment)", "composer-gdv", "Entorno de orquestación de producción activo"],
        ["Región GCP (Location)", "us-east1", "Ubicación geográfica de la infraestructura productiva"],
        ["Estado del Servicio (Status)", "Enabled (Habilitado / Activo en Producción)", "Confirma que el servicio está activo y operando formalmente"],
        ["Tipo de API (Type)", "Public API (Google Enterprise API)", "Servicio empresarial gestionado de alta disponibilidad por Google"],
        ["Métricas de Tráfico (Traffic / Activity)", "Respuestas HTTP 200 OK operativas", "Peticiones procesadas con éxito durante la operación continua en producción"],
        ["Métodos de API Auditados", "71 métodos activos (v1 y v1beta1)", "Operaciones de orquestación, gestión de DAGs, ejecuciones y conexiones"],
        ["Integraciones Validadas", "BigQuery (valledata), GCS (datalake_gdv_pdn)", "Carga de capas Bronze/Silver y sincronización de evidencias de ingesta"]
    ]

    # Clear excess columns in existing rows or rebuild table cells
    # We will reset rows to match len(metadata_data)
    while len(t2.rows) < len(metadata_data):
        t2.add_row()

    # If t2 has 5 columns, we can merge or use first 3 columns, or recreate t2 rows cleanly
    for r_i, row_values in enumerate(metadata_data):
        row = t2.rows[r_i]
        # Make sure row has at least 3 cells
        for c_i, val in enumerate(row_values):
            if c_i < len(row.cells):
                bg = "003366" if r_i == 0 else ("F8F9FA" if r_i % 2 == 1 else "FFFFFF")
                tc = RGBColor(255, 255, 255) if r_i == 0 else RGBColor(0, 0, 0)
                set_cell(row.cells[c_i], val, font_name="Arial Narrow", font_size=Pt(10), bold=(r_i == 0), bg_hex=bg, text_color=tc)

    # 5. Clear old body paragraphs starting at P70 and write audit certificate sections
    p_start = 70
    total_p = len(doc.paragraphs)
    
    # Empty existing text from P70 onwards
    for i in range(p_start, total_p):
        doc.paragraphs[i].text = ""

    current_idx = p_start

    def add_p(text, style="normal", font_name="Arial Narrow", font_size=Pt(11), bold=False, italic=False, align=WD_ALIGN_PARAGRAPH.JUSTIFY):
        nonlocal current_idx
        if current_idx < len(doc.paragraphs):
            p = doc.paragraphs[current_idx]
            p.style = doc.styles[style] if style in doc.styles else doc.styles["normal"]
        else:
            p = doc.add_paragraph(style=style)
        current_idx += 1
        p.alignment = align
        run = p.add_run(text)
        run.font.name = font_name
        if font_size:
            run.font.size = font_size
        run.bold = bold
        run.italic = italic
        return p

    # Section 1
    add_p("1. Definiciones y Explicación para Personal No Técnico", style="Heading 1", font_name="Arial", font_size=Pt(14), bold=True)
    add_p("¿Qué es Google Cloud Composer / Apache Airflow en Producción? (Explicación Didáctica)", font_name="Arial Narrow", font_size=Pt(12), bold=True)
    add_p("Imagine que la plataforma ValleDATA es una fábrica digital que recopila, procesa y publica datos de diferentes entidades (como el DANE, la Gobernación del Valle del Cauca, el Portal Nacional de Datos Abiertos y agencias internacionales) de manera autónoma y periódica.")
    add_p("Para que ningún funcionario tenga que ingresar manualmente a páginas web a descargar archivos Excel o CSV ni ejecutar tareas manuales de bases de datos, se utiliza un orquestador digital en producción llamado Apache Airflow, alojado en la nube de Google mediante el servicio gestionado Google Cloud Composer.")
    add_p("•  El Director de Orquesta Productivo: Google Cloud Composer actúa como el director de una orquesta digital en tiempo real. Su trabajo es supervisar que los pipelines de recolección, limpieza y carga a BigQuery se ejecuten a la hora exacta, controlar errores, activar alertas y garantizar que la información esté disponible para tableros e indicadores sin interrupciones.")
    add_p("•  Beneficio Institucional en Producción: Garantiza que la información agrícola, de precios y climática del Valle del Cauca esté permanentemente actualizada, lista para la toma de decisiones gubernamentales y resguardada con altos estándares de disponibilidad y auditoría cloud.")

    # Section 2
    add_p("2. Declaración de Constancia y Verificación de Auditoría en Producción", style="Heading 1", font_name="Arial", font_size=Pt(14), bold=True)
    add_p("Por medio del presente documento se certifica y hace constar que el servicio gestionado de orquestación Google Cloud Composer (basado en Apache Airflow 2.x) se encuentra correctamente instalado, configurado, desplegado y en estado plenamente operativo en el esquema de PRODUCCIÓN dentro de la consola oficial de Google Cloud Platform para el proyecto gubernamental e institucional datagov-477214 (Entorno: composer-gdv, Región: us-east1).")

    # Section 3
    add_p("3. Evidencia Fotográfica y Métrica de Auditoría", style="Heading 1", font_name="Arial", font_size=Pt(14), bold=True)
    add_p("A continuación se adjunta la captura oficial extraída de la Consola GCP (API & Services Details / console.cloud.google.com), la cual constituye evidencia directa de auditoría de operatividad:")
    
    # Add Image
    img_path = Path("/home/jjvallej/work/enigma/pyimport/gcp_cloud_composer_audit.png")
    if img_path.exists():
        if current_idx < len(doc.paragraphs):
            p_img = doc.paragraphs[current_idx]
        else:
            p_img = doc.add_paragraph()
        current_idx += 1
        p_img.alignment = WD_ALIGN_PARAGRAPH.CENTER
        run_img = p_img.add_run()
        run_img.add_picture(str(img_path), width=Inches(5.6))
        
        cap = add_p("Figura: Evidencia de Auditoría - Estado y Métricas de Cloud Composer API en GCP Producción", align=WD_ALIGN_PARAGRAPH.CENTER)
        cap.runs[0].italic = True
        cap.runs[0].font.size = Pt(9.5)
        cap.runs[0].font.color.rgb = RGBColor(100, 100, 100)

    # Section 4
    add_p("4. Registro de Metadatos de la Evidencia Auditada (Esquema Producción)", style="Heading 1", font_name="Arial", font_size=Pt(14), bold=True)

    # Section 5
    add_p("5. Análisis Técnico de la Operatividad Auditada en Producción", style="Heading 1", font_name="Arial", font_size=Pt(14), bold=True)
    add_p("De acuerdo con las métricas y registros de telemetría de Google Cloud Platform visibles en la evidencia auditada y las verificaciones ejecutadas en el entorno productivo:")
    add_p("1. Habilitación y Despliegue en Producción: La API composer.googleapis.com figura explícitamente en estado Status: Enabled, y el entorno composer-gdv se encuentra ejecutando los DAGs del pipeline (src_ingest_* y src_load_*) de forma autónoma.")
    add_p("2. Registro de Actividad y Ejecución: El gráfico de tráfico (Traffic by response code) muestra invocaciones periódicas exitosas (código de respuesta HTTP 200 OK), confirmando que la programación de tareas (scheduling) y la resolución de conexiones dinámicas operan sin interrupciones.")
    add_p("3. Monitoreo y Resiliencia conforme a SLA: Las métricas de disponibilidad y los registros en Cloud Logging certifican una tasa de éxito operativa alineada con las especificaciones del SLA de Google Enterprise API y el plan de alertas ante contingencias.")

    # Section 6
    add_p("6. Firma y Validez del Documento", style="Heading 1", font_name="Arial", font_size=Pt(14), bold=True)
    add_p("Este documento sirve como evidencia técnica formal y final ante auditorías internas, externas o de cumplimiento de infraestructura cloud para certificar la existencia y correcto funcionamiento de Google Cloud Composer en el esquema de producción del proyecto datagov-477214.")
    add_p("FIN DEL DOCUMENTO.", align=WD_ALIGN_PARAGRAPH.CENTER)

    # Output paths
    targets = [
        Path("/home/jjvallej/work/enigma/pyimport/10. VF - CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.docx"),
        Path("/home/jjvallej/work/enigma/pyimport/1.3.1.1 CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.docx"),
        Path("/home/jjvallej/work/enigma/pyimport/CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.docx"),
        Path("/home/jjvallej/work/enigma/airflow/10. VF - CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.docx"),
        Path("/home/jjvallej/work/enigma/airflow/1.3.1.1 CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.docx"),
        Path("/home/jjvallej/work/enigma/airflow/CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.docx"),
    ]

    for target in targets:
        target.parent.mkdir(parents=True, exist_ok=True)
        doc.save(str(target))
        print(f"✅ Certificado generado exitosamente con formato VF en: {target}")

if __name__ == "__main__":
    main()
