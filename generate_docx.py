"""Script para convertir archivos Markdown (.md) a documentos .docx y .doc usando la plantilla con membrete oficial de la Gobernación.

Reglas de Estilo Solicitadas:
- Fuente general: Arial
- Títulos principales (H1 / #): Negro, Arial 16pt, Negrilla
- Subtítulos (H2 / ##, H3 / ###, H4 / ####): Negro, Arial 14pt, Negrilla
- Texto normal / Párrafos / Viñetas / Tablas: Negro, Arial 12pt, Texto Justificado
"""

from __future__ import annotations

import base64
import io
from pathlib import Path
import re
import urllib.request
import docx
from docx.enum.table import WD_TABLE_ALIGNMENT
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml import parse_xml
from docx.oxml.ns import nsdecls
from docx.shared import Inches, Pt, RGBColor


TEMPLATE_PATH = Path("/home/jjvallej/tmp/Documento word con membrete .docx")


def fetch_mermaid_image(mermaid_code: str) -> io.BytesIO | None:
    """Convierte código Mermaid a una imagen PNG usando la API de mermaid.ink."""
    try:
        clean_code = mermaid_code.strip()
        graphbytes = clean_code.encode("utf-8")
        base64_bytes = base64.urlsafe_b64encode(graphbytes)
        base64_string = base64_bytes.decode("ascii")
        url = f"https://mermaid.ink/img/{base64_string}"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = resp.read()
            return io.BytesIO(data)
    except Exception as err:
        print(f"⚠️ No se pudo renderizar diagrama Mermaid vía mermaid.ink: {err}")
        return None


def set_cell_background(cell: docx.table._Cell, fill_hex: str) -> None:
    tcPr = cell._element.get_or_add_tcPr()
    shd = parse_xml(f'<w:shd {nsdecls("w")} w:fill="{fill_hex}"/>')
    tcPr.append(shd)


def set_cell_margins(cell: docx.table._Cell, top=120, bottom=120, left=180, right=180) -> None:
    tcPr = cell._element.get_or_add_tcPr()
    tcMar = parse_xml(
        f'<w:tcMar {nsdecls("w")}>'
        f'<w:top w:w="{top}" w:type="dxa"/>'
        f'<w:bottom w:w="{bottom}" w:type="dxa"/>'
        f'<w:left w:w="{left}" w:type="dxa"/>'
        f'<w:right w:w="{right}" w:type="dxa"/>'
        f'</w:tcMar>'
    )
    tcPr.append(tcMar)


def add_formatted_text(paragraph, text: str, font_size=12, default_color=RGBColor(0, 0, 0)) -> None:
    """Parsea markdown simple (**negrita**, *cursiva*, `código`) y lo añade con tipografía Arial 12pt."""
    token_pattern = re.compile(r'(\*\*.*?\*\*|\*.*?\*|`.*?`)')
    parts = token_pattern.split(text)
    
    for part in parts:
        if not part:
            continue
        if part.startswith('**') and part.endswith('**'):
            run = paragraph.add_run(part[2:-2])
            run.font.name = 'Arial'
            run.font.size = Pt(font_size)
            run.font.color.rgb = default_color
            run.bold = True
        elif part.startswith('*') and part.endswith('*'):
            run = paragraph.add_run(part[1:-1])
            run.font.name = 'Arial'
            run.font.size = Pt(font_size)
            run.font.color.rgb = default_color
            run.italic = True
        elif part.startswith('`') and part.endswith('`'):
            run = paragraph.add_run(part[1:-1])
            run.font.name = 'Consolas'
            run.font.size = Pt(10)
            run.font.color.rgb = RGBColor(199, 37, 78)
        else:
            run = paragraph.add_run(part)
            run.font.name = 'Arial'
            run.font.size = Pt(font_size)
            run.font.color.rgb = default_color


def md_to_docx(md_path: Path, output_path: Path, use_template: bool = True) -> None:
    if use_template and TEMPLATE_PATH.exists():
        doc = docx.Document(str(TEMPLATE_PATH))
        for p in list(doc.paragraphs):
            p._element.getparent().remove(p._element)
    else:
        doc = docx.Document()
    
    # Configuración global del estilo Normal (Arial 12pt, Negro)
    try:
        normal_style = doc.styles['Normal']
        normal_style.font.name = 'Arial'
        normal_style.font.size = Pt(12)
        normal_style.font.color.rgb = RGBColor(0, 0, 0)
    except Exception:
        pass
    
    lines = md_path.read_text(encoding='utf-8').splitlines()
    
    in_code_block = False
    code_block_lines = []
    code_block_lang = ""
    
    in_table = False
    table_lines = []

    def flush_table():
        nonlocal in_table, table_lines
        if not table_lines:
            return
        
        rows_data = []
        for tl in table_lines:
            cells = [c.strip() for c in tl.strip('|').split('|')]
            rows_data.append(cells)
            
        filtered_rows = []
        for row in rows_data:
            if all(re.match(r'^\:?\-+\:?$', c) for c in row if c):
                continue
            filtered_rows.append(row)
            
        if filtered_rows:
            num_rows = len(filtered_rows)
            num_cols = max(len(r) for r in filtered_rows)
            
            table = doc.add_table(rows=num_rows, cols=num_cols)
            table.alignment = WD_TABLE_ALIGNMENT.CENTER
            table.autofit = False

            for r_idx, row in enumerate(filtered_rows):
                for c_idx, val in enumerate(row):
                    if c_idx < num_cols:
                        cell = table.cell(r_idx, c_idx)
                        cell.text = ""
                        p = cell.paragraphs[0]
                        p.alignment = WD_ALIGN_PARAGRAPH.LEFT
                        p.paragraph_format.space_before = Pt(3)
                        p.paragraph_format.space_after = Pt(3)
                        
                        if r_idx == 0:
                            set_cell_background(cell, "003366")
                            add_formatted_text(p, val, font_size=12, default_color=RGBColor(255, 255, 255))
                            for run in p.runs:
                                run.bold = True
                        else:
                            if r_idx % 2 == 1:
                                set_cell_background(cell, "F8F9FA")
                            else:
                                set_cell_background(cell, "FFFFFF")
                            add_formatted_text(p, val, font_size=12, default_color=RGBColor(0, 0, 0))
                        
                        set_cell_margins(cell, top=100, bottom=100, left=150, right=150)
                                
            doc.add_paragraph()
            
        table_lines = []
        in_table = False

    def flush_code_block():
        nonlocal in_code_block, code_block_lines, code_block_lang
        if not code_block_lines:
            in_code_block = False
            return
        
        code_text = "\n".join(code_block_lines)
        
        if code_block_lang.strip().lower() == "mermaid":
            img_stream = fetch_mermaid_image(code_text)
            if img_stream:
                p = doc.add_paragraph()
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                p.paragraph_format.space_before = Pt(10)
                p.paragraph_format.space_after = Pt(4)
                
                run = p.add_run()
                run.add_picture(img_stream, width=Inches(5.6))
                
                cap = doc.add_paragraph()
                cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
                cap.paragraph_format.space_after = Pt(12)
                cap_run = cap.add_run("Diagrama de Flujo / Estructura del Sistema")
                cap_run.font.name = 'Arial'
                cap_run.font.size = Pt(10)
                cap_run.font.italic = True
                cap_run.font.color.rgb = RGBColor(100, 100, 100)
                
                code_block_lines = []
                in_code_block = False
                return

        tbl = doc.add_table(rows=1, cols=1)
        tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
        cell = tbl.cell(0, 0)
        set_cell_background(cell, "F5F5F5")
        set_cell_margins(cell, top=140, bottom=140, left=200, right=200)
        
        p = cell.paragraphs[0]
        p.alignment = WD_ALIGN_PARAGRAPH.LEFT
        p.paragraph_format.space_before = Pt(2)
        p.paragraph_format.space_after = Pt(2)
        run = p.add_run(code_text)
        run.font.name = 'Consolas'
        run.font.size = Pt(9.5)
        run.font.color.rgb = RGBColor(30, 30, 30)
        
        doc.add_paragraph()
        code_block_lines = []
        in_code_block = False

    i = 0
    while i < len(lines):
        line = lines[i]
        
        # Bloques de código
        if line.strip().startswith("```"):
            if in_code_block:
                flush_code_block()
            else:
                if in_table:
                    flush_table()
                in_code_block = True
                code_block_lang = line.strip().lstrip("`")
                code_block_lines = []
            i += 1
            continue
            
        if in_code_block:
            code_block_lines.append(line)
            i += 1
            continue
            
        # Tabla
        if line.strip().startswith("|") and line.strip().endswith("|"):
            in_table = True
            table_lines.append(line)
            i += 1
            continue
        elif in_table:
            flush_table()
            
        # Imagen de markdown ![alt](path)
        img_match = re.match(r'^!\[(.*?)\]\((.*?)\)$', line.strip())
        if img_match:
            alt_text = img_match.group(1)
            img_path_str = img_match.group(2).replace('file://', '')
            img_path = Path(img_path_str)
            if img_path.exists():
                p = doc.add_paragraph()
                p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                p.paragraph_format.space_before = Pt(10)
                p.paragraph_format.space_after = Pt(4)
                run = p.add_run()
                run.add_picture(str(img_path), width=Inches(5.8))
                
                cap = doc.add_paragraph()
                cap.alignment = WD_ALIGN_PARAGRAPH.CENTER
                cap.paragraph_format.space_after = Pt(12)
                cap_run = cap.add_run(f"Figura: {alt_text}")
                cap_run.font.name = 'Arial'
                cap_run.font.size = Pt(10)
                cap_run.font.italic = True
                cap_run.font.color.rgb = RGBColor(100, 100, 100)
            else:
                print(f"⚠️ Imagen no encontrada en ruta: {img_path_str}")
            i += 1
            continue

        # Encabezado Principal H1 (# ): Arial 16pt, Negro (#000000), Negrilla
        if line.startswith("# "):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(18)
            p.paragraph_format.space_after = Pt(6)
            run = p.add_run(line[2:].strip())
            run.font.name = 'Arial'
            run.font.size = Pt(16)
            run.font.bold = True
            run.font.color.rgb = RGBColor(0, 0, 0)
        # Subtítulo H2 (## ): Arial 14pt, Negro (#000000), Negrilla
        elif line.startswith("## "):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(14)
            p.paragraph_format.space_after = Pt(4)
            run = p.add_run(line[3:].strip())
            run.font.name = 'Arial'
            run.font.size = Pt(14)
            run.font.bold = True
            run.font.color.rgb = RGBColor(0, 0, 0)
        # Subtítulo H3 (### ): Arial 14pt, Negro (#000000), Negrilla
        elif line.startswith("### "):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(12)
            p.paragraph_format.space_after = Pt(4)
            run = p.add_run(line[4:].strip())
            run.font.name = 'Arial'
            run.font.size = Pt(14)
            run.font.bold = True
            run.font.color.rgb = RGBColor(0, 0, 0)
        # Subtítulo H4 (#### ): Arial 14pt, Negro (#000000), Negrilla
        elif line.startswith("#### "):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.LEFT
            p.paragraph_format.space_before = Pt(10)
            p.paragraph_format.space_after = Pt(2)
            run = p.add_run(line[5:].strip())
            run.font.name = 'Arial'
            run.font.size = Pt(14)
            run.font.bold = True
            run.font.color.rgb = RGBColor(0, 0, 0)
        # Viñetas: Arial 12pt, Justificado
        elif line.strip().startswith("- ") or line.strip().startswith("* "):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            p.paragraph_format.line_spacing = 1.15
            p.paragraph_format.left_indent = Inches(0.25)
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after = Pt(3)
            content = "•  " + line.strip()[2:]
            add_formatted_text(p, content, font_size=12, default_color=RGBColor(0, 0, 0))
        # Listas numeradas: Arial 12pt, Justificado
        elif re.match(r'^\d+\.\s', line.strip()):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            p.paragraph_format.line_spacing = 1.15
            p.paragraph_format.left_indent = Inches(0.25)
            p.paragraph_format.space_before = Pt(1)
            p.paragraph_format.space_after = Pt(3)
            content = line.strip()
            add_formatted_text(p, content, font_size=12, default_color=RGBColor(0, 0, 0))
        # Alertas / Citas: Arial 12pt, Justificado
        elif line.strip().startswith("> [!") or line.strip().startswith(">"):
            tbl = doc.add_table(rows=1, cols=1)
            tbl.alignment = WD_TABLE_ALIGNMENT.CENTER
            cell = tbl.cell(0, 0)
            set_cell_background(cell, "EBF5FB")
            set_cell_margins(cell, top=100, bottom=100, left=180, right=180)
            
            p = cell.paragraphs[0]
            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            p.paragraph_format.line_spacing = 1.15
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.space_after = Pt(2)
            content = line.strip().lstrip(">").strip()
            add_formatted_text(p, content, font_size=12, default_color=RGBColor(0, 0, 0))
            doc.add_paragraph()
        # Línea separadora
        elif line.strip() in ("---", "***", "___"):
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.CENTER
            p.paragraph_format.space_before = Pt(6)
            p.paragraph_format.space_after = Pt(6)
            run = p.add_run("―" * 45)
            run.font.color.rgb = RGBColor(200, 200, 200)
        # Párrafo normal: Arial 12pt, Negro (#000000), Justificado
        elif line.strip():
            p = doc.add_paragraph()
            p.alignment = WD_ALIGN_PARAGRAPH.JUSTIFY
            p.paragraph_format.line_spacing = 1.15
            p.paragraph_format.space_before = Pt(2)
            p.paragraph_format.space_after = Pt(4)
            add_formatted_text(p, line.strip(), font_size=12, default_color=RGBColor(0, 0, 0))
            
        i += 1
        
    if in_table:
        flush_table()
    if in_code_block:
        flush_code_block()
        
    output_path.parent.mkdir(parents=True, exist_ok=True)
    doc.save(str(output_path))
    print(f"✅ Documento generado con éxito en: {output_path}")


def main():
    pyimport_dir = Path("/home/jjvallej/work/enigma/pyimport")
    airflow_dir = Path("/home/jjvallej/work/enigma/airflow")

    files_to_process = [
        ("1.3.1.1 CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.md", "1.3.1.1 CERTIFICADO_AUDITORIA_CLOUD_COMPOSER"),
        ("CERTIFICADO_AUDITORIA_CLOUD_COMPOSER.md", "CERTIFICADO_AUDITORIA_CLOUD_COMPOSER"),
        ("1.3.1.2 DOCUMENTACION_DAGS.md", "1.3.1.2 DOCUMENTACION_DAGS"),
        ("1.3.1.3 DOCUMENTACION_CONEXIONES_AIRFLOW institucionales.md", "1.3.1.3 DOCUMENTACION_CONEXIONES_AIRFLOW institucionales"),
        ("DOCUMENTACION_CONEXIONES_AIRFLOW.md", "DOCUMENTACION_CONEXIONES_AIRFLOW"),
        ("MODELO_DE_SOFTWARE_VALLEDATA.md", "MODELO_DE_SOFTWARE_VALLEDATA"),
        ("PROPUESTA_DASHBOARD_SENTIMIENTO_EMPRESA_REPORTES.md", "PROPUESTA_DASHBOARD_SENTIMIENTO_EMPRESA_REPORTES"),
        ("PROPUESTA_ANALISIS_GEORREFERENCIADOS_VALLEDATA.md", "PROPUESTA_ANALISIS_GEORREFERENCIADOS_VALLEDATA"),
        ("ANALISIS_EXPLORATORIO_DATOS_AGRICOLA_VALLEDATA.md", "ANALISIS_EXPLORATORIO_DATOS_AGRICOLA_VALLEDATA"),
        ("MATRIZ_DE_EVENTOS_ALARMAS_DAGS.md", "1.3.1.4 MATRIZ_DE_EVENTOS_ALARMAS_DAGS"),
        ("1.3.1.5 DOCUMENTACION_MONITOREO_Y_ALERTAS_AIRFLOW.md", "1.3.1.5 DOCUMENTACION_MONITOREO_Y_ALERTAS_AIRFLOW"),
    ]

    for md_name, base_out in files_to_process:
        md_file = pyimport_dir / md_name
        if md_file.exists():
            md_to_docx(md_file, pyimport_dir / f"{base_out}.docx", use_template=True)
            md_to_docx(md_file, pyimport_dir / f"{base_out}.doc", use_template=True)

            if "1.3.1.3" in base_out:
                md_to_docx(md_file, pyimport_dir / "1.3.1.3 DOCUMENTACION_CONEXIONES_AIRFLOW.docx", use_template=True)
                md_to_docx(md_file, pyimport_dir / "1.3.1.3 DOCUMENTACION_CONEXIONES_AIRFLOW.doc", use_template=True)

            md_to_docx(md_file, airflow_dir / f"{base_out}.docx", use_template=True)
            md_to_docx(md_file, airflow_dir / f"{base_out}.doc", use_template=True)
            if "1.3.1.3" in base_out:
                md_to_docx(md_file, airflow_dir / "1.3.1.3 DOCUMENTACION_CONEXIONES_AIRFLOW.docx", use_template=True)
                md_to_docx(md_file, airflow_dir / "1.3.1.3 DOCUMENTACION_CONEXIONES_AIRFLOW.doc", use_template=True)


if __name__ == "__main__":
    main()
