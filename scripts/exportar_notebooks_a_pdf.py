"""Script para exportar automáticamente los notebooks de análisis a HTML y PDF.
Utiliza nbconvert y Google Chrome headless para garantizar que todas las tablas,
figuras e interpretaciones salgan completas sin truncamiento.
"""

import subprocess
import sys
from pathlib import Path

def exportar_notebooks():
    root_dir = Path(__file__).resolve().parent.parent
    notebooks_dir = root_dir / "notebooks"
    venv_python = root_dir / ".venv" / "bin" / "python"
    
    python_cmd = str(venv_python) if venv_python.exists() else sys.executable
    
    notebooks = [
        notebooks_dir / "analisis_exploratorio_rentabilidad_cultivos.ipynb",
        notebooks_dir / "eda_y_prediccion_arima_plus.ipynb",
    ]
    
    for nb_path in notebooks:
        if not nb_path.exists():
            print(f"⚠️ Advertencia: No se encontró {nb_path}")
            continue
            
        print(f"📄 Procesando {nb_path.name}...")
        
        # 1. Convertir Notebook a HTML
        cmd_html = [
            python_cmd, "-m", "jupyter", "nbconvert",
            "--to", "html",
            str(nb_path)
        ]
        res_html = subprocess.run(cmd_html, capture_output=True, text=True)
        if res_html.returncode != 0:
            print(f"❌ Error al convertir {nb_path.name} a HTML:\n{res_html.stderr}")
            continue
            
        html_path = nb_path.with_suffix(".html")
        pdf_path = nb_path.with_suffix(".pdf")
        
        # 2. Convertir HTML a PDF con Google Chrome headless
        cmd_pdf = [
            "/usr/bin/google-chrome",
            "--headless",
            "--no-sandbox",
            "--disable-gpu",
            "--no-pdf-header-footer",
            f"--print-to-pdf={pdf_path}",
            str(html_path)
        ]
        res_pdf = subprocess.run(cmd_pdf, capture_output=True, text=True)
        if res_pdf.returncode == 0 and pdf_path.exists():
            print(f"✅ PDF generado exitosamente: {pdf_path} ({pdf_path.stat().st_size / 1024:.1f} KB)")
        else:
            print(f"❌ Error al generar PDF para {html_path.name}:\n{res_pdf.stderr}")

if __name__ == "__main__":
    exportar_notebooks()
