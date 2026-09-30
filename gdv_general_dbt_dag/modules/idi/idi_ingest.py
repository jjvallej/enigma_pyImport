# modules/idi/idi_ingest.py
"""
Módulo para ingestar archivos Excel de IDI desde URLs de Función Pública.
Lee configuración desde Excel en Google Drive, hace web scraping para encontrar
el enlace de descarga, y sube el archivo transformado a GCS como CSV.
"""
from google.cloud import storage
import os
import tempfile
import requests
import pandas as pd
import re
from bs4 import BeautifulSoup
from modules.config import PROJECT_ID, CONF, DEFAULT_BUCKET_NAME
from modules.gcp_utils import get_gcs_client

# === CONFIGURACIÓN ===
DEBUG = CONF.global_config.debug


def normalize_column_name(name: str) -> str:
    """
    Normaliza nombre de columna para BigQuery:
    - Remueve tildes y caracteres especiales
    - Convierte a minúsculas
    - Reemplaza espacios por guiones bajos
    - Asegura que empiece con letra
    
    Args:
        name: Nombre original de la columna
    
    Returns:
        Nombre normalizado compatible con BigQuery
    """
    import unicodedata
    
    # Decodificar caracteres unicode y eliminar tildes
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')
    
    # Convertir a minúsculas
    name = name.lower()
    
    # Reemplazar caracteres no alfanuméricos (excepto espacios) por nada
    name = re.sub(r'[^a-z0-9\s]', '', name)
    
    # Reemplazar espacios por guiones bajos
    name = re.sub(r'\s+', '_', name)
    
    # Asegurar que empiece por letra (BigQuery requirement)
    if name and not name[0].isalpha():
        name = 'col_' + name
    
    # Si quedó vacío, asignar nombre genérico
    if not name:
        name = 'col_unnamed'
    
    # Limitar longitud (BigQuery max 300, pero mejor mantenerlo corto)
    return name[:128]


def extract_file_id_from_gdrive_url(drive_url: str) -> str:
    """Extrae el File ID de una URL de Google Drive."""
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'/spreadsheets/d/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, drive_url)
        if match:
            return match.group(1)
    
    raise ValueError(f"No se pudo extraer File ID de: {drive_url}")


def download_config_excel_from_gdrive(drive_url: str) -> pd.DataFrame:
    """
    Descarga el Excel de configuración desde Google Drive y lo retorna como DataFrame.
    
    Args:
        drive_url: URL pública de Google Drive con el Excel de configuración
    
    Returns:
        DataFrame con las columnas: AÑO, NOMBRE, IDI, ENLACE
    """
    try:
        file_id = extract_file_id_from_gdrive_url(drive_url)
        download_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=xlsx"
        
        if DEBUG:
            print(f"[INFO] Descargando config desde Drive: {file_id}")
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(download_url, headers=headers, timeout=60)
        response.raise_for_status()
        
        # Leer Excel directo desde memoria
        df = pd.read_excel(response.content, engine='openpyxl', skiprows=1)
        
        # Limpiar nombres de columnas (eliminar espacios)
        df.columns = df.columns.str.strip()
        
        if DEBUG:
            print(f"[OK] Config leída: {len(df)} filas")
            print(f"[DEBUG] Columnas: {list(df.columns)}")
        
        return df
        
    except Exception as e:
        raise Exception(f"Error descargando config de Google Drive: {e}")


def find_download_link_in_page(page_url: str, link_text_options: list) -> str:
    """
    Busca en una página HTML el enlace <a> que contenga alguno de los textos especificados
    y que apunte a un archivo .xlsx
    
    Args:
        page_url: URL de la página a analizar
        link_text_options: Lista de textos a buscar (ej: ["Resultados Territorio", "Resultados consolidados"])
    
    Returns:
        URL completa del archivo para descargar
    """
    try:
        if DEBUG:
            print(f"[INFO] Buscando enlace en: {page_url}")
            print(f"[INFO] Palabras clave: {link_text_options}")
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(page_url, headers=headers, timeout=60)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Buscar todos los enlaces <a>
        for link in soup.find_all('a', href=True):
            link_text = link.get_text(strip=True)
            href = link['href']
            
            # Verificar si el texto del enlace contiene ALGUNA de las palabras clave
            for keyword in link_text_options:
                if keyword.lower() in link_text.lower():
                    # IMPORTANTE: Verificar que sea un archivo .xlsx
                    if '.xlsx' not in href.lower() and '.xls' not in href.lower():
                        if DEBUG:
                            print(f"[DEBUG] Saltando enlace '{link_text}' (no es archivo Excel): {href}")
                        continue
                    
                    # Si el href es relativo, construir URL completa
                    if href.startswith('http'):
                        download_url = href
                    else:
                        # Construir URL absoluta
                        from urllib.parse import urljoin
                        download_url = urljoin(page_url, href)
                    
                    if DEBUG:
                        print(f"[OK] Enlace encontrado con palabra clave '{keyword}': {link_text}")
                        print(f"[OK] URL: {download_url}")
                    
                    return download_url
        
        raise Exception(f"No se encontró enlace .xlsx con ninguna de las palabras clave: {link_text_options}")
        
    except Exception as e:
        raise Exception(f"Error buscando enlace en página: {e}")


def ingest_idi_from_config(
    config_drive_url: str,
    year: int,
    link_name_keywords: list,
    bucket_name: str,
    folder_name: str,
    destination_file_name: str,
    skip_rows: int = 2,
    sheet_name=None,
    columns_to_drop=None
):
    """
    Pipeline completo:
    1. Lee Excel de configuración desde Google Drive
    2. Busca la fila por año y nombre (usando palabras clave)
    3. Extrae la URL de la página
    4. Hace web scraping para encontrar el enlace de descarga
    5. Descarga el Excel
    6. Transforma con pandas
    7. Sube a GCS como CSV
    
    Args:
        config_drive_url: URL pública del Excel de configuración en Google Drive
        year: Año a buscar en la columna AÑO
        link_name_keywords: Lista de palabras clave para buscar (ej: ["Resultados Territorio", "Resultados consolidados"])
        bucket_name: Bucket de GCS
        folder_name: Carpeta en GCS
        destination_file_name: Nombre del CSV de salida
        skip_rows: Filas a eliminar al inicio (default: 2)
        sheet_name: Hoja del Excel (None = primera)
        columns_to_drop: Columnas a eliminar
    
    Returns:
        URI de GCS (gs://bucket/folder/file.csv)
    """
    excel_path = None
    csv_path = None
    
    try:
        # 1. Leer configuración
        df_config = download_config_excel_from_gdrive(config_drive_url)
        
        # 2. Buscar fila por año y nombre (con CUALQUIERA de las palabras clave)
        # Crear máscara que busque cualquiera de las palabras clave
        name_mask = False
        for keyword in link_name_keywords:
            name_mask = name_mask | df_config['NOMBRE'].str.contains(keyword, case=False, na=False)
        
        mask = (df_config['AÑO'] == year) & name_mask
        matching_rows = df_config[mask]
        
        if len(matching_rows) == 0:
            raise Exception(f"No se encontró configuración para año={year} con palabras clave={link_name_keywords}")
        
        page_url = matching_rows.iloc[0]['ENLACE']
        
        if DEBUG:
            print(f"[OK] Configuración encontrada:")
            print(f"     Año: {year}")
            print(f"     Nombre: {matching_rows.iloc[0]['NOMBRE']}")
            print(f"     Página: {page_url}")
        
        # 3. Buscar enlace de descarga en la página
        download_url = find_download_link_in_page(page_url, link_name_keywords)
        
        # 4. Descargar Excel
        if DEBUG:
            print(f"[INFO] Descargando Excel desde: {download_url}")
        
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(download_url, stream=True, headers=headers, timeout=120)
        response.raise_for_status()
        
        fd, excel_path = tempfile.mkstemp(suffix='.xlsx')
        os.close(fd)
        
        with open(excel_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        if DEBUG:
            print(f"[OK] Excel descargado")
        
        # 5. Transformar con pandas
        if DEBUG:
            print(f"[INFO] Transformando (skip_rows={skip_rows})...")
        
        df = pd.read_excel(
            excel_path,
            sheet_name=sheet_name if sheet_name else 0,
            skiprows=skip_rows,
            engine='openpyxl'
        )
        
        # Limpiar espacios en nombres de columnas
        df.columns = df.columns.str.strip()
        
        # Normalizar nombres de columnas para BigQuery
        new_columns = []
        seen_columns = {}
        
        for col in df.columns:
            new_col = normalize_column_name(str(col))
            
            # Manejar duplicados agregando sufijo numérico
            if new_col in seen_columns:
                seen_columns[new_col] += 1
                new_col = f"{new_col}_{seen_columns[new_col]}"
            else:
                seen_columns[new_col] = 0
            
            new_columns.append(new_col)
        
        df.columns = new_columns
        
        # Eliminar filas completamente vacías
        df = df.dropna(how='all')
        
        # Eliminar columnas especificadas (usando nombres ya normalizados)
        if columns_to_drop:
            # Normalizar nombres de columnas a eliminar
            normalized_drops = [normalize_column_name(col) for col in columns_to_drop]
            df = df.drop(columns=normalized_drops, errors='ignore')
        
        if DEBUG:
            print(f"[OK] Datos: {len(df)} filas, {len(df.columns)} columnas")
            print(f"[DEBUG] Columnas normalizadas: {list(df.columns)[:5]}...")  # Mostrar primeras 5
        
        # 6. Guardar como CSV
        fd, csv_path = tempfile.mkstemp(suffix='.csv')
        os.close(fd)
        df.to_csv(csv_path, index=False, encoding='utf-8', sep=';')  # ⬅️ sep=';'
        
        # 7. Subir a GCS
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        
        if not destination_file_name.endswith('.csv'):
            destination_file_name += '.csv'
        
        blob_path = f"{folder_name.strip('/')}/{destination_file_name}"
        blob = bucket.blob(blob_path)
        
        if blob.exists():
            blob.delete()
        
        blob.upload_from_filename(csv_path)
        gcs_uri = f"gs://{bucket_name}/{blob_path}"
        
        if DEBUG:
            print(f"[OK] Subido a: {gcs_uri}")
        
        return gcs_uri
        
    except Exception as e:
        print(f"[ERROR] {e}")
        raise
        
    finally:
        for tmp_file in [excel_path, csv_path]:
            if tmp_file and os.path.exists(tmp_file):
                os.unlink(tmp_file)


def ingest_all_years_idi(
    config_drive_url: str,
    link_name_keywords: list,
    bucket_name: str,
    base_folder_name: str,
    skip_rows: int = 2,
    sheet_name=None,
    columns_to_drop=None
):
    """
    Procesa TODOS los años encontrados en el Excel de configuración.
    
    Args:
        config_drive_url: URL pública del Excel de configuración en Google Drive
        link_name_keywords: Lista de palabras clave para buscar (ej: ["Resultados Territorio", "Resultados consolidados"])
        bucket_name: Bucket de GCS
        base_folder_name: Carpeta base en GCS (se agregará el año automáticamente)
        skip_rows: Filas a eliminar al inicio (default: 2)
        sheet_name: Hoja del Excel (None = primera)
        columns_to_drop: Columnas a eliminar
    
    Returns:
        Lista de URIs de GCS procesados
    """
    try:
        # 1. Leer configuración
        df_config = download_config_excel_from_gdrive(config_drive_url)
        
        # 2. Filtrar filas que contengan CUALQUIERA de las palabras clave y tengan enlace
        name_mask = False
        for keyword in link_name_keywords:
            name_mask = name_mask | df_config['NOMBRE'].str.contains(keyword, case=False, na=False)
        
        mask = name_mask & df_config['ENLACE'].notna()
        filtered_df = df_config[mask]
        
        if len(filtered_df) == 0:
            raise Exception(f"No se encontraron registros con palabras clave={link_name_keywords}")
        
        # 3. Obtener años únicos
        years = filtered_df['AÑO'].dropna().unique()
        years = sorted([int(y) for y in years], reverse=True)  # Ordenar de más reciente a más antiguo
        
        if DEBUG:
            print(f"[INFO] Años encontrados: {years}")
        
        # 4. Procesar cada año
        results = []
        for year in years:
            try:
                if DEBUG:
                    print(f"\n{'='*60}")
                    print(f"PROCESANDO AÑO: {year}")
                    print(f"{'='*60}\n")
                
                folder_name = f"{base_folder_name}/{year}"
                file_name = f"resultados_{year}.csv"
                
                gcs_uri = ingest_idi_from_config(
                    config_drive_url=config_drive_url,
                    year=year,
                    link_name_keywords=link_name_keywords,
                    bucket_name=bucket_name,
                    folder_name=folder_name,
                    destination_file_name=file_name,
                    skip_rows=skip_rows,
                    sheet_name=sheet_name,
                    columns_to_drop=columns_to_drop
                )
                
                results.append({
                    'year': year,
                    'gcs_uri': gcs_uri,
                    'status': 'success'
                })
                
                if DEBUG:
                    print(f"[OK] Año {year} procesado exitosamente")
                
            except Exception as e:
                if DEBUG:
                    print(f"[ERROR] Error procesando año {year}: {e}")
                
                results.append({
                    'year': year,
                    'gcs_uri': None,
                    'status': 'error',
                    'error': str(e)
                })
        
        # 5. Resumen
        if DEBUG:
            print(f"\n{'='*60}")
            print(f"RESUMEN DE PROCESAMIENTO")
            print(f"{'='*60}")
            for result in results:
                status_icon = "✅" if result['status'] == 'success' else "❌"
                print(f"{status_icon} Año {result['year']}: {result['status']}")
                if result['status'] == 'success':
                    print(f"   URI: {result['gcs_uri']}")
            print(f"{'='*60}\n")
        
        return results
        
    except Exception as e:
        print(f"[ERROR] {e}")
        raise