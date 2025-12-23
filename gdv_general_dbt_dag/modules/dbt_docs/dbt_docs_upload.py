"""
Módulo para generar y subir la documentación de dbt a Google Cloud Storage.

Este módulo permite:
1. Generar la documentación de dbt usando `dbt docs generate`
2. Subir los archivos generados a un bucket de GCS
3. Configurar el bucket para servir la documentación como sitio web estático
"""
import os
import subprocess
import shutil
from pathlib import Path
from typing import Optional
from google.cloud import storage
from modules.gcp_utils import get_gcs_client
from modules.config import CONF

# Obtener DEBUG desde la configuración global (mismo patrón que otros módulos)
# Manejo robusto en caso de que global_config no esté disponible
try:
    DEBUG = CONF.global_config.debug
except (AttributeError, KeyError):
    # Fallback: usar False si no está disponible
    DEBUG = False
    print("[WARN] No se pudo obtener DEBUG desde config, usando False por defecto")

def generate_dbt_docs(dbt_project_dir: str, target: str = "dev") -> str:
    """
    Genera la documentación de dbt ejecutando `dbt docs generate`.
    
    Args:
        dbt_project_dir: Ruta al directorio del proyecto dbt
        target: Target de dbt a usar (dev, prod, local)
    
    Returns:
        Ruta al directorio donde se generó la documentación (normalmente dbt_project_dir/target)
    
    Raises:
        RuntimeError: Si el comando dbt falla
    """
    if DEBUG:
        print(f"[INFO] Generando documentación de dbt en: {dbt_project_dir}")
        print(f"[INFO] Target: {target}")
    
    # Cambiar al directorio del proyecto dbt
    original_dir = os.getcwd()
    try:
        os.chdir(dbt_project_dir)
        
        # Construir comando dbt con --profiles-dir y --project-dir explícitos
        # Esto asegura que dbt encuentre el profiles.yml y el dbt_project.yml correctamente
        cmd = [
            "dbt", 
            "docs", 
            "generate", 
            "--target", target,
            "--profiles-dir", dbt_project_dir,
            "--project-dir", dbt_project_dir
        ]
        
        if DEBUG:
            print(f"[DEBUG] Ejecutando comando: {' '.join(cmd)}")
            print(f"[DEBUG] Directorio de trabajo: {os.getcwd()}")
            print(f"[DEBUG] profiles.yml existe: {os.path.exists(os.path.join(dbt_project_dir, 'profiles.yml'))}")
            print(f"[DEBUG] dbt_project.yml existe: {os.path.exists(os.path.join(dbt_project_dir, 'dbt_project.yml'))}")
        
        # Ejecutar comando
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            cwd=dbt_project_dir  # Asegurar que se ejecute desde el directorio correcto
        )
        
        # Verificar si los archivos de documentación se generaron correctamente
        # incluso si hubo errores al generar el catálogo (puede fallar si los datasets no existen)
        target_dir = os.path.join(dbt_project_dir, "target")
        index_html = os.path.join(target_dir, "index.html")
        manifest_json = os.path.join(target_dir, "manifest.json")
        
        docs_generated = os.path.exists(index_html) and os.path.exists(manifest_json)
        
        if result.returncode != 0:
            # Si los archivos de documentación se generaron, solo mostrar advertencia
            if docs_generated:
                if DEBUG:
                    print(f"[WARN] dbt docs generate terminó con exit code {result.returncode}, pero la documentación se generó correctamente.")
                    print(f"[WARN] Esto puede deberse a errores al generar el catálogo (p. ej., datasets no existentes en BigQuery).")
                    if "Catalog written" in result.stdout:
                        print(f"[INFO] Catálogo generado parcialmente (algunos datasets pueden no existir).")
            else:
                # Si no se generaron los archivos, es un error real
                error_msg = f"Error al generar documentación de dbt (exit code: {result.returncode}):\n"
                if result.stderr:
                    error_msg += f"STDERR:\n{result.stderr}\n"
                if result.stdout:
                    error_msg += f"STDOUT:\n{result.stdout}\n"
                if not result.stderr and not result.stdout:
                    error_msg += "No se obtuvo salida del comando. Verifica que dbt esté instalado y configurado correctamente.\n"
                
                if DEBUG:
                    print(f"[ERROR] {error_msg}")
                raise RuntimeError(error_msg)
        
        if DEBUG:
            print(f"[OK] Documentación generada exitosamente")
            if result.stdout:
                print(f"[DEBUG] Salida: {result.stdout[:500]}...")  # Primeros 500 caracteres
        
        # La documentación se genera en dbt_project_dir/target
        target_dir = os.path.join(dbt_project_dir, "target")
        return target_dir
        
    finally:
        os.chdir(original_dir)


def upload_dbt_docs_to_gcs(
    local_docs_dir: str,
    bucket_name: str,
    destination_prefix: str = "dbt_docs",
    overwrite: bool = True
) -> str:
    """
    Sube todos los archivos de la documentación de dbt a Google Cloud Storage.
    
    Args:
        local_docs_dir: Ruta local al directorio con la documentación generada (normalmente dbt_project_dir/target)
        bucket_name: Nombre del bucket en GCS
        destination_prefix: Prefijo en GCS donde se subirán los archivos (ej: 'dbt_docs')
        overwrite: Si es True, sobrescribe los archivos si ya existen
    
    Returns:
        URI base del sitio web en GCS (gs://bucket/prefix)
    """
    if DEBUG:
        print(f"[INFO] Subiendo documentación de dbt a GCS")
        print(f"[INFO] Directorio local: {local_docs_dir}")
        print(f"[INFO] Bucket: {bucket_name}")
        print(f"[INFO] Prefijo destino: {destination_prefix}")
    
    if not os.path.exists(local_docs_dir):
        raise ValueError(f"El directorio de documentación no existe: {local_docs_dir}")
    
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    
    # Normalizar el prefijo
    destination_prefix = destination_prefix.strip('/')
    
    # Contar archivos a subir
    # Solo subir archivos necesarios para la documentación web:
    # - index.html, manifest.json, catalog.json (si existe)
    # - Archivos estáticos (CSS, JS, imágenes, fuentes)
    # - NO subir archivos compilados (.sql) que pueden no existir
    files_to_upload = []
    doc_extensions = {'.html', '.json', '.css', '.js', '.png', '.jpg', '.jpeg', '.svg', '.ico', '.woff', '.woff2', '.ttf', '.eot'}
    
    for root, dirs, files in os.walk(local_docs_dir):
        for file in files:
            local_path = os.path.join(root, file)
            
            # Omitir archivos .sql compilados que pueden no existir
            if file.endswith('.sql'):
                continue
            
            # Solo incluir archivos con extensiones de documentación
            file_ext = os.path.splitext(file)[1].lower()
            if file_ext not in doc_extensions and file not in ['index.html', 'manifest.json', 'catalog.json']:
                continue
            
            # Calcular ruta relativa desde local_docs_dir
            rel_path = os.path.relpath(local_path, local_docs_dir)
            # Construir ruta en GCS
            gcs_path = f"{destination_prefix}/{rel_path}".replace("\\", "/")
            files_to_upload.append((local_path, gcs_path))
    
    if DEBUG:
        print(f"[INFO] Total de archivos a subir: {len(files_to_upload)}")
    
    # Subir archivos
    uploaded_count = 0
    skipped_count = 0
    error_count = 0
    
    for local_path, gcs_path in files_to_upload:
        try:
            # Verificar que el archivo existe antes de intentar subirlo
            if not os.path.exists(local_path):
                print(f"[WARN] Archivo no encontrado, saltando: {local_path}")
                skipped_count += 1
                continue
            
            # Verificar que es un archivo (no un directorio o enlace simbólico roto)
            if not os.path.isfile(local_path):
                print(f"[WARN] No es un archivo válido, saltando: {local_path}")
                skipped_count += 1
                continue
            
            blob = bucket.blob(gcs_path)
            
            # Verificar si existe
            if blob.exists() and not overwrite:
                if DEBUG:
                    print(f"[SKIP] {gcs_path} ya existe (overwrite=False)")
                skipped_count += 1
                continue
            
            # Determinar content type basado en extensión
            content_type = _get_content_type(local_path)
            
            # Asegurar que los archivos JSON tengan el content-type correcto
            if file.endswith('.json'):
                content_type = 'application/json'
            
            # Subir archivo con metadata adicional
            blob.upload_from_filename(local_path, content_type=content_type)
            
            # Configurar metadata para CORS y caché
            blob.metadata = {
                'Cache-Control': 'public, max-age=3600'  # Cache por 1 hora
            }
            blob.patch()
            uploaded_count += 1
            
            if DEBUG and uploaded_count % 10 == 0:
                print(f"[INFO] Subidos {uploaded_count}/{len(files_to_upload)} archivos...")
                
        except FileNotFoundError as e:
            print(f"[WARN] Archivo no encontrado durante la subida, saltando: {local_path} - {e}")
            skipped_count += 1
            error_count += 1
        except Exception as e:
            print(f"[ERROR] Error al subir {gcs_path}: {e}")
            error_count += 1
            # Continuar con el siguiente archivo en lugar de fallar todo el proceso
            continue
    
    gcs_base_uri = f"gs://{bucket_name}/{destination_prefix}"
    
    # Resumen de la operación
    print(f"[OK] Documentación subida a: {gcs_base_uri}")
    print(f"[INFO] Archivos subidos: {uploaded_count}/{len(files_to_upload)}")
    if skipped_count > 0:
        print(f"[INFO] Archivos saltados: {skipped_count}")
    if error_count > 0:
        print(f"[WARN] Archivos con errores: {error_count}")
    
    # Si no se subió ningún archivo, lanzar error
    if uploaded_count == 0:
        raise RuntimeError(
            f"No se pudo subir ningún archivo. "
            f"Subidos: {uploaded_count}, Saltados: {skipped_count}, Errores: {error_count}"
        )
    
    # Si hubo muchos errores, lanzar advertencia pero no fallar
    if error_count > len(files_to_upload) * 0.1:  # Más del 10% de errores
        print(f"[WARN] Muchos archivos tuvieron errores ({error_count}/{len(files_to_upload)}). "
              f"La documentación puede estar incompleta.")
    
    return gcs_base_uri


def _get_content_type(file_path: str) -> Optional[str]:
    """
    Determina el content type basado en la extensión del archivo.
    
    Args:
        file_path: Ruta al archivo
    
    Returns:
        Content type o None
    """
    ext = os.path.splitext(file_path)[1].lower()
    content_types = {
        '.html': 'text/html',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.json': 'application/json',
        '.png': 'image/png',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.svg': 'image/svg+xml',
        '.ico': 'image/x-icon',
        '.woff': 'font/woff',
        '.woff2': 'font/woff2',
        '.ttf': 'font/ttf',
        '.eot': 'application/vnd.ms-fontobject',
    }
    return content_types.get(ext)


def configure_bucket_for_static_website(
    bucket_name: str,
    index_page: str = "index.html",
    error_page: Optional[str] = None
) -> None:
    """
    Configura un bucket de GCS para servir contenido estático como sitio web público.
    
    Esta función:
    1. Configura IAM para permitir acceso público a los objetos (allUsers)
    2. Configura CORS para que el navegador pueda cargar manifest.json, catalog.json, etc.
    
    NOTA: Esta función requiere permisos de administrador del bucket.
    
    Args:
        bucket_name: Nombre del bucket
        index_page: Página principal (normalmente 'index.html' o 'dbt_docs/index.html')
        error_page: Página de error (opcional)
    """
    if DEBUG:
        print(f"[INFO] Configurando bucket {bucket_name} para acceso público y CORS")
    
    gcs_client = get_gcs_client()
    bucket = gcs_client.bucket(bucket_name)
    
    try:
        # 1. Configurar IAM para permitir acceso público a los objetos
        # Esto permite que cualquier usuario (allUsers) pueda leer los objetos del bucket
        policy = bucket.get_iam_policy(requested_policy_version=3)
        
        # Verificar si ya existe un binding para allUsers
        has_public_access = False
        viewer_binding = None
        
        for binding in policy.bindings:
            if binding.get("role") == "roles/storage.objectViewer":
                if "allUsers" in binding.get("members", set()):
                    has_public_access = True
                    break
                viewer_binding = binding
        
        # Agregar o actualizar binding para allUsers si no existe
        if not has_public_access:
            if viewer_binding:
                # Agregar allUsers al binding existente
                if "members" not in viewer_binding:
                    viewer_binding["members"] = set()
                viewer_binding["members"].add("allUsers")
            else:
                # Crear nuevo binding
                policy.bindings.append({
                    "role": "roles/storage.objectViewer",
                    "members": {"allUsers"}
                })
            
            bucket.set_iam_policy(policy)
            
            if DEBUG:
                print(f"[OK] IAM configurado: allUsers puede leer objetos del bucket")
        else:
            if DEBUG:
                print(f"[INFO] El bucket ya tiene acceso público configurado")
        
        # 2. Configurar CORS para permitir que el navegador cargue manifest.json y catalog.json
        # desde el bucket cuando se accede a index.html
        cors_policy = [
            {
                "origin": ["*"],  # Permitir cualquier origen
                "method": ["GET", "HEAD"],
                "responseHeader": [
                    "Content-Type", 
                    "Content-Length", 
                    "Cache-Control",
                    "Access-Control-Allow-Origin"
                ],
                "maxAgeSeconds": 3600
            }
        ]
        bucket.cors = cors_policy
        bucket.patch()  # Aplicar los cambios
        
        if DEBUG:
            print(f"[OK] CORS configurado para el bucket {bucket_name}")
            print(f"[INFO] CORS permite cargar recursos desde cualquier origen")
            
    except Exception as e:
        print(f"[ERROR] Error al configurar bucket: {e}")
        print(f"[INFO] Puedes configurar IAM y CORS manualmente en la consola de GCP")
        raise

