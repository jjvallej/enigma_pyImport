# Instrucciones: Google Drive Público → GCS

Esta solución es **mucho más simple** porque usa enlaces públicos de Google Drive. **No requiere autenticación ni compartir archivos.**

## Pasos Super Simples

### 1. Hacer el archivo público en Google Drive

1. Abre Google Drive y encuentra tu archivo `2_IPM_DANE.xlsx`
2. Haz clic derecho en el archivo → **"Compartir"**
3. En la ventana de compartir, haz clic en **"Cambiar a cualquiera con el enlace"**
4. Asegúrate que dice **"Cualquiera con el enlace"** y tiene permisos de **"Lector"**
5. Copia el enlace (será algo como: `https://drive.google.com/file/d/1ABC123XYZ.../view?usp=sharing`)
6. Haz clic en **"Listo"** o **"Copiar enlace"**

### 2. Obtener el File ID (opcional, pero recomendado)

Puedes usar directamente la URL completa o solo el File ID:

- **URL completa:** `https://drive.google.com/file/d/1ABC123XYZ.../view?usp=sharing`
- **Solo File ID:** `1ABC123XYZ...` (la parte entre `/d/` y `/view`)

### 3. Configurar en Airflow

**Opción A: Usando Variables de Airflow (Recomendado)**

1. Ve a Airflow UI → **Admin** → **Variables**
2. Agrega estas variables:
   - `ipm_drive_url` = `https://drive.google.com/file/d/TU_FILE_ID/view?usp=sharing`
     - O simplemente: `TU_FILE_ID` (solo el ID)
   - `ipm_gcs_bucket` = `datalake_gdv` (opcional, tiene valor por defecto)
   - `ipm_gcs_folder` = `IPM` (opcional, tiene valor por defecto)
   - `ipm_drive_use_public_link` = `True` (opcional, por defecto es True)

**Opción B: Pasar parámetros al ejecutar el DAG**

Al ejecutar el DAG manualmente, puedes pasar parámetros:
```json
{
  "drive_url": "https://drive.google.com/file/d/TU_FILE_ID/view?usp=sharing",
  "destination_file_name": "2_IPM_DANE.xlsx",
  "bucket_name": "datalake_gdv",
  "folder_name": "IPM"
}
```

### 4. Ejecutar el DAG

1. Ve a la lista de DAGs en Airflow
2. Busca `gdv_upload_excel_from_drive_to_gcs_dag`
3. Activa el DAG (toggle)
4. Haz clic en "Play" para ejecutarlo manualmente

## Resultado

El archivo se subirá a:
```
gs://datalake_gdv/IPM/2_IPM_DANE.xlsx
```

## Ventajas de usar enlace público

✅ **No requiere autenticación**  
✅ **No necesitas compartir con Service Account**  
✅ **No necesitas credenciales OAuth**  
✅ **Configuración súper simple**  
✅ **Funciona inmediatamente**

## Ejemplo de URLs soportadas

El código acepta cualquiera de estos formatos:

- ✅ `https://drive.google.com/file/d/FILE_ID/view?usp=sharing`
- ✅ `https://drive.google.com/file/d/FILE_ID/view`
- ✅ `https://drive.google.com/open?id=FILE_ID`
- ✅ `FILE_ID` (solo el ID)

## Notas importantes

- ⚠️ El archivo **debe estar configurado como público** ("Cualquiera con el enlace")
- ⚠️ Si el archivo es muy grande (>100MB), Google Drive puede mostrar una advertencia, pero el código lo maneja automáticamente
- ✅ Puedes cambiar el nombre del archivo en GCS usando `destination_file_name`

## Solución de problemas

### Error: "No se pudo extraer el File ID"
- Verifica que la URL sea válida
- Puedes usar solo el File ID directamente

### Error: "Permission denied" o "Archivo no encontrado"
- Verifica que el archivo esté configurado como **"Cualquiera con el enlace"**
- Verifica que el enlace sea correcto

### Error: "No se pudo acceder al bucket"
- Verifica que el bucket `datalake_gdv` exista
- Verifica que la Service Account tenga permisos de escritura en el bucket

## ¿Necesitas usar autenticación?

Si prefieres no hacer el archivo público, puedes:
1. Configurar `ipm_drive_use_public_link` = `False`
2. Compartir el archivo con la Service Account: `dev-enigma@datagov-473122.iam.gserviceaccount.com`

Pero **recomiendo usar enlace público** porque es mucho más simple.

