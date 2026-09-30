# Publicar dbt docs como sitio estático en un bucket GCS

Puedes subir la carpeta `dbt/target/` (generada con `dbt docs generate`) a un bucket de Google Cloud Storage y configurar el bucket para servir un sitio web estático.

---

## 1. Generar la documentación localmente

Desde la raíz del repo (`template-datastack`), con Poetry:

```powershell
poetry run dbt docs generate --project-dir gdv_general_dbt_dag/dbt --profiles-dir gdv_general_dbt_dag/dbt
```

La documentación quedará en: `gdv_general_dbt_dag/dbt/target/`  
(contiene `index.html`, `manifest.json`, `catalog.json` y archivos estáticos).

---

## 2. Crear un bucket (o usar uno existente)

- En **Google Cloud Console** → **Cloud Storage** → **Buckets**.
- **Crear bucket** (o elige uno que ya tengas para docs).
- Nombre único, región según tu proyecto.
- No es obligatorio hacer el bucket público; más adelante puedes usar **Load Balancer** o **Firebase Hosting** si quieres dominio propio y HTTPS. Para una URL pública simple, puedes permitir acceso público a los objetos (ver paso 4).

---

## 3. Subir la carpeta al bucket

**Opción A – Consola (arrastrar):**

1. Entra al bucket.
2. **Subir carpeta** (o subir archivos).
3. Sube **todo el contenido** de `gdv_general_dbt_dag/dbt/target/`.
4. Si quieres que la raíz del sitio sea la documentación, sube los archivos **en la raíz del bucket** o en una carpeta (ej. `dbt_docs/`). La URL final será algo como:
   - `https://storage.googleapis.com/MI_BUCKET/index.html` (si subiste en la raíz)
   - `https://storage.googleapis.com/MI_BUCKET/dbt_docs/index.html` (si subiste dentro de `dbt_docs/`).

**Opción B – gcloud (línea de comandos):**

Desde la raíz del repo, en PowerShell:

```powershell
cd gdv_general_dbt_dag/dbt
gcloud storage cp -r target/* gs://NOMBRE_DE_TU_BUCKET/dbt_docs/
```

(Sustituye `NOMBRE_DE_TU_BUCKET` por el nombre real del bucket.)

---

## 4. Configurar el bucket para sitio web estático

En GCS hay dos formas típicas de “sitio estático”:

### A) Página principal y 404 (recomendado para dbt docs)

1. En la consola: **Cloud Storage** → tu bucket → pestaña **Configuración**.
2. En **Sitio web** (o “Edit website configuration”):
   - **Página principal:** `index.html` (o `dbt_docs/index.html` si subiste dentro de esa carpeta).
   - **Página de error 404:** `index.html` (así los enlaces internos de la SPA de dbt no devuelven 404).

Sin Load Balancer, la URL del sitio suele ser:

```text
https://storage.googleapis.com/NOMBRE_BUCKET/dbt_docs/index.html
```

o, si configuraste la página principal:

```text
https://storage.googleapis.com/NOMBRE_BUCKET/dbt_docs/
```

### B) Acceso público a los objetos

Para que la URL anterior sea accesible sin autenticación:

1. **Permisos del bucket:** añade el principal `allUsers` con el rol **Storage Object Viewer** (solo si quieres que cualquiera pueda ver los docs).
2. O mantén el bucket privado y usa **IAM** para dar acceso solo a usuarios/grupos que deban ver la documentación.

---

## 5. Resumen rápido

| Paso | Acción |
|------|--------|
| 1 | `dbt docs generate` → archivos en `gdv_general_dbt_dag/dbt/target/` |
| 2 | Crear o elegir un bucket en GCS |
| 3 | Subir el contenido de `target/` al bucket (p. ej. prefijo `dbt_docs/`) |
| 4 | Configurar “Sitio web” del bucket: página principal y 404 = `index.html` (o `dbt_docs/index.html`) |
| 5 | (Opcional) Dar acceso público al bucket o a la carpeta `dbt_docs/` |

URL típica:

```text
https://storage.googleapis.com/TU_BUCKET/dbt_docs/index.html
```

(o solo `.../dbt_docs/` si definiste `index.html` como página principal).

---

## Nota

El DAG **src_dbt_docs_generate** del proyecto ya hace “generar + subir a un bucket” (según `config.yaml` → `dbt_docs` y `environments.*.dbt_docs_bucket_name`). Si quieres que ese mismo bucket actúe como sitio estático, solo necesitas configurar en el bucket la **página principal** y la **página de error** como arriba; no hace falta subir la carpeta a mano cada vez si usas el DAG.
