# 🎯 Guía: Configuración de Environments (dev, prod, local)

## 📖 ¿Qué son los Environments?

Los **environments** (entornos) te permiten tener **configuraciones diferentes** para desarrollo, producción y local, **todo en un solo archivo** `config.yaml`. 

El sistema automáticamente selecciona la configuración correcta según el environment que estés usando.

## 🔧 ¿Cómo se Configura?

### 1. En `config.yaml` (Ya está hecho)

Los environments están definidos en la sección `environments`:

```yaml
environments:
  dev:
    project_id: "datagov-473122"
    location: "us-central1"
    bucket_name: "datalake_gdv_dev"
    dataset_bronze: "bronze_dpt_planeacion_municipal_dev"
    dataset_silver: "silver_dpt_planeacion_municipal_dev"
    dataset_gold: "gold_dpt_planeacion_municipal_dev"
  prod:
    project_id: "datagov-473122"
    location: "us-central1"
    bucket_name: "datalake_gdv_prod"
    dataset_bronze: "bronze_dpt_planeacion_municipal_prod"
    dataset_silver: "silver_dpt_planeacion_municipal_prod"
    dataset_gold: "gold_dpt_planeacion_municipal_prod"
  local:
    project_id: "datagov-473122"
    location: "us-central1"
    bucket_name: "datalake_gdv_dev"
    dataset_bronze: "bronze_dpt_planeacion_municipal_dev"
    dataset_silver: "silver_dpt_planeacion_municipal_dev"
    dataset_gold: "gold_dpt_planeacion_municipal_dev"
```

### 2. Seleccionar el Environment

El environment se selecciona mediante una **variable de entorno** llamada `ENVIRONMENT`:

- `ENVIRONMENT=dev` → Usa la configuración de `dev`
- `ENVIRONMENT=prod` → Usa la configuración de `prod`
- `ENVIRONMENT=local` → Usa la configuración de `local`
- Si no se define → **Por defecto usa `dev`** (seguro)

## 🚀 ¿Cómo Configurarlo en Diferentes Ambientes?

### Opción 1: En Google Cloud Composer (Producción)

#### Método A: Variables de Entorno de Composer

1. Ve a **Composer** en Google Cloud Console
2. Selecciona tu ambiente de Composer
3. Ve a **Environment Variables** o **Airflow Config Overrides**
4. Agrega:
   ```
   ENVIRONMENT=prod
   ```

#### Método B: En el DAG (No recomendado, pero posible)

```python
import os
os.environ["ENVIRONMENT"] = "prod"  # Antes de importar modules.config
```

### Opción 2: Desarrollo Local (Tu Computadora)

#### En Windows (PowerShell):
```powershell
$env:ENVIRONMENT="local"
python tu_script.py
```

#### En Windows (CMD):
```cmd
set ENVIRONMENT=local
python tu_script.py
```

#### En Linux/Mac:
```bash
export ENVIRONMENT=local
python tu_script.py
```

#### En un script Python:
```python
import os
os.environ["ENVIRONMENT"] = "local"
# Ahora importa tus módulos
from modules.config import PROJECT_ID, DEFAULT_BUCKET_NAME
```

### Opción 3: En Docker (Si usas Docker)

En tu `docker-compose.yml`:
```yaml
services:
  airflow:
    environment:
      - ENVIRONMENT=dev
```

## 📊 ¿Cómo Funciona Internamente?

El código en `modules/config.py` hace esto:

```python
# 1. Lee la variable de entorno (o usa 'dev' por defecto)
ENV = os.getenv("ENVIRONMENT", "dev")

# 2. Selecciona la configuración del environment
try:
    current_config = getattr(CONF.environments, ENV)
except AttributeError:
    # Si no existe, usa 'dev' como fallback
    current_config = CONF.environments.dev

# 3. Usa esa configuración para todo
PROJECT_ID = current_config.project_id
DEFAULT_BUCKET_NAME = current_config.bucket_name
DATASET_ID_BRONZE = current_config.dataset_bronze
# etc...
```

## 🎯 Ejemplos Prácticos

### Ejemplo 1: Desarrollo en Composer

**Configuración:**
- Variable de entorno en Composer: `ENVIRONMENT=dev`
- Resultado: Los DAGs usan:
  - Bucket: `datalake_gdv_dev`
  - Datasets: `bronze_dpt_planeacion_municipal_dev`, `silver_...`, `gold_...`

### Ejemplo 2: Producción en Composer

**Configuración:**
- Variable de entorno en Composer: `ENVIRONMENT=prod`
- Resultado: Los DAGs usan:
  - Bucket: `datalake_gdv_prod`
  - Datasets: `bronze_dpt_planeacion_municipal_prod`, `silver_...`, `gold_...`

### Ejemplo 3: Desarrollo Local

**Configuración:**
```bash
export ENVIRONMENT=local
python -m pytest tests/
```

**Resultado:** Usa la configuración de `local` (generalmente igual a `dev`)

## ✅ Verificar qué Environment está Activo

Puedes agregar esto temporalmente en un DAG para verificar:

```python
from modules.config import ENV, PROJECT_ID, DEFAULT_BUCKET_NAME, DATASET_ID_BRONZE

def print_config(**context):
    print(f"[DEBUG] Environment activo: {ENV}")
    print(f"[DEBUG] Project ID: {PROJECT_ID}")
    print(f"[DEBUG] Bucket: {DEFAULT_BUCKET_NAME}")
    print(f"[DEBUG] Dataset Bronze: {DATASET_ID_BRONZE}")

# En tu DAG
debug_task = PythonOperator(
    task_id="debug_config",
    python_callable=print_config
)
```

## 🔄 Flujo Completo

```
1. Airflow inicia
   ↓
2. Lee variable de entorno ENVIRONMENT (o usa 'dev')
   ↓
3. modules/config.py lee config.yaml
   ↓
4. Selecciona la sección environments.{ENV}
   ↓
5. Todos los módulos usan esa configuración:
   - PROJECT_ID
   - DEFAULT_BUCKET_NAME
   - DATASET_ID_BRONZE/SILVER/GOLD
   ↓
6. Los DAGs ejecutan con esa configuración
   ↓
7. dbt también recibe esas variables automáticamente
```

## ⚠️ Importante

1. **Por defecto usa `dev`** si no configuras `ENVIRONMENT` (seguro)
2. **Solo necesitas configurar UNA variable de entorno**: `ENVIRONMENT`
3. **Todo lo demás se lee automáticamente** de `config.yaml`
4. **No necesitas cambiar código** - solo la variable de entorno

## 🎓 Casos de Uso

### Caso 1: Mismo Proyecto, Diferentes Buckets/Datasets

```yaml
environments:
  dev:
    project_id: "mi-proyecto"  # Mismo proyecto
    bucket_name: "mi-bucket-dev"
    dataset_bronze: "bronze_dev"
  prod:
    project_id: "mi-proyecto"  # Mismo proyecto
    bucket_name: "mi-bucket-prod"
    dataset_bronze: "bronze_prod"
```

### Caso 2: Proyectos Completamente Diferentes

```yaml
environments:
  dev:
    project_id: "mi-proyecto-dev"  # Proyecto diferente
    bucket_name: "mi-bucket-dev"
  prod:
    project_id: "mi-proyecto-prod"  # Proyecto diferente
    bucket_name: "mi-bucket-prod"
```

### Caso 3: Desarrollo Local con Credenciales Locales

```yaml
environments:
  local:
    project_id: "mi-proyecto-dev"
    bucket_name: "mi-bucket-dev"
    # Usa GOOGLE_APPLICATION_CREDENTIALS del sistema
```

## 📝 Resumen

| Ambiente | Variable de Entorno | Uso |
|----------|---------------------|-----|
| **Desarrollo** | `ENVIRONMENT=dev` | Desarrollo en Composer |
| **Producción** | `ENVIRONMENT=prod` | Producción en Composer |
| **Local** | `ENVIRONMENT=local` | Desarrollo en tu computadora |
| **Por defecto** | (no configurar) | Usa `dev` automáticamente |

**¡Solo configura `ENVIRONMENT` y todo funciona automáticamente!** 🎉

