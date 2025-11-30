import yaml
import os
import sys
from types import SimpleNamespace

def find_config_file():
    """
    Busca el archivo config.yaml en múltiples ubicaciones posibles.
    Funciona tanto en desarrollo local como en Composer.
    """
    # Rutas posibles donde puede estar el config.yaml
    possible_paths = []
    
    # 1. Ruta relativa desde este archivo (desarrollo local)
    current_file_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(current_file_dir)
    possible_paths.append(os.path.join(base_dir, "config", "config.yaml"))
    
    # 2. Rutas comunes en Composer
    possible_paths.extend([
        "/home/airflow/gcs/dags/gdv_general_dbt_dag/config/config.yaml",
        os.path.join("/home/airflow/gcs/dags", "gdv_general_dbt_dag", "config", "config.yaml"),
    ])
    
    # 3. Buscar en sys.path (útil cuando se importa desde diferentes ubicaciones)
    for path in sys.path:
        if path and os.path.isdir(path):
            # Buscar en el path directamente
            possible_paths.append(os.path.join(path, "config", "config.yaml"))
            # Buscar en el padre del path
            parent = os.path.dirname(path)
            if parent and os.path.isdir(parent):
                possible_paths.append(os.path.join(parent, "config", "config.yaml"))
                # Buscar en gdv_general_dbt_dag dentro del path
                gdv_path = os.path.join(parent, "gdv_general_dbt_dag", "config", "config.yaml")
                possible_paths.append(gdv_path)
    
    # 4. Buscar recursivamente desde el directorio actual hacia arriba
    search_dir = current_file_dir
    for _ in range(5):  # Buscar hasta 5 niveles arriba
        config_path = os.path.join(search_dir, "config", "config.yaml")
        if config_path not in possible_paths:
            possible_paths.append(config_path)
        search_dir = os.path.dirname(search_dir)
        if search_dir == "/" or search_dir == search_dir:
            break
    
    # Intentar encontrar el archivo
    for config_path in possible_paths:
        if os.path.exists(config_path):
            print(f"DEBUG: Found config.yaml at: {config_path}")
            return config_path
    
    # Si no se encuentra, mostrar información de debugging
    print(f"ERROR: Configuration file NOT found in any of the following paths:")
    for path in possible_paths[:10]:  # Mostrar primeros 10
        print(f"  - {path}")
    
    raise FileNotFoundError(
        f"Configuration file (config.yaml) not found.\n"
        f"Please ensure config.yaml is uploaded to Composer at: /home/airflow/gcs/dags/gdv_general_dbt_dag/config/config.yaml\n"
        f"Searched in {len(possible_paths)} possible locations."
    )

def load_config(config_path=None):
    """
    Carga la configuración desde un archivo YAML y la convierte en un objeto
    con acceso por puntos (dot-notation).
    """
    if config_path is None:
        config_path = find_config_file()
        print(f"DEBUG: Using config path: {config_path}")
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found at {config_path}")
    
    print(f"DEBUG: Configuration file found at {config_path}")

    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)

    if config_dict is None:
        print(f"ERROR: Configuration file at {config_path} is empty or invalid YAML.")
        raise ValueError(f"Configuration file at {config_path} is empty or invalid YAML.")

    return _dict_to_namespace(config_dict)

def _dict_to_namespace(d):
    """
    Convierte recursivamente un diccionario en un SimpleNamespace para permitir
    acceso tipo objeto (config.seccion.valor).
    """
    if isinstance(d, dict):
        for k, v in d.items():
            d[k] = _dict_to_namespace(v)
        return SimpleNamespace(**d)
    elif isinstance(d, list):
        return [_dict_to_namespace(v) for v in d]
    else:
        return d

# Instancia global de configuración
config = load_config()
