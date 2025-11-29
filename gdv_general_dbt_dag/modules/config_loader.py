import yaml
import os
from types import SimpleNamespace

def load_config(config_path=None):
    """
    Carga la configuración desde un archivo YAML y la convierte en un objeto
    con acceso por puntos (dot-notation).
    """
    if config_path is None:
        # Asume que config.yaml está en ../config/config.yaml relativo a este archivo
        base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(base_dir, "config", "config.yaml")
        print(f"DEBUG: Calculated config path: {config_path}")

    if not os.path.exists(config_path):
        print(f"ERROR: Configuration file NOT found at {config_path}")
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
# Instancia global de configuración
config = load_config()
