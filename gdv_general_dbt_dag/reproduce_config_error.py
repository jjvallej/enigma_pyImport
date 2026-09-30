import sys
import os

# Add current directory to sys.path
sys.path.insert(0, os.getcwd())

try:
    from modules.config_loader import config
    print(f"Config type: {type(config)}")
    print(f"Config content: {config}")
    
    if config is None:
        print("ERROR: Config is None!")
    elif not hasattr(config, 'environments'):
        print("ERROR: Config has no 'environments' attribute!")
    else:
        print("SUCCESS: Config loaded correctly.")
        print(f"Environments: {config.environments}")

except Exception as e:
    print(f"CRITICAL ERROR: {e}")
    import traceback
    traceback.print_exc()
