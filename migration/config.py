import json
from typing import Dict, Any

CONFIG_FILE = 'migration_config.json'


def save_config(config: Dict[str, Any], path: str = CONFIG_FILE):
    with open(path, 'w') as f:
        json.dump(config, f, indent=2)

def load_config(path: str = CONFIG_FILE) -> Dict[str, Any]:
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {} 