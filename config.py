import os
import yaml
from dataclasses import dataclass

CONFIG_FILE = 'config.yaml'

@dataclass
class Config:
    include_subfolders: bool = False
    max_concurrent_tasks: int = 5
    language: str = 'auto'
    default_directory: str = ''
    watch_directory: bool = False

def load_config() -> Config:
    if not os.path.exists(CONFIG_FILE):
        return Config()
    with open(CONFIG_FILE, 'r') as f:
        config_data = yaml.safe_load(f)
    return Config(**config_data)

def save_config(config: Config):
    with open(CONFIG_FILE, 'w') as f:
        yaml.dump(config.__dict__, f)
