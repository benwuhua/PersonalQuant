from __future__ import annotations

from pathlib import Path
import yaml


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def load_config() -> dict:
    config_path = project_root() / 'config' / 'config.yaml'
    with config_path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return data
