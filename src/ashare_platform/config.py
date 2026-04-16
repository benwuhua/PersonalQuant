from __future__ import annotations

from pathlib import Path
import os
import yaml


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def resolve_config_path() -> Path:
    override = os.environ.get('PERSONALQUANT_CONFIG', '').strip()
    if override:
        return Path(override).expanduser().resolve()
    return project_root() / 'config' / 'config.yaml'


def load_config() -> dict:
    config_path = resolve_config_path()
    with config_path.open('r', encoding='utf-8') as f:
        data = yaml.safe_load(f)
    return data
