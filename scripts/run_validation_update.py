
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

import pandas as pd

from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir
from ashare_platform.validation import update_validation_artifacts


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    validation_dir = ensure_dir(ROOT / cfg['paths'].get('validation_dir', 'data/validation'))
    topk = pd.read_csv(outputs_dir / 'top30_candidates.csv')
    priority = pd.read_csv(outputs_dir / 'priority_candidates.csv')
    risk_path = outputs_dir / 'risk_candidates.csv'
    risk = pd.read_csv(risk_path) if risk_path.exists() else pd.DataFrame()
    paths = update_validation_artifacts(validation_dir, outputs_dir, topk, priority, risk)
    for name, path in paths.items():
        print(f'{name} {path}')


if __name__ == '__main__':
    main()
