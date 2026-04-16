
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.evaluation import run_backtest_evaluation
from ashare_platform.io_utils import ensure_dir
from ashare_platform.qlib_pipeline import FEATURE_COLS, build_training_artifacts, init_qlib


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    processed_dir = ensure_dir(ROOT / cfg['paths']['processed_dir'])
    init_qlib(cfg['qlib']['provider_uri'], cfg['qlib']['region'])
    model, train, valid, score = build_training_artifacts(cfg)
    scored = score.copy()
    scored['score'] = model.predict(scored[FEATURE_COLS])
    paths = run_backtest_evaluation(scored, outputs_dir, processed_dir, top_k=int(cfg['qlib'].get('top_k', 30)))
    for name, path in paths.items():
        print(f'{name} {path}')


if __name__ == '__main__':
    main()
