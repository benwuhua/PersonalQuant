from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir, write_json
from ashare_platform.qlib_pipeline import build_feature_importance, build_training_artifacts, init_qlib, save_model_artifact


MODEL_FILENAME = 'lightgbm_model.pkl'


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    processed_dir = ensure_dir(ROOT / cfg['paths']['processed_dir'])

    provider_uri = init_qlib(cfg['qlib']['provider_uri'], cfg['qlib']['region'])
    print('provider_uri', provider_uri, flush=True)

    model, train, valid, score, walk_forward_summary = build_training_artifacts(cfg)
    print('train_rows', len(train), 'valid_rows', len(valid), 'historical_score_rows', len(score), flush=True)

    model_path = save_model_artifact(model, processed_dir / MODEL_FILENAME)
    print('model_artifact_path', model_path, flush=True)

    importance = build_feature_importance(model)
    importance_path = processed_dir / 'feature_importance.csv'
    importance.to_csv(importance_path, index=False)
    print('feature_importance_path', importance_path, flush=True)
    print(importance.head(10).to_string(index=False), flush=True)

    walk_forward_summary_path = outputs_dir / 'model_validation_summary.json'
    write_json(walk_forward_summary_path, walk_forward_summary)
    print('model_validation_summary_path', walk_forward_summary_path, flush=True)
    print('walk_forward_rank_ic_mean', walk_forward_summary.get('rank_ic_mean', 0.0), flush=True)
    print('walk_forward_topk_avg_label_mean', walk_forward_summary.get('topk_avg_label_mean', 0.0), flush=True)


if __name__ == '__main__':
    main()
