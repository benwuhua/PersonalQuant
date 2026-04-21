from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.qlib_pipeline import FEATURE_COLS, build_panel, init_qlib, split_panel, train_model


def main() -> None:
    cfg = load_config()
    init_qlib(cfg['qlib']['provider_uri'], cfg['qlib']['region'])
    panel = build_panel(cfg)
    model_cfg = {
        **cfg['model'],
        'train_start': '2017-01-01',
        'train_end': '2020-12-31',
        'valid_start': '2021-01-01',
        'valid_end': '2021-12-31',
        'score_start': '2021-01-01',
        'score_end': '2021-12-31',
    }
    train, valid, score = split_panel(panel, model_cfg)
    print('train', len(train), 'valid', len(valid), 'score', len(score), flush=True)
    print('score_label_nulls', int(score['label'].isna().sum()), flush=True)
    print('score_dates', score['datetime'].min(), score['datetime'].max(), 'date_n', score['datetime'].nunique(), flush=True)
    model = train_model(train, valid, cfg['model'])
    scored = score[['datetime', 'instrument', 'label'] + FEATURE_COLS].copy()
    scored['score'] = model.predict(scored[FEATURE_COLS])
    scored = scored.dropna(subset=['score', 'label']).copy()
    print('after_dropna', len(scored), 'date_n', scored['datetime'].nunique(), flush=True)
    group_sizes = scored.groupby('datetime').size()
    print('group_size_min', int(group_sizes.min()), 'group_size_median', int(group_sizes.median()), 'group_size_max', int(group_sizes.max()), flush=True)
    topk = scored.groupby('datetime', group_keys=False).apply(lambda g: g.sort_values('score', ascending=False).head(int(cfg['qlib'].get('top_k', 30))))
    print('topk_rows', len(topk), flush=True)
    print('topk_label_nulls', int(topk['label'].isna().sum()), flush=True)
    print('topk_label_mean', topk['label'].mean(), flush=True)


if __name__ == '__main__':
    main()
