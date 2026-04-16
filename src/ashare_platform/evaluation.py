
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import write_json
from .qlib_pipeline import FEATURE_COLS


def rank_ic_by_date(scored_panel: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for dt, df in scored_panel.groupby('datetime'):
        sample = df[['score', 'label']].dropna()
        if len(sample) < 5:
            continue
        ic = sample['score'].rank().corr(sample['label'].rank())
        rows.append({'datetime': pd.Timestamp(dt).date().isoformat(), 'rank_ic': ic, 'sample_size': int(len(sample))})
    return pd.DataFrame(rows)


def grouped_future_returns(scored_panel: pd.DataFrame, groups: int = 5) -> pd.DataFrame:
    rows = []
    for dt, df in scored_panel.groupby('datetime'):
        sample = df[['instrument', 'score', 'label']].dropna().copy()
        if len(sample) < groups:
            continue
        sample['group'] = pd.qcut(sample['score'].rank(method='first'), groups, labels=False) + 1
        grp = sample.groupby('group')['label'].mean().reset_index()
        for _, row in grp.iterrows():
            rows.append({
                'datetime': pd.Timestamp(dt).date().isoformat(),
                'group': int(row['group']),
                'avg_future_return': float(row['label']),
                'group_size': int((sample['group'] == row['group']).sum()),
            })
    return pd.DataFrame(rows)


def topk_backtest_summary(scored_panel: pd.DataFrame, top_k: int = 30) -> dict[str, Any]:
    rows = []
    for dt, df in scored_panel.groupby('datetime'):
        sample = df[['instrument', 'score', 'label']].dropna().sort_values('score', ascending=False)
        top = sample.head(top_k)
        if top.empty:
            continue
        rows.append({'datetime': pd.Timestamp(dt).date().isoformat(), 'topk_avg_return': float(top['label'].mean())})
    top_df = pd.DataFrame(rows)
    if top_df.empty:
        return {'count': 0}
    return {
        'count': int(len(top_df)),
        'avg_topk_return': round(float(top_df['topk_avg_return'].mean()), 6),
        'positive_ratio': round(float((top_df['topk_avg_return'] > 0).mean()), 4),
        'best_day': round(float(top_df['topk_avg_return'].max()), 6),
        'worst_day': round(float(top_df['topk_avg_return'].min()), 6),
    }


def feature_snapshot(model) -> pd.DataFrame:
    return pd.DataFrame({'feature': FEATURE_COLS, 'importance': model.feature_importances_}).sort_values('importance', ascending=False)


def run_backtest_evaluation(score_panel: pd.DataFrame, outputs_dir: Path, processed_dir: Path, *, top_k: int = 30) -> dict[str, Path]:
    scored = score_panel.copy()
    scored = scored.dropna(subset=['score', 'label'])
    rank_ic = rank_ic_by_date(scored)
    group_returns = grouped_future_returns(scored)
    summary = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'rank_ic_mean': round(float(rank_ic['rank_ic'].mean()), 6) if not rank_ic.empty else None,
        'rank_ic_ir': round(float(rank_ic['rank_ic'].mean() / rank_ic['rank_ic'].std()), 6) if len(rank_ic) > 1 and rank_ic['rank_ic'].std() else None,
        'topk': topk_backtest_summary(scored, top_k=top_k),
    }

    rank_ic_path = processed_dir / 'rank_ic.csv'
    group_returns_path = processed_dir / 'group_returns.csv'
    summary_json_path = outputs_dir / 'backtest_summary.json'
    summary_md_path = outputs_dir / 'backtest_summary.md'

    rank_ic.to_csv(rank_ic_path, index=False)
    group_returns.to_csv(group_returns_path, index=False)
    write_json(summary_json_path, summary)

    lines = [
        '# Backtest Summary',
        '',
        f"- rank_ic_mean: {summary['rank_ic_mean']}",
        f"- rank_ic_ir: {summary['rank_ic_ir']}",
        f"- top{top_k}_avg_return: {summary['topk'].get('avg_topk_return')}",
        f"- top{top_k}_positive_ratio: {summary['topk'].get('positive_ratio')}",
        f"- top{top_k}_best_day: {summary['topk'].get('best_day')}",
        f"- top{top_k}_worst_day: {summary['topk'].get('worst_day')}",
        '',
    ]
    summary_md_path.write_text('\n'.join(lines), encoding='utf-8')
    return {
        'rank_ic_path': rank_ic_path,
        'group_returns_path': group_returns_path,
        'summary_json_path': summary_json_path,
        'summary_md_path': summary_md_path,
    }
