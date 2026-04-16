
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import ensure_dir, list_recent_archives, write_json


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path).fillna('')


def build_archive_diff(archives_dir: Path) -> dict[str, Any]:
    manifests = list_recent_archives(archives_dir, limit=2)
    if len(manifests) < 2:
        return {'status': 'not_enough_archives'}
    latest, previous = manifests[0], manifests[1]
    latest_dir = archives_dir / latest['batch_name']
    previous_dir = archives_dir / previous['batch_name']
    latest_priority = _read_csv(latest_dir / 'priority_candidates.csv')
    previous_priority = _read_csv(previous_dir / 'priority_candidates.csv')
    latest_risk = _read_csv(latest_dir / 'risk_candidates.csv')
    previous_risk = _read_csv(previous_dir / 'risk_candidates.csv')

    latest_set = set(latest_priority.get('instrument', pd.Series(dtype=str)).astype(str))
    previous_set = set(previous_priority.get('instrument', pd.Series(dtype=str)).astype(str))
    latest_risk_set = set(latest_risk.get('instrument', pd.Series(dtype=str)).astype(str))
    previous_risk_set = set(previous_risk.get('instrument', pd.Series(dtype=str)).astype(str))

    diff_rows = []
    if not latest_priority.empty and not previous_priority.empty:
        merged = latest_priority[['instrument', 'priority_rank']].merge(
            previous_priority[['instrument', 'priority_rank']], on='instrument', how='inner', suffixes=('_latest', '_prev')
        )
        merged['rank_change'] = pd.to_numeric(merged['priority_rank_prev']) - pd.to_numeric(merged['priority_rank_latest'])
        diff_rows = merged.sort_values('rank_change', ascending=False).head(10).to_dict(orient='records')

    return {
        'latest_batch': latest['batch_name'],
        'previous_batch': previous['batch_name'],
        'new_priority_entries': sorted(latest_set - previous_set),
        'removed_priority_entries': sorted(previous_set - latest_set),
        'new_risk_entries': sorted(latest_risk_set - previous_risk_set),
        'removed_risk_entries': sorted(previous_risk_set - latest_risk_set),
        'top_rank_improvers': diff_rows,
    }


def write_archive_diff(archives_dir: Path, output_dir: Path) -> dict[str, Path]:
    ensure_dir(output_dir)
    diff = build_archive_diff(archives_dir)
    json_path = output_dir / 'archive_diff.json'
    md_path = output_dir / 'archive_diff.md'
    write_json(json_path, diff)
    lines = ['# Archive Diff', '']
    for key in ['latest_batch', 'previous_batch']:
        if key in diff:
            lines.append(f'- {key}: {diff[key]}')
    for key in ['new_priority_entries', 'removed_priority_entries', 'new_risk_entries', 'removed_risk_entries']:
        lines.append(f'- {key}: {diff.get(key, [])}')
    if diff.get('top_rank_improvers'):
        lines.append('')
        lines.append('## Top Rank Improvers')
        for row in diff['top_rank_improvers']:
            lines.append(f"- {row.get('instrument')}: rank_change={row.get('rank_change')}, latest={row.get('priority_rank_latest')}, prev={row.get('priority_rank_prev')}")
    md_path.write_text('\n'.join(lines), encoding='utf-8')
    return {'json_path': json_path, 'md_path': md_path}
