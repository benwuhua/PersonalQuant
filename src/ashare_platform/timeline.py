
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .io_utils import ensure_dir, list_recent_archives, write_json


def _load_json(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def _load_priority_rows(archive_dir: Path) -> list[dict[str, Any]]:
    path = archive_dir / 'priority_candidates.csv'
    if not path.exists():
        return []
    import pandas as pd
    df = pd.read_csv(path)
    return df.fillna('').to_dict(orient='records')


def build_instrument_timeline(archives_dir: Path, instrument: str, limit: int = 20) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for manifest in list_recent_archives(archives_dir, limit=200):
        archive_dir = archives_dir / manifest['batch_name']
        for item in _load_priority_rows(archive_dir):
            if str(item.get('instrument', '')) != instrument:
                continue
            rows.append({
                'run_batch': manifest['batch_name'],
                'run_date': manifest.get('archived_at', ''),
                'priority_rank': item.get('priority_rank', ''),
                'priority_score': item.get('priority_score', ''),
                'quant_score_norm': item.get('quant_score_norm', ''),
                'event_score': item.get('event_score', ''),
                'top_event_type': item.get('top_event_type', ''),
                'top_event_title': item.get('top_event_title', ''),
                'top_event_summary': item.get('top_event_summary', ''),
            })
        risk_path = archive_dir / 'risk_candidates.csv'
        if risk_path.exists():
            import pandas as pd
            risk_df = pd.read_csv(risk_path).fillna('')
            match = risk_df[risk_df['instrument'].astype(str) == instrument]
            if not match.empty:
                risk_row = match.iloc[0].to_dict()
                if rows:
                    rows[-1]['risk_rank'] = risk_row.get('risk_rank', '')
                    rows[-1]['risk_attention_score'] = risk_row.get('risk_attention_score', '')
                    rows[-1]['top_risk_title'] = risk_row.get('top_risk_title', '')
    return rows[:limit]


def write_instrument_timeline(archives_dir: Path, output_dir: Path, instrument: str, limit: int = 20) -> dict[str, Path]:
    ensure_dir(output_dir)
    rows = build_instrument_timeline(archives_dir, instrument, limit=limit)
    json_path = output_dir / f'{instrument}.json'
    md_path = output_dir / f'{instrument}.md'
    write_json(json_path, rows)
    lines = [f'# Timeline - {instrument}', '']
    if not rows:
        lines.append('- no archive history found')
    for row in rows:
        lines.extend([
            f"## {row.get('run_batch', '')}",
            f"- run_date: {row.get('run_date', '')}",
            f"- priority_rank: {row.get('priority_rank', '')}",
            f"- priority_score: {row.get('priority_score', '')}",
            f"- event_score: {row.get('event_score', '')}",
            f"- top_event_type: {row.get('top_event_type', '')}",
            f"- top_event_title: {row.get('top_event_title', '')}",
            f"- risk_rank: {row.get('risk_rank', '')}",
            f"- risk_attention_score: {row.get('risk_attention_score', '')}",
            '',
        ])
    md_path.write_text('\n'.join(lines), encoding='utf-8')
    return {'json_path': json_path, 'md_path': md_path}
