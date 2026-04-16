from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import list_recent_archives


ROOT = Path(__file__).resolve().parents[2]
OUTPUTS_DIR = ROOT / 'data' / 'outputs'
ARCHIVES_DIR = ROOT / 'data' / 'archives'


def _read_json(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8') as f:
        return json.load(f)


def _read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    df = pd.read_csv(path)
    return df.fillna('').to_dict(orient='records')


def _read_text(path: Path) -> str:
    if not path.exists():
        return ''
    return path.read_text(encoding='utf-8')


def _read_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, dict) else {}


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value == '':
            return default
        return float(value)
    except Exception:
        return default


def _top_items(rows: list[dict[str, Any]], score_key: str, limit: int = 5) -> list[dict[str, Any]]:
    return sorted(rows, key=lambda x: _to_float(x.get(score_key, 0.0)), reverse=True)[:limit]


def build_dashboard_payload() -> dict[str, Any]:
    priority_candidates = _read_csv(OUTPUTS_DIR / 'priority_candidates.csv')
    risk_candidates = _read_csv(OUTPUTS_DIR / 'risk_candidates.csv')
    top30_candidates = _read_csv(OUTPUTS_DIR / 'top30_candidates.csv')
    event_cards = _read_json(OUTPUTS_DIR / 'event_cards.json')
    announcements = _read_json(OUTPUTS_DIR / 'announcements_raw.json')

    priority_by_instrument = {row.get('instrument', ''): row for row in priority_candidates}
    risk_by_instrument = {row.get('instrument', ''): row for row in risk_candidates}

    instruments = sorted(
        {row.get('instrument', '') for row in priority_candidates + risk_candidates + event_cards if row.get('instrument', '')}
    )

    instrument_details = []
    for instrument in instruments:
        cards = [card for card in event_cards if card.get('instrument') == instrument]
        cards = sorted(cards, key=lambda x: _to_float(x.get('card_score', 0.0)), reverse=True)
        raw_announcements = [item for item in announcements if item.get('instrument') == instrument]
        instrument_details.append(
            {
                'instrument': instrument,
                'priority': priority_by_instrument.get(instrument, {}),
                'risk': risk_by_instrument.get(instrument, {}),
                'event_cards': cards,
                'announcements': raw_announcements,
                'event_count': len(cards),
                'has_risk': instrument in risk_by_instrument,
            }
        )

    content_source_counts: dict[str, int] = {}
    quality_buckets = {'high_quality': 0, 'fallback_or_low_quality': 0}
    for item in announcements:
        source = item.get('content_source', 'unknown') or 'unknown'
        content_source_counts[source] = content_source_counts.get(source, 0) + 1
        if _to_float(item.get('content_quality_score', 0.0)) >= 0.45 and source == 'pdf_excerpt':
            quality_buckets['high_quality'] += 1
        else:
            quality_buckets['fallback_or_low_quality'] += 1

    recent_archives = list_recent_archives(ARCHIVES_DIR, limit=8)
    strategy_validation_summary = _read_json_object(OUTPUTS_DIR / 'strategy_validation_summary.json')
    backtest_summary = _read_json_object(OUTPUTS_DIR / 'backtest_summary.json')
    archive_diff = _read_json_object(OUTPUTS_DIR / 'archive_diff.json')
    summary = {
        'priority_count': len(priority_candidates),
        'risk_count': len(risk_candidates),
        'event_card_count': len(event_cards),
        'announcement_count': len(announcements),
        'pdf_excerpt_count': content_source_counts.get('pdf_excerpt', 0),
        'title_only_count': content_source_counts.get('title_only', 0),
        'high_quality_excerpt_count': quality_buckets['high_quality'],
        'fallback_or_low_quality_count': quality_buckets['fallback_or_low_quality'],
        'archive_count': len(recent_archives),
    }

    return {
        'summary': summary,
        'priority_candidates': priority_candidates,
        'risk_candidates': risk_candidates,
        'top30_candidates': top30_candidates,
        'event_cards': event_cards,
        'instrument_details': instrument_details,
        'recent_archives': recent_archives,
        'strategy_validation_summary': strategy_validation_summary,
        'backtest_summary': backtest_summary,
        'archive_diff': archive_diff,
        'daily_watchlist': _read_text(OUTPUTS_DIR / 'daily_watchlist.md'),
        'weekly_watchlist': _read_text(OUTPUTS_DIR / 'weekly_watchlist.md'),
        'risk_watchlist': _read_text(OUTPUTS_DIR / 'risk_watchlist.md'),
        'strategy_validation_report': _read_text(OUTPUTS_DIR / 'strategy_validation_report.md'),
        'backtest_report': _read_text(OUTPUTS_DIR / 'backtest_summary.md'),
        'archive_diff_report': _read_text(OUTPUTS_DIR / 'archive_diff.md'),
        'highlights': {
            'priority_top5': _top_items(priority_candidates, 'priority_score', limit=5),
            'risk_top5': _top_items(risk_candidates, 'risk_attention_score', limit=5),
            'event_top10': _top_items(event_cards, 'card_score', limit=10),
        },
    }


def write_dashboard_payload(path: Path | None = None) -> Path:
    output_path = path or (OUTPUTS_DIR / 'dashboard_data.json')
    payload = build_dashboard_payload()
    output_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
    return output_path
