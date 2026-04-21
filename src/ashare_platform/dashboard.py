from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import list_recent_archives


ROOT = Path(__file__).resolve().parents[2]
OUTPUTS_DIR = ROOT / 'data' / 'outputs'
ARCHIVES_DIR = ROOT / 'data' / 'archives'
TIMELINES_DIR = OUTPUTS_DIR / 'timelines'


def _read_json(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open('r', encoding='utf-8') as f:
        data = json.load(f)
    return data if isinstance(data, list) else []


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


def _read_timeline_map(timelines_dir: Path) -> dict[str, list[dict[str, Any]]]:
    if not timelines_dir.exists():
        return {}
    timeline_map: dict[str, list[dict[str, Any]]] = {}
    for path in sorted(timelines_dir.glob('*.json')):
        try:
            with path.open('r', encoding='utf-8') as f:
                rows = json.load(f)
        except Exception:
            continue
        if isinstance(rows, list):
            timeline_map[path.stem] = rows
    return timeline_map


def _build_ops_snapshot(
    recent_archives: list[dict[str, Any]],
    strategy_validation_summary: dict[str, Any],
    backtest_summary: dict[str, Any],
    archive_diff: dict[str, Any],
) -> dict[str, Any]:
    compare = strategy_validation_summary.get('compare_priority_vs_quant_top10', {})
    latest_archive = recent_archives[0] if recent_archives else {}
    previous_archive = recent_archives[1] if len(recent_archives) > 1 else {}
    return {
        'latest_archive': latest_archive,
        'previous_archive': previous_archive,
        'validation_compare': {
            'days_compared': compare.get('days_compared', 0),
            'priority_win_days_1d': compare.get('priority_win_days_1d'),
            'priority_win_days_3d': compare.get('priority_win_days_3d'),
            'priority_win_days_5d': compare.get('priority_win_days_5d'),
            'avg_excess_delta_1d': compare.get('avg_excess_delta_1d'),
            'avg_excess_delta_3d': compare.get('avg_excess_delta_3d'),
            'avg_excess_delta_5d': compare.get('avg_excess_delta_5d'),
        },
        'backtest': {
            'rank_ic_mean': backtest_summary.get('rank_ic_mean'),
            'rank_ic_ir': backtest_summary.get('rank_ic_ir'),
            'topk_count': backtest_summary.get('topk', {}).get('count'),
            'topk_avg_return': backtest_summary.get('topk', {}).get('avg_topk_return'),
            'topk_positive_ratio': backtest_summary.get('topk', {}).get('positive_ratio'),
        },
        'archive_diff': {
            'latest_batch': archive_diff.get('latest_batch', ''),
            'previous_batch': archive_diff.get('previous_batch', ''),
            'new_priority_count': len(archive_diff.get('new_priority_entries', [])),
            'removed_priority_count': len(archive_diff.get('removed_priority_entries', [])),
            'new_risk_count': len(archive_diff.get('new_risk_entries', [])),
            'removed_risk_count': len(archive_diff.get('removed_risk_entries', [])),
        },
    }


def _format_int_list(values: list[Any]) -> str:
    cleaned = [str(int(value)) for value in values if value not in (None, '')]
    return ' / '.join(cleaned) if cleaned else 'n/a'


def _build_model_scanner(top30_candidates: list[dict[str, Any]]) -> dict[str, Any]:
    candidate_date = ''
    candidate_source = ''
    if top30_candidates:
        candidate_date = str(top30_candidates[0].get('datetime', ''))
        candidate_source = str(top30_candidates[0].get('candidate_source', ''))
    lines = [
        '# model-scanner',
        '',
        f'- total_candidates: {len(top30_candidates)}',
        f'- candidate_source: {candidate_source or "n/a"}',
        f'- candidate_date: {candidate_date or "n/a"}',
        '',
        '## Top Candidates',
    ]
    if not top30_candidates:
        lines.append('- none')
    else:
        for row in top30_candidates[:15]:
            lines.append(
                f"- rank={row.get('rank', '')} | {row.get('instrument', '')} | score={row.get('score', '')} | source={row.get('candidate_source', '')}"
            )
    return {
        'name': 'model-scanner',
        'rows': top30_candidates,
        'summary': {
            'total': len(top30_candidates),
            'candidate_source': candidate_source,
            'candidate_date': candidate_date,
            'top_instruments': [row.get('instrument', '') for row in top30_candidates[:10]],
        },
        'report': '\n'.join(lines),
    }


def _build_consolidation_breakout_scanner_payload() -> dict[str, Any]:
    summary = _read_json_object(OUTPUTS_DIR / 'consolidation-breakout-scanner_summary.json')
    if not summary:
        summary = _read_json_object(OUTPUTS_DIR / 'wangji-scanner_summary.json')
    relax_rows = _read_csv(OUTPUTS_DIR / 'consolidation-breakout-scanner_relax_candidates.csv')
    if not relax_rows:
        relax_rows = _read_csv(OUTPUTS_DIR / 'wangji-scanner_relax_candidates.csv')
    relax_report = _read_text(OUTPUTS_DIR / 'consolidation-breakout-scanner_relax_report.md')
    if not relax_report:
        relax_report = _read_text(OUTPUTS_DIR / 'wangji-scanner_relax_report.md')
    relax_summary = summary.get('relax', summary)
    payload = {
        'name': 'consolidation-breakout-scanner',
        'display_name': '盘整突破扫描器',
        'summary': {'relax': relax_summary},
        'relax': {
            'rows': relax_rows,
            'report': relax_report,
            'summary': relax_summary,
        },
    }
    return payload


def _build_multitask_label_overview(multitask_label_spec: dict[str, Any]) -> dict[str, Any]:
    tasks = multitask_label_spec.get('tasks', []) if isinstance(multitask_label_spec.get('tasks', []), list) else []
    group_counts: dict[str, int] = {}
    task_ids_by_group: dict[str, list[str]] = {}
    task_rows: list[dict[str, Any]] = []
    for task in tasks:
        if not isinstance(task, dict):
            continue
        group = str(task.get('group', 'other') or 'other')
        task_id = str(task.get('id', '') or '')
        group_counts[group] = group_counts.get(group, 0) + 1
        task_ids_by_group.setdefault(group, []).append(task_id)
        task_rows.append(
            {
                'id': task_id,
                'group': group,
                'label_key': str(task.get('label_key', '') or ''),
                'objective': str(task.get('objective', '') or ''),
                'horizon': task.get('horizon', ''),
                'threshold': task.get('threshold', ''),
            }
        )
    return {
        'task_count': len(task_rows),
        'group_counts': group_counts,
        'task_ids_by_group': task_ids_by_group,
        'task_rows': task_rows,
    }


def _build_multitask_training_linkage(
    multitask_label_spec: dict[str, Any],
    strategy_validation_summary: dict[str, Any],
) -> dict[str, Any]:
    primary_horizon = int(multitask_label_spec.get('primary_horizon', 0) or 0)
    training_label_key = f'excess_ret_{primary_horizon}_label' if primary_horizon > 0 else ''
    training_task_id = f'excess_return_{primary_horizon}d' if primary_horizon > 0 else ''
    task_lookup = {
        str(task.get('id', '') or ''): task
        for task in multitask_label_spec.get('tasks', [])
        if isinstance(task, dict)
    }
    training_task = task_lookup.get(training_task_id, {})
    return {
        'primary_horizon': primary_horizon,
        'training_label_key': training_label_key,
        'training_task_id': training_task_id,
        'training_objective': str(training_task.get('objective', '') or ''),
        'validation_metric_name': 'topk_avg_label_mean',
        'validation_metric_label_key': training_label_key,
        'fold_count': int(strategy_validation_summary.get('fold_count', 0) or 0),
        'rank_ic_mean': _to_float(strategy_validation_summary.get('rank_ic_mean', 0.0)),
        'topk_avg_label_mean': _to_float(strategy_validation_summary.get('topk_avg_label_mean', 0.0)),
        'generated_at': str(strategy_validation_summary.get('generated_at', '') or ''),
    }


def _build_multitask_task_status(
    multitask_label_overview: dict[str, Any],
    multitask_training_linkage: dict[str, Any],
) -> dict[str, Any]:
    training_task_id = str(multitask_training_linkage.get('training_task_id', '') or '')
    status_counts = {
        'training_primary': 0,
        'data_ready_not_training': 0,
        'auxiliary_diagnostic': 0,
    }
    task_rows: list[dict[str, Any]] = []
    for row in multitask_label_overview.get('task_rows', []):
        task = dict(row)
        task_id = str(task.get('id', '') or '')
        group = str(task.get('group', '') or '')
        if task_id == training_task_id:
            pipeline_status = 'training_primary'
        elif group in {'classification', 'risk', 'event'}:
            pipeline_status = 'auxiliary_diagnostic'
        else:
            pipeline_status = 'data_ready_not_training'
        status_counts[pipeline_status] += 1
        task['pipeline_status'] = pipeline_status
        task_rows.append(task)
    return {
        'status_counts': status_counts,
        'task_rows': task_rows,
    }


def build_dashboard_payload() -> dict[str, Any]:
    priority_candidates = _read_csv(OUTPUTS_DIR / 'priority_candidates.csv')
    risk_candidates = _read_csv(OUTPUTS_DIR / 'risk_candidates.csv')
    top30_candidates = _read_csv(OUTPUTS_DIR / 'top30_candidates.csv')
    event_cards = _read_json(OUTPUTS_DIR / 'event_cards.json')
    announcements = _read_json(OUTPUTS_DIR / 'announcements_raw.json')
    timeline_map = _read_timeline_map(TIMELINES_DIR)

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
                'timeline': timeline_map.get(instrument, []),
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
    model_validation_summary = _read_json_object(OUTPUTS_DIR / 'model_validation_summary.json')
    backtest_summary = _read_json_object(OUTPUTS_DIR / 'backtest_summary.json')
    archive_diff = _read_json_object(OUTPUTS_DIR / 'archive_diff.json')
    multitask_label_spec = _read_json_object(OUTPUTS_DIR / 'multitask_label_spec.json')
    ops = _build_ops_snapshot(recent_archives, strategy_validation_summary, backtest_summary, archive_diff)

    model_scanner = _build_model_scanner(top30_candidates)
    consolidation_breakout_scanner = _build_consolidation_breakout_scanner_payload()
    relax_summary = consolidation_breakout_scanner.get('relax', {}).get('summary', {})

    label_tasks = multitask_label_spec.get('tasks', []) if isinstance(multitask_label_spec.get('tasks', []), list) else []
    return_horizons = multitask_label_spec.get('return_horizons', []) if isinstance(multitask_label_spec.get('return_horizons', []), list) else []
    event_windows = multitask_label_spec.get('event_windows', []) if isinstance(multitask_label_spec.get('event_windows', []), list) else []
    multitask_label_overview = _build_multitask_label_overview(multitask_label_spec)
    multitask_training_linkage = _build_multitask_training_linkage(multitask_label_spec, model_validation_summary)
    multitask_task_status = _build_multitask_task_status(multitask_label_overview, multitask_training_linkage)

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
        'timeline_count': len(timeline_map),
        'validation_days_compared': ops['validation_compare'].get('days_compared', 0),
        'latest_batch': ops['latest_archive'].get('batch_name', ''),
        'model_scanner_count': model_scanner['summary'].get('total', 0),
        'consolidation_breakout_relax_passed': relax_summary.get('passed', 0),
        'wangji_relax_passed': relax_summary.get('passed', 0),
        'multitask_task_count': len(label_tasks),
        'label_primary_horizon': int(multitask_label_spec.get('primary_horizon', 0) or 0),
        'label_return_horizons': _format_int_list(return_horizons),
        'label_event_windows': _format_int_list(event_windows),
        'label_hit_threshold': _to_float(multitask_label_spec.get('hit_threshold', 0.0)),
        'label_risk_threshold': _to_float(multitask_label_spec.get('risk_threshold', 0.0)),
    }

    return {
        'summary': summary,
        'quant_pipeline_snapshot': _read_json_object(OUTPUTS_DIR / 'quant_pipeline_snapshot.json'),
        'quant_pipeline_blueprint': _read_text(OUTPUTS_DIR / 'quant_pipeline_blueprint.md'),
        'multitask_label_spec': multitask_label_spec,
        'multitask_label_overview': multitask_label_overview,
        'multitask_training_linkage': multitask_training_linkage,
        'multitask_task_status': multitask_task_status,
        'priority_candidates': priority_candidates,
        'risk_candidates': risk_candidates,
        'top30_candidates': top30_candidates,
        'event_cards': event_cards,
        'instrument_details': instrument_details,
        'recent_archives': recent_archives,
        'strategy_validation_summary': strategy_validation_summary,
        'backtest_summary': backtest_summary,
        'archive_diff': archive_diff,
        'ops': ops,
        'screeners': {
            'model_scanner': model_scanner,
            'consolidation_breakout_scanner': consolidation_breakout_scanner,
            'wangji_scanner': consolidation_breakout_scanner,
        },
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
