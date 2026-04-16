
from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import akshare as ak
except Exception:
    ak = None

from .io_utils import ensure_dir, write_json

SNAPSHOT_COLUMNS = [
    'run_date', 'list_type', 'rank', 'instrument', 'candidate_source', 'datetime', 'raw_score',
    'quant_score_norm', 'event_score', 'priority_score', 'risk_attention_score', 'risk_event_score',
    'top_event_type', 'top_event_title', 'top_event_bias', 'top_event_importance', 'top_event_confidence',
    'top_risk_event_type', 'top_risk_title', 'top_risk_bias', 'top_risk_importance', 'top_risk_confidence',
]


def _instrument_to_code(instrument: str) -> str:
    return ''.join(ch for ch in str(instrument) if ch.isdigit())


def _code_to_tx_symbol(stock_code: str) -> str:
    return f"sh{stock_code}" if stock_code.startswith('6') else f"sz{stock_code}"


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value in {'', None}:
            return default
        return float(value)
    except Exception:
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value in {'', None}:
            return default
        return int(float(value))
    except Exception:
        return default


def _normalize_dates(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors='coerce').dt.tz_localize(None)


def build_validation_snapshot(
    topk: pd.DataFrame,
    priority_candidates: pd.DataFrame,
    risk_candidates: pd.DataFrame,
    *,
    run_date: str | None = None,
) -> pd.DataFrame:
    date_value = run_date or datetime.now().date().isoformat()
    frames: list[pd.DataFrame] = []

    quant = topk.copy()
    if not quant.empty:
        quant['run_date'] = date_value
        quant['list_type'] = 'quant'
        quant['raw_score'] = quant['score']
        quant['rank'] = quant['rank'].astype(int)
        quant['quant_score_norm'] = pd.NA
        quant['event_score'] = pd.NA
        quant['priority_score'] = pd.NA
        quant['risk_attention_score'] = pd.NA
        quant['risk_event_score'] = pd.NA
        quant['top_event_type'] = ''
        quant['top_event_title'] = ''
        quant['top_event_bias'] = ''
        quant['top_event_importance'] = ''
        quant['top_event_confidence'] = ''
        quant['top_risk_event_type'] = ''
        quant['top_risk_title'] = ''
        quant['top_risk_bias'] = ''
        quant['top_risk_importance'] = ''
        quant['top_risk_confidence'] = ''
        frames.append(quant[SNAPSHOT_COLUMNS])

    priority = priority_candidates.copy()
    if not priority.empty:
        priority['run_date'] = date_value
        priority['list_type'] = 'priority'
        priority['raw_score'] = priority['score']
        priority['rank'] = priority['priority_rank'].astype(int)
        priority['risk_attention_score'] = priority.get('risk_attention_score', pd.NA)
        priority['risk_event_score'] = priority.get('risk_event_score', pd.NA)
        priority['top_risk_event_type'] = priority.get('top_risk_event_type', '')
        priority['top_risk_title'] = priority.get('top_risk_title', '')
        priority['top_risk_bias'] = priority.get('top_risk_bias', '')
        priority['top_risk_importance'] = priority.get('top_risk_importance', '')
        priority['top_risk_confidence'] = priority.get('top_risk_confidence', '')
        frames.append(priority[SNAPSHOT_COLUMNS])

    risk = risk_candidates.copy()
    if not risk.empty:
        risk['run_date'] = date_value
        risk['list_type'] = 'risk'
        risk['raw_score'] = risk['score']
        risk['rank'] = risk['risk_rank'].astype(int)
        risk['event_score'] = risk.get('event_score', pd.NA)
        risk['priority_score'] = risk.get('priority_score', pd.NA)
        risk['top_event_type'] = risk.get('top_event_type', '')
        risk['top_event_title'] = risk.get('top_event_title', '')
        risk['top_event_bias'] = risk.get('top_event_bias', '')
        risk['top_event_importance'] = risk.get('top_event_importance', '')
        risk['top_event_confidence'] = risk.get('top_event_confidence', '')
        frames.append(risk[SNAPSHOT_COLUMNS])

    if not frames:
        return pd.DataFrame(columns=SNAPSHOT_COLUMNS)

    snapshot = pd.concat(frames, ignore_index=True)
    snapshot['datetime'] = _normalize_dates(snapshot['datetime']).dt.date.astype(str)
    return snapshot


def append_validation_snapshot(snapshot: pd.DataFrame, path: Path) -> pd.DataFrame:
    ensure_dir(path.parent)
    if path.exists():
        existing = pd.read_csv(path)
        merged = pd.concat([existing, snapshot], ignore_index=True)
        merged = merged.drop_duplicates(subset=['run_date', 'list_type', 'instrument', 'rank'], keep='last')
    else:
        merged = snapshot.copy()
    merged = merged.sort_values(['run_date', 'list_type', 'rank', 'instrument']).reset_index(drop=True)
    merged.to_csv(path, index=False)
    return merged


def _fetch_stock_forward_returns(instrument: str, start_date: str) -> dict[str, float | str]:
    if ak is None:
        return {'price_status': 'akshare_unavailable'}
    stock_code = _instrument_to_code(instrument)
    symbol = _code_to_tx_symbol(stock_code)
    start_fmt = pd.Timestamp(start_date).strftime('%Y-%m-%d')
    end_fmt = (pd.Timestamp(start_date) + pd.Timedelta(days=20)).strftime('%Y-%m-%d')
    try:
        df = ak.stock_zh_a_hist_tx(symbol=symbol, start_date=start_fmt, end_date=end_fmt, adjust='qfq')
    except Exception as exc:
        return {'price_status': f'fetch_error:{exc}'}
    if df is None or df.empty or 'date' not in df.columns:
        return {'price_status': 'no_price_data'}
    data = df.copy()
    data['date'] = pd.to_datetime(data['date'])
    data['close'] = pd.to_numeric(data['close'], errors='coerce')
    data = data.dropna(subset=['close']).sort_values('date').reset_index(drop=True)
    target_date = pd.Timestamp(start_date)
    base_rows = data[data['date'] >= target_date]
    if base_rows.empty:
        return {'price_status': 'base_date_missing'}
    base_idx = int(base_rows.index[0])
    base_close = float(data.loc[base_idx, 'close'])
    result: dict[str, float | str] = {'price_status': 'ok', 'base_close': base_close}
    for horizon in [1, 3, 5]:
        idx = base_idx + horizon
        key = f'ret_{horizon}d'
        if idx < len(data):
            result[key] = round(float(data.loc[idx, 'close']) / base_close - 1, 6)
        else:
            result[key] = pd.NA
    return result


def _fetch_benchmark_returns(start_date: str) -> dict[str, float | str]:
    if ak is None:
        return {'benchmark_status': 'akshare_unavailable'}
    start_ts = pd.Timestamp(start_date)
    end_ts = start_ts + pd.Timedelta(days=20)
    start_fmt = start_ts.strftime('%Y%m%d')
    end_fmt = end_ts.strftime('%Y%m%d')
    try:
        df = ak.index_zh_a_hist(symbol='000300', period='daily', start_date=start_fmt, end_date=end_fmt)
    except Exception as exc:
        return {'benchmark_status': f'fetch_error:{exc}'}
    if df is None or df.empty:
        return {'benchmark_status': 'no_benchmark_data'}
    data = df.copy()
    date_col = '日期' if '日期' in data.columns else data.columns[0]
    close_col = '收盘' if '收盘' in data.columns else 'close'
    data['date'] = pd.to_datetime(data[date_col])
    data['close'] = pd.to_numeric(data[close_col], errors='coerce')
    data = data.dropna(subset=['close']).sort_values('date').reset_index(drop=True)
    base_rows = data[data['date'] >= start_ts]
    if base_rows.empty:
        return {'benchmark_status': 'base_date_missing'}
    base_idx = int(base_rows.index[0])
    base_close = float(data.loc[base_idx, 'close'])
    result: dict[str, float | str] = {'benchmark_status': 'ok', 'benchmark_close': base_close}
    for horizon in [1, 3, 5]:
        idx = base_idx + horizon
        key = f'benchmark_ret_{horizon}d'
        if idx < len(data):
            result[key] = round(float(data.loc[idx, 'close']) / base_close - 1, 6)
        else:
            result[key] = pd.NA
    return result


def update_forward_returns(snapshot_path: Path, returns_path: Path) -> pd.DataFrame:
    ensure_dir(returns_path.parent)
    if not snapshot_path.exists():
        return pd.DataFrame()
    snapshots = pd.read_csv(snapshot_path)
    if snapshots.empty:
        return snapshots

    if returns_path.exists():
        existing = pd.read_csv(returns_path)
    else:
        existing = pd.DataFrame()

    key_cols = ['run_date', 'list_type', 'instrument', 'rank']
    merged = snapshots.copy()
    if not existing.empty:
        merged = merged.merge(existing, on=key_cols, how='left', suffixes=('', '_old'))
    else:
        for col in [
            'price_status', 'base_close', 'ret_1d', 'ret_3d', 'ret_5d',
            'benchmark_status', 'benchmark_close', 'benchmark_ret_1d', 'benchmark_ret_3d', 'benchmark_ret_5d',
            'excess_ret_1d', 'excess_ret_3d', 'excess_ret_5d'
        ]:
            merged[col] = pd.NA

    benchmark_cache: dict[str, dict[str, float | str]] = {}
    stock_cache: dict[tuple[str, str], dict[str, float | str]] = {}

    for idx, row in merged.iterrows():
        if pd.notna(row.get('ret_5d')) and pd.notna(row.get('benchmark_ret_5d')):
            continue
        run_date = str(row['run_date'])
        instrument = str(row['instrument'])
        stock_key = (instrument, run_date)
        if stock_key not in stock_cache:
            stock_cache[stock_key] = _fetch_stock_forward_returns(instrument, run_date)
        if run_date not in benchmark_cache:
            benchmark_cache[run_date] = _fetch_benchmark_returns(run_date)
        stock_data = stock_cache[stock_key]
        bench_data = benchmark_cache[run_date]
        for key, value in stock_data.items():
            merged.at[idx, key] = value
        for key, value in bench_data.items():
            merged.at[idx, key] = value
        for horizon in [1, 3, 5]:
            s = stock_data.get(f'ret_{horizon}d')
            b = bench_data.get(f'benchmark_ret_{horizon}d')
            if isinstance(s, float) and isinstance(b, float):
                merged.at[idx, f'excess_ret_{horizon}d'] = round(s - b, 6)

    keep_cols = [
        *key_cols,
        'candidate_source', 'datetime', 'raw_score', 'quant_score_norm', 'event_score', 'priority_score',
        'risk_attention_score', 'risk_event_score', 'top_event_type', 'top_event_title', 'top_event_bias',
        'top_event_importance', 'top_event_confidence', 'top_risk_event_type', 'top_risk_title',
        'top_risk_bias', 'top_risk_importance', 'top_risk_confidence',
        'price_status', 'base_close', 'ret_1d', 'ret_3d', 'ret_5d',
        'benchmark_status', 'benchmark_close', 'benchmark_ret_1d', 'benchmark_ret_3d', 'benchmark_ret_5d',
        'excess_ret_1d', 'excess_ret_3d', 'excess_ret_5d',
    ]
    final = merged[keep_cols].copy()
    final.to_csv(returns_path, index=False)
    return final


def _topn_summary(df: pd.DataFrame, top_n: int, horizons: list[int]) -> dict[str, Any]:
    if df.empty:
        return {'count': 0}
    subset = df[df['rank'] <= top_n].copy()
    if subset.empty:
        return {'count': 0}
    stats: dict[str, Any] = {'count': int(len(subset))}
    for h in horizons:
        ret_col = f'ret_{h}d'
        excess_col = f'excess_ret_{h}d'
        if ret_col in subset.columns:
            returns = pd.to_numeric(subset[ret_col], errors='coerce').dropna()
            stats[f'avg_ret_{h}d'] = round(float(returns.mean()), 6) if not returns.empty else None
            stats[f'win_rate_{h}d'] = round(float((returns > 0).mean()), 4) if not returns.empty else None
        if excess_col in subset.columns:
            excess = pd.to_numeric(subset[excess_col], errors='coerce').dropna()
            stats[f'avg_excess_ret_{h}d'] = round(float(excess.mean()), 6) if not excess.empty else None
            stats[f'excess_win_rate_{h}d'] = round(float((excess > 0).mean()), 4) if not excess.empty else None
    return stats


def build_strategy_validation_summary(records: pd.DataFrame) -> dict[str, Any]:
    if records.empty:
        return {'generated_at': datetime.now().isoformat(timespec='seconds'), 'groups': {}}
    horizons = [1, 3, 5]
    groups: dict[str, Any] = {}
    for list_type in ['quant', 'priority', 'risk']:
        subset = records[records['list_type'] == list_type].copy()
        groups[list_type] = {
            'top10': _topn_summary(subset, 10, horizons),
            'top20': _topn_summary(subset, 20, horizons),
            'top30': _topn_summary(subset, 30, horizons),
        }

    compare = {'days_compared': 0}
    for horizon in horizons:
        q = records[(records['list_type'] == 'quant') & (records['rank'] <= 10)].groupby('run_date')[f'excess_ret_{horizon}d'].mean()
        p = records[(records['list_type'] == 'priority') & (records['rank'] <= 10)].groupby('run_date')[f'excess_ret_{horizon}d'].mean()
        joined = pd.concat([q.rename('quant'), p.rename('priority')], axis=1).dropna()
        if not joined.empty:
            compare['days_compared'] = max(compare['days_compared'], int(len(joined)))
            compare[f'priority_better_days_{horizon}d'] = int((joined['priority'] > joined['quant']).sum())
            compare[f'quant_better_days_{horizon}d'] = int((joined['priority'] < joined['quant']).sum())
            compare[f'avg_priority_minus_quant_{horizon}d'] = round((joined['priority'] - joined['quant']).mean(), 6)
    return {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'groups': groups,
        'compare_priority_vs_quant_top10': compare,
    }


def build_strategy_validation_report(summary: dict[str, Any]) -> str:
    lines = ['# Strategy Validation Report', '']
    groups = summary.get('groups', {})
    for list_type in ['quant', 'priority', 'risk']:
        block = groups.get(list_type, {})
        lines.append(f'## {list_type}')
        for bucket in ['top10', 'top20', 'top30']:
            stats = block.get(bucket, {})
            if not stats or stats.get('count', 0) == 0:
                lines.append(f'- {bucket}: no data')
                continue
            def fmt(value: Any) -> str:
                return 'n/a' if value is None else f'{value:.4f}'
            lines.append(
                f"- {bucket}: count={stats['count']}, "
                f"1d={fmt(stats.get('avg_ret_1d'))} / excess={fmt(stats.get('avg_excess_ret_1d'))}, "
                f"3d={fmt(stats.get('avg_ret_3d'))} / excess={fmt(stats.get('avg_excess_ret_3d'))}, "
                f"5d={fmt(stats.get('avg_ret_5d'))} / excess={fmt(stats.get('avg_excess_ret_5d'))}"
            )
        lines.append('')
    compare = summary.get('compare_priority_vs_quant_top10', {})
    lines.append('## Priority vs Quant (Top10)')
    lines.append(f"- days_compared: {compare.get('days_compared', 0)}")
    for horizon in [1, 3, 5]:
        lines.append(
            f"- {horizon}d: priority_better={compare.get(f'priority_better_days_{horizon}d', 0)}, "
            f"quant_better={compare.get(f'quant_better_days_{horizon}d', 0)}, "
            f"avg_priority_minus_quant={compare.get(f'avg_priority_minus_quant_{horizon}d', 0):.4f}"
        )
    lines.append('')
    return '\n'.join(lines)


def update_validation_artifacts(
    validation_dir: Path,
    outputs_dir: Path,
    topk: pd.DataFrame,
    priority_candidates: pd.DataFrame,
    risk_candidates: pd.DataFrame,
) -> dict[str, Path]:
    ensure_dir(validation_dir)
    snapshot_path = validation_dir / 'snapshots.csv'
    returns_path = validation_dir / 'validation_records.csv'
    summary_path = outputs_dir / 'strategy_validation_summary.json'
    report_path = outputs_dir / 'strategy_validation_report.md'

    snapshot = build_validation_snapshot(topk, priority_candidates, risk_candidates)
    append_validation_snapshot(snapshot, snapshot_path)
    records = update_forward_returns(snapshot_path, returns_path)
    summary = build_strategy_validation_summary(records)
    write_json(summary_path, summary)
    report_path.write_text(build_strategy_validation_report(summary), encoding='utf-8')
    return {
        'snapshot_path': snapshot_path,
        'records_path': returns_path,
        'summary_path': summary_path,
        'report_path': report_path,
    }
