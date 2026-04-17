from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import ensure_dir, write_json
from .qlib_pipeline import fetch_csi300_constituents, fetch_recent_hist_for_code


OUTPUT_SLUG = 'wangji-scanner'
LEGACY_OUTPUT_SLUG = 'wangji-sacnner'
PROFILE_CONFIGS: dict[str, dict[str, float]] = {
    'strict': {
        'close_range_10_max': 0.06,
        'max_daily_abs_ret_10_max': 0.04,
        'breakout_ret_min': 0.07,
        'vol_ratio_5_min': 2.0,
        'pullback_daily_min': -0.03,
        'pullback_ret_3d_min': -0.06,
        'pullback_avg_vol_ratio_max': 0.70,
    },
    'relax': {
        'close_range_10_max': 0.08,
        'max_daily_abs_ret_10_max': 0.05,
        'breakout_ret_min': 0.05,
        'vol_ratio_5_min': 1.5,
        'pullback_daily_min': -0.035,
        'pullback_ret_3d_min': -0.08,
        'pullback_avg_vol_ratio_max': 0.85,
    },
}


def _build_live_hist_panel(cfg: dict) -> pd.DataFrame:
    lcfg = cfg.get('live_data', {})
    lookback_days = max(int(lcfg.get('lookback_days', 120)), 220)
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days)
    start_date = str(lcfg.get('start_date') or start_dt.strftime('%Y%m%d'))
    end_date = str(lcfg.get('end_date') or end_dt.strftime('%Y%m%d'))
    max_workers = int(lcfg.get('max_workers', 4))
    stock_codes = fetch_csi300_constituents()
    panels: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_recent_hist_for_code, code, start_date, end_date): code for code in stock_codes}
        for future in as_completed(futures):
            try:
                hist = future.result()
            except Exception:
                hist = pd.DataFrame()
            if hist is not None and not hist.empty:
                panels.append(hist)

    if not panels:
        raise RuntimeError('未抓到任何实时行情数据，无法运行 wangji-scanner。')

    panel = pd.concat(panels, ignore_index=True)
    panel = panel.sort_values(['instrument', 'datetime']).reset_index(drop=True)
    return panel


def _build_weekly_panel(daily: pd.DataFrame) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for instrument, group in daily.groupby('instrument'):
        g = group.sort_values('datetime').copy()
        g = g.set_index('datetime')
        weekly = pd.DataFrame(
            {
                'open': g['open'].resample('W-FRI').first(),
                'high': g['high'].resample('W-FRI').max(),
                'low': g['low'].resample('W-FRI').min(),
                'close': g['close'].resample('W-FRI').last(),
                'volume': g['volume'].resample('W-FRI').sum(),
            }
        ).dropna(subset=['close'])
        if weekly.empty:
            continue
        weekly['instrument'] = instrument
        weekly = weekly.reset_index()
        weekly['ma5'] = weekly['close'].rolling(5).mean()
        weekly['ma13'] = weekly['close'].rolling(13).mean()
        weekly['ma21'] = weekly['close'].rolling(21).mean()
        frames.append(weekly)
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True).sort_values(['instrument', 'datetime']).reset_index(drop=True)


def _safe_ratio(a: float, b: float) -> float | None:
    if b in {0, None} or pd.isna(b):
        return None
    return float(a) / float(b)


def _evaluate_instrument(daily: pd.DataFrame, weekly: pd.DataFrame, profile_name: str, rules: dict[str, float]) -> dict[str, Any] | None:
    daily = daily.sort_values('datetime').tail(14).reset_index(drop=True)
    if len(daily) < 14:
        return None
    weekly = weekly.sort_values('datetime').reset_index(drop=True)
    if len(weekly) < 24:
        return None
    weekly_valid = weekly.dropna(subset=['ma5', 'ma13', 'ma21']).reset_index(drop=True)
    if len(weekly_valid) < 4:
        return None

    d = {f'd{i + 1}': daily.iloc[i] for i in range(14)}
    w_prev1 = weekly_valid.iloc[-2]
    w_prev2 = weekly_valid.iloc[-3]
    w_prev3 = weekly_valid.iloc[-4]

    close_window = daily.iloc[:10]['close']
    daily_ret_window = daily.iloc[1:10]['close'].reset_index(drop=True) / daily.iloc[:9]['close'].reset_index(drop=True) - 1
    volume_ref_5 = float(daily.iloc[5:10]['volume'].mean())
    breakout_close = float(d['d11']['close'])
    breakout_open = float(d['d11']['open'])
    breakout_prev_close = float(d['d10']['close'])
    breakout_volume = float(d['d11']['volume'])
    pullback_closes = daily.iloc[11:14]['close'].reset_index(drop=True)
    pullback_prev = daily.iloc[10:13]['close'].reset_index(drop=True)
    pullback_daily_rets = pullback_closes / pullback_prev - 1
    pullback_avg_vol = float(daily.iloc[11:14]['volume'].mean())

    weekly_ma21_slope = _safe_ratio(float(w_prev1['ma21']), float(w_prev3['ma21']))
    if weekly_ma21_slope is not None:
        weekly_ma21_slope -= 1.0

    close_range_10 = float(close_window.max() / close_window.min() - 1)
    max_daily_abs_ret_10 = float(daily_ret_window.abs().max()) if not daily_ret_window.empty else 0.0
    breakout_ret = float(breakout_close / breakout_prev_close - 1)
    vol_ratio_5 = _safe_ratio(breakout_volume, volume_ref_5)
    pullback_ret_3d = float(float(d['d14']['close']) / breakout_close - 1)
    pullback_avg_vol_ratio = _safe_ratio(pullback_avg_vol, breakout_volume)
    resistance_close_10 = float(close_window.max())

    rule_weekly_uptrend = bool(weekly_ma21_slope is not None and weekly_ma21_slope > 0)
    rule_weekly_bull_alignment = bool(float(w_prev1['ma5']) > float(w_prev1['ma13']) > float(w_prev1['ma21']))
    rule_weekly_upward_stack = bool(
        float(w_prev1['ma5']) > float(w_prev2['ma5'])
        and float(w_prev1['ma13']) >= float(w_prev2['ma13'])
        and float(w_prev1['ma21']) >= float(w_prev2['ma21'])
    )
    rule_close_above_weekly_ma13 = bool(float(d['d14']['close']) >= float(w_prev1['ma13']))

    rule_close_range_10 = close_range_10 <= rules['close_range_10_max']
    rule_max_daily_abs_ret_10 = max_daily_abs_ret_10 <= rules['max_daily_abs_ret_10_max']
    rule_breakout_green = breakout_close > breakout_open
    rule_breakout_ret = breakout_ret >= rules['breakout_ret_min']
    rule_breakout_volume = bool(vol_ratio_5 is not None and vol_ratio_5 >= rules['vol_ratio_5_min'])
    rule_breakout_close = breakout_close > resistance_close_10
    rule_pullback_daily = bool((pullback_daily_rets > rules['pullback_daily_min']).all())
    rule_pullback_3d = pullback_ret_3d >= rules['pullback_ret_3d_min']
    rule_pullback_volume = bool(pullback_avg_vol_ratio is not None and pullback_avg_vol_ratio <= rules['pullback_avg_vol_ratio_max'])

    rule_map = {
        'rule_weekly_uptrend': rule_weekly_uptrend,
        'rule_weekly_bull_alignment': rule_weekly_bull_alignment,
        'rule_weekly_upward_stack': rule_weekly_upward_stack,
        'rule_close_above_weekly_ma13': rule_close_above_weekly_ma13,
        'rule_close_range_10': rule_close_range_10,
        'rule_max_daily_abs_ret_10': rule_max_daily_abs_ret_10,
        'rule_breakout_green': rule_breakout_green,
        'rule_breakout_ret': rule_breakout_ret,
        'rule_breakout_volume': rule_breakout_volume,
        'rule_breakout_close': rule_breakout_close,
        'rule_pullback_daily': rule_pullback_daily,
        'rule_pullback_3d': rule_pullback_3d,
        'rule_pullback_volume': rule_pullback_volume,
    }
    core_passed = all(
        rule_map[key]
        for key in [
            'rule_weekly_uptrend',
            'rule_weekly_bull_alignment',
            'rule_weekly_upward_stack',
            'rule_close_range_10',
            'rule_max_daily_abs_ret_10',
            'rule_breakout_green',
            'rule_breakout_ret',
            'rule_breakout_volume',
            'rule_breakout_close',
            'rule_pullback_daily',
            'rule_pullback_3d',
            'rule_pullback_volume',
        ]
    )
    rules_passed_count = int(sum(1 for value in rule_map.values() if value))

    return {
        'instrument': str(d['d14']['instrument']),
        'signal_date': pd.Timestamp(d['d14']['datetime']).date().isoformat(),
        'latest_close': float(d['d14']['close']),
        'scanner_name': 'wangji-scanner',
        'pattern_name': 'wangji-scanner',
        'pattern_profile': profile_name,
        'pattern_passed': core_passed,
        'rules_passed_count': rules_passed_count,
        'weekly_ma5': round(float(w_prev1['ma5']), 6),
        'weekly_ma13': round(float(w_prev1['ma13']), 6),
        'weekly_ma21': round(float(w_prev1['ma21']), 6),
        'weekly_ma21_slope': round(float(weekly_ma21_slope or 0.0), 6),
        'close_range_10': round(close_range_10, 6),
        'max_daily_abs_ret_10': round(max_daily_abs_ret_10, 6),
        'breakout_ret': round(breakout_ret, 6),
        'vol_ratio_5': round(float(vol_ratio_5 or 0.0), 6),
        'pullback_ret_3d': round(pullback_ret_3d, 6),
        'pullback_avg_vol_ratio': round(float(pullback_avg_vol_ratio or 0.0), 6),
        'resistance_close_10': round(resistance_close_10, 6),
        **rule_map,
    }


def run_wangji_scanner(cfg: dict, profile_name: str, daily: pd.DataFrame | None = None) -> pd.DataFrame:
    if profile_name not in PROFILE_CONFIGS:
        raise KeyError(f'unknown profile: {profile_name}')
    daily_panel = daily.copy() if daily is not None else _build_live_hist_panel(cfg)
    weekly = _build_weekly_panel(daily_panel)
    rows: list[dict[str, Any]] = []
    for instrument, group in daily_panel.groupby('instrument'):
        weekly_group = weekly[weekly['instrument'] == instrument].copy()
        row = _evaluate_instrument(group.copy(), weekly_group, profile_name, PROFILE_CONFIGS[profile_name])
        if row is not None:
            rows.append(row)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df = df.sort_values(
        ['pattern_passed', 'rules_passed_count', 'breakout_ret', 'vol_ratio_5', 'pullback_avg_vol_ratio'],
        ascending=[False, False, False, False, True],
    ).reset_index(drop=True)
    df['scanner_rank'] = df.index + 1
    return df


def run_all_wangji_scanner_profiles(cfg: dict) -> dict[str, pd.DataFrame]:
    daily = _build_live_hist_panel(cfg)
    return {profile_name: run_wangji_scanner(cfg, profile_name, daily=daily) for profile_name in PROFILE_CONFIGS}


def build_wangji_scanner_report(df: pd.DataFrame, profile_name: str) -> str:
    lines = [f'# wangji-scanner / {profile_name}', '']
    if df.empty:
        lines.append('- no candidates generated')
        return '\n'.join(lines)

    passed = df[df['pattern_passed']].copy()
    lines.append(f'- total_evaluated: {len(df)}')
    lines.append(f'- passed: {len(passed)}')
    lines.append('')
    lines.append('## Top Passed Candidates')
    if passed.empty:
        lines.append('- none')
    else:
        for _, row in passed.head(20).iterrows():
            lines.append(
                f"- {row['instrument']}: breakout_ret={row['breakout_ret']:.4f}, vol_ratio_5={row['vol_ratio_5']:.2f}, pullback_ret_3d={row['pullback_ret_3d']:.4f}, close_range_10={row['close_range_10']:.4f}, rules_passed={row['rules_passed_count']}"
            )
    lines.append('')
    lines.append('## Near Miss Candidates')
    near_miss = df[~df['pattern_passed']].head(10)
    if near_miss.empty:
        lines.append('- none')
    else:
        for _, row in near_miss.iterrows():
            lines.append(
                f"- {row['instrument']}: breakout_ret={row['breakout_ret']:.4f}, vol_ratio_5={row['vol_ratio_5']:.2f}, pullback_ret_3d={row['pullback_ret_3d']:.4f}, close_range_10={row['close_range_10']:.4f}, rules_passed={row['rules_passed_count']}"
            )
    lines.append('')
    lines.append('## Rule Notes')
    lines.append('- weekly trend filter uses completed weekly MA5 > MA13 > MA21 and upward MA slope checks')
    lines.append(f"- profile thresholds: {PROFILE_CONFIGS[profile_name]}")
    lines.append('- daily structure uses 10-day tight close range + day11 breakout + day12-14 pullback volume contraction')
    return '\n'.join(lines)


def write_wangji_scanner_outputs(profile_frames: dict[str, pd.DataFrame], outputs_dir: Path) -> dict[str, dict[str, Path]]:
    ensure_dir(outputs_dir)
    output_paths: dict[str, dict[str, Path]] = {}
    for profile_name, df in profile_frames.items():
        slug = f'{OUTPUT_SLUG}_{profile_name}'
        csv_path = outputs_dir / f'{slug}_candidates.csv'
        json_path = outputs_dir / f'{slug}_candidates.json'
        md_path = outputs_dir / f'{slug}_report.md'
        df.to_csv(csv_path, index=False)
        write_json(json_path, df.fillna('').to_dict(orient='records'))
        md_path.write_text(build_wangji_scanner_report(df, profile_name), encoding='utf-8')
        output_paths[profile_name] = {'csv_path': csv_path, 'json_path': json_path, 'md_path': md_path}

    summary_path = outputs_dir / f'{OUTPUT_SLUG}_summary.json'
    summary_payload = {
        profile_name: {
            'total': int(len(df)),
            'passed': int(df['pattern_passed'].sum()) if not df.empty else 0,
            'top_instruments': df.head(10)['instrument'].astype(str).tolist() if not df.empty else [],
        }
        for profile_name, df in profile_frames.items()
    }
    write_json(summary_path, summary_payload)
    for legacy_profile in profile_frames:
        legacy_csv = outputs_dir / f'{LEGACY_OUTPUT_SLUG}_{legacy_profile}_candidates.csv'
        legacy_json = outputs_dir / f'{LEGACY_OUTPUT_SLUG}_{legacy_profile}_candidates.json'
        legacy_report = outputs_dir / f'{LEGACY_OUTPUT_SLUG}_{legacy_profile}_report.md'
        profile_paths = output_paths[legacy_profile]
        legacy_csv.write_text(profile_paths['csv_path'].read_text(encoding='utf-8'), encoding='utf-8')
        legacy_json.write_text(profile_paths['json_path'].read_text(encoding='utf-8'), encoding='utf-8')
        legacy_report.write_text(profile_paths['md_path'].read_text(encoding='utf-8'), encoding='utf-8')
    return output_paths


# Backward-compatible aliases for the earlier typoed naming.
def run_wangji_sacnner(cfg: dict, profile_name: str = 'strict', daily: pd.DataFrame | None = None) -> pd.DataFrame:
    return run_wangji_scanner(cfg, profile_name, daily=daily)


def run_all_wangji_sacnner_profiles(cfg: dict) -> dict[str, pd.DataFrame]:
    return run_all_wangji_scanner_profiles(cfg)


def build_wangji_sacnner_report(df: pd.DataFrame, profile_name: str = 'strict') -> str:
    return build_wangji_scanner_report(df, profile_name)


def write_wangji_sacnner_outputs(profile_frames: dict[str, pd.DataFrame], outputs_dir: Path) -> dict[str, dict[str, Path]]:
    return write_wangji_scanner_outputs(profile_frames, outputs_dir)
