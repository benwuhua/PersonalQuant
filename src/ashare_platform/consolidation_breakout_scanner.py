from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from .io_utils import ensure_dir, write_json
from .qlib_pipeline import (
    fetch_all_a_share_codes,
    fetch_csi300_constituents,
    fetch_recent_hist_for_code,
    fetch_top_a_share_codes_by_turnover,
    normalize_stock_code_list,
)


SCANNER_DISPLAY_NAME = 'consolidation-breakout-scanner'
OUTPUT_SLUG = SCANNER_DISPLAY_NAME
LEGACY_OUTPUT_SLUGS = ('wangji-scanner', 'wangji-sacnner')
DEFAULT_CALIBRATION_CASES: list[dict[str, str]] = [
    {'code': '600089', 'instrument': 'SH600089', 'label': 'sample_600089', 'start_date': '20251215', 'end_date': '20260115'},
    {'code': '301005', 'instrument': 'SZ301005', 'label': 'sample_301005', 'start_date': '20260315', 'end_date': '20260415'},
    {'code': '300136', 'instrument': 'SZ300136', 'label': 'sample_300136', 'start_date': '20260313', 'end_date': '20260415'},
]
PROFILE_CONFIGS: dict[str, dict[str, float | int]] = {
    'relax': {
        'setup_days': 7,
        'breakout_search_days': 6,
        'pullback_days': 4,
        'close_range_max': 0.10,
        'max_daily_abs_ret_max': 0.07,
        'breakout_ret_min': 0.04,
        'vol_ratio_lookback': 5,
        'vol_ratio_min': 1.3,
        'pullback_daily_min': -0.04,
        'pullback_ret_min': -0.08,
        'pullback_avg_vol_ratio_max': 0.95,
    },
}
FLOAT_RULE_KEYS = {
    'close_range_max',
    'max_daily_abs_ret_max',
    'breakout_ret_min',
    'vol_ratio_min',
    'pullback_daily_min',
    'pullback_ret_min',
    'pullback_avg_vol_ratio_max',
}
INT_RULE_KEYS = {'setup_days', 'breakout_search_days', 'pullback_days', 'vol_ratio_lookback'}


def _resolve_wangji_stock_codes(cfg: dict) -> list[str]:
    lcfg = cfg.get('live_data', {})
    universe = str(lcfg.get('consolidation_breakout_universe') or lcfg.get('wangji_universe') or 'csi300').strip().lower()
    prefilter_mode = str(lcfg.get('consolidation_breakout_prefilter_mode') or lcfg.get('wangji_prefilter_mode') or 'turnover_top_n').strip().lower()
    prefilter_top_n = int(lcfg.get('consolidation_breakout_prefilter_top_n') or lcfg.get('wangji_prefilter_top_n') or 1200)
    extra_codes = normalize_stock_code_list(lcfg.get('consolidation_breakout_extra_codes') or lcfg.get('wangji_extra_codes') or [])
    if universe == 'all_a':
        if prefilter_mode == 'none':
            base_codes = fetch_all_a_share_codes()
        else:
            base_codes = fetch_top_a_share_codes_by_turnover(prefilter_top_n)
    else:
        base_codes = fetch_csi300_constituents()
    merged = normalize_stock_code_list([*base_codes, *extra_codes])
    return merged


def _build_live_hist_panel(cfg: dict, progress_callback=None) -> pd.DataFrame:
    lcfg = cfg.get('live_data', {})
    lookback_days = max(int(lcfg.get('lookback_days', 120)), 220)
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days)
    start_date = str(lcfg.get('start_date') or start_dt.strftime('%Y%m%d'))
    end_date = str(lcfg.get('end_date') or end_dt.strftime('%Y%m%d'))
    max_workers = int(lcfg.get('max_workers', 4))
    stock_codes = _resolve_wangji_stock_codes(cfg)
    panels: list[pd.DataFrame] = []

    total_codes = len(stock_codes)
    completed_codes = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_recent_hist_for_code, code, start_date, end_date): code for code in stock_codes}
        for future in as_completed(futures):
            completed_codes += 1
            try:
                hist = future.result()
            except Exception:
                hist = pd.DataFrame()
            if hist is not None and not hist.empty:
                panels.append(hist)
            if progress_callback and (completed_codes == 1 or completed_codes == total_codes or completed_codes % 30 == 0):
                progress_callback('fetching', f'正在抓取行情数据：{completed_codes}/{total_codes}')

    if not panels:
        raise RuntimeError(f'未抓到任何实时行情数据，无法运行 {SCANNER_DISPLAY_NAME}。')

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


def _clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


def _evaluate_washout(
    pullback_daily_rets: pd.Series,
    pullback_avg_vol_ratio: float | None,
    pullback_ret_total: float,
    support_revisit_ratio: float,
    volume_trend_ratio: float | None,
    *,
    daily_min: float,
    total_min: float,
    avg_vol_max: float,
) -> dict[str, Any]:
    negative_days = int((pullback_daily_rets < 0).sum())
    max_down_day = float(pullback_daily_rets.min()) if not pullback_daily_rets.empty else 0.0
    down_speed = abs(float(pullback_daily_rets[pullback_daily_rets < 0].mean())) if negative_days else 0.0
    gentle_price = bool(
        (pullback_daily_rets > daily_min).all()
        and negative_days >= max(1, len(pullback_daily_rets) // 2)
        and down_speed <= abs(daily_min) * 0.8
        and max_down_day >= daily_min
    )
    volume_contracting = bool(
        pullback_avg_vol_ratio is not None
        and pullback_avg_vol_ratio <= avg_vol_max
        and (volume_trend_ratio is None or volume_trend_ratio <= 1.0)
    )
    structure_intact = bool(
        pullback_ret_total >= total_min
        and support_revisit_ratio >= -0.06
        and support_revisit_ratio <= 0.12
    )
    return {
        'is_washout': bool(gentle_price and volume_contracting and structure_intact),
        'gentle_price': gentle_price,
        'volume_contracting': volume_contracting,
        'structure_intact': structure_intact,
        'negative_days': negative_days,
        'max_down_day': round(max_down_day, 6),
        'down_speed': round(down_speed, 6),
    }


def _evaluate_digestion(
    pullback_daily_rets: pd.Series,
    pullback_avg_vol_ratio: float | None,
    pullback_ret_total: float,
    support_revisit_ratio: float,
    confirmation_ret: float,
    volume_trend_ratio: float | None,
) -> dict[str, Any]:
    negative_days = int((pullback_daily_rets < 0).sum())
    positive_days = int((pullback_daily_rets > 0).sum())
    max_down_day = float(pullback_daily_rets.min()) if not pullback_daily_rets.empty else 0.0
    max_up_day = float(pullback_daily_rets.max()) if not pullback_daily_rets.empty else 0.0
    mean_abs_ret = float(pullback_daily_rets.abs().mean()) if not pullback_daily_rets.empty else 0.0
    orderly_price = bool(
        max_down_day >= -0.085
        and max_up_day <= 0.11
        and mean_abs_ret <= 0.045
        and negative_days >= 1
    )
    contained_volume = bool(
        pullback_avg_vol_ratio is not None
        and pullback_avg_vol_ratio <= 1.45
        and (volume_trend_ratio is None or volume_trend_ratio <= 1.25)
    )
    structure_intact = bool(
        pullback_ret_total >= -0.12
        and support_revisit_ratio >= -0.10
        and support_revisit_ratio <= 0.22
        and confirmation_ret >= 0.035
    )
    return {
        'is_digestion': bool(orderly_price and contained_volume and structure_intact),
        'orderly_price': orderly_price,
        'contained_volume': contained_volume,
        'structure_intact': structure_intact,
        'negative_days': negative_days,
        'positive_days': positive_days,
        'max_down_day': round(max_down_day, 6),
        'max_up_day': round(max_up_day, 6),
        'mean_abs_ret': round(mean_abs_ret, 6),
    }


def _close_location_ratio(close: float, low: float, high: float) -> float:
    spread = float(high) - float(low)
    if spread <= 0:
        return 1.0
    return _clamp((float(close) - float(low)) / spread)


def evaluate_relax_ignition_flags(metrics: dict[str, float | bool | None]) -> dict[str, bool]:
    breakout_ret = float(metrics.get('breakout_ret', 0.0) or 0.0)
    confirmation_ret = float(metrics.get('confirmation_ret', 0.0) or 0.0)
    vol_ratio_5 = float(metrics.get('vol_ratio_5', 0.0) or 0.0)
    confirmation_vol_ratio = float(metrics.get('confirmation_vol_ratio', 0.0) or 0.0)
    breakout_close = float(metrics.get('breakout_close', 0.0) or 0.0)
    breakout_prev_close = float(metrics.get('breakout_prev_close', 0.0) or 0.0)
    breakout_open = float(metrics.get('breakout_open', 0.0) or 0.0)
    close_location_ratio = float(metrics.get('close_location_ratio', 0.0) or 0.0)
    ignition_day_strength = float(metrics.get('ignition_day_strength', 0.0) or 0.0)
    breakout_ret_min = float(metrics.get('breakout_ret_min', 0.04) or 0.04)
    vol_ratio_min = float(metrics.get('vol_ratio_min', 1.3) or 1.3)

    canonical_green = breakout_close > breakout_open
    continuation_green = bool(
        breakout_close > breakout_prev_close
        and close_location_ratio >= 0.5
        and breakout_ret >= max(breakout_ret_min * 0.7, 0.028)
    )
    breakout_green = bool(canonical_green or continuation_green)
    breakout_ret_pass = bool(
        breakout_ret >= max(breakout_ret_min, 0.04)
        or (
            breakout_close > breakout_prev_close
            and confirmation_ret >= max(breakout_ret_min, 0.04)
            and ignition_day_strength >= 65
        )
    )
    breakout_volume_pass = bool(
        vol_ratio_5 >= max(vol_ratio_min, 1.25)
        or (
            vol_ratio_5 >= max(vol_ratio_min * 0.9, 1.18)
            and confirmation_vol_ratio >= max(vol_ratio_min, 1.3)
            and ignition_day_strength >= 65
        )
    )
    return {
        'rule_breakout_green': breakout_green,
        'rule_breakout_ret': breakout_ret_pass,
        'rule_breakout_volume': breakout_volume_pass,
        'canonical_green': canonical_green,
        'continuation_green': continuation_green,
    }


def evaluate_relax_digestion_flags(metrics: dict[str, float | bool | None]) -> dict[str, bool]:
    pattern_digestion = bool(metrics.get('pattern_digestion', False))
    digestion_score = float(metrics.get('digestion_score', 0.0) or 0.0)
    controlled_down_days = int(metrics.get('controlled_down_days', 0) or 0)
    negative_pullback_days = int(metrics.get('negative_pullback_days', 0) or 0)
    mild_up_days = int(metrics.get('mild_up_days', 0) or 0)
    abnormal_down_days = int(metrics.get('abnormal_down_days', 0) or 0)
    abnormal_up_days = int(metrics.get('abnormal_up_days', 0) or 0)
    digestion_volume_ratio = float(metrics.get('digestion_volume_ratio', 0.0) or 0.0)
    max_washout_vol_spike = float(metrics.get('max_washout_vol_spike', 0.0) or 0.0)
    structure_break_days = int(metrics.get('structure_break_days', 0) or 0)
    washout_direction_bias = float(metrics.get('washout_direction_bias', 0.0) or 0.0)
    pullback_ret_3d = float(metrics.get('pullback_ret_3d', 0.0) or 0.0)
    pullback_ret_min = float(metrics.get('pullback_ret_min', -0.08) or -0.08)
    pullback_days = int(metrics.get('pullback_days', 4) or 4)
    rule_pullback_daily = bool(metrics.get('rule_pullback_daily', False))
    rule_pullback_3d = bool(metrics.get('rule_pullback_3d', False))
    rule_pullback_volume = bool(metrics.get('rule_pullback_volume', False))

    canonical_washout = bool(
        digestion_score >= 55
        and controlled_down_days >= 2
        and abnormal_down_days <= 1
        and digestion_volume_ratio <= 0.95
        and negative_pullback_days >= max(2, pullback_days - 1)
        and washout_direction_bias >= 0.25
        and rule_pullback_daily
        and rule_pullback_3d
        and rule_pullback_volume
    )
    controlled_digestion_variant = bool(
        pattern_digestion
        and digestion_score >= 68
        and controlled_down_days >= 2
        and negative_pullback_days >= 2
        and mild_up_days <= 2
        and abnormal_down_days == 0
        and abnormal_up_days <= 1
        and digestion_volume_ratio <= 0.90
        and max_washout_vol_spike <= 1.10
        and structure_break_days <= 1
        and pullback_ret_3d <= 0.03
        and pullback_ret_3d >= pullback_ret_min
        and washout_direction_bias >= 0.0
    )
    return {
        'canonical_washout': canonical_washout,
        'controlled_digestion_variant': controlled_digestion_variant,
        'digestion_pass': bool(canonical_washout or controlled_digestion_variant),
    }



def evaluate_relax_common_entry_gates(metrics: dict[str, float | bool | None]) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if float(metrics.get('base_background_score', 0.0) or 0.0) < 40:
        fail_reasons.append('base_background_score_low')
    if not bool(metrics.get('rule_background_base', False)):
        fail_reasons.append('background_base_fail')
    if not bool(metrics.get('rule_close_range_10', False)):
        fail_reasons.append('close_range_10_fail')
    if not bool(metrics.get('rule_max_daily_abs_ret_10', False)):
        fail_reasons.append('max_daily_abs_ret_10_fail')
    if not bool(metrics.get('rule_breakout_ret', False)):
        fail_reasons.append('breakout_ret_fail')
    if not bool(metrics.get('rule_breakout_volume', False)):
        fail_reasons.append('breakout_volume_fail')
    if not bool(metrics.get('rule_breakout_green', False)):
        fail_reasons.append('breakout_green_fail')
    if float(metrics.get('ignition_day_strength', 0.0) or 0.0) < 60:
        fail_reasons.append('ignition_day_strength_low')
    return {
        'entry_gate_pass': not fail_reasons,
        'entry_gate_fail_reasons': fail_reasons,
    }



def evaluate_relax_prepass_flags(metrics: dict[str, float | bool | None]) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if not bool(metrics.get('recent_ignition_window', False)):
        fail_reasons.append('not_recent_ignition_window')
    if not bool(metrics.get('entry_gate_pass', False)):
        fail_reasons.append('entry_gate_fail')
    if float(metrics.get('breakout_ret', 0.0) or 0.0) < max(float(metrics.get('breakout_ret_min', 0.04) or 0.04), 0.04):
        fail_reasons.append('breakout_ret_low')
    if float(metrics.get('vol_ratio_5', 0.0) or 0.0) < max(float(metrics.get('vol_ratio_min', 1.3) or 1.3) * 0.95, 1.2):
        fail_reasons.append('breakout_volume_low')
    if float(metrics.get('close_location_ratio', 0.0) or 0.0) < 0.65:
        fail_reasons.append('close_location_ratio_low')
    if int(metrics.get('observed_pullback_days', 0) or 0) < 2:
        fail_reasons.append('observed_pullback_days_low')
    if int(metrics.get('structure_break_days', 0) or 0) != 0:
        fail_reasons.append('structure_break_days_high')
    if int(metrics.get('abnormal_down_days', 0) or 0) != 0:
        fail_reasons.append('abnormal_down_days_high')
    if float(metrics.get('latest_close_vs_breakout_close', 0.0) or 0.0) < 0.97:
        fail_reasons.append('latest_close_vs_breakout_close_low')
    pullback_avg_vol_ratio = metrics.get('pullback_avg_vol_ratio')
    if pullback_avg_vol_ratio in {None, ''} or float(pullback_avg_vol_ratio) > 0.85:
        fail_reasons.append('pullback_avg_vol_ratio_high')
    if float(metrics.get('max_washout_vol_spike', 0.0) or 0.0) > 1.10:
        fail_reasons.append('max_washout_vol_spike_high')
    volume_trend_ratio = metrics.get('volume_trend_ratio')
    if volume_trend_ratio not in {None, ''} and float(volume_trend_ratio) > 1.0:
        fail_reasons.append('volume_trend_ratio_high')
    return {
        'recent_ignition_prepass': not fail_reasons,
        'prepass_fail_reasons': fail_reasons,
    }


def build_framework_scores(metrics: dict[str, float | bool | None]) -> dict[str, float]:
    background_score = round(float(metrics.get('base_background_score', 0.0) or 0.0), 4)
    digestion_score = float(metrics.get('digestion_score', 0.0) or 0.0)
    impulse_score = float(metrics.get('impulse_score', 0.0) or 0.0)
    second_leg_score = float(metrics.get('second_leg_score', 0.0) or 0.0)
    close_range_10 = float(metrics.get('close_range_10', 1.0) or 1.0)
    max_daily_abs_ret_10 = float(metrics.get('max_daily_abs_ret_10', 1.0) or 1.0)

    compression_score = 100 * (1 - _clamp(close_range_10 / 0.10))
    stability_score = 100 * (1 - _clamp(max_daily_abs_ret_10 / 0.07))
    consolidation_score = round(0.5 * digestion_score + 0.3 * compression_score + 0.2 * stability_score, 4)
    breakout_quality_score = round(impulse_score, 4)
    followthrough_score = round(0.65 * digestion_score + 0.35 * second_leg_score, 4)
    final_score = round(
        0.20 * background_score
        + 0.30 * consolidation_score
        + 0.30 * breakout_quality_score
        + 0.20 * followthrough_score,
        4,
    )
    return {
        'background_score': background_score,
        'consolidation_score': consolidation_score,
        'breakout_quality_score': breakout_quality_score,
        'followthrough_score': followthrough_score,
        'final_score': final_score,
    }


def infer_pattern_stage(metrics: dict[str, float | bool | None]) -> str:
    if bool(metrics.get('second_leg_pass', False)) and bool(metrics.get('pattern_passed', False)):
        return 'second_leg'
    if bool(metrics.get('recent_ignition_prepass', False)):
        return 'breakout'
    if bool(metrics.get('digestion_pass', False)):
        return 'digestion'
    if bool(metrics.get('rule_breakout_ret', False)) and bool(metrics.get('rule_breakout_volume', False)):
        return 'breakout'
    return 'setup'


def evaluate_second_leg_breakout(metrics: dict[str, float | bool | None]) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if not bool(metrics.get('impulse_pass', False)):
        fail_reasons.append('impulse_pass_required')
    if not bool(metrics.get('digestion_pass', False)):
        fail_reasons.append('digestion_pass_required')
    if bool(metrics.get('recent_ignition_prepass', False)):
        fail_reasons.append('recent_ignition_prepass_only')
    if int(metrics.get('structure_break_days', 0) or 0) > 1:
        fail_reasons.append('structure_break_days_high')
    if float(metrics.get('support_revisit_ratio', 0.0) or 0.0) < -0.04:
        fail_reasons.append('support_revisit_ratio_low')
    if float(metrics.get('confirmation_ret', 0.0) or 0.0) < 0.09:
        fail_reasons.append('confirmation_ret_low')
    if float(metrics.get('confirmation_vol_ratio', 0.0) or 0.0) < 1.4:
        fail_reasons.append('confirmation_vol_ratio_low')
    if float(metrics.get('second_leg_distance', 1.0) or 1.0) > 0.12:
        fail_reasons.append('second_leg_distance_high')
    if float(metrics.get('retest_high_ratio', -1.0) or -1.0) < -0.03:
        fail_reasons.append('retest_high_ratio_low')
    if float(metrics.get('latest_close_vs_breakout_close', 0.0) or 0.0) < 1.03:
        fail_reasons.append('latest_close_vs_breakout_close_low')

    triggered = not fail_reasons
    return {
        'second_leg_breakout_triggered': triggered,
        'second_leg_breakout_family': 'second_leg_breakout' if triggered else 'platform_breakout',
        'second_leg_breakout_fail_reasons': fail_reasons,
    }


def evaluate_new_high_breakout(metrics: dict[str, float | bool | None]) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if not bool(metrics.get('impulse_pass', False)):
        fail_reasons.append('impulse_pass_required')
    if not bool(metrics.get('rule_breakout_ret', False)):
        fail_reasons.append('breakout_ret_required')
    if not bool(metrics.get('rule_breakout_volume', False)):
        fail_reasons.append('breakout_volume_required')

    new_high_candidates = {
        120: bool(metrics.get('is_new_high_120', False)),
        60: bool(metrics.get('is_new_high_60', False)),
        20: bool(metrics.get('is_new_high_20', False)),
    }
    new_high_window = next((window for window, triggered in new_high_candidates.items() if triggered), 0)
    if new_high_window == 0:
        fail_reasons.append('not_a_new_high_breakout')

    compression_ready = bool(
        metrics.get('new_high_precompression_pass', False)
        or (
            float(metrics.get('close_range_10', 1.0) or 1.0) <= 0.12
            and float(metrics.get('max_daily_abs_ret_10', 1.0) or 1.0) <= 0.08
        )
    )
    if not compression_ready:
        fail_reasons.append('pre_breakout_compression_missing')

    breakout_quality = bool(
        float(metrics.get('breakout_ret', 0.0) or 0.0) >= 0.04
        and float(metrics.get('confirmation_vol_ratio', 0.0) or 0.0) >= 1.2
    )
    if not breakout_quality:
        fail_reasons.append('breakout_quality_low')

    latest_extension = float(metrics.get('latest_close_vs_breakout_close', 0.0) or 0.0)
    if latest_extension > 1.12:
        fail_reasons.append('post_breakout_extension_too_high')

    triggered = not fail_reasons
    pass_fail_reasons: list[str] = []
    if not triggered:
        pass_fail_reasons.extend(fail_reasons)
    if int(metrics.get('structure_break_days', 0) or 0) > 0:
        pass_fail_reasons.append('structure_break_days_high')
    if float(metrics.get('digestion_score', 0.0) or 0.0) < 60:
        pass_fail_reasons.append('digestion_score_low')
    weekly_structure_score = float(metrics.get('weekly_structure_score', 0.0) or 0.0)
    if weekly_structure_score < 20:
        pass_fail_reasons.append('weekly_structure_score_low')
    support_revisit_ratio = float(metrics.get('support_revisit_ratio', 0.0) or 0.0)
    if support_revisit_ratio < -0.03:
        pass_fail_reasons.append('support_revisit_ratio_low')
    if support_revisit_ratio > 0.15:
        pass_fail_reasons.append('support_revisit_ratio_high')
    if float(metrics.get('latest_close_vs_breakout_close', 0.0) or 0.0) < 0.97:
        pass_fail_reasons.append('latest_close_vs_breakout_close_low')

    breakout_pass = not pass_fail_reasons
    return {
        'new_high_breakout_triggered': triggered,
        'new_high_breakout_family': 'new_high_breakout' if triggered else 'platform_breakout',
        'new_high_breakout_fail_reasons': fail_reasons,
        'new_high_breakout_pass': breakout_pass,
        'new_high_breakout_pass_fail_reasons': pass_fail_reasons,
        'new_high_window': new_high_window,
    }


def evaluate_high_turnover_breakout(metrics: dict[str, float | bool | None]) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if not bool(metrics.get('impulse_pass', False)):
        fail_reasons.append('impulse_pass_required')
    if not bool(metrics.get('rule_breakout_ret', False)):
        fail_reasons.append('breakout_ret_required')
    if not bool(metrics.get('rule_breakout_volume', False)):
        fail_reasons.append('breakout_volume_required')

    setup_avg_amount_ratio = float(metrics.get('setup_avg_amount_ratio', 0.0) or 0.0)
    setup_high_turnover_days = int(metrics.get('setup_high_turnover_days', 0) or 0)
    setup_amount_zscore = float(metrics.get('setup_amount_zscore', 0.0) or 0.0)
    if setup_avg_amount_ratio < 1.15:
        fail_reasons.append('setup_avg_amount_ratio_low')
    if setup_high_turnover_days < 2:
        fail_reasons.append('setup_high_turnover_days_low')
    if setup_amount_zscore < 0.2:
        fail_reasons.append('setup_amount_zscore_low')

    if float(metrics.get('close_range_10', 1.0) or 1.0) > 0.12:
        fail_reasons.append('platform_range_too_wide')
    if float(metrics.get('max_daily_abs_ret_10', 1.0) or 1.0) > 0.09:
        fail_reasons.append('platform_volatility_too_high')
    if float(metrics.get('latest_close_vs_breakout_close', 0.0) or 0.0) < 0.99:
        fail_reasons.append('breakout_hold_low')

    triggered = not fail_reasons
    pass_fail_reasons: list[str] = []
    if not triggered:
        pass_fail_reasons.extend(fail_reasons)
    if int(metrics.get('structure_break_days', 0) or 0) > 0:
        pass_fail_reasons.append('structure_break_days_high')
    if float(metrics.get('digestion_score', 0.0) or 0.0) < 62:
        pass_fail_reasons.append('digestion_score_low')
    if float(metrics.get('weekly_structure_score', 0.0) or 0.0) < 20:
        pass_fail_reasons.append('weekly_structure_score_low')
    support_revisit_ratio = float(metrics.get('support_revisit_ratio', 0.0) or 0.0)
    if support_revisit_ratio < -0.02:
        pass_fail_reasons.append('support_revisit_ratio_low')
    if support_revisit_ratio > 0.18:
        pass_fail_reasons.append('support_revisit_ratio_high')

    breakout_pass = not pass_fail_reasons
    return {
        'high_turnover_breakout_triggered': triggered,
        'high_turnover_breakout_family': 'high_turnover_breakout' if triggered else 'platform_breakout',
        'high_turnover_breakout_fail_reasons': fail_reasons,
        'high_turnover_breakout_pass': breakout_pass,
        'high_turnover_breakout_pass_fail_reasons': pass_fail_reasons,
    }


def evaluate_triangle_box_breakout(metrics: dict[str, Any]) -> dict[str, Any]:
    fail_reasons: list[str] = []
    if not bool(metrics.get('impulse_pass', False)):
        fail_reasons.append('impulse_pass_required')
    if not bool(metrics.get('rule_breakout_ret', False)):
        fail_reasons.append('breakout_ret_required')
    if not bool(metrics.get('rule_breakout_volume', False)):
        fail_reasons.append('breakout_volume_required')

    triangle_converging = bool(metrics.get('triangle_converging', False))
    box_valid = bool(metrics.get('box_valid', False))
    triangle_breakout = bool(metrics.get('triangle_breakout_confirmed', False))
    box_breakout = bool(metrics.get('box_breakout_confirmed', False))
    box_width_ratio = float(metrics.get('box_width_ratio', 1.0) or 1.0)
    box_upper_touches = int(metrics.get('box_upper_touches', 0) or 0)
    box_lower_touches = int(metrics.get('box_lower_touches', 0) or 0)
    breakout_ret = float(metrics.get('breakout_ret', 0.0) or 0.0)
    triangle_active = triangle_converging and triangle_breakout
    box_active = box_valid and box_breakout
    if not triangle_active and not box_active:
        if not triangle_converging and not box_valid:
            fail_reasons.append('no_triangle_or_box_structure')
        if (triangle_converging or box_valid) and not (triangle_breakout or box_breakout):
            fail_reasons.append('boundary_breakout_missing')
        if triangle_breakout and not triangle_converging:
            fail_reasons.append('triangle_not_converging')
        if box_breakout and not box_valid:
            fail_reasons.append('box_not_valid')

    if box_active:
        if box_width_ratio > 0.06:
            fail_reasons.append('box_width_ratio_high')
        if box_upper_touches < 3 or box_lower_touches < 3:
            fail_reasons.append('box_touch_count_low')
        if breakout_ret < 0.04:
            fail_reasons.append('box_breakout_ret_low')
    if float(metrics.get('close_range_10', 1.0) or 1.0) > 0.14:
        fail_reasons.append('platform_range_too_wide')
    if float(metrics.get('max_daily_abs_ret_10', 1.0) or 1.0) > 0.09:
        fail_reasons.append('platform_volatility_too_high')
    if float(metrics.get('latest_close_vs_breakout_close', 0.0) or 0.0) < 0.985:
        fail_reasons.append('breakout_hold_low')

    triggered = not fail_reasons
    pattern = 'triangle_breakout' if triangle_active else 'box_breakout' if box_active else 'platform_breakout'
    profile_name = str(metrics.get('profile_name', '') or '')
    weekly_structure_min = 0 if profile_name == 'relax' and pattern == 'triangle_breakout' else 15
    pass_fail_reasons: list[str] = []
    if not triggered:
        pass_fail_reasons.extend(fail_reasons)
    if int(metrics.get('structure_break_days', 0) or 0) > 0:
        pass_fail_reasons.append('structure_break_days_high')
    if float(metrics.get('digestion_score', 0.0) or 0.0) < 58:
        pass_fail_reasons.append('digestion_score_low')
    if float(metrics.get('weekly_structure_score', 0.0) or 0.0) < weekly_structure_min:
        pass_fail_reasons.append('weekly_structure_score_low')
    support_revisit_ratio = float(metrics.get('support_revisit_ratio', 0.0) or 0.0)
    if support_revisit_ratio < -0.04:
        pass_fail_reasons.append('support_revisit_ratio_low')
    if support_revisit_ratio > 0.20:
        pass_fail_reasons.append('support_revisit_ratio_high')

    breakout_pass = not pass_fail_reasons
    return {
        'triangle_box_breakout_triggered': triggered,
        'triangle_box_breakout_family': 'triangle_box_breakout' if triggered else 'platform_breakout',
        'triangle_box_breakout_pattern': pattern,
        'triangle_box_breakout_fail_reasons': fail_reasons,
        'triangle_box_breakout_pass': breakout_pass,
        'triangle_box_breakout_pass_fail_reasons': pass_fail_reasons,
    }


def normalize_profile_rules(profile_name: str, overrides: dict[str, Any] | None = None) -> dict[str, float | int]:
    if profile_name not in PROFILE_CONFIGS:
        raise KeyError(f'unknown profile: {profile_name}')
    rules: dict[str, float | int] = dict(PROFILE_CONFIGS[profile_name])
    for key, value in (overrides or {}).items():
        if value in {'', None} or key not in rules:
            continue
        if key in INT_RULE_KEYS:
            rules[key] = int(value)
        elif key in FLOAT_RULE_KEYS:
            rules[key] = float(value)
    rules['setup_days'] = max(3, int(rules['setup_days']))
    rules['breakout_search_days'] = max(1, int(rules.get('breakout_search_days', 1)))
    rules['pullback_days'] = max(1, int(rules['pullback_days']))
    rules['vol_ratio_lookback'] = max(1, min(int(rules['vol_ratio_lookback']), int(rules['setup_days'])))
    return rules


def _evaluate_instrument(daily: pd.DataFrame, weekly: pd.DataFrame, profile_name: str, rules: dict[str, float | int]) -> dict[str, Any] | None:
    setup_days = int(rules['setup_days'])
    breakout_search_days = int(rules.get('breakout_search_days', 1))
    pullback_days = int(rules['pullback_days'])
    context_buffer_days = 120
    total_days = setup_days + breakout_search_days + pullback_days
    required_days = total_days + context_buffer_days
    daily = daily.sort_values('datetime').tail(required_days).reset_index(drop=True)
    if len(daily) < total_days:
        return None
    weekly = weekly.sort_values('datetime').reset_index(drop=True)
    if len(weekly) < 24:
        return None
    weekly_valid = weekly.dropna(subset=['ma5', 'ma13', 'ma21']).reset_index(drop=True)
    if len(weekly_valid) < 4:
        return None

    latest_row = daily.iloc[-1]
    latest_close = float(latest_row['close'])
    latest_date = pd.Timestamp(latest_row['datetime']).date().isoformat()
    w_prev1 = weekly_valid.iloc[-2]
    w_prev2 = weekly_valid.iloc[-3]
    w_prev3 = weekly_valid.iloc[-4]

    weekly_ma21_slope = _safe_ratio(float(w_prev1['ma21']), float(w_prev3['ma21']))
    if weekly_ma21_slope is not None:
        weekly_ma21_slope -= 1.0

    rule_weekly_uptrend = bool(weekly_ma21_slope is not None and weekly_ma21_slope > 0)
    if profile_name == 'relax':
        rule_weekly_bull_alignment = bool(
            float(w_prev1['ma5']) > float(w_prev1['ma13'])
            and float(w_prev1['ma13']) >= float(w_prev1['ma21']) * 0.98
        )
        rule_weekly_upward_stack = bool(
            float(w_prev1['ma5']) >= float(w_prev2['ma5'])
            and float(w_prev1['ma13']) >= float(w_prev2['ma13'])
        )
    else:
        rule_weekly_bull_alignment = bool(float(w_prev1['ma5']) > float(w_prev1['ma13']) > float(w_prev1['ma21']))
        rule_weekly_upward_stack = bool(
            float(w_prev1['ma5']) > float(w_prev2['ma5'])
            and float(w_prev1['ma13']) >= float(w_prev2['ma13'])
            and float(w_prev1['ma21']) >= float(w_prev2['ma21'])
        )
    rule_close_above_weekly_ma13 = bool(latest_close >= float(w_prev1['ma13']))

    common_rules = {
        'rule_weekly_uptrend': rule_weekly_uptrend,
        'rule_weekly_bull_alignment': rule_weekly_bull_alignment,
        'rule_weekly_upward_stack': rule_weekly_upward_stack,
        'rule_close_above_weekly_ma13': rule_close_above_weekly_ma13,
    }

    candidate_results: list[dict[str, Any]] = []
    min_recent_pullback_days = 2 if profile_name == 'relax' else pullback_days
    max_breakout_idx = len(daily) - min_recent_pullback_days - 1
    min_breakout_idx = max(setup_days, len(daily) - pullback_days - breakout_search_days)
    for breakout_idx in range(min_breakout_idx, max_breakout_idx + 1):
        setup_start = breakout_idx - setup_days
        setup_slice = daily.iloc[setup_start:breakout_idx].reset_index(drop=True)
        if len(setup_slice) < setup_days:
            continue
        breakout_row = daily.iloc[breakout_idx]
        breakout_prev_row = daily.iloc[breakout_idx - 1]
        wash_slice = daily.iloc[breakout_idx + 1: breakout_idx + 1 + pullback_days].reset_index(drop=True)
        observed_pullback_days = len(wash_slice)
        if observed_pullback_days < min_recent_pullback_days:
            continue
        recent_ignition_window = bool(profile_name == 'relax' and observed_pullback_days < pullback_days)

        close_window = setup_slice['close']
        daily_ret_window = close_window.pct_change().dropna()
        vol_lookback = min(int(rules['vol_ratio_lookback']), setup_days)
        volume_ref = float(setup_slice['volume'].tail(vol_lookback).mean())
        breakout_close = float(breakout_row['close'])
        breakout_open = float(breakout_row['open'])
        breakout_high = float(breakout_row['high'])
        breakout_low = float(breakout_row['low'])
        breakout_prev_close = float(breakout_prev_row['close'])
        breakout_volume = float(breakout_row['volume'])
        breakout_2d_idx = min(breakout_idx + 1, len(daily) - 1)
        breakout_2d_close = float(daily.iloc[breakout_2d_idx]['close'])
        breakout_2d_volume = float(daily.iloc[breakout_idx: breakout_2d_idx + 1]['volume'].mean())
        impulse_slice = daily.iloc[setup_start: breakout_2d_idx + 1].reset_index(drop=True)
        post_breakout_window = daily.iloc[breakout_idx: breakout_idx + 1 + pullback_days].copy()
        pullback_prev = pd.concat([
            pd.Series([breakout_close]),
            wash_slice['close'].iloc[:-1].reset_index(drop=True),
        ], ignore_index=True)
        pullback_closes = wash_slice['close'].reset_index(drop=True)
        pullback_daily_rets = pullback_closes / pullback_prev - 1
        pullback_avg_vol = float(wash_slice['volume'].mean())

        close_range_10 = float(close_window.max() / close_window.min() - 1)
        max_daily_abs_ret_10 = float(daily_ret_window.abs().max()) if not daily_ret_window.empty else 0.0
        breakout_ret = float(breakout_close / breakout_prev_close - 1)
        breakout_2d_ret = float(breakout_2d_close / breakout_prev_close - 1)
        vol_ratio_5 = _safe_ratio(breakout_volume, volume_ref)
        vol_ratio_2d = _safe_ratio(breakout_2d_volume, volume_ref)
        pullback_ret_3d = float(latest_close / breakout_close - 1)
        pullback_avg_vol_ratio = _safe_ratio(pullback_avg_vol, breakout_volume)
        resistance_close_10 = float(close_window.max())
        post_breakout_max_close = float(post_breakout_window['close'].max())
        confirmation_ret = float(post_breakout_max_close / breakout_prev_close - 1)
        post_breakout_max_volume = float(post_breakout_window['volume'].max())
        confirmation_vol_ratio = _safe_ratio(post_breakout_max_volume, volume_ref)
        support_close = float(setup_slice['close'].iloc[-1])
        support_revisit_ratio = latest_close / support_close - 1
        wash_volumes = wash_slice['volume'].reset_index(drop=True)
        volume_trend_ratio = _safe_ratio(float(wash_volumes.iloc[-1]), float(wash_volumes.iloc[0])) if len(wash_volumes) >= 2 else None
        lookback_start = max(0, setup_start - 120)
        context_slice = daily.iloc[lookback_start: breakout_idx].reset_index(drop=True)
        context_high = float(context_slice['close'].max()) if not context_slice.empty else float(close_window.max())
        context_low = float(context_slice['close'].min()) if not context_slice.empty else float(close_window.min())
        prior_drop_from_context_high = float(1 - close_window.min() / context_high) if context_high > 0 else 0.0
        base_anchor = float(close_window.median()) if not close_window.empty else support_close
        base_position_ratio = float((base_anchor - context_low) / max(context_high - context_low, 1e-9)) if context_high > context_low else 1.0
        pre_base_drawdown = float(1 - support_close / context_high) if context_high > 0 else 0.0
        pre_breakout_history = daily.iloc[:breakout_idx].reset_index(drop=True)
        high_windows: dict[int, float] = {}
        dist_to_high: dict[int, float] = {}
        is_new_high: dict[int, bool] = {}
        for window in (20, 60, 120):
            hist_window = pre_breakout_history.tail(window)
            ref_high = float(hist_window['high'].max()) if not hist_window.empty else breakout_close
            high_windows[window] = ref_high
            dist = float(breakout_close / ref_high - 1) if ref_high > 0 else 0.0
            dist_to_high[window] = dist
            is_new_high[window] = bool(dist >= 0)
        precompression_range = close_range_10
        precompression_stability = max_daily_abs_ret_10
        new_high_precompression_pass = bool(precompression_range <= 0.08 and precompression_stability <= 0.05)
        context_volume_ref = float(context_slice['volume'].mean()) if not context_slice.empty else float(setup_slice['volume'].mean())
        setup_avg_amount = float(setup_slice['volume'].mean())
        setup_avg_amount_ratio = float(setup_avg_amount / context_volume_ref) if context_volume_ref > 0 else 0.0
        setup_high_turnover_days = int((setup_slice['volume'] >= context_volume_ref * 1.2).sum()) if context_volume_ref > 0 else 0
        setup_amount_std = float(context_slice['volume'].std()) if not context_slice.empty else 0.0
        setup_amount_zscore = float((setup_avg_amount - context_volume_ref) / setup_amount_std) if setup_amount_std > 0 else 0.0
        half_idx = max(2, len(setup_slice) // 2)
        first_half = setup_slice.iloc[:half_idx].reset_index(drop=True)
        second_half = setup_slice.iloc[half_idx:].reset_index(drop=True)
        first_half_high = float(first_half['high'].max())
        second_half_high = float(second_half['high'].max())
        first_half_low = float(first_half['low'].min())
        second_half_low = float(second_half['low'].min())
        first_height = max(first_half_high - first_half_low, 1e-9)
        second_height = max(second_half_high - second_half_low, 1e-9)
        triangle_upper_slope = float(second_half_high / first_half_high - 1) if first_half_high > 0 else 0.0
        triangle_lower_slope = float(second_half_low / first_half_low - 1) if first_half_low > 0 else 0.0
        triangle_convergence_ratio = float(second_height / first_height)
        strict_triangle_converging = bool(
            triangle_upper_slope <= -0.012
            and triangle_lower_slope >= 0.012
            and triangle_convergence_ratio <= 0.75
        )
        triangle_converging = bool(
            strict_triangle_converging
            or (
                triangle_upper_slope <= -0.005
                and triangle_lower_slope >= 0.008
                and triangle_convergence_ratio <= 0.75
            )
        )
        triangle_boundary = min(first_half_high, second_half_high)
        triangle_breakout_confirmed = bool(breakout_close >= triangle_boundary * 1.01)
        box_upper = float(setup_slice['high'].quantile(0.8))
        box_lower = float(setup_slice['low'].quantile(0.2))
        box_width_ratio = float(box_upper / max(box_lower, 1e-9) - 1)
        box_upper_touches = int((setup_slice['high'] >= box_upper * 0.992).sum())
        box_lower_touches = int((setup_slice['low'] <= box_lower * 1.008).sum())
        box_valid = bool(box_width_ratio <= 0.06 and box_upper_touches >= 3 and box_lower_touches >= 3)
        box_breakout_confirmed = bool(breakout_close >= box_upper * 1.012)
        close_location_ratio = _close_location_ratio(breakout_close, breakout_low, breakout_high)
        ignition_day_strength = round(
            55 * _clamp(max(breakout_ret, 0.0) / 0.07)
            + 45 * _clamp(float(vol_ratio_5 or 0.0) / 1.8),
            4,
        )

        impulse_rets = impulse_slice['close'].pct_change().fillna(0.0)
        impulse_up_day_min = max(float(rules['breakout_ret_min']) * 0.5, 0.02)
        impulse_vol_ratio_min = max(float(rules['vol_ratio_min']) * 0.8, 1.2)
        impulse_vol_ratios = impulse_slice['volume'].apply(lambda v: _safe_ratio(float(v), volume_ref) or 0.0)
        impulse_day_count = int((impulse_rets >= impulse_up_day_min).sum())
        impulse_volume_day_count = int((impulse_vol_ratios >= impulse_vol_ratio_min).sum())
        impulse_combo_day_count = int(((impulse_rets >= impulse_up_day_min) & (impulse_vol_ratios >= impulse_vol_ratio_min)).sum())
        max_impulse_ret_1d = float(impulse_rets.max()) if not impulse_rets.empty else 0.0
        impulse_2d_rets = impulse_slice['close'].pct_change(2).fillna(0.0)
        max_impulse_ret_2d = float(impulse_2d_rets.max()) if not impulse_2d_rets.empty else breakout_2d_ret
        max_impulse_vol_ratio_1d = float(impulse_vol_ratios.max()) if not impulse_vol_ratios.empty else float(vol_ratio_5 or 0.0)
        vol_2d_series = impulse_slice['volume'].rolling(2).mean().fillna(0.0)
        max_impulse_vol_ratio_2d = max((_safe_ratio(float(v), volume_ref) or 0.0) for v in vol_2d_series)
        impulse_last_idx = 0
        for idx2, (ret_val, vol_val) in enumerate(zip(impulse_rets.tolist(), impulse_vol_ratios.tolist())):
            if ret_val >= impulse_up_day_min or vol_val >= impulse_vol_ratio_min:
                impulse_last_idx = idx2
        impulse_recency_days = max(0, len(impulse_slice) - 1 - impulse_last_idx)

        controlled_down_min = float(rules['pullback_daily_min'])
        controlled_down_days = int(((pullback_daily_rets < 0) & (pullback_daily_rets >= controlled_down_min)).sum())
        mild_up_cap = max(float(rules['max_daily_abs_ret_max']) * 0.6, 0.025)
        mild_up_days = int(((pullback_daily_rets > 0) & (pullback_daily_rets <= mild_up_cap)).sum())
        abnormal_down_days = int((pullback_daily_rets < controlled_down_min).sum())
        abnormal_up_days = int((pullback_daily_rets > float(rules['max_daily_abs_ret_max'])).sum())
        negative_pullback_days = int((pullback_daily_rets < 0).sum())
        chase_up_days_after_ignition = int((pullback_daily_rets > mild_up_cap).sum())
        digestion_days = int(len(wash_slice))
        recent_ignition_prepass = False
        digestion_range = float(wash_slice['close'].max() / wash_slice['close'].min() - 1) if not wash_slice.empty else 0.0
        digestion_mean_abs_ret = float(pullback_daily_rets.abs().mean()) if not pullback_daily_rets.empty else 0.0
        digestion_close_drift = pullback_ret_3d
        digestion_volume_ratio = float(pullback_avg_vol_ratio or 0.0)
        digestion_volume_trend_ratio = float(volume_trend_ratio or 0.0)
        structure_break_days = int((wash_slice['close'] < support_close * 0.97).sum())
        washout_direction_bias = round((negative_pullback_days - mild_up_days) / max(len(pullback_daily_rets), 1), 6)
        max_washout_vol_spike = float(wash_volumes.max() / breakout_volume) if breakout_volume > 0 and not wash_volumes.empty else 0.0

        recency_cap = max(1, breakout_search_days + pullback_days)
        impulse_score = round(
            30 * _clamp(max_impulse_ret_1d / 0.08)
            + 25 * _clamp(max_impulse_ret_2d / 0.12)
            + 20 * _clamp(max_impulse_vol_ratio_1d / 2.0)
            + 15 * _clamp(impulse_combo_day_count / 3.0)
            + 10 * _clamp((recency_cap - impulse_recency_days) / recency_cap),
            4,
        )
        support_zone_bonus = 1.0 if -0.05 <= support_revisit_ratio <= 0.10 else max(0.0, 1.0 - abs(support_revisit_ratio - 0.025) / 0.20)
        structure_bonus = 1.0 if structure_break_days == 0 else max(0.0, 1.0 - structure_break_days / 3.0)
        base_background_score = round(
            45 * _clamp(prior_drop_from_context_high / 0.22)
            + 35 * (1 - _clamp(base_position_ratio / 0.55))
            + 20 * _clamp(pre_base_drawdown / 0.18),
            4,
        )
        digestion_score = round(
            25 * _clamp(controlled_down_days / 6.0)
            + 20 * (1 - _clamp(abnormal_down_days / 2.0))
            + 15 * (1 - _clamp(digestion_mean_abs_ret / 0.05))
            + 20 * (1 - _clamp(max(digestion_volume_ratio - 0.6, 0.0) / 0.8))
            + 10 * support_zone_bonus
            + 10 * structure_bonus,
            4,
        )

        washout = _evaluate_washout(
            pullback_daily_rets,
            pullback_avg_vol_ratio,
            pullback_ret_3d,
            support_revisit_ratio,
            volume_trend_ratio,
            daily_min=float(rules['pullback_daily_min']),
            total_min=float(rules['pullback_ret_min']),
            avg_vol_max=float(rules['pullback_avg_vol_ratio_max']),
        )
        digestion = _evaluate_digestion(
            pullback_daily_rets,
            pullback_avg_vol_ratio,
            pullback_ret_3d,
            support_revisit_ratio,
            confirmation_ret,
            volume_trend_ratio,
        )

        rule_close_range_10 = close_range_10 <= float(rules['close_range_max'])
        rule_max_daily_abs_ret_10 = max_daily_abs_ret_10 <= float(rules['max_daily_abs_ret_max'])
        rule_breakout_green = breakout_close > breakout_open
        pattern_digestion = False
        canonical_washout = False
        controlled_digestion_variant = False
        if profile_name == 'relax':
            rule_breakout_close = bool(post_breakout_max_close >= resistance_close_10 * 1.01)
            delayed_followthrough = bool(
                confirmation_ret >= float(rules['breakout_ret_min']) + 0.08
                and latest_close >= breakout_close * 1.05
            )
            pattern_digestion = bool(delayed_followthrough or digestion['is_digestion'] or washout['is_washout'])
            ignition_flags = evaluate_relax_ignition_flags(
                {
                    'breakout_ret': breakout_ret,
                    'confirmation_ret': confirmation_ret,
                    'vol_ratio_5': vol_ratio_5,
                    'confirmation_vol_ratio': confirmation_vol_ratio,
                    'breakout_close': breakout_close,
                    'breakout_prev_close': breakout_prev_close,
                    'breakout_open': breakout_open,
                    'close_location_ratio': close_location_ratio,
                    'ignition_day_strength': ignition_day_strength,
                    'breakout_ret_min': float(rules['breakout_ret_min']),
                    'vol_ratio_min': float(rules['vol_ratio_min']),
                }
            )
            rule_breakout_green = ignition_flags['rule_breakout_green']
            rule_breakout_ret = ignition_flags['rule_breakout_ret']
            rule_breakout_volume = ignition_flags['rule_breakout_volume']
            rule_pullback_daily = bool(
                negative_pullback_days >= max(2, pullback_days - 1)
                and controlled_down_days >= 2
                and abnormal_up_days == 0
                and chase_up_days_after_ignition == 0
            )
            rule_pullback_3d = bool(
                pullback_ret_3d <= 0
                and pullback_ret_3d >= float(rules['pullback_ret_min'])
                and -0.08 <= support_revisit_ratio <= 0.12
            )
            rule_pullback_volume = bool(
                pullback_avg_vol_ratio is not None
                and pullback_avg_vol_ratio <= 0.95
                and max_washout_vol_spike <= 1.05
                and (volume_trend_ratio is None or volume_trend_ratio <= 1.0)
            )
            rule_background_base = bool(
                prior_drop_from_context_high >= 0.10
                and base_position_ratio <= 0.80
                and pre_base_drawdown >= 0.08
            )
            common_entry_flags = evaluate_relax_common_entry_gates(
                {
                    'base_background_score': base_background_score,
                    'rule_background_base': rule_background_base,
                    'rule_close_range_10': rule_close_range_10,
                    'rule_max_daily_abs_ret_10': rule_max_daily_abs_ret_10,
                    'rule_breakout_ret': rule_breakout_ret,
                    'rule_breakout_volume': rule_breakout_volume,
                    'rule_breakout_green': rule_breakout_green,
                    'ignition_day_strength': ignition_day_strength,
                }
            )
            impulse_pass = common_entry_flags['entry_gate_pass']
            digestion_flags = evaluate_relax_digestion_flags(
                {
                    'pattern_digestion': pattern_digestion,
                    'digestion_score': digestion_score,
                    'controlled_down_days': controlled_down_days,
                    'negative_pullback_days': negative_pullback_days,
                    'mild_up_days': mild_up_days,
                    'abnormal_down_days': abnormal_down_days,
                    'abnormal_up_days': abnormal_up_days,
                    'digestion_volume_ratio': digestion_volume_ratio,
                    'max_washout_vol_spike': max_washout_vol_spike,
                    'structure_break_days': structure_break_days,
                    'washout_direction_bias': washout_direction_bias,
                    'pullback_ret_3d': pullback_ret_3d,
                    'pullback_ret_min': float(rules['pullback_ret_min']),
                    'pullback_days': pullback_days,
                    'rule_pullback_daily': rule_pullback_daily,
                    'rule_pullback_3d': rule_pullback_3d,
                    'rule_pullback_volume': rule_pullback_volume,
                }
            )
            canonical_washout = digestion_flags['canonical_washout']
            controlled_digestion_variant = digestion_flags['controlled_digestion_variant']
            digestion_pass = digestion_flags['digestion_pass']
            prepass_flags = evaluate_relax_prepass_flags(
                {
                    'recent_ignition_window': recent_ignition_window,
                    'entry_gate_pass': common_entry_flags['entry_gate_pass'],
                    'breakout_ret': breakout_ret,
                    'breakout_ret_min': float(rules['breakout_ret_min']),
                    'vol_ratio_5': float(vol_ratio_5 or 0.0),
                    'vol_ratio_min': float(rules['vol_ratio_min']),
                    'close_location_ratio': close_location_ratio,
                    'observed_pullback_days': observed_pullback_days,
                    'structure_break_days': structure_break_days,
                    'abnormal_down_days': abnormal_down_days,
                    'latest_close_vs_breakout_close': (latest_close / breakout_close) if breakout_close > 0 else 0.0,
                    'pullback_avg_vol_ratio': pullback_avg_vol_ratio,
                    'max_washout_vol_spike': max_washout_vol_spike,
                    'volume_trend_ratio': volume_trend_ratio,
                }
            )
            recent_ignition_prepass = prepass_flags['recent_ignition_prepass']
        else:
            rule_breakout_ret = breakout_ret >= float(rules['breakout_ret_min'])
            rule_breakout_volume = bool(vol_ratio_5 is not None and vol_ratio_5 >= float(rules['vol_ratio_min']))
            rule_breakout_close = breakout_close > resistance_close_10
            rule_pullback_daily = bool((pullback_daily_rets > float(rules['pullback_daily_min'])).all())
            rule_pullback_3d = pullback_ret_3d >= float(rules['pullback_ret_min'])
            rule_pullback_volume = bool(
                pullback_avg_vol_ratio is not None and pullback_avg_vol_ratio <= float(rules['pullback_avg_vol_ratio_max'])
            )
            impulse_pass = bool(
                impulse_score >= 70
                and impulse_combo_day_count >= 2
                and max_impulse_ret_1d >= 0.06
                and max_impulse_vol_ratio_1d >= 1.8
                and impulse_recency_days <= 8
            )
            digestion_pass = bool(
                digestion_score >= 70
                and controlled_down_days >= 3
                and abnormal_down_days == 0
                and digestion_volume_ratio <= 0.85
                and -0.04 <= support_revisit_ratio <= 0.10
                and washout['is_washout']
            )

        weekly_structure_score = round(
            35 * float(rule_weekly_uptrend)
            + 35 * float(rule_weekly_bull_alignment)
            + 30 * float(rule_weekly_upward_stack),
            4,
        )
        second_leg_distance = max(0.0, float(post_breakout_max_close / latest_close - 1)) if latest_close > 0 else 0.0
        retest_high_ratio = float(latest_close / post_breakout_max_close - 1) if post_breakout_max_close > 0 else 0.0
        second_leg_ready = bool(
            latest_close >= support_close * 0.98
            and second_leg_distance <= (0.15 if profile_name == 'relax' else 0.08)
            and structure_break_days <= (1 if profile_name == 'relax' else 0)
        )
        second_leg_score = round(
            45 * (1 - _clamp(second_leg_distance / (0.15 if profile_name == 'relax' else 0.10)))
            + 25 * _clamp(weekly_structure_score / 100.0)
            + 15 * structure_bonus
            + 15 * support_zone_bonus,
            4,
        )
        second_leg_pass = bool(second_leg_ready and weekly_structure_score >= (35 if profile_name == 'relax' else 65))
        if recent_ignition_prepass:
            digestion_pass = True
        shape_score = round(0.45 * impulse_score + 0.40 * digestion_score + 0.15 * second_leg_score, 4)
        base_shape_pass = bool(
            impulse_pass
            and digestion_pass
            and (second_leg_pass or recent_ignition_prepass)
        )
        shape_pass = base_shape_pass

        impulse_fail_reasons: list[str] = []
        digestion_fail_reasons: list[str] = []
        second_leg_fail_reasons: list[str] = []
        prepass_fail_reasons: list[str] = []
        if not impulse_pass:
            if impulse_score < (55 if profile_name == 'relax' else 70):
                impulse_fail_reasons.append('impulse_score_low')
            if profile_name == 'relax':
                impulse_fail_reasons.extend(common_entry_flags.get('entry_gate_fail_reasons', []))
            if profile_name == 'relax' and not rule_background_base:
                impulse_fail_reasons.append('background_base_fail')
            if impulse_day_count < (2 if profile_name == 'relax' else 1):
                impulse_fail_reasons.append('impulse_day_count_low')
            if impulse_volume_day_count < 1:
                impulse_fail_reasons.append('impulse_volume_day_count_low')
        if not digestion_pass:
            if digestion_score < (55 if profile_name == 'relax' else 70):
                digestion_fail_reasons.append('digestion_score_low')
            if abnormal_down_days > (1 if profile_name == 'relax' else 0):
                digestion_fail_reasons.append('abnormal_down_days_high')
            if digestion_volume_ratio > (1.10 if profile_name == 'relax' else 0.85):
                digestion_fail_reasons.append('digestion_volume_ratio_high')
            if structure_break_days > (1 if profile_name == 'relax' else 0):
                digestion_fail_reasons.append('structure_break_days_high')
        if profile_name == 'relax' and not recent_ignition_prepass:
            prepass_fail_reasons = prepass_flags.get('prepass_fail_reasons', [])
        if not second_leg_pass and not recent_ignition_prepass:
            if second_leg_distance > (0.15 if profile_name == 'relax' else 0.08):
                second_leg_fail_reasons.append('second_leg_distance_high')
            if weekly_structure_score < (35 if profile_name == 'relax' else 65):
                second_leg_fail_reasons.append('weekly_structure_score_low')
            if not second_leg_ready:
                second_leg_fail_reasons.append('second_leg_not_ready')

        rule_map = {
            **common_rules,
            'rule_background_base': rule_background_base if profile_name == 'relax' else True,
            'rule_close_range_10': rule_close_range_10,
            'rule_max_daily_abs_ret_10': rule_max_daily_abs_ret_10,
            'rule_breakout_green': rule_breakout_green,
            'rule_breakout_ret': rule_breakout_ret,
            'rule_breakout_volume': rule_breakout_volume,
            'rule_breakout_close': rule_breakout_close,
            'rule_pullback_daily': rule_pullback_daily,
            'rule_pullback_3d': rule_pullback_3d,
            'rule_pullback_volume': rule_pullback_volume,
            'impulse_pass': impulse_pass,
            'digestion_pass': digestion_pass,
            'second_leg_pass': second_leg_pass,
            'recent_ignition_prepass': recent_ignition_prepass,
            'shape_pass': shape_pass,
        }
        framework_scores = build_framework_scores({
            'base_background_score': base_background_score,
            'close_range_10': close_range_10,
            'max_daily_abs_ret_10': max_daily_abs_ret_10,
            'impulse_score': impulse_score,
            'digestion_score': digestion_score,
            'second_leg_score': second_leg_score,
        })
        latest_close_vs_breakout_close = (latest_close / breakout_close) if breakout_close > 0 else 0.0
        second_leg_breakout = evaluate_second_leg_breakout({
            'impulse_pass': impulse_pass,
            'digestion_pass': digestion_pass,
            'recent_ignition_prepass': recent_ignition_prepass,
            'structure_break_days': structure_break_days,
            'support_revisit_ratio': support_revisit_ratio,
            'confirmation_ret': confirmation_ret,
            'confirmation_vol_ratio': float(confirmation_vol_ratio or 0.0),
            'second_leg_distance': second_leg_distance,
            'retest_high_ratio': retest_high_ratio,
            'latest_close_vs_breakout_close': latest_close_vs_breakout_close,
        })
        new_high_breakout = evaluate_new_high_breakout({
            'impulse_pass': impulse_pass,
            'rule_breakout_ret': rule_breakout_ret,
            'rule_breakout_volume': rule_breakout_volume,
            'is_new_high_20': is_new_high[20],
            'is_new_high_60': is_new_high[60],
            'is_new_high_120': is_new_high[120],
            'new_high_precompression_pass': new_high_precompression_pass,
            'close_range_10': close_range_10,
            'max_daily_abs_ret_10': max_daily_abs_ret_10,
            'breakout_ret': breakout_ret,
            'confirmation_vol_ratio': float(confirmation_vol_ratio or 0.0),
            'latest_close_vs_breakout_close': latest_close_vs_breakout_close,
            'structure_break_days': structure_break_days,
            'digestion_score': digestion_score,
            'weekly_structure_score': weekly_structure_score,
            'support_revisit_ratio': support_revisit_ratio,
        })
        high_turnover_breakout = evaluate_high_turnover_breakout({
            'impulse_pass': impulse_pass,
            'rule_breakout_ret': rule_breakout_ret,
            'rule_breakout_volume': rule_breakout_volume,
            'setup_avg_amount_ratio': setup_avg_amount_ratio,
            'setup_high_turnover_days': setup_high_turnover_days,
            'setup_amount_zscore': setup_amount_zscore,
            'close_range_10': close_range_10,
            'max_daily_abs_ret_10': max_daily_abs_ret_10,
            'latest_close_vs_breakout_close': latest_close_vs_breakout_close,
            'structure_break_days': structure_break_days,
            'digestion_score': digestion_score,
            'weekly_structure_score': weekly_structure_score,
            'support_revisit_ratio': support_revisit_ratio,
        })
        triangle_box_breakout = evaluate_triangle_box_breakout({
            'profile_name': profile_name,
            'impulse_pass': impulse_pass,
            'rule_breakout_ret': rule_breakout_ret,
            'rule_breakout_volume': rule_breakout_volume,
            'triangle_converging': triangle_converging,
            'triangle_breakout_confirmed': triangle_breakout_confirmed,
            'box_valid': box_valid,
            'box_breakout_confirmed': box_breakout_confirmed,
            'box_width_ratio': box_width_ratio,
            'box_upper_touches': box_upper_touches,
            'box_lower_touches': box_lower_touches,
            'breakout_ret': breakout_ret,
            'close_range_10': close_range_10,
            'max_daily_abs_ret_10': max_daily_abs_ret_10,
            'latest_close_vs_breakout_close': latest_close_vs_breakout_close,
            'structure_break_days': structure_break_days,
            'digestion_score': digestion_score,
            'weekly_structure_score': weekly_structure_score,
            'support_revisit_ratio': support_revisit_ratio,
        })
        if second_leg_breakout['second_leg_breakout_triggered']:
            pattern_family = second_leg_breakout['second_leg_breakout_family']
        elif high_turnover_breakout['high_turnover_breakout_triggered']:
            pattern_family = high_turnover_breakout['high_turnover_breakout_family']
        elif triangle_converging and triangle_box_breakout['triangle_box_breakout_triggered']:
            pattern_family = triangle_box_breakout['triangle_box_breakout_family']
        elif new_high_breakout['new_high_breakout_triggered']:
            pattern_family = new_high_breakout['new_high_breakout_family']
        elif triangle_box_breakout['triangle_box_breakout_triggered']:
            pattern_family = triangle_box_breakout['triangle_box_breakout_family']
        else:
            pattern_family = 'platform_breakout'
        if pattern_family == 'new_high_breakout' and new_high_breakout['new_high_breakout_pass']:
            shape_pass = True
        if pattern_family == 'high_turnover_breakout' and high_turnover_breakout['high_turnover_breakout_pass']:
            shape_pass = True
        if pattern_family == 'triangle_box_breakout' and triangle_box_breakout['triangle_box_breakout_pass']:
            shape_pass = True
        rule_map['new_high_breakout_triggered'] = new_high_breakout['new_high_breakout_triggered']
        rule_map['new_high_breakout_pass'] = new_high_breakout['new_high_breakout_pass']
        rule_map['high_turnover_breakout_triggered'] = high_turnover_breakout['high_turnover_breakout_triggered']
        rule_map['high_turnover_breakout_pass'] = high_turnover_breakout['high_turnover_breakout_pass']
        rule_map['triangle_box_breakout_triggered'] = triangle_box_breakout['triangle_box_breakout_triggered']
        rule_map['triangle_box_breakout_pass'] = triangle_box_breakout['triangle_box_breakout_pass']
        rule_map['shape_pass'] = shape_pass
        pattern_stage = infer_pattern_stage({
            'pattern_passed': shape_pass,
            'second_leg_pass': second_leg_pass,
            'digestion_pass': digestion_pass,
            'recent_ignition_prepass': recent_ignition_prepass,
            'rule_breakout_ret': rule_breakout_ret,
            'rule_breakout_volume': rule_breakout_volume,
        })
        rules_passed_count = int(sum(1 for value in rule_map.values() if value))
        candidate_results.append({
            'instrument': str(latest_row['instrument']),
            'signal_date': latest_date,
            'latest_close': latest_close,
            'scanner_name': SCANNER_DISPLAY_NAME,
            'pattern_name': SCANNER_DISPLAY_NAME,
            'pattern_family': pattern_family,
            'pattern_stage': pattern_stage,
            'pattern_profile': profile_name,
            'pattern_passed': shape_pass,
            'base_shape_pass': base_shape_pass,
            'rules_passed_count': rules_passed_count,
            'prior_drop_from_context_high': round(prior_drop_from_context_high, 6),
            'base_position_ratio': round(base_position_ratio, 6),
            'pre_base_drawdown': round(pre_base_drawdown, 6),
            'base_background_score': base_background_score,
            'background_score': framework_scores['background_score'],
            'consolidation_score': framework_scores['consolidation_score'],
            'breakout_quality_score': framework_scores['breakout_quality_score'],
            'followthrough_score': framework_scores['followthrough_score'],
            'final_score': framework_scores['final_score'],
            'ignition_day_strength': ignition_day_strength,
            'close_location_ratio': round(close_location_ratio, 6),
            'impulse_score': impulse_score,
            'impulse_day_count': impulse_day_count,
            'impulse_volume_day_count': impulse_volume_day_count,
            'impulse_combo_day_count': impulse_combo_day_count,
            'max_impulse_ret_1d': round(max_impulse_ret_1d, 6),
            'max_impulse_ret_2d': round(max_impulse_ret_2d, 6),
            'max_impulse_vol_ratio_1d': round(max_impulse_vol_ratio_1d, 6),
            'max_impulse_vol_ratio_2d': round(max_impulse_vol_ratio_2d, 6),
            'impulse_recency_days': impulse_recency_days,
            'digestion_score': digestion_score,
            'digestion_days': digestion_days,
            'observed_pullback_days': observed_pullback_days,
            'controlled_down_days': controlled_down_days,
            'negative_pullback_days': negative_pullback_days,
            'mild_up_days': mild_up_days,
            'chase_up_days_after_ignition': chase_up_days_after_ignition,
            'abnormal_down_days': abnormal_down_days,
            'abnormal_up_days': abnormal_up_days,
            'digestion_range': round(digestion_range, 6),
            'digestion_mean_abs_ret': round(digestion_mean_abs_ret, 6),
            'digestion_close_drift': round(digestion_close_drift, 6),
            'digestion_volume_ratio': round(digestion_volume_ratio, 6),
            'digestion_volume_trend_ratio': round(digestion_volume_trend_ratio, 6),
            'washout_direction_bias': washout_direction_bias,
            'max_washout_vol_spike': round(max_washout_vol_spike, 6),
            'structure_break_days': structure_break_days,
            'weekly_structure_score': weekly_structure_score,
            'second_leg_score': second_leg_score,
            'second_leg_ready': second_leg_ready,
            'second_leg_breakout_triggered': second_leg_breakout['second_leg_breakout_triggered'],
            'second_leg_breakout_fail_reasons': ','.join(second_leg_breakout['second_leg_breakout_fail_reasons']),
            'new_high_breakout_triggered': new_high_breakout['new_high_breakout_triggered'],
            'new_high_breakout_pass': new_high_breakout['new_high_breakout_pass'],
            'new_high_breakout_fail_reasons': ','.join(new_high_breakout['new_high_breakout_fail_reasons']),
            'new_high_breakout_pass_fail_reasons': ','.join(new_high_breakout['new_high_breakout_pass_fail_reasons']),
            'new_high_window': new_high_breakout['new_high_window'],
            'high_turnover_breakout_triggered': high_turnover_breakout['high_turnover_breakout_triggered'],
            'high_turnover_breakout_pass': high_turnover_breakout['high_turnover_breakout_pass'],
            'high_turnover_breakout_fail_reasons': ','.join(high_turnover_breakout['high_turnover_breakout_fail_reasons']),
            'high_turnover_breakout_pass_fail_reasons': ','.join(high_turnover_breakout['high_turnover_breakout_pass_fail_reasons']),
            'triangle_box_breakout_triggered': triangle_box_breakout['triangle_box_breakout_triggered'],
            'triangle_box_breakout_pass': triangle_box_breakout['triangle_box_breakout_pass'],
            'triangle_box_breakout_pattern': triangle_box_breakout['triangle_box_breakout_pattern'],
            'triangle_box_breakout_fail_reasons': ','.join(triangle_box_breakout['triangle_box_breakout_fail_reasons']),
            'triangle_box_breakout_pass_fail_reasons': ','.join(triangle_box_breakout['triangle_box_breakout_pass_fail_reasons']),
            'setup_avg_amount': round(setup_avg_amount, 6),
            'setup_avg_amount_ratio': round(setup_avg_amount_ratio, 6),
            'setup_high_turnover_days': setup_high_turnover_days,
            'setup_amount_zscore': round(setup_amount_zscore, 6),
            'triangle_upper_slope': round(triangle_upper_slope, 6),
            'triangle_lower_slope': round(triangle_lower_slope, 6),
            'triangle_convergence_ratio': round(triangle_convergence_ratio, 6),
            'triangle_converging': triangle_converging,
            'triangle_breakout_confirmed': triangle_breakout_confirmed,
            'box_width_ratio': round(box_width_ratio, 6),
            'box_upper_touches': box_upper_touches,
            'box_lower_touches': box_lower_touches,
            'box_valid': box_valid,
            'box_breakout_confirmed': box_breakout_confirmed,
            'dist_to_high_20': round(dist_to_high[20], 6),
            'dist_to_high_60': round(dist_to_high[60], 6),
            'dist_to_high_120': round(dist_to_high[120], 6),
            'is_new_high_20': is_new_high[20],
            'is_new_high_60': is_new_high[60],
            'is_new_high_120': is_new_high[120],
            'new_high_precompression_pass': new_high_precompression_pass,
            'second_leg_distance': round(second_leg_distance, 6),
            'retest_high_ratio': round(retest_high_ratio, 6),
            'shape_score': shape_score,
            'shape_pass': shape_pass,
            'impulse_fail_reasons': ','.join(impulse_fail_reasons),
            'digestion_fail_reasons': ','.join(digestion_fail_reasons),
            'second_leg_fail_reasons': ','.join(second_leg_fail_reasons),
            'prepass_fail_reasons': ','.join(prepass_fail_reasons),
            'weekly_ma5': round(float(w_prev1['ma5']), 6),
            'weekly_ma13': round(float(w_prev1['ma13']), 6),
            'weekly_ma21': round(float(w_prev1['ma21']), 6),
            'weekly_ma21_slope': round(float(weekly_ma21_slope or 0.0), 6),
            'close_range_10': round(close_range_10, 6),
            'max_daily_abs_ret_10': round(max_daily_abs_ret_10, 6),
            'breakout_ret': round(breakout_ret, 6),
            'breakout_2d_ret': round(breakout_2d_ret, 6),
            'confirmation_ret': round(confirmation_ret, 6),
            'vol_ratio_5': round(float(vol_ratio_5 or 0.0), 6),
            'vol_ratio_2d': round(float(vol_ratio_2d or 0.0), 6),
            'confirmation_vol_ratio': round(float(confirmation_vol_ratio or 0.0), 6),
            'pullback_ret_3d': round(pullback_ret_3d, 6),
            'pullback_avg_vol_ratio': round(float(pullback_avg_vol_ratio or 0.0), 6),
            'resistance_close_10': round(resistance_close_10, 6),
            'post_breakout_max_close': round(post_breakout_max_close, 6),
            'support_close': round(support_close, 6),
            'support_revisit_ratio': round(support_revisit_ratio, 6),
            'candidate_breakout_date': pd.Timestamp(breakout_row['datetime']).date().isoformat(),
            'washout_gentle_price': washout['gentle_price'],
            'washout_volume_contracting': washout['volume_contracting'],
            'washout_structure_intact': washout['structure_intact'],
            'digestion_orderly_price': digestion['orderly_price'],
            'digestion_contained_volume': digestion['contained_volume'],
            'digestion_structure_intact': digestion['structure_intact'],
            'pattern_digestion': pattern_digestion if profile_name == 'relax' else False,
            'canonical_washout': canonical_washout if profile_name == 'relax' else False,
            'controlled_digestion_variant': controlled_digestion_variant if profile_name == 'relax' else False,
            'recent_ignition_window': recent_ignition_window if profile_name == 'relax' else False,
            'recent_ignition_prepass': recent_ignition_prepass if profile_name == 'relax' else False,
            **rule_map,
        })

    if not candidate_results:
        return None
    candidate_results = sorted(
        candidate_results,
        key=lambda row: (
            row['pattern_passed'],
            row['shape_score'],
            row['impulse_score'],
            row['digestion_score'],
            row['second_leg_score'],
            row['confirmation_ret'],
        ),
        reverse=True,
    )
    return candidate_results[0]


def run_wangji_scanner(
    cfg: dict,
    profile_name: str,
    daily: pd.DataFrame | None = None,
    overrides: dict[str, Any] | None = None,
    progress_callback=None,
) -> pd.DataFrame:
    rules = normalize_profile_rules(profile_name, overrides)
    if progress_callback:
        progress_callback('preparing', '正在准备扫描参数')
    daily_panel = daily.copy() if daily is not None else _build_live_hist_panel(cfg, progress_callback=progress_callback)
    if progress_callback:
        progress_callback('weekly', '正在聚合周线并计算均线')
    weekly = _build_weekly_panel(daily_panel)
    rows: list[dict[str, Any]] = []
    groups = list(daily_panel.groupby('instrument'))
    total_groups = len(groups)
    for idx, (instrument, group) in enumerate(groups, start=1):
        weekly_group = weekly[weekly['instrument'] == instrument].copy()
        row = _evaluate_instrument(group.copy(), weekly_group, profile_name, rules)
        if row is not None:
            rows.append(row)
        if progress_callback and (idx == 1 or idx == total_groups or idx % 50 == 0):
            progress_callback('scanning', f'正在逐票筛选：{idx}/{total_groups}')
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if progress_callback:
        progress_callback('ranking', '正在排序候选并生成结果')
    if profile_name == 'relax':
        df = df.sort_values(
            ['pattern_passed', 'shape_score', 'digestion_score', 'impulse_score', 'second_leg_distance', 'digestion_volume_ratio', 'close_range_10'],
            ascending=[False, False, False, False, True, True, True],
        ).reset_index(drop=True)
    else:
        df = df.sort_values(
            ['pattern_passed', 'shape_score', 'impulse_score', 'digestion_score', 'weekly_structure_score', 'second_leg_score'],
            ascending=[False, False, False, False, False, False],
        ).reset_index(drop=True)
    df['scanner_rank'] = df.index + 1
    if progress_callback:
        progress_callback('done', '候选生成完成')
    return df


def run_all_wangji_scanner_profiles(cfg: dict) -> dict[str, pd.DataFrame]:
    daily = _build_live_hist_panel(cfg)
    return {profile_name: run_wangji_scanner(cfg, profile_name, daily=daily) for profile_name in PROFILE_CONFIGS}


def summarize_wangji_scanner_run(df: pd.DataFrame, profile_name: str, rules: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        'profile': profile_name,
        'rules': rules or normalize_profile_rules(profile_name),
        'total': int(len(df)),
        'passed': int(df['pattern_passed'].sum()) if not df.empty else 0,
        'top_instruments': df.head(10)['instrument'].astype(str).tolist() if not df.empty else [],
    }


def select_best_calibration_window(replay_df: pd.DataFrame) -> dict[str, Any]:
    if replay_df.empty:
        return {
            'pass_count': 0,
            'best_signal_date': '',
            'best_breakout_date': '',
            'best_shape_score': 0.0,
            'best_row': {},
        }
    ranked = replay_df.sort_values(
        ['pattern_passed', 'shape_score', 'impulse_score', 'digestion_score', 'signal_date'],
        ascending=[False, False, False, False, False],
    ).reset_index(drop=True)
    best = ranked.iloc[0].to_dict()
    return {
        'pass_count': int(replay_df['pattern_passed'].sum()),
        'best_signal_date': str(best.get('signal_date', '')),
        'best_breakout_date': str(best.get('candidate_breakout_date', '')),
        'best_shape_score': round(float(best.get('shape_score', 0.0) or 0.0), 4),
        'best_row': best,
    }


def replay_calibration_case(
    code: str,
    profile_name: str = 'relax',
    start_date: str = '20240101',
    end_date: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    target_start_dt = datetime.strptime(start_date, '%Y%m%d')
    target_end = end_date or datetime.now().strftime('%Y%m%d')
    target_end_dt = datetime.strptime(target_end, '%Y%m%d')
    fetch_start = (target_start_dt - timedelta(days=420)).strftime('%Y%m%d')
    rules = normalize_profile_rules(profile_name)
    daily = fetch_recent_hist_for_code(code, fetch_start, target_end)
    daily = daily.sort_values('datetime').reset_index(drop=True)
    weekly = _build_weekly_panel(daily)
    rows: list[dict[str, Any]] = []
    for dt in daily['datetime']:
        if pd.Timestamp(dt).to_pydatetime() < target_start_dt or pd.Timestamp(dt).to_pydatetime() > target_end_dt:
            continue
        dsub = daily[daily['datetime'] <= dt].copy()
        wsub = weekly[weekly['datetime'] <= dt].copy()
        row = _evaluate_instrument(dsub, wsub, profile_name, rules)
        if row is not None:
            rows.append(row)
    replay_df = pd.DataFrame(rows)
    summary = select_best_calibration_window(replay_df)
    summary.update({'code': code, 'profile': profile_name, 'rows': int(len(replay_df)), 'target_start_date': start_date, 'target_end_date': target_end})
    return replay_df, summary


def build_wangji_scanner_report(df: pd.DataFrame, profile_name: str, rules: dict[str, Any] | None = None) -> str:
    lines = [f'# {SCANNER_DISPLAY_NAME} / {profile_name}', '']
    if df.empty:
        lines.append('- no candidates generated')
        return '\n'.join(lines)

    passed = df[df['pattern_passed']].copy()
    family_counts = df['pattern_family'].fillna('platform_breakout').astype(str).value_counts().to_dict()
    family_pass_counts = passed['pattern_family'].fillna('platform_breakout').astype(str).value_counts().to_dict()
    family_c = df[df['pattern_family'] == 'new_high_breakout'].copy()
    family_c_passed = family_c[family_c['pattern_passed']].copy()
    family_c_near_miss = family_c[~family_c['pattern_passed']].copy()
    family_d = df[df['pattern_family'] == 'high_turnover_breakout'].copy()
    family_d_passed = family_d[family_d['pattern_passed']].copy()
    family_d_near_miss = family_d[~family_d['pattern_passed']].copy()
    family_e = df[df['pattern_family'] == 'triangle_box_breakout'].copy()
    family_e_passed = family_e[family_e['pattern_passed']].copy()
    family_e_near_miss = family_e[~family_e['pattern_passed']].copy()

    lines.append(f'- total_evaluated: {len(df)}')
    lines.append(f'- passed: {len(passed)}')
    lines.append(f'- family_counts: {family_counts}')
    lines.append(f'- family_pass_counts: {family_pass_counts}')
    lines.append('')
    lines.append('## Top Passed Candidates')
    if passed.empty:
        lines.append('- none')
    else:
        for _, row in passed.head(20).iterrows():
            lines.append(
                f"- {row['instrument']}: family={row['pattern_family']}, shape_score={row['shape_score']:.2f}, impulse_score={row['impulse_score']:.2f}, digestion_score={row['digestion_score']:.2f}, controlled_down_days={row['controlled_down_days']}, second_leg_distance={row['second_leg_distance']:.4f}, rules_passed={row['rules_passed_count']}"
            )

    lines.append('')
    lines.append('## Family C / new_high_breakout')
    lines.append(f'- total: {len(family_c)}')
    lines.append(f'- passed: {len(family_c_passed)}')
    if family_c_passed.empty:
        lines.append('- passed_candidates: none')
    else:
        for _, row in family_c_passed.head(10).iterrows():
            lines.append(
                f"- PASS {row['instrument']}: window={row['new_high_window']}, breakout_ret={row['breakout_ret']:.4f}, vol_ratio_5={row['vol_ratio_5']:.4f}, weekly_structure_score={row['weekly_structure_score']:.2f}, digestion_score={row['digestion_score']:.2f}, family_pass={row['new_high_breakout_pass']}"
            )
    if family_c_near_miss.empty:
        lines.append('- near_miss_candidates: none')
    else:
        for _, row in family_c_near_miss.head(10).iterrows():
            lines.append(
                f"- MISS {row['instrument']}: window={row['new_high_window']}, breakout_ret={row['breakout_ret']:.4f}, vol_ratio_5={row['vol_ratio_5']:.4f}, family_triggered={row['new_high_breakout_triggered']}, family_pass_fail={row['new_high_breakout_pass_fail_reasons'] or '-'}, base_shape_pass={row.get('base_shape_pass', False)}"
            )

    lines.append('')
    lines.append('## Family D / high_turnover_breakout')
    lines.append(f'- total: {len(family_d)}')
    lines.append(f'- passed: {len(family_d_passed)}')
    if family_d_passed.empty:
        lines.append('- passed_candidates: none')
    else:
        for _, row in family_d_passed.head(10).iterrows():
            lines.append(
                f"- PASS {row['instrument']}: setup_avg_amount_ratio={row['setup_avg_amount_ratio']:.4f}, setup_high_turnover_days={int(row['setup_high_turnover_days'])}, setup_amount_zscore={row['setup_amount_zscore']:.4f}, breakout_ret={row['breakout_ret']:.4f}, vol_ratio_5={row['vol_ratio_5']:.4f}, family_pass={row['high_turnover_breakout_pass']}"
            )
    if family_d_near_miss.empty:
        lines.append('- near_miss_candidates: none')
    else:
        for _, row in family_d_near_miss.head(10).iterrows():
            lines.append(
                f"- MISS {row['instrument']}: setup_avg_amount_ratio={row['setup_avg_amount_ratio']:.4f}, setup_high_turnover_days={int(row['setup_high_turnover_days'])}, setup_amount_zscore={row['setup_amount_zscore']:.4f}, family_pass_fail={row['high_turnover_breakout_pass_fail_reasons'] or '-'}, base_shape_pass={row.get('base_shape_pass', False)}"
            )

    lines.append('')
    lines.append('## Family E / triangle_box_breakout')
    lines.append(f'- total: {len(family_e)}')
    lines.append(f'- passed: {len(family_e_passed)}')
    lines.append(f"- pattern_mix: {family_e['triangle_box_breakout_pattern'].fillna('platform_breakout').astype(str).value_counts().to_dict() if not family_e.empty else {}}")
    if family_e_passed.empty:
        lines.append('- passed_candidates: none')
    else:
        for _, row in family_e_passed.head(10).iterrows():
            lines.append(
                f"- PASS {row['instrument']}: pattern={row['triangle_box_breakout_pattern']}, box_width_ratio={row['box_width_ratio']:.4f}, box_touches=({int(row['box_upper_touches'])},{int(row['box_lower_touches'])}), breakout_ret={row['breakout_ret']:.4f}, vol_ratio_5={row['vol_ratio_5']:.4f}, family_pass={row['triangle_box_breakout_pass']}"
            )
    if family_e_near_miss.empty:
        lines.append('- near_miss_candidates: none')
    else:
        for _, row in family_e_near_miss.head(10).iterrows():
            lines.append(
                f"- MISS {row['instrument']}: pattern={row['triangle_box_breakout_pattern']}, box_width_ratio={row['box_width_ratio']:.4f}, box_touches=({int(row['box_upper_touches'])},{int(row['box_lower_touches'])}), family_pass_fail={row['triangle_box_breakout_pass_fail_reasons'] or '-'}, base_shape_pass={row.get('base_shape_pass', False)}"
            )

    lines.append('')
    lines.append('## Near Miss Candidates')
    near_miss = df[~df['pattern_passed']].head(10)
    if near_miss.empty:
        lines.append('- none')
    else:
        for _, row in near_miss.iterrows():
            lines.append(
                f"- {row['instrument']}: family={row['pattern_family']}, shape_score={row['shape_score']:.2f}, impulse_score={row['impulse_score']:.2f}, digestion_score={row['digestion_score']:.2f}, impulse_fail={row['impulse_fail_reasons'] or '-'}, digestion_fail={row['digestion_fail_reasons'] or '-'}, second_leg_fail={row['second_leg_fail_reasons'] or '-'}, family_c_fail={row.get('new_high_breakout_pass_fail_reasons', '') or '-'}"
            )
    lines.append('')
    lines.append('## Rule Notes')
    lines.append('- 主判定已改为：启动冲击 + 受控消化 + 二次攻击保留，而不是只盯 breakout/pullback 单点规则')
    lines.append('- Family C(new_high_breakout)、Family D(high_turnover_breakout)、Family E(triangle_box_breakout) 现在都拥有独立 pass 语义；base_shape_pass 仅表示旧主框架是否通过')
    lines.append('- weekly trend filters remain as context / second-leg quality inputs, not the sole template core')
    lines.append(f"- profile thresholds: {rules or normalize_profile_rules(profile_name)}")
    lines.append('- daily structure now exposes impulse_score / digestion_score / shape_score and explicit fail reasons')
    return '\n'.join(lines)


def write_wangji_scanner_outputs(
    profile_frames: dict[str, pd.DataFrame],
    outputs_dir: Path,
    profile_rules: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Path]]:
    ensure_dir(outputs_dir)
    output_paths: dict[str, dict[str, Path]] = {}
    profile_rules = profile_rules or {profile_name: normalize_profile_rules(profile_name) for profile_name in profile_frames}
    all_output_slugs = (OUTPUT_SLUG, *LEGACY_OUTPUT_SLUGS)
    for profile_name, df in profile_frames.items():
        slug = f'{OUTPUT_SLUG}_{profile_name}'
        csv_path = outputs_dir / f'{slug}_candidates.csv'
        json_path = outputs_dir / f'{slug}_candidates.json'
        md_path = outputs_dir / f'{slug}_report.md'
        df.to_csv(csv_path, index=False)
        write_json(json_path, df.fillna('').to_dict(orient='records'))
        md_path.write_text(build_wangji_scanner_report(df, profile_name, profile_rules.get(profile_name)), encoding='utf-8')
        output_paths[profile_name] = {'csv_path': csv_path, 'json_path': json_path, 'md_path': md_path}

        for legacy_slug in LEGACY_OUTPUT_SLUGS:
            legacy_prefix = f'{legacy_slug}_{profile_name}'
            (outputs_dir / f'{legacy_prefix}_candidates.csv').write_text(csv_path.read_text(encoding='utf-8'), encoding='utf-8')
            (outputs_dir / f'{legacy_prefix}_candidates.json').write_text(json_path.read_text(encoding='utf-8'), encoding='utf-8')
            (outputs_dir / f'{legacy_prefix}_report.md').write_text(md_path.read_text(encoding='utf-8'), encoding='utf-8')

    summary_payload = {
        profile_name: summarize_wangji_scanner_run(df, profile_name, profile_rules.get(profile_name))
        for profile_name, df in profile_frames.items()
    }
    for slug in all_output_slugs:
        write_json(outputs_dir / f'{slug}_summary.json', summary_payload)
    return output_paths


def run_consolidation_breakout_scanner(
    cfg: dict,
    profile_name: str = 'relax',
    daily: pd.DataFrame | None = None,
    overrides: dict[str, Any] | None = None,
    progress_callback=None,
) -> pd.DataFrame:
    return run_wangji_scanner(cfg, profile_name, daily=daily, overrides=overrides, progress_callback=progress_callback)


def run_all_consolidation_breakout_scanner_profiles(cfg: dict) -> dict[str, pd.DataFrame]:
    return run_all_wangji_scanner_profiles(cfg)


def summarize_consolidation_breakout_scanner_run(
    df: pd.DataFrame,
    profile_name: str,
    rules: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return summarize_wangji_scanner_run(df, profile_name, rules)


def build_consolidation_breakout_scanner_report(
    df: pd.DataFrame,
    profile_name: str = 'relax',
    rules: dict[str, Any] | None = None,
) -> str:
    return build_wangji_scanner_report(df, profile_name, rules)


def write_consolidation_breakout_scanner_outputs(
    profile_frames: dict[str, pd.DataFrame],
    outputs_dir: Path,
    profile_rules: dict[str, dict[str, Any]] | None = None,
) -> dict[str, dict[str, Path]]:
    return write_wangji_scanner_outputs(profile_frames, outputs_dir, profile_rules=profile_rules)


# Backward-compatible aliases for the earlier internal naming and typoed naming.
def run_wangji_sacnner(cfg: dict, profile_name: str = 'relax', daily: pd.DataFrame | None = None) -> pd.DataFrame:
    return run_wangji_scanner(cfg, profile_name, daily=daily)


def run_all_wangji_sacnner_profiles(cfg: dict) -> dict[str, pd.DataFrame]:
    return run_all_wangji_scanner_profiles(cfg)


def build_wangji_sacnner_report(df: pd.DataFrame, profile_name: str = 'relax') -> str:
    return build_wangji_scanner_report(df, profile_name)


def write_wangji_sacnner_outputs(profile_frames: dict[str, pd.DataFrame], outputs_dir: Path) -> dict[str, dict[str, Path]]:
    return write_wangji_scanner_outputs(profile_frames, outputs_dir)
