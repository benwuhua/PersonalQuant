from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import consolidation_breakout_scanner as wangji_scanner


def test_select_best_calibration_window_prefers_passed_then_shape_score() -> None:
    replay_df = pd.DataFrame(
        [
            {
                'instrument': 'SH600089',
                'signal_date': '2025-07-25',
                'candidate_breakout_date': '2025-07-18',
                'pattern_passed': False,
                'shape_score': 91.0,
                'impulse_score': 95.0,
                'digestion_score': 80.0,
                'controlled_down_days': 3,
                'impulse_day_count': 2,
                'impulse_volume_day_count': 3,
                'weekly_structure_score': 100.0,
                'second_leg_distance': 0.01,
            },
            {
                'instrument': 'SH600089',
                'signal_date': '2025-07-29',
                'candidate_breakout_date': '2025-07-22',
                'pattern_passed': True,
                'shape_score': 88.7,
                'impulse_score': 95.0,
                'digestion_score': 80.7,
                'controlled_down_days': 3,
                'impulse_day_count': 2,
                'impulse_volume_day_count': 3,
                'weekly_structure_score': 100.0,
                'second_leg_distance': 0.03,
            },
            {
                'instrument': 'SH600089',
                'signal_date': '2025-07-28',
                'candidate_breakout_date': '2025-07-22',
                'pattern_passed': True,
                'shape_score': 87.0,
                'impulse_score': 93.0,
                'digestion_score': 79.0,
                'controlled_down_days': 3,
                'impulse_day_count': 2,
                'impulse_volume_day_count': 3,
                'weekly_structure_score': 100.0,
                'second_leg_distance': 0.04,
            },
        ]
    )

    summary = wangji_scanner.select_best_calibration_window(replay_df)

    assert summary['pass_count'] == 2
    assert summary['best_signal_date'] == '2025-07-29'
    assert summary['best_breakout_date'] == '2025-07-22'
    assert summary['best_shape_score'] == 88.7


def test_default_calibration_cases_include_user_examples() -> None:
    codes = {case['code'] for case in wangji_scanner.DEFAULT_CALIBRATION_CASES}

    assert {'600089', '301005', '300136'} <= codes


def test_relax_ignition_flags_allow_continuation_green_variant() -> None:
    flags = wangji_scanner.evaluate_relax_ignition_flags(
        {
            'breakout_ret': 0.029958,
            'confirmation_ret': 0.040692,
            'vol_ratio_5': 2.071102,
            'confirmation_vol_ratio': 2.247409,
            'breakout_close': 64.29,
            'breakout_prev_close': 62.42,
            'breakout_open': 65.60,
            'close_location_ratio': 0.51,
            'ignition_day_strength': 68.5387,
            'breakout_ret_min': 0.04,
            'vol_ratio_min': 1.3,
        }
    )

    assert flags['canonical_green'] is False
    assert flags['continuation_green'] is True
    assert flags['rule_breakout_green'] is True
    assert flags['rule_breakout_ret'] is True
    assert flags['rule_breakout_volume'] is True


def test_relax_digestion_flags_allow_controlled_variant_without_strict_bearish_washout() -> None:
    flags = wangji_scanner.evaluate_relax_digestion_flags(
        {
            'pattern_digestion': True,
            'digestion_score': 70.4816,
            'controlled_down_days': 2,
            'negative_pullback_days': 2,
            'mild_up_days': 2,
            'abnormal_down_days': 0,
            'abnormal_up_days': 0,
            'digestion_volume_ratio': 0.831842,
            'max_washout_vol_spike': 1.085127,
            'structure_break_days': 0,
            'washout_direction_bias': 0.0,
            'pullback_ret_3d': 0.024732,
            'pullback_ret_min': -0.08,
            'pullback_days': 4,
            'rule_pullback_daily': False,
            'rule_pullback_3d': False,
            'rule_pullback_volume': False,
        }
    )

    assert flags['canonical_washout'] is False
    assert flags['controlled_digestion_variant'] is True
    assert flags['digestion_pass'] is True


def test_relax_common_entry_gates_are_shared_by_pass_and_prepass() -> None:
    gates = wangji_scanner.evaluate_relax_common_entry_gates(
        {
            'base_background_score': 68.0,
            'rule_background_base': True,
            'rule_close_range_10': True,
            'rule_max_daily_abs_ret_10': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'rule_breakout_green': True,
            'ignition_day_strength': 82.0,
        }
    )

    assert gates['entry_gate_pass'] is True
    assert gates['entry_gate_fail_reasons'] == []


def test_relax_common_entry_gates_fail_when_base_is_not_platform_like() -> None:
    gates = wangji_scanner.evaluate_relax_common_entry_gates(
        {
            'base_background_score': 68.0,
            'rule_background_base': True,
            'rule_close_range_10': False,
            'rule_max_daily_abs_ret_10': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'rule_breakout_green': True,
            'ignition_day_strength': 82.0,
        }
    )

    assert gates['entry_gate_pass'] is False
    assert 'close_range_10_fail' in gates['entry_gate_fail_reasons']



def test_relax_prepass_requires_shared_entry_gate_before_recent_ignition_override() -> None:
    prepass = wangji_scanner.evaluate_relax_prepass_flags(
        {
            'recent_ignition_window': True,
            'entry_gate_pass': False,
            'breakout_ret': 0.07,
            'breakout_ret_min': 0.04,
            'vol_ratio_5': 1.8,
            'vol_ratio_min': 1.3,
            'close_location_ratio': 0.9,
            'observed_pullback_days': 2,
            'structure_break_days': 0,
            'abnormal_down_days': 0,
            'latest_close_vs_breakout_close': 0.99,
            'pullback_avg_vol_ratio': 0.55,
            'max_washout_vol_spike': 0.8,
            'volume_trend_ratio': 0.9,
        }
    )

    assert prepass['recent_ignition_prepass'] is False
    assert 'entry_gate_fail' in prepass['prepass_fail_reasons']
