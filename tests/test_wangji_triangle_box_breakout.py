from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import consolidation_breakout_scanner as wangji_scanner


def test_triangle_box_breakout_triggers_on_converging_triangle() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': True,
            'triangle_breakout_confirmed': True,
            'box_valid': False,
            'box_breakout_confirmed': False,
            'box_width_ratio': 0.12,
            'box_upper_touches': 1,
            'box_lower_touches': 1,
            'breakout_ret': 0.052,
            'close_range_10': 0.092,
            'max_daily_abs_ret_10': 0.061,
            'latest_close_vs_breakout_close': 1.02,
            'structure_break_days': 0,
            'digestion_score': 70.0,
            'weekly_structure_score': 28.0,
            'support_revisit_ratio': 0.06,
        }
    )

    assert result['triangle_box_breakout_triggered'] is True
    assert result['triangle_box_breakout_family'] == 'triangle_box_breakout'
    assert result['triangle_box_breakout_pattern'] == 'triangle_breakout'
    assert result['triangle_box_breakout_fail_reasons'] == []
    assert result['triangle_box_breakout_pass'] is True
    assert result['triangle_box_breakout_pass_fail_reasons'] == []



def test_triangle_box_breakout_triggers_on_softened_triangle_near_miss() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': True,
            'triangle_breakout_confirmed': True,
            'box_valid': True,
            'box_breakout_confirmed': True,
            'box_width_ratio': 0.043,
            'box_upper_touches': 3,
            'box_lower_touches': 3,
            'breakout_ret': 0.048,
            'close_range_10': 0.074,
            'max_daily_abs_ret_10': 0.052,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 66.0,
            'weekly_structure_score': 20.0,
            'support_revisit_ratio': 0.04,
        }
    )

    assert result['triangle_box_breakout_triggered'] is True
    assert result['triangle_box_breakout_pattern'] == 'triangle_breakout'
    assert result['triangle_box_breakout_pass'] is True



def test_triangle_box_breakout_triggers_on_box_breakout() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': False,
            'triangle_breakout_confirmed': False,
            'box_valid': True,
            'box_breakout_confirmed': True,
            'box_width_ratio': 0.052,
            'box_upper_touches': 3,
            'box_lower_touches': 3,
            'breakout_ret': 0.046,
            'close_range_10': 0.071,
            'max_daily_abs_ret_10': 0.049,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 68.0,
            'weekly_structure_score': 22.0,
            'support_revisit_ratio': 0.05,
        }
    )

    assert result['triangle_box_breakout_triggered'] is True
    assert result['triangle_box_breakout_pattern'] == 'box_breakout'
    assert result['triangle_box_breakout_pass'] is True



def test_triangle_box_breakout_keeps_box_when_upper_boundary_is_not_descending() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': False,
            'triangle_breakout_confirmed': True,
            'box_valid': True,
            'box_breakout_confirmed': True,
            'box_width_ratio': 0.05,
            'box_upper_touches': 4,
            'box_lower_touches': 3,
            'breakout_ret': 0.048,
            'close_range_10': 0.07,
            'max_daily_abs_ret_10': 0.05,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 68.0,
            'weekly_structure_score': 22.0,
            'support_revisit_ratio': 0.05,
        }
    )

    assert result['triangle_box_breakout_triggered'] is True
    assert result['triangle_box_breakout_pattern'] == 'box_breakout'
    assert result['triangle_box_breakout_pass'] is True



def test_triangle_box_breakout_relax_allows_triangle_with_weak_weekly_structure() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'profile_name': 'relax',
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': True,
            'triangle_breakout_confirmed': True,
            'box_valid': True,
            'box_breakout_confirmed': True,
            'box_width_ratio': 0.042,
            'box_upper_touches': 3,
            'box_lower_touches': 4,
            'breakout_ret': 0.046,
            'close_range_10': 0.072,
            'max_daily_abs_ret_10': 0.051,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 66.0,
            'weekly_structure_score': 0.0,
            'support_revisit_ratio': 0.04,
        }
    )

    assert result['triangle_box_breakout_triggered'] is True
    assert result['triangle_box_breakout_pattern'] == 'triangle_breakout'
    assert result['triangle_box_breakout_pass'] is True
    assert 'weekly_structure_score_low' not in result['triangle_box_breakout_pass_fail_reasons']



def test_triangle_box_breakout_strictly_requires_weekly_structure_for_triangle() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'profile_name': 'strict',
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': True,
            'triangle_breakout_confirmed': True,
            'box_valid': True,
            'box_breakout_confirmed': True,
            'box_width_ratio': 0.042,
            'box_upper_touches': 3,
            'box_lower_touches': 4,
            'breakout_ret': 0.046,
            'close_range_10': 0.072,
            'max_daily_abs_ret_10': 0.051,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 66.0,
            'weekly_structure_score': 0.0,
            'support_revisit_ratio': 0.04,
        }
    )

    assert result['triangle_box_breakout_triggered'] is True
    assert result['triangle_box_breakout_pattern'] == 'triangle_breakout'
    assert result['triangle_box_breakout_pass'] is False
    assert 'weekly_structure_score_low' in result['triangle_box_breakout_pass_fail_reasons']



def test_triangle_box_breakout_rejects_missing_geometry() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': False,
            'triangle_breakout_confirmed': False,
            'box_valid': False,
            'box_breakout_confirmed': False,
            'box_width_ratio': 0.085,
            'box_upper_touches': 2,
            'box_lower_touches': 2,
            'breakout_ret': 0.038,
            'close_range_10': 0.085,
            'max_daily_abs_ret_10': 0.054,
            'latest_close_vs_breakout_close': 1.00,
            'structure_break_days': 0,
            'digestion_score': 72.0,
            'weekly_structure_score': 25.0,
            'support_revisit_ratio': 0.03,
        }
    )

    assert result['triangle_box_breakout_triggered'] is False
    assert result['triangle_box_breakout_family'] == 'platform_breakout'
    assert 'no_triangle_or_box_structure' in result['triangle_box_breakout_fail_reasons']



def test_triangle_box_breakout_rejects_loose_box_even_if_breakout_exists() -> None:
    result = wangji_scanner.evaluate_triangle_box_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'triangle_converging': False,
            'triangle_breakout_confirmed': False,
            'box_valid': True,
            'box_breakout_confirmed': True,
            'box_width_ratio': 0.072,
            'box_upper_touches': 2,
            'box_lower_touches': 3,
            'breakout_ret': 0.039,
            'close_range_10': 0.072,
            'max_daily_abs_ret_10': 0.05,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 70.0,
            'weekly_structure_score': 25.0,
            'support_revisit_ratio': 0.05,
        }
    )

    assert result['triangle_box_breakout_triggered'] is False
    assert result['triangle_box_breakout_family'] == 'platform_breakout'
    assert 'box_width_ratio_high' in result['triangle_box_breakout_fail_reasons'] or 'box_touch_count_low' in result['triangle_box_breakout_fail_reasons'] or 'box_breakout_ret_low' in result['triangle_box_breakout_fail_reasons']
