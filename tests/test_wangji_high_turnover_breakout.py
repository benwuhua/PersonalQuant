from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import consolidation_breakout_scanner as wangji_scanner


def test_high_turnover_breakout_triggers_and_passes_on_active_platform() -> None:
    result = wangji_scanner.evaluate_high_turnover_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'setup_avg_amount_ratio': 1.42,
            'setup_high_turnover_days': 3,
            'setup_amount_zscore': 0.88,
            'close_range_10': 0.074,
            'max_daily_abs_ret_10': 0.052,
            'latest_close_vs_breakout_close': 1.03,
            'structure_break_days': 0,
            'digestion_score': 74.0,
            'weekly_structure_score': 32.0,
            'support_revisit_ratio': 0.06,
        }
    )

    assert result['high_turnover_breakout_triggered'] is True
    assert result['high_turnover_breakout_family'] == 'high_turnover_breakout'
    assert result['high_turnover_breakout_fail_reasons'] == []
    assert result['high_turnover_breakout_pass'] is True
    assert result['high_turnover_breakout_pass_fail_reasons'] == []



def test_high_turnover_breakout_rejects_low_turnover_platform() -> None:
    result = wangji_scanner.evaluate_high_turnover_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'setup_avg_amount_ratio': 0.92,
            'setup_high_turnover_days': 1,
            'setup_amount_zscore': -0.15,
            'close_range_10': 0.068,
            'max_daily_abs_ret_10': 0.041,
            'latest_close_vs_breakout_close': 1.01,
            'structure_break_days': 0,
            'digestion_score': 78.0,
            'weekly_structure_score': 28.0,
            'support_revisit_ratio': 0.04,
        }
    )

    assert result['high_turnover_breakout_triggered'] is False
    assert result['high_turnover_breakout_family'] == 'platform_breakout'
    assert 'setup_avg_amount_ratio_low' in result['high_turnover_breakout_fail_reasons']
    assert 'setup_high_turnover_days_low' in result['high_turnover_breakout_fail_reasons']
    assert 'setup_amount_zscore_low' in result['high_turnover_breakout_fail_reasons']
    assert result['high_turnover_breakout_pass'] is False



def test_high_turnover_breakout_can_trigger_but_fail_post_breakout_health() -> None:
    result = wangji_scanner.evaluate_high_turnover_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'setup_avg_amount_ratio': 1.30,
            'setup_high_turnover_days': 3,
            'setup_amount_zscore': 0.55,
            'close_range_10': 0.082,
            'max_daily_abs_ret_10': 0.058,
            'latest_close_vs_breakout_close': 1.00,
            'structure_break_days': 0,
            'digestion_score': 80.0,
            'weekly_structure_score': 0.0,
            'support_revisit_ratio': 0.03,
        }
    )

    assert result['high_turnover_breakout_triggered'] is True
    assert result['high_turnover_breakout_pass'] is False
    assert 'weekly_structure_score_low' in result['high_turnover_breakout_pass_fail_reasons']
