from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import consolidation_breakout_scanner as wangji_scanner


def test_new_high_breakout_triggers_and_passes_on_compressed_120d_new_high() -> None:
    result = wangji_scanner.evaluate_new_high_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'is_new_high_20': True,
            'is_new_high_60': True,
            'is_new_high_120': True,
            'new_high_precompression_pass': True,
            'close_range_10': 0.055,
            'max_daily_abs_ret_10': 0.028,
            'breakout_ret': 0.061,
            'confirmation_vol_ratio': 1.55,
            'latest_close_vs_breakout_close': 1.04,
            'structure_break_days': 0,
            'digestion_score': 72.0,
            'weekly_structure_score': 35.0,
            'support_revisit_ratio': 0.04,
        }
    )

    assert result['new_high_breakout_triggered'] is True
    assert result['new_high_breakout_family'] == 'new_high_breakout'
    assert result['new_high_window'] == 120
    assert result['new_high_breakout_fail_reasons'] == []
    assert result['new_high_breakout_pass'] is True
    assert result['new_high_breakout_pass_fail_reasons'] == []



def test_new_high_breakout_can_trigger_but_fail_family_specific_pass() -> None:
    result = wangji_scanner.evaluate_new_high_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'is_new_high_20': True,
            'is_new_high_60': False,
            'is_new_high_120': False,
            'new_high_precompression_pass': True,
            'close_range_10': 0.054,
            'max_daily_abs_ret_10': 0.032,
            'breakout_ret': 0.045,
            'confirmation_vol_ratio': 1.35,
            'latest_close_vs_breakout_close': 1.02,
            'structure_break_days': 0,
            'digestion_score': 80.0,
            'weekly_structure_score': 0.0,
            'support_revisit_ratio': 0.03,
        }
    )

    assert result['new_high_breakout_triggered'] is True
    assert result['new_high_breakout_family'] == 'new_high_breakout'
    assert result['new_high_window'] == 20
    assert result['new_high_breakout_fail_reasons'] == []
    assert result['new_high_breakout_pass'] is False
    assert 'weekly_structure_score_low' in result['new_high_breakout_pass_fail_reasons']



def test_new_high_breakout_rejects_overextended_noncompressed_case() -> None:
    result = wangji_scanner.evaluate_new_high_breakout(
        {
            'impulse_pass': True,
            'rule_breakout_ret': True,
            'rule_breakout_volume': True,
            'is_new_high_20': True,
            'is_new_high_60': False,
            'is_new_high_120': False,
            'new_high_precompression_pass': False,
            'close_range_10': 0.165,
            'max_daily_abs_ret_10': 0.095,
            'breakout_ret': 0.032,
            'confirmation_vol_ratio': 1.08,
            'latest_close_vs_breakout_close': 1.18,
            'structure_break_days': 2,
            'digestion_score': 45.0,
            'weekly_structure_score': 0.0,
            'support_revisit_ratio': 0.18,
        }
    )

    assert result['new_high_breakout_triggered'] is False
    assert result['new_high_breakout_family'] == 'platform_breakout'
    assert result['new_high_window'] == 20
    assert 'pre_breakout_compression_missing' in result['new_high_breakout_fail_reasons']
    assert 'breakout_quality_low' in result['new_high_breakout_fail_reasons']
    assert 'post_breakout_extension_too_high' in result['new_high_breakout_fail_reasons']
    assert result['new_high_breakout_pass'] is False
    assert 'structure_break_days_high' in result['new_high_breakout_pass_fail_reasons']
    assert 'digestion_score_low' in result['new_high_breakout_pass_fail_reasons']
