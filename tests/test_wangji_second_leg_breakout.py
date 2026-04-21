from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import consolidation_breakout_scanner as wangji_scanner


def test_second_leg_breakout_triggers_on_controlled_rebreakout() -> None:
    result = wangji_scanner.evaluate_second_leg_breakout(
        {
            'impulse_pass': True,
            'digestion_pass': True,
            'recent_ignition_prepass': False,
            'structure_break_days': 0,
            'support_revisit_ratio': 0.02,
            'confirmation_ret': 0.12,
            'confirmation_vol_ratio': 1.8,
            'second_leg_distance': 0.05,
            'retest_high_ratio': -0.01,
            'latest_close_vs_breakout_close': 1.06,
        }
    )

    assert result['second_leg_breakout_triggered'] is True
    assert result['second_leg_breakout_family'] == 'second_leg_breakout'
    assert result['second_leg_breakout_fail_reasons'] == []


def test_second_leg_breakout_rejects_recent_ignition_prepass_only_case() -> None:
    result = wangji_scanner.evaluate_second_leg_breakout(
        {
            'impulse_pass': True,
            'digestion_pass': True,
            'recent_ignition_prepass': True,
            'structure_break_days': 0,
            'support_revisit_ratio': 0.01,
            'confirmation_ret': 0.11,
            'confirmation_vol_ratio': 1.7,
            'second_leg_distance': 0.04,
            'retest_high_ratio': -0.01,
            'latest_close_vs_breakout_close': 1.05,
        }
    )

    assert result['second_leg_breakout_triggered'] is False
    assert result['second_leg_breakout_family'] == 'platform_breakout'
    assert 'recent_ignition_prepass_only' in result['second_leg_breakout_fail_reasons']
