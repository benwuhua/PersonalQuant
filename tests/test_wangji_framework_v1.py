from __future__ import annotations

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import consolidation_breakout_scanner as wangji_scanner


def test_build_framework_scores_returns_explainable_subscores() -> None:
    scores = wangji_scanner.build_framework_scores(
        {
            'base_background_score': 72.5,
            'close_range_10': 0.045,
            'max_daily_abs_ret_10': 0.031,
            'impulse_score': 81.0,
            'digestion_score': 76.0,
            'second_leg_score': 68.0,
        }
    )

    assert set(scores) == {
        'background_score',
        'consolidation_score',
        'breakout_quality_score',
        'followthrough_score',
        'final_score',
    }
    assert scores['background_score'] == 72.5
    assert scores['breakout_quality_score'] == 81.0
    assert 0 <= scores['consolidation_score'] <= 100
    assert 0 <= scores['followthrough_score'] <= 100
    assert 0 <= scores['final_score'] <= 100


def test_infer_pattern_stage_prefers_second_leg_then_breakout_then_setup() -> None:
    assert (
        wangji_scanner.infer_pattern_stage(
            {
                'pattern_passed': True,
                'second_leg_pass': True,
                'digestion_pass': True,
                'recent_ignition_prepass': False,
                'rule_breakout_ret': True,
                'rule_breakout_volume': True,
            }
        )
        == 'second_leg'
    )

    assert (
        wangji_scanner.infer_pattern_stage(
            {
                'pattern_passed': False,
                'second_leg_pass': False,
                'digestion_pass': False,
                'recent_ignition_prepass': True,
                'rule_breakout_ret': True,
                'rule_breakout_volume': True,
            }
        )
        == 'breakout'
    )

    assert (
        wangji_scanner.infer_pattern_stage(
            {
                'pattern_passed': False,
                'second_leg_pass': False,
                'digestion_pass': True,
                'recent_ignition_prepass': False,
                'rule_breakout_ret': True,
                'rule_breakout_volume': True,
            }
        )
        == 'digestion'
    )

    assert (
        wangji_scanner.infer_pattern_stage(
            {
                'pattern_passed': False,
                'second_leg_pass': False,
                'digestion_pass': False,
                'recent_ignition_prepass': False,
                'rule_breakout_ret': False,
                'rule_breakout_volume': False,
            }
        )
        == 'setup'
    )
