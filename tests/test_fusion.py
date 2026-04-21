from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform.fusion import build_model_consolidation_breakout_fusion, build_model_wangji_fusion



def test_build_model_consolidation_breakout_fusion_promotes_high_scanner_score_and_passed_name() -> None:
    model = pd.DataFrame(
        [
            {'rank': 1, 'datetime': '2026-04-14', 'instrument': 'AAA', 'score': 10.0, 'candidate_source': 'model'},
            {'rank': 2, 'datetime': '2026-04-14', 'instrument': 'BBB', 'score': 9.5, 'candidate_source': 'model'},
            {'rank': 3, 'datetime': '2026-04-14', 'instrument': 'CCC', 'score': 9.0, 'candidate_source': 'model'},
        ]
    )
    wangji = pd.DataFrame(
        [
            {'instrument': 'AAA', 'pattern_family': 'platform_breakout', 'pattern_stage': 'breakout', 'pattern_passed': False, 'final_score': 60.0, 'scanner_rank': 30},
            {'instrument': 'BBB', 'pattern_family': 'platform_breakout', 'pattern_stage': 'breakout', 'pattern_passed': True, 'final_score': 90.0, 'scanner_rank': 1},
            {'instrument': 'CCC', 'pattern_family': 'platform_breakout', 'pattern_stage': 'breakout', 'pattern_passed': False, 'final_score': 50.0, 'scanner_rank': 80},
        ]
    )

    fused = build_model_consolidation_breakout_fusion(model, wangji, cfg=None)

    assert fused.iloc[0]['instrument'] == 'BBB'
    assert list(fused['fusion_rank']) == [1, 2, 3]
    assert 'fusion_score' in fused.columns



def test_build_model_consolidation_breakout_fusion_uses_shared_pool_not_model_only() -> None:
    model = pd.DataFrame(
        [
            {'rank': 1, 'datetime': '2026-04-14', 'instrument': 'AAA', 'score': 10.0, 'candidate_source': 'model'},
        ]
    )
    wangji = pd.DataFrame(
        [
            {'instrument': 'AAA', 'pattern_family': 'platform_breakout', 'pattern_stage': 'breakout', 'pattern_passed': False, 'final_score': 55.0, 'scanner_rank': 30},
            {'instrument': 'DDD', 'pattern_family': 'platform_breakout', 'pattern_stage': 'breakout', 'pattern_passed': True, 'final_score': 95.0, 'scanner_rank': 1},
        ]
    )

    fused = build_model_consolidation_breakout_fusion(model, wangji, cfg=None)

    assert set(fused['instrument']) == {'AAA', 'DDD'}
    ddd = fused[fused['instrument'] == 'DDD'].iloc[0]
    assert ddd['pool_source'] == 'consolidation_breakout_only'
    assert ddd['rank'] != ddd['rank']  # NaN
