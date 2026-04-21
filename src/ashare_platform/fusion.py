from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_FUSION_WEIGHTS = {
    'quant_rank': 0.45,
    'consolidation_breakout_score': 0.30,
    'consolidation_breakout_rank': 0.15,
    'pattern_passed': 0.10,
}

LEGACY_FUSION_WEIGHT_KEYS = {
    'wangji_score': 'consolidation_breakout_score',
    'wangji_rank': 'consolidation_breakout_rank',
}

FUSION_OUTPUT_COLUMNS = [
    'fusion_rank',
    'instrument',
    'fusion_score',
    'quant_rank_score',
    'consolidation_breakout_score_norm',
    'consolidation_breakout_rank_score',
    'wangji_score_norm',
    'wangji_rank_score',
    'pattern_passed_score',
    'pool_source',
    'rank',
    'score',
    'candidate_source',
    'pattern_family',
    'pattern_stage',
    'pattern_passed',
    'final_score',
    'scanner_rank',
]


def _safe_minmax(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype(float)
    numeric = pd.to_numeric(series, errors='coerce')
    min_value = numeric.min()
    max_value = numeric.max()
    if pd.isna(min_value) or pd.isna(max_value):
        return pd.Series(0.0, index=series.index, dtype=float)
    if abs(float(max_value) - float(min_value)) < 1e-12:
        return pd.Series(0.5, index=series.index, dtype=float)
    return ((numeric - min_value) / (max_value - min_value)).astype(float)



def _rank_to_score(series: pd.Series, *, ascending: bool = True) -> pd.Series:
    numeric = pd.to_numeric(series, errors='coerce')
    valid = numeric.dropna()
    if valid.empty:
        return pd.Series(0.0, index=series.index, dtype=float)
    order = valid.rank(method='first', ascending=ascending)
    max_rank = float(order.max())
    if max_rank <= 1:
        scored = pd.Series(1.0, index=valid.index, dtype=float)
    else:
        scored = 1 - ((order - 1) / (max_rank - 1))
    return scored.reindex(series.index).fillna(0.0).astype(float)



def build_model_consolidation_breakout_fusion(
    model_candidates: pd.DataFrame,
    consolidation_breakout_candidates: pd.DataFrame,
    cfg: dict[str, Any] | None = None,
) -> pd.DataFrame:
    model = model_candidates.copy()
    consolidation_breakout = consolidation_breakout_candidates.copy()
    if model.empty and consolidation_breakout.empty:
        return pd.DataFrame(columns=FUSION_OUTPUT_COLUMNS)

    if not model.empty:
        model['instrument'] = model['instrument'].astype(str)
    if not consolidation_breakout.empty:
        consolidation_breakout['instrument'] = consolidation_breakout['instrument'].astype(str)

    keep_cols = [
        'instrument',
        'pattern_family',
        'pattern_stage',
        'pattern_passed',
        'final_score',
        'scanner_rank',
    ]
    if model.empty:
        merged = consolidation_breakout[keep_cols].copy()
    elif consolidation_breakout.empty:
        merged = model.copy()
    else:
        merged = model.merge(consolidation_breakout[keep_cols], on='instrument', how='outer')

    merged['pool_source'] = 'shared'
    merged.loc[merged['rank'].isna(), 'pool_source'] = 'consolidation_breakout_only'
    merged.loc[merged['pattern_family'].isna(), 'pool_source'] = 'model_only'

    fusion_cfg = (cfg or {}).get('fusion_scoring', {}) if cfg else {}
    for legacy_key, new_key in LEGACY_FUSION_WEIGHT_KEYS.items():
        if legacy_key in fusion_cfg and new_key not in fusion_cfg:
            fusion_cfg[new_key] = fusion_cfg[legacy_key]
    weights = {**DEFAULT_FUSION_WEIGHTS, **fusion_cfg}

    merged['quant_rank_score'] = _rank_to_score(merged['rank'], ascending=True)
    merged['consolidation_breakout_score_norm'] = _safe_minmax(merged['final_score'].fillna(0.0))
    merged['consolidation_breakout_rank_score'] = _rank_to_score(merged['scanner_rank'], ascending=True)
    merged['pattern_passed_score'] = merged['pattern_passed'].fillna(False).astype(float)

    merged['wangji_score_norm'] = merged['consolidation_breakout_score_norm']
    merged['wangji_rank_score'] = merged['consolidation_breakout_rank_score']

    merged['fusion_score'] = (
        merged['quant_rank_score'] * float(weights['quant_rank'])
        + merged['consolidation_breakout_score_norm'] * float(weights['consolidation_breakout_score'])
        + merged['consolidation_breakout_rank_score'] * float(weights['consolidation_breakout_rank'])
        + merged['pattern_passed_score'] * float(weights['pattern_passed'])
    ).round(6)
    merged = merged.sort_values(['fusion_score', 'rank', 'scanner_rank'], ascending=[False, True, True], na_position='last').reset_index(drop=True)
    merged['fusion_rank'] = merged.index + 1

    for column in FUSION_OUTPUT_COLUMNS:
        if column not in merged.columns:
            merged[column] = pd.NA
    return merged[FUSION_OUTPUT_COLUMNS].copy()



def build_model_wangji_fusion(
    model_candidates: pd.DataFrame,
    wangji_candidates: pd.DataFrame,
    cfg: dict[str, Any] | None = None,
) -> pd.DataFrame:
    return build_model_consolidation_breakout_fusion(model_candidates, wangji_candidates, cfg)
