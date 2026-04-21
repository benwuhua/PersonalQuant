from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import qlib_pipeline



def _sample_panel() -> pd.DataFrame:
    dates = pd.date_range('2024-01-01', periods=30, freq='D')
    rows = []
    for instrument, offset in [('SH600000', 0.0), ('SZ000001', 10.0)]:
        for i, dt in enumerate(dates):
            close = 100 + offset + i
            rows.append(
                {
                    'instrument': instrument,
                    'datetime': dt,
                    'close': float(close),
                    'open': float(close - 0.5),
                    'high': float(close + 1.0),
                    'low': float(close - 1.0),
                    'volume': float(1000 + i * 10 + offset),
                }
            )
    return pd.DataFrame(rows)



def test_add_derived_features_builds_excess_return_label_and_structure_features() -> None:
    panel = _sample_panel()

    featured = qlib_pipeline.add_derived_features(
        panel,
        label_horizon=5,
        label_horizons=[5, 10],
        hit_threshold=0.03,
        risk_threshold=-0.05,
        event_windows=[1, 3, 5],
    )

    for col in [
        'future_ret_5',
        'future_ret_10',
        'market_future_ret_5',
        'market_future_ret_10',
        'excess_ret_5_label',
        'excess_ret_10_label',
        'hit_rate_5',
        'downside_risk_5',
        'event_alpha_1',
        'event_alpha_3',
        'event_alpha_5',
        'label',
        'drawdown_from_60d_high',
        'distance_from_high_60',
        'distance_from_high_120',
        'close_range_10',
        'max_daily_abs_ret_10',
        'compression_score_10',
        'stability_score_10',
        'near_high_20_flag',
        'near_high_60_flag',
        'near_high_120_flag',
        'base_range_10',
        'base_position_60',
        'close_stability_10',
        'breakout_ret_1d',
        'breakout_volume_ratio_1d',
        'ignition_day_strength',
        'pullback_ret_3d_like',
        'negative_pullback_days_3',
        'pullback_avg_vol_ratio_3',
        'washout_direction_bias_3',
        'structure_break_days_3',
    ]:
        assert col in featured.columns

    row = featured[(featured['instrument'] == 'SH600000') & (featured['datetime'] == pd.Timestamp('2024-01-20'))].iloc[0]
    expected_stock_future_ret_5 = (124 / 119) - 1
    peer_future_ret_5 = (134 / 129) - 1
    expected_market_future_ret_5 = (expected_stock_future_ret_5 + peer_future_ret_5) / 2
    expected_label_5 = expected_stock_future_ret_5 - expected_market_future_ret_5

    expected_stock_future_ret_10 = (129 / 119) - 1
    peer_future_ret_10 = (139 / 129) - 1
    expected_market_future_ret_10 = (expected_stock_future_ret_10 + peer_future_ret_10) / 2
    expected_label_10 = expected_stock_future_ret_10 - expected_market_future_ret_10

    expected_stock_future_ret_1 = (120 / 119) - 1
    peer_future_ret_1 = (130 / 129) - 1
    expected_market_future_ret_1 = (expected_stock_future_ret_1 + peer_future_ret_1) / 2
    expected_event_alpha_1 = expected_stock_future_ret_1 - expected_market_future_ret_1

    expected_stock_future_ret_3 = (122 / 119) - 1
    peer_future_ret_3 = (132 / 129) - 1
    expected_market_future_ret_3 = (expected_stock_future_ret_3 + peer_future_ret_3) / 2
    expected_event_alpha_3 = expected_stock_future_ret_3 - expected_market_future_ret_3

    object_feature_cols = featured[qlib_pipeline.FEATURE_COLS].select_dtypes(include=['object']).columns.tolist()

    assert abs(row['future_ret_5'] - expected_stock_future_ret_5) < 1e-9
    assert abs(row['market_future_ret_5'] - expected_market_future_ret_5) < 1e-9
    assert abs(row['excess_ret_5_label'] - expected_label_5) < 1e-9
    assert abs(row['future_ret_10'] - expected_stock_future_ret_10) < 1e-9
    assert abs(row['market_future_ret_10'] - expected_market_future_ret_10) < 1e-9
    assert abs(row['excess_ret_10_label'] - expected_label_10) < 1e-9
    assert abs(row['event_alpha_1'] - expected_event_alpha_1) < 1e-9
    assert abs(row['event_alpha_3'] - expected_event_alpha_3) < 1e-9
    assert abs(row['event_alpha_5'] - expected_label_5) < 1e-9
    assert row['hit_rate_5'] == 1.0
    assert row['downside_risk_5'] == 0.0
    assert abs(row['label'] - expected_label_5) < 1e-9
    assert object_feature_cols == []



def test_add_derived_features_keeps_numeric_dtypes_when_rolling_denominators_hit_zero() -> None:
    rows = []
    dates = pd.date_range('2024-01-01', periods=130, freq='D')
    for instrument in ['SH600000', 'SZ000001']:
        for idx, dt in enumerate(dates):
            if idx < 120:
                close = 10.0
                high = 10.0
                low = 10.0
            else:
                close = 10.0 + (idx - 119)
                high = close
                low = close - 0.5
            rows.append(
                {
                    'instrument': instrument,
                    'datetime': dt,
                    'close': close,
                    'open': close,
                    'high': high,
                    'low': low,
                    'volume': 1000.0,
                }
            )
    featured = qlib_pipeline.add_derived_features(pd.DataFrame(rows), label_horizon=5)
    object_feature_cols = featured[qlib_pipeline.FEATURE_COLS].select_dtypes(include=['object']).columns.tolist()
    assert object_feature_cols == []



def test_build_walk_forward_summary_reports_rank_ic_and_topk_stats() -> None:
    score_panel = pd.DataFrame(
        [
            {'datetime': '2024-01-02', 'instrument': 'A', 'score': 0.9, 'label': 0.05},
            {'datetime': '2024-01-02', 'instrument': 'B', 'score': 0.1, 'label': -0.02},
            {'datetime': '2024-01-03', 'instrument': 'A', 'score': 0.2, 'label': 0.01},
            {'datetime': '2024-01-03', 'instrument': 'B', 'score': 0.8, 'label': 0.03},
        ]
    )
    folds = [
        {'fold': 'fold1', 'train_start': '2020-01-01', 'train_end': '2022-12-31', 'valid_start': '2023-01-01', 'valid_end': '2023-12-31'}
    ]

    summary = qlib_pipeline.build_walk_forward_summary(folds, {'fold1': score_panel}, top_k=1)

    assert summary['fold_count'] == 1
    assert summary['rank_ic_mean'] == 1.0
    assert summary['topk_avg_label_mean'] == 0.04
    assert summary['folds'][0]['topk_avg_label'] == 0.04



def test_build_walk_forward_summary_ignores_non_finite_scores_and_labels() -> None:
    score_panel = pd.DataFrame(
        [
            {'datetime': '2024-01-02', 'instrument': 'A', 'score': 0.9, 'label': 0.05},
            {'datetime': '2024-01-02', 'instrument': 'B', 'score': float('inf'), 'label': 0.01},
            {'datetime': '2024-01-03', 'instrument': 'A', 'score': 0.2, 'label': float('nan')},
            {'datetime': '2024-01-03', 'instrument': 'B', 'score': 0.8, 'label': 0.03},
        ]
    )
    folds = [
        {'fold': 'fold1', 'train_start': '2020-01-01', 'train_end': '2022-12-31', 'valid_start': '2023-01-01', 'valid_end': '2023-12-31'}
    ]

    summary = qlib_pipeline.build_walk_forward_summary(folds, {'fold1': score_panel}, top_k=1)

    assert summary['fold_count'] == 1
    assert summary['rank_ic_mean'] == 0.0
    assert summary['topk_avg_label_mean'] == 0.04
    assert summary['folds'][0]['rows'] == 2



def test_align_extension_to_qlib_rescales_price_and_volume_by_overlap() -> None:
    base = pd.DataFrame(
        [
            {'instrument': 'SH600000', 'datetime': pd.Timestamp('2020-09-24'), 'open': 9.0, 'close': 10.0, 'high': 10.5, 'low': 8.8, 'volume': 1000.0},
            {'instrument': 'SH600000', 'datetime': pd.Timestamp('2020-09-25'), 'open': 10.0, 'close': 12.0, 'high': 12.5, 'low': 9.8, 'volume': 1200.0},
        ]
    )
    extension = pd.DataFrame(
        [
            {'instrument': 'SH600000', 'datetime': pd.Timestamp('2020-09-24'), 'open': 18.0, 'close': 20.0, 'high': 21.0, 'low': 17.6, 'volume': 100.0},
            {'instrument': 'SH600000', 'datetime': pd.Timestamp('2020-09-25'), 'open': 20.0, 'close': 24.0, 'high': 25.0, 'low': 19.6, 'volume': 120.0},
            {'instrument': 'SH600000', 'datetime': pd.Timestamp('2020-09-28'), 'open': 22.0, 'close': 26.0, 'high': 27.0, 'low': 21.0, 'volume': 130.0},
        ]
    )

    aligned = qlib_pipeline.align_extension_to_qlib(base, extension)
    row = aligned[aligned['datetime'] == pd.Timestamp('2020-09-28')].iloc[0]

    assert abs(row['close'] - 13.0) < 1e-9
    assert abs(row['open'] - 11.0) < 1e-9
    assert abs(row['volume'] - 1300.0) < 1e-9



def test_save_and_load_model_artifact_roundtrip(tmp_path: Path) -> None:
    model = {'name': 'demo-model', 'version': 1}

    path = qlib_pipeline.save_model_artifact(model, tmp_path / 'lightgbm_model.pkl')
    restored = qlib_pipeline.load_model_artifact(path)

    assert path.exists()
    assert restored == model



def test_fetch_recent_hist_for_code_uses_daily_api_and_normalizes_columns(monkeypatch) -> None:
    calls = []

    def fake_daily(symbol: str, start_date: str, end_date: str, adjust: str):
        calls.append(
            {
                'symbol': symbol,
                'start_date': start_date,
                'end_date': end_date,
                'adjust': adjust,
            }
        )
        return pd.DataFrame(
            [
                {
                    'date': '2024-01-02',
                    'open': 10.0,
                    'high': 10.5,
                    'low': 9.8,
                    'close': 10.2,
                    'volume': 123456.0,
                }
            ]
        )

    monkeypatch.setattr(qlib_pipeline.ak, 'stock_zh_a_daily', fake_daily)

    hist = qlib_pipeline.fetch_recent_hist_for_code('600519', '20240101', '20240131')

    assert calls == [
        {
            'symbol': 'sh600519',
            'start_date': '20240101',
            'end_date': '20240131',
            'adjust': 'qfq',
        }
    ]
    assert hist.iloc[0]['instrument'] == 'SH600519'
    assert hist.iloc[0]['close'] == 10.2
    assert hist.iloc[0]['volume'] == 123456.0



def test_feature_columns_do_not_depend_on_future_rows() -> None:
    base = _sample_panel()
    target_dt = pd.Timestamp('2024-01-21')

    perturbed = base.copy()
    future_mask = perturbed['datetime'] > target_dt
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'close'] *= 5
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'open'] *= 5
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'high'] *= 5
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'low'] *= 5
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'volume'] *= 7

    featured_base = qlib_pipeline.add_derived_features(base, label_horizon=5)
    featured_perturbed = qlib_pipeline.add_derived_features(perturbed, label_horizon=5)

    row_base = featured_base[(featured_base['instrument'] == 'SH600000') & (featured_base['datetime'] == target_dt)].iloc[0]
    row_perturbed = featured_perturbed[(featured_perturbed['instrument'] == 'SH600000') & (featured_perturbed['datetime'] == target_dt)].iloc[0]

    assert set(qlib_pipeline.FEATURE_COLS).isdisjoint(set(qlib_pipeline.ANALYSIS_ONLY_FUTURE_COLS))

    for col in qlib_pipeline.FEATURE_COLS:
        left = row_base[col]
        right = row_perturbed[col]
        if pd.isna(left) and pd.isna(right):
            continue
        assert left == right, f'{col} should not depend on future rows'



def test_analysis_only_future_columns_capture_future_dependence_but_stay_out_of_training() -> None:
    base = _sample_panel()
    target_dt = pd.Timestamp('2024-01-21')

    perturbed = base.copy()
    future_mask = perturbed['datetime'] > target_dt
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'close'] *= 5
    perturbed.loc[future_mask & (perturbed['instrument'] == 'SH600000'), 'volume'] *= 7

    featured_base = qlib_pipeline.add_derived_features(base, label_horizon=5)
    featured_perturbed = qlib_pipeline.add_derived_features(perturbed, label_horizon=5)

    row_base = featured_base[(featured_base['instrument'] == 'SH600000') & (featured_base['datetime'] == target_dt)].iloc[0]
    row_perturbed = featured_perturbed[(featured_perturbed['instrument'] == 'SH600000') & (featured_perturbed['datetime'] == target_dt)].iloc[0]

    changed = []
    for col in qlib_pipeline.ANALYSIS_ONLY_FUTURE_COLS:
        left = row_base[col]
        right = row_perturbed[col]
        if pd.isna(left) and pd.isna(right):
            continue
        if left != right:
            changed.append(col)

    assert changed, 'analysis-only future columns should reflect future-path changes'
    assert set(changed).issubset(set(qlib_pipeline.ANALYSIS_ONLY_FUTURE_COLS))
