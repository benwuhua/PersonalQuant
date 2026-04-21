from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import json
import pickle
from pathlib import Path
import time
from typing import Any

import lightgbm as lgb
import numpy as np
import pandas as pd
import qlib
from qlib.data import D
from qlib.tests.data import GetData

try:
    from .quant_pipeline import build_multitask_label_spec
except ImportError:
    try:
        from ashare_platform.quant_pipeline import build_multitask_label_spec
    except ModuleNotFoundError:
        from src.ashare_platform.quant_pipeline import build_multitask_label_spec

try:
    import akshare as ak
except Exception:
    ak = None

# Current experiment: a narrower top-k-oriented core set.
# Keep the feature list biased toward the strongest medium-term trend, breakout,
# and structure signals instead of broad coverage.
FEATURE_COLS = [
    'ret_1',
    'ret_20',
    'overnight_ret',
    'range_pct',
    'close_ma5_ratio',
    'close_ma20_ratio',
    'volume_ma20_ratio',
    'volatility_20',
    'price_position_20',
    'excess_ret_5',
    'excess_ret_10',
    'excess_ret_20',
    'close_ma5_slope',
    'close_ma10_slope',
    'close_ma20_slope',
    'distance_from_high_120',
    'max_daily_abs_ret_10',
    'drawdown_from_120d_high',
    'base_range_20',
    'base_position_60',
    'breakout_ret_2d',
    'breakout_close_location',
]

# These fields intentionally use shift(-n) future observations.
# Keep them available for post-event diagnostics only; they must never enter FEATURE_COLS.
ANALYSIS_ONLY_FUTURE_COLS = [
    'pullback_ret_3d_like',
    'negative_pullback_days_3',
    'controlled_down_days_3',
    'pullback_avg_vol_ratio_3',
    'max_washout_vol_spike_3',
    'washout_direction_bias_3',
    'structure_break_days_3',
    'future_ret_1',
    'future_ret_3',
    'future_ret_5',
    'future_ret_10',
    'future_ret_20',
    'market_future_ret_1',
    'market_future_ret_3',
    'market_future_ret_5',
    'market_future_ret_10',
    'market_future_ret_20',
    'excess_ret_1_label',
    'excess_ret_3_label',
    'excess_ret_5_label',
    'excess_ret_10_label',
    'excess_ret_20_label',
    'hit_rate_5',
    'downside_risk_5',
    'event_alpha_1',
    'event_alpha_3',
    'event_alpha_5',
]

RAW_COLS = ['close', 'open', 'high', 'low', 'volume']


def save_model_artifact(model: Any, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('wb') as f:
        pickle.dump(model, f)
    return path


def load_model_artifact(path: Path) -> Any:
    with path.open('rb') as f:
        return pickle.load(f)


def init_qlib(provider_uri: str, region: str) -> str:
    expanded = str(Path(provider_uri).expanduser())
    GetData(delete_zip_file=True).qlib_data(
        name='qlib_data_simple',
        target_dir=expanded,
        region=region,
        interval='1d',
        delete_old=False,
        exists_skip=True,
    )
    qlib.init(
        provider_uri=expanded,
        region=region,
        expression_cache=None,
        dataset_cache=None,
        kernels=1,
    )
    return expanded


def _safe_price_position(close: pd.Series, rolling_low: pd.Series, rolling_high: pd.Series) -> pd.Series:
    denom = (rolling_high - rolling_low).replace(0, np.nan)
    return (close - rolling_low) / denom


def _safe_zscore(series: pd.Series, rolling_mean: pd.Series, rolling_std: pd.Series) -> pd.Series:
    denom = rolling_std.replace(0, np.nan)
    return (series - rolling_mean) / denom


def _rolling_slope(series: pd.Series, window: int) -> pd.Series:
    ma = series.rolling(window).mean()
    return ma / ma.shift(window) - 1


def _streak_up_days(ret_series: pd.Series) -> pd.Series:
    streaks = []
    streak = 0
    for value in ret_series.fillna(0):
        if value > 0:
            streak += 1
        else:
            streak = 0
        streaks.append(streak)
    return pd.Series(streaks, index=ret_series.index, dtype=float)


def add_derived_features(
    panel: pd.DataFrame,
    label_horizon: int | None = None,
    label_horizons: list[int] | None = None,
    hit_threshold: float = 0.02,
    risk_threshold: float = -0.04,
    event_windows: list[int] | None = None,
) -> pd.DataFrame:
    panel = panel.sort_values(['instrument', 'datetime']).reset_index(drop=True)
    g = panel.groupby('instrument', group_keys=False)

    panel['close_lag1'] = g['close'].shift(1)
    panel['close_lag5'] = g['close'].shift(5)
    panel['close_ma5'] = g['close'].transform(lambda s: s.rolling(5).mean())
    panel['close_ma10'] = g['close'].transform(lambda s: s.rolling(10).mean())
    panel['close_ma20'] = g['close'].transform(lambda s: s.rolling(20).mean())
    panel['volume_ma5'] = g['volume'].transform(lambda s: s.rolling(5).mean())
    panel['volume_ma10'] = g['volume'].transform(lambda s: s.rolling(10).mean())
    panel['volume_ma20'] = g['volume'].transform(lambda s: s.rolling(20).mean())

    panel['ret_1'] = g['close'].pct_change(1, fill_method=None)
    panel['ret_5'] = g['close'].pct_change(5, fill_method=None)
    panel['ret_10'] = g['close'].pct_change(10, fill_method=None)
    panel['ret_20'] = g['close'].pct_change(20, fill_method=None)

    panel['intraday_ret'] = panel['close'] / panel['open'] - 1
    panel['overnight_ret'] = panel['open'] / panel['close_lag1'] - 1
    panel['range_pct'] = panel['high'] / panel['low'] - 1

    panel['close_ma5_ratio'] = panel['close'] / panel['close_ma5']
    panel['close_ma10_ratio'] = panel['close'] / panel['close_ma10']
    panel['close_ma20_ratio'] = panel['close'] / panel['close_ma20']
    panel['volume_ma5_ratio'] = panel['volume'] / panel['volume_ma5']
    panel['volume_ma10_ratio'] = panel['volume'] / panel['volume_ma10']
    panel['volume_ma20_ratio'] = panel['volume'] / panel['volume_ma20']

    panel['volatility_5'] = g['ret_1'].transform(lambda s: s.rolling(5).std())
    panel['volatility_20'] = g['ret_1'].transform(lambda s: s.rolling(20).std())

    rolling_high_20 = g['high'].transform(lambda s: s.rolling(20).max())
    rolling_low_20 = g['low'].transform(lambda s: s.rolling(20).min())
    rolling_high_60 = g['high'].transform(lambda s: s.rolling(60).max())
    rolling_high_120 = g['high'].transform(lambda s: s.rolling(120).max())
    panel['price_position_20'] = _safe_price_position(panel['close'], rolling_low_20, rolling_high_20)

    volume_std_20 = g['volume'].transform(lambda s: s.rolling(20).std())
    panel['volume_zscore_20'] = _safe_zscore(panel['volume'], panel['volume_ma20'], volume_std_20)

    market_returns = panel.groupby('datetime')['ret_1'].mean().sort_index()
    market_frame = pd.DataFrame({'datetime': market_returns.index, 'market_ret_1': market_returns.values})
    market_frame['market_ret_5'] = (1 + market_frame['market_ret_1']).rolling(5).apply(lambda x: x.prod(), raw=True) - 1
    market_frame['market_ret_10'] = (1 + market_frame['market_ret_1']).rolling(10).apply(lambda x: x.prod(), raw=True) - 1
    market_frame['market_ret_20'] = (1 + market_frame['market_ret_1']).rolling(20).apply(lambda x: x.prod(), raw=True) - 1
    panel = panel.merge(market_frame[['datetime', 'market_ret_5', 'market_ret_10', 'market_ret_20']], on='datetime', how='left')
    panel['excess_ret_5'] = panel['ret_5'] - panel['market_ret_5']
    panel['excess_ret_10'] = panel['ret_10'] - panel['market_ret_10']
    panel['excess_ret_20'] = panel['ret_20'] - panel['market_ret_20']

    panel['close_ma5_slope'] = g['close'].transform(lambda s: _rolling_slope(s, 5))
    panel['close_ma10_slope'] = g['close'].transform(lambda s: _rolling_slope(s, 10))
    panel['close_ma20_slope'] = g['close'].transform(lambda s: _rolling_slope(s, 20))
    panel['volatility_regime_20'] = panel['volatility_5'] / panel['volatility_20'].replace(0, np.nan)
    panel['breakout_strength_20'] = panel['close'] / rolling_high_20.replace(0, np.nan) - 1
    panel['distance_from_high_20'] = panel['close'] / rolling_high_20.replace(0, np.nan) - 1
    panel['distance_from_high_60'] = panel['close'] / rolling_high_60.replace(0, np.nan) - 1
    panel['distance_from_high_120'] = panel['close'] / rolling_high_120.replace(0, np.nan) - 1
    panel['close_range_10'] = g['close'].transform(lambda s: s.rolling(10).max()) / g['close'].transform(lambda s: s.rolling(10).min()).replace(0, np.nan) - 1
    panel['max_daily_abs_ret_10'] = g['ret_1'].transform(lambda s: s.abs().rolling(10).max())
    panel['compression_score_10'] = (1 - (panel['close_range_10'] / 0.10).clip(lower=0, upper=1)) * 100
    panel['stability_score_10'] = (1 - (panel['max_daily_abs_ret_10'] / 0.07).clip(lower=0, upper=1)) * 100
    panel['near_high_20_flag'] = panel['distance_from_high_20'].ge(-0.02).astype(float)
    panel['near_high_60_flag'] = panel['distance_from_high_60'].ge(-0.03).astype(float)
    panel['near_high_120_flag'] = panel['distance_from_high_120'].ge(-0.05).astype(float)

    up_volume = panel['volume'].where(panel['ret_1'] > 0, 0.0)
    down_volume = panel['volume'].where(panel['ret_1'] <= 0, 0.0)
    panel['up_volume_ratio_10'] = up_volume.groupby(panel['instrument']).transform(lambda s: s.rolling(10).sum()) / g['volume'].transform(lambda s: s.rolling(10).sum()).replace(0, np.nan)
    panel['down_volume_ratio_10'] = down_volume.groupby(panel['instrument']).transform(lambda s: s.rolling(10).sum()) / g['volume'].transform(lambda s: s.rolling(10).sum()).replace(0, np.nan)
    panel['streak_up_days'] = g['ret_1'].transform(_streak_up_days)

    rolling_high_60 = g['high'].transform(lambda s: s.rolling(60).max())
    rolling_high_120 = g['high'].transform(lambda s: s.rolling(120).max())
    rolling_low_60 = g['low'].transform(lambda s: s.rolling(60).min())
    rolling_close_min_10 = g['close'].transform(lambda s: s.rolling(10).min())
    rolling_close_max_10 = g['close'].transform(lambda s: s.rolling(10).max())
    rolling_close_min_20 = g['close'].transform(lambda s: s.rolling(20).min())
    rolling_close_max_20 = g['close'].transform(lambda s: s.rolling(20).max())
    panel['drawdown_from_60d_high'] = 1 - panel['close'] / rolling_high_60.replace(0, np.nan)
    panel['drawdown_from_120d_high'] = 1 - panel['close'] / rolling_high_120.replace(0, np.nan)
    panel['base_range_10'] = rolling_close_max_10 / rolling_close_min_10.replace(0, np.nan) - 1
    panel['base_range_20'] = rolling_close_max_20 / rolling_close_min_20.replace(0, np.nan) - 1
    panel['base_position_60'] = _safe_price_position(panel['close'], rolling_low_60, rolling_high_60)
    panel['close_stability_10'] = g['ret_1'].transform(lambda s: s.rolling(10).std())

    panel['breakout_ret_1d'] = panel['ret_1']
    panel['breakout_ret_2d'] = g['close'].pct_change(2, fill_method=None)
    panel['breakout_volume_ratio_1d'] = panel['volume_ma5_ratio']
    panel['breakout_volume_ratio_2d'] = (
        g['volume'].transform(lambda s: s.rolling(2).mean()) / panel['volume_ma10'].replace(0, np.nan)
    )
    breakout_range = (panel['high'] - panel['low']).replace(0, np.nan)
    panel['breakout_close_location'] = (panel['close'] - panel['low']) / breakout_range
    panel['ignition_day_strength'] = (
        0.6 * panel['breakout_ret_1d'].clip(lower=0).fillna(0) / 0.1
        + 0.4 * panel['breakout_volume_ratio_1d'].fillna(0) / 2.0
    )

    panel['pullback_ret_3d_like'] = g['close'].shift(-3) / panel['close'] - 1
    future_rets = pd.concat([
        g['close'].shift(-1) / panel['close'] - 1,
        g['close'].shift(-2) / panel['close'] - 1,
        g['close'].shift(-3) / panel['close'] - 1,
    ], axis=1)
    panel['negative_pullback_days_3'] = future_rets.lt(0).sum(axis=1)
    panel['controlled_down_days_3'] = future_rets.apply(lambda row: ((row < 0) & (row >= -0.04)).sum(), axis=1)
    future_vols = pd.concat([g['volume'].shift(-1), g['volume'].shift(-2), g['volume'].shift(-3)], axis=1)
    panel['pullback_avg_vol_ratio_3'] = future_vols.mean(axis=1) / panel['volume'].replace(0, np.nan)
    panel['max_washout_vol_spike_3'] = future_vols.max(axis=1) / panel['volume'].replace(0, np.nan)
    panel['washout_direction_bias_3'] = (
        future_rets.lt(0).sum(axis=1) - future_rets.gt(0).sum(axis=1)
    ) / 3.0
    panel['structure_break_days_3'] = (
        pd.concat([
            g['close'].shift(-1), g['close'].shift(-2), g['close'].shift(-3)
        ], axis=1).lt(panel['close'] * 0.97, axis=0).sum(axis=1)
    )

    if label_horizon is not None:
        default_label_horizons = sorted({int(label_horizon), 10, 20})
        horizons = sorted({int(h) for h in (label_horizons or default_label_horizons)})
        if int(label_horizon) not in horizons:
            horizons.append(int(label_horizon))
            horizons = sorted(horizons)
        event_horizons = sorted({int(h) for h in (event_windows or [1, 3, int(label_horizon)])})
        future_windows = sorted(set(horizons + event_horizons))

        for horizon in future_windows:
            future_col = f'future_ret_{horizon}'
            market_col = f'market_future_ret_{horizon}'
            excess_col = f'excess_ret_{horizon}_label'
            panel[future_col] = g['close'].shift(-horizon) / panel['close'] - 1
            market_future = panel.groupby('datetime')[future_col].mean().rename(market_col).reset_index()
            panel = panel.merge(market_future, on='datetime', how='left')
            panel[excess_col] = panel[future_col] - panel[market_col]

        primary_horizon = int(label_horizon)
        panel['label'] = panel[f'excess_ret_{primary_horizon}_label']
        panel[f'hit_rate_{primary_horizon}'] = panel[f'future_ret_{primary_horizon}'].ge(hit_threshold).astype(float)

        future_return_paths = pd.concat(
            [g['close'].shift(-step) / panel['close'] - 1 for step in range(1, primary_horizon + 1)],
            axis=1,
        )
        panel[f'downside_risk_{primary_horizon}'] = future_return_paths.min(axis=1).le(risk_threshold).astype(float)

        for window in event_horizons:
            panel[f'event_alpha_{window}'] = panel[f'excess_ret_{window}_label']
    return panel


def align_extension_to_qlib(base_panel: pd.DataFrame, extension: pd.DataFrame) -> pd.DataFrame:
    overlap = base_panel[['instrument', 'datetime', 'close', 'volume']].merge(
        extension[['instrument', 'datetime', 'close', 'volume']],
        on=['instrument', 'datetime'],
        how='inner',
        suffixes=('_base', '_ext'),
    )
    if overlap.empty:
        return extension

    overlap = overlap.replace([np.inf, -np.inf], np.nan)
    overlap = overlap[
        overlap['close_base'].gt(0)
        & overlap['close_ext'].gt(0)
        & overlap['volume_base'].gt(0)
        & overlap['volume_ext'].gt(0)
    ].copy()
    if overlap.empty:
        return extension

    overlap['price_scale'] = overlap['close_base'] / overlap['close_ext']
    overlap['volume_scale'] = overlap['volume_base'] / overlap['volume_ext']
    scale_map = (
        overlap.groupby('instrument')[['price_scale', 'volume_scale']]
        .median()
        .replace([np.inf, -np.inf], np.nan)
        .dropna(subset=['price_scale', 'volume_scale'])
        .reset_index()
    )
    if scale_map.empty:
        return extension

    aligned = extension.merge(scale_map, on='instrument', how='left')
    aligned['price_scale'] = aligned['price_scale'].fillna(1.0)
    aligned['volume_scale'] = aligned['volume_scale'].fillna(1.0)
    for col in ['open', 'close', 'high', 'low']:
        aligned[col] = aligned[col] * aligned['price_scale']
    aligned['volume'] = aligned['volume'] * aligned['volume_scale']
    return aligned.drop(columns=['price_scale', 'volume_scale'])


def build_panel(cfg: dict) -> pd.DataFrame:
    qcfg = cfg['qlib']
    instruments = D.instruments(qcfg['universe'])
    panel = D.features(
        instruments,
        fields=qcfg['features'],
        start_time=qcfg['start_date'],
        end_time=qcfg['end_date'],
        freq='day',
    ).reset_index()
    panel.columns = ['instrument', 'datetime', 'close', 'open', 'high', 'low', 'volume']
    panel['datetime'] = pd.to_datetime(panel['datetime'])

    ext_cfg = cfg.get('historical_extension', {})
    if ext_cfg.get('enabled'):
        extension_path = Path(ext_cfg.get('path', 'data/processed/akshare_recent_history.csv.gz'))
        if not extension_path.is_absolute():
            extension_path = Path(__file__).resolve().parents[2] / extension_path
        if extension_path.exists():
            extension = pd.read_csv(extension_path, parse_dates=['datetime'])
            required_cols = ['instrument', 'datetime', 'close', 'open', 'high', 'low', 'volume']
            extension = extension[required_cols].dropna(subset=RAW_COLS)
            extension = align_extension_to_qlib(panel, extension)
            panel = pd.concat([panel, extension], ignore_index=True)
            panel = panel.drop_duplicates(subset=['instrument', 'datetime'], keep='last').sort_values(['instrument', 'datetime']).reset_index(drop=True)

    label_spec = build_multitask_label_spec(cfg)
    horizon = int(qcfg.get('label_horizon', 5))
    return add_derived_features(
        panel,
        label_horizon=horizon,
        label_horizons=label_spec.get('return_horizons'),
        hit_threshold=float(label_spec.get('hit_threshold', 0.02)),
        risk_threshold=float(label_spec.get('risk_threshold', -0.04)),
        event_windows=label_spec.get('event_windows'),
    )


def split_panel(panel: pd.DataFrame, model_cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    train = panel[(panel['datetime'] >= model_cfg['train_start']) & (panel['datetime'] <= model_cfg['train_end'])]
    valid = panel[(panel['datetime'] >= model_cfg['valid_start']) & (panel['datetime'] <= model_cfg['valid_end'])]
    score = panel[(panel['datetime'] >= model_cfg['score_start']) & (panel['datetime'] <= model_cfg['score_end'])]
    train = train.dropna(subset=FEATURE_COLS + ['label'])
    valid = valid.dropna(subset=FEATURE_COLS + ['label'])
    score = score.dropna(subset=FEATURE_COLS)
    return train, valid, score


def train_model(train: pd.DataFrame, valid: pd.DataFrame, model_cfg: dict):
    params = dict(model_cfg['params'])
    model = lgb.LGBMRegressor(**params)
    model.fit(
        train[FEATURE_COLS],
        train['label'],
        eval_set=[(valid[FEATURE_COLS], valid['label'])],
        eval_metric='l2',
        callbacks=[lgb.early_stopping(30), lgb.log_evaluation(20)],
    )
    return model


def build_feature_importance(model) -> pd.DataFrame:
    return pd.DataFrame({'feature': FEATURE_COLS, 'importance': model.feature_importances_}).sort_values('importance', ascending=False).reset_index(drop=True)


def score_latest_topk(model, score: pd.DataFrame, top_k: int, source_label: str = 'qlib_sample') -> pd.DataFrame:
    scored = score[['datetime', 'instrument'] + FEATURE_COLS].copy()
    scored['score'] = model.predict(scored[FEATURE_COLS])
    latest_dt = scored['datetime'].max()
    latest = scored[scored['datetime'] == latest_dt].copy()
    latest = latest.sort_values('score', ascending=False).head(top_k).reset_index(drop=True)
    latest['rank'] = latest.index + 1
    latest['candidate_source'] = source_label
    return latest[['rank', 'datetime', 'instrument', 'score', 'candidate_source']]


def stock_code_to_instrument(stock_code: str) -> str:
    if stock_code.startswith('6'):
        return f'SH{stock_code}'
    return f'SZ{stock_code}'


def stock_code_to_tx_symbol(stock_code: str) -> str:
    if stock_code.startswith('6'):
        return f'sh{stock_code}'
    return f'sz{stock_code}'


def fetch_csi300_constituents() -> list[str]:
    if ak is None:
        raise ImportError('akshare 未安装，无法抓取当前市场成分股。')
    df = ak.index_stock_cons_csindex(symbol='000300')
    return df['成分券代码'].astype(str).str.zfill(6).tolist()


def fetch_all_a_share_codes() -> list[str]:
    if ak is None:
        raise ImportError('akshare 未安装，无法抓取当前A股列表。')
    df = ak.stock_info_a_code_name()
    return df['code'].astype(str).str.zfill(6).tolist()


def fetch_top_a_share_codes_by_turnover(top_n: int = 1200) -> list[str]:
    if ak is None:
        raise ImportError('akshare 未安装，无法抓取当前A股成交额快照。')

    outputs_dir = Path(__file__).resolve().parents[2] / 'data' / 'outputs'
    cache_path = outputs_dir / 'consolidation_breakout_turnover_cache.json'
    legacy_cache_path = outputs_dir / 'wangji_turnover_cache.json'
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_spot()
            df = df.copy()
            df['代码'] = df['代码'].astype(str).str.zfill(6)
            df = df[df['代码'].str.match(r'^(0|3|6)\d{5}$')]
            df['成交额'] = pd.to_numeric(df['成交额'], errors='coerce')
            df = df.dropna(subset=['成交额']).sort_values('成交额', ascending=False)
            codes = df.head(int(top_n))['代码'].tolist()
            if not codes:
                raise RuntimeError('A股成交额快照返回空列表')
            cache_payload = {
                'created_at': datetime.now().isoformat(timespec='seconds'),
                'source': 'ak.stock_zh_a_spot',
                'top_n': int(top_n),
                'count': len(codes),
                'codes': codes,
            }
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            cache_text = json.dumps(cache_payload, ensure_ascii=False, indent=2)
            cache_path.write_text(cache_text, encoding='utf-8')
            legacy_cache_path.write_text(cache_text, encoding='utf-8')
            return codes
        except Exception as exc:
            last_error = exc
            time.sleep(1.5 * (attempt + 1))

    readable_cache_path = cache_path if cache_path.exists() else legacy_cache_path
    if readable_cache_path.exists():
        payload_text = readable_cache_path.read_text(encoding='utf-8')
        if readable_cache_path == legacy_cache_path and not cache_path.exists():
            cache_path.write_text(payload_text, encoding='utf-8')
        payload = json.loads(payload_text)
        codes = normalize_stock_code_list(payload.get('codes') or [])
        if codes:
            return codes[:int(top_n)]

    fallback_codes = fetch_csi300_constituents()
    cache_payload = {
        'created_at': datetime.now().isoformat(timespec='seconds'),
        'source': 'fallback_csi300',
        'top_n': int(top_n),
        'count': len(fallback_codes),
        'error': repr(last_error) if last_error else '',
        'codes': normalize_stock_code_list(fallback_codes),
    }
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_text = json.dumps(cache_payload, ensure_ascii=False, indent=2)
    cache_path.write_text(cache_text, encoding='utf-8')
    legacy_cache_path.write_text(cache_text, encoding='utf-8')
    return normalize_stock_code_list(fallback_codes)[: min(int(top_n), len(fallback_codes))]


def normalize_stock_code_list(codes: list[str] | tuple[str, ...] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for code in codes or []:
        digits = ''.join(ch for ch in str(code) if ch.isdigit())
        if len(digits) != 6:
            continue
        code6 = digits.zfill(6)
        if code6 in seen:
            continue
        seen.add(code6)
        normalized.append(code6)
    return normalized


def fetch_recent_hist_for_code(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    market_symbol = stock_code.lower()
    if len(stock_code) == 6 and stock_code.isdigit():
        market_symbol = f"sh{stock_code}" if stock_code.startswith('6') else f"sz{stock_code}"

    last_error: Exception | None = None
    for attempt in range(3):
        try:
            df = ak.stock_zh_a_daily(
                symbol=market_symbol,
                start_date=start_date,
                end_date=end_date,
                adjust='qfq',
            )
            break
        except Exception as exc:
            last_error = exc
            if attempt == 2:
                raise
            time.sleep(1.5 * (attempt + 1))
    else:  # pragma: no cover
        raise last_error if last_error is not None else RuntimeError(f'failed to fetch history for {stock_code}')

    if df.empty:
        return pd.DataFrame()
    hist = pd.DataFrame(
        {
            'datetime': pd.to_datetime(df['date']),
            'instrument': stock_code_to_instrument(stock_code),
            'open': pd.to_numeric(df['open'], errors='coerce'),
            'close': pd.to_numeric(df['close'], errors='coerce'),
            'high': pd.to_numeric(df['high'], errors='coerce'),
            'low': pd.to_numeric(df['low'], errors='coerce'),
            'volume': pd.to_numeric(df['volume'], errors='coerce'),
        }
    )
    hist = hist.dropna(subset=RAW_COLS)
    return hist


def build_live_feature_panel(cfg: dict) -> pd.DataFrame:
    lcfg = cfg.get('live_data', {})
    lookback_days = int(lcfg.get('lookback_days', 120))
    end_dt = datetime.now()
    start_dt = end_dt - timedelta(days=lookback_days)
    start_date = str(lcfg.get('start_date') or start_dt.strftime('%Y%m%d'))
    end_date = str(lcfg.get('end_date') or end_dt.strftime('%Y%m%d'))
    max_workers = int(lcfg.get('max_workers', 4))
    stock_codes = fetch_csi300_constituents()
    panels: list[pd.DataFrame] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_recent_hist_for_code, code, start_date, end_date): code for code in stock_codes}
        for future in as_completed(futures):
            try:
                hist = future.result()
            except Exception:
                hist = pd.DataFrame()
            if hist is not None and not hist.empty:
                panels.append(hist)

    if not panels:
        raise RuntimeError('未抓到任何实时行情数据，无法生成当前候选池。')

    panel = pd.concat(panels, ignore_index=True)
    panel = add_derived_features(panel, label_horizon=None)
    panel = panel.dropna(subset=FEATURE_COLS)
    return panel


def score_live_topk(model, cfg: dict) -> pd.DataFrame:
    qcfg = cfg['qlib']
    panel = build_live_feature_panel(cfg)
    latest = panel.sort_values(['instrument', 'datetime']).groupby('instrument', as_index=False).tail(1).copy()
    latest['score'] = model.predict(latest[FEATURE_COLS])
    latest = latest.sort_values('score', ascending=False).head(int(qcfg['top_k'])).reset_index(drop=True)
    latest['rank'] = latest.index + 1
    latest['candidate_source'] = 'akshare_live_csi300'
    return latest[['rank', 'datetime', 'instrument', 'score', 'candidate_source']]


def generate_topk_candidates(model, score: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    mode = cfg.get('candidate_generation', {}).get('mode', 'live_akshare')
    if mode == 'historical_qlib':
        return score_latest_topk(model, score, int(cfg['qlib']['top_k']), source_label='qlib_sample')
    if mode == 'live_akshare':
        return score_live_topk(model, cfg)
    raise NotImplementedError(f'不支持的候选池模式: {mode}')


def build_walk_forward_summary(folds: list[dict[str, str]], scored_folds: dict[str, pd.DataFrame], top_k: int = 30) -> dict[str, Any]:
    def _safe_mean(values: pd.Series | list[float]) -> float:
        series = pd.Series(values, dtype='float64').replace([np.inf, -np.inf], np.nan).dropna()
        return round(float(series.mean()), 6) if not series.empty else 0.0

    fold_rows: list[dict[str, Any]] = []
    for fold in folds:
        fold_name = str(fold['fold'])
        scored = scored_folds.get(fold_name, pd.DataFrame()).copy()
        scored['datetime'] = pd.to_datetime(scored['datetime'])
        scored = scored.replace([np.inf, -np.inf], np.nan).dropna(subset=['score', 'label'])
        if scored.empty:
            fold_rows.append({**fold, 'rank_ic': 0.0, 'topk_avg_label': 0.0, 'rows': 0})
            continue
        daily_ic = scored.groupby('datetime').apply(lambda g: g['score'].rank().corr(g['label'].rank(), method='pearson'))
        daily_ic = daily_ic.replace([np.inf, -np.inf], np.nan).dropna()
        topk = scored.groupby('datetime', group_keys=False).apply(lambda g: g.sort_values('score', ascending=False).head(top_k))
        topk_labels = topk['label'].replace([np.inf, -np.inf], np.nan).dropna() if not topk.empty else pd.Series(dtype='float64')
        fold_rows.append({
            **fold,
            'rank_ic': _safe_mean(daily_ic),
            'topk_avg_label': _safe_mean(topk_labels),
            'rows': int(len(scored)),
        })
    rank_ic_values = [row['rank_ic'] for row in fold_rows]
    topk_values = [row['topk_avg_label'] for row in fold_rows]
    return {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'fold_count': len(fold_rows),
        'rank_ic_mean': _safe_mean(rank_ic_values),
        'topk_avg_label_mean': _safe_mean(topk_values),
        'folds': fold_rows,
    }



def build_training_artifacts(cfg: dict) -> tuple[Any, pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    panel = build_panel(cfg)
    train, valid, score = split_panel(panel, cfg['model'])
    if train.empty or valid.empty or score.empty:
        raise ValueError(
            '训练/验证/评分窗口超出当前Qlib数据覆盖范围。'
            f" panel_range={panel['datetime'].min().date()}~{panel['datetime'].max().date()}"
            f" train_rows={len(train)} valid_rows={len(valid)} score_rows={len(score)}"
        )
    model = train_model(train, valid, cfg['model'])
    panel_max_dt = panel['datetime'].max()
    folds = [
        {'fold': 'fold1', 'train_start': '2017-01-01', 'train_end': '2020-12-31', 'valid_start': '2021-01-01', 'valid_end': '2021-12-31'},
        {'fold': 'fold2', 'train_start': '2018-01-01', 'train_end': '2021-12-31', 'valid_start': '2022-01-01', 'valid_end': '2022-12-31'},
        {'fold': 'fold3', 'train_start': '2019-01-01', 'train_end': '2022-12-31', 'valid_start': '2023-01-01', 'valid_end': '2023-12-31'},
        {'fold': 'fold4', 'train_start': '2020-01-01', 'train_end': '2023-12-31', 'valid_start': '2024-01-01', 'valid_end': '2024-12-31'},
    ]
    folds = [fold for fold in folds if pd.Timestamp(fold['valid_start']) <= panel_max_dt]
    scored_folds: dict[str, pd.DataFrame] = {}
    for fold in folds:
        model_cfg = {**cfg['model'], 'train_start': fold['train_start'], 'train_end': fold['train_end'], 'valid_start': fold['valid_start'], 'valid_end': fold['valid_end'], 'score_start': fold['valid_start'], 'score_end': fold['valid_end']}
        fold_train, fold_valid, fold_score = split_panel(panel, model_cfg)
        if fold_train.empty or fold_valid.empty or fold_score.empty:
            scored_folds[fold['fold']] = pd.DataFrame(columns=['datetime', 'instrument', 'score', 'label'])
            continue
        fold_model = train_model(fold_train, fold_valid, cfg['model'])
        scored_fold = fold_score[['datetime', 'instrument', 'label'] + FEATURE_COLS].copy()
        scored_fold['score'] = fold_model.predict(scored_fold[FEATURE_COLS])
        scored_folds[fold['fold']] = scored_fold[['datetime', 'instrument', 'score', 'label']]
    walk_forward_summary = build_walk_forward_summary(folds, scored_folds, top_k=int(cfg['qlib'].get('top_k', 30)))
    return model, train, valid, score, walk_forward_summary
