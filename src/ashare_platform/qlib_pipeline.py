from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import lightgbm as lgb
import pandas as pd
import qlib
from qlib.data import D
from qlib.tests.data import GetData

try:
    import akshare as ak
except Exception:
    ak = None

FEATURE_COLS = [
    'close_lag1',
    'close_lag5',
    'close_ma5',
    'close_ma10',
    'close_ma20',
    'volume_ma5',
    'volume_ma10',
    'volume_ma20',
    'ret_1',
    'ret_5',
    'ret_10',
    'ret_20',
    'intraday_ret',
    'overnight_ret',
    'range_pct',
    'close_ma5_ratio',
    'close_ma10_ratio',
    'close_ma20_ratio',
    'volume_ma5_ratio',
    'volume_ma10_ratio',
    'volume_ma20_ratio',
    'volatility_5',
    'volatility_20',
    'price_position_20',
    'volume_zscore_20',
    'excess_ret_5',
    'excess_ret_10',
    'excess_ret_20',
    'close_ma5_slope',
    'close_ma10_slope',
    'close_ma20_slope',
    'volatility_regime_20',
    'breakout_strength_20',
    'distance_from_high_20',
    'up_volume_ratio_10',
    'down_volume_ratio_10',
    'streak_up_days',
]

RAW_COLS = ['close', 'open', 'high', 'low', 'volume']


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
    denom = (rolling_high - rolling_low).replace(0, pd.NA)
    return (close - rolling_low) / denom


def _safe_zscore(series: pd.Series, rolling_mean: pd.Series, rolling_std: pd.Series) -> pd.Series:
    denom = rolling_std.replace(0, pd.NA)
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


def add_derived_features(panel: pd.DataFrame, label_horizon: int | None = None) -> pd.DataFrame:
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
    panel['volatility_regime_20'] = panel['volatility_5'] / panel['volatility_20'].replace(0, pd.NA)
    panel['breakout_strength_20'] = panel['close'] / rolling_high_20.replace(0, pd.NA) - 1
    panel['distance_from_high_20'] = panel['close'] / rolling_high_20.replace(0, pd.NA) - 1

    up_volume = panel['volume'].where(panel['ret_1'] > 0, 0.0)
    down_volume = panel['volume'].where(panel['ret_1'] <= 0, 0.0)
    panel['up_volume_ratio_10'] = up_volume.groupby(panel['instrument']).transform(lambda s: s.rolling(10).sum()) / g['volume'].transform(lambda s: s.rolling(10).sum()).replace(0, pd.NA)
    panel['down_volume_ratio_10'] = down_volume.groupby(panel['instrument']).transform(lambda s: s.rolling(10).sum()) / g['volume'].transform(lambda s: s.rolling(10).sum()).replace(0, pd.NA)
    panel['streak_up_days'] = g['ret_1'].transform(_streak_up_days)

    if label_horizon is not None:
        panel['label'] = g['close'].shift(-label_horizon) / panel['close'] - 1
    return panel


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
    horizon = int(qcfg.get('label_horizon', 5))
    return add_derived_features(panel, label_horizon=horizon)


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


def fetch_recent_hist_for_code(stock_code: str, start_date: str, end_date: str) -> pd.DataFrame:
    tx_symbol = stock_code_to_tx_symbol(stock_code)
    df = ak.stock_zh_a_hist_tx(
        symbol=tx_symbol,
        start_date=f'{start_date[:4]}-{start_date[4:6]}-{start_date[6:8]}',
        end_date=f'{end_date[:4]}-{end_date[4:6]}-{end_date[6:8]}',
        adjust='qfq',
    )
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
            'volume': pd.to_numeric(df['amount'], errors='coerce'),
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


def build_training_artifacts(cfg: dict) -> tuple[Any, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    panel = build_panel(cfg)
    train, valid, score = split_panel(panel, cfg['model'])
    model = train_model(train, valid, cfg['model'])
    return model, train, valid, score
