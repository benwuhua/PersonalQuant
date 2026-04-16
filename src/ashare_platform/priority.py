from __future__ import annotations

from typing import Any

import pandas as pd


DEFAULT_CARD_WEIGHTS = {
    'importance': 0.30,
    'bias': 0.20,
    'confidence': 0.15,
    'freshness': 0.20,
    'event_type': 0.10,
    'density': 0.05,
}
DEFAULT_RISK_CARD_WEIGHTS = {
    'importance': 0.35,
    'negativity': 0.25,
    'freshness': 0.20,
    'confidence': 0.10,
    'event_type': 0.10,
}

DEFAULT_IMPORTANCE_MAP = {'high': 1.0, 'medium': 0.6, 'low': 0.2}
DEFAULT_BIAS_MAP = {'positive': 1.0, 'neutral': 0.45, 'negative': 0.0}
DEFAULT_NEGATIVITY_MAP = {'negative': 1.0, 'neutral': 0.35, 'positive': 0.0}
DEFAULT_CONFIDENCE_MAP = {'high': 1.0, 'medium': 0.65, 'low': 0.3}
DEFAULT_EVENT_TYPE_MAP = {
    '业绩': 1.0,
    '经营进展': 0.8,
    '分红回购': 0.75,
    '融资并购': 0.7,
    '风险事项': 0.35,
    '其他': 0.2,
}
DEFAULT_RISK_EVENT_TYPE_MAP = {
    '风险事项': 1.0,
    '融资并购': 0.55,
    '其他': 0.25,
    '业绩': 0.15,
    '经营进展': 0.1,
    '分红回购': 0.1,
}
LOW_SIGNAL_KEYWORDS = [
    '董事会决议',
    '监事会决议',
    '股东会的通知',
    '股东大会的通知',
    'H股公告',
    '审计报告',
    '独立董事述职报告',
    '内部控制自我评价报告',
    'ESG报告',
    '关于召开',
]


def _safe_minmax(series: pd.Series) -> pd.Series:
    if series.empty:
        return series.astype(float)
    min_value = float(series.min())
    max_value = float(series.max())
    if abs(max_value - min_value) < 1e-12:
        return pd.Series(0.5, index=series.index, dtype=float)
    return (series - min_value) / (max_value - min_value)


def _normalize_datetime(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors='coerce').dt.tz_localize(None)


def _reference_date(candidates: pd.DataFrame, cards: pd.DataFrame) -> pd.Timestamp:
    dates = []
    if 'datetime' in candidates.columns:
        candidate_dates = _normalize_datetime(candidates['datetime'])
        if not candidate_dates.dropna().empty:
            dates.append(candidate_dates.max())
    if 'publish_date' in cards.columns:
        card_dates = _normalize_datetime(cards['publish_date'])
        if not card_dates.dropna().empty:
            dates.append(card_dates.max())
    if not dates:
        return pd.Timestamp.today().normalize()
    return max(dates)


def _signal_quality(title: str, event_type: str, bias: str, confidence: str) -> float:
    title = title or ''
    if event_type in {'业绩', '经营进展', '分红回购', '融资并购'} and bias == 'positive':
        return 1.0
    if event_type == '风险事项':
        return 0.9
    if event_type == '其他' and bias == 'neutral' and confidence == 'low':
        return 0.55
    if any(keyword in title for keyword in LOW_SIGNAL_KEYWORDS):
        return 0.65
    return 1.0


def score_event_cards(event_cards: list[dict], cfg: dict, reference_date: pd.Timestamp | None = None) -> pd.DataFrame:
    if not event_cards:
        return pd.DataFrame(
            columns=[
                'instrument',
                'title',
                'publish_date',
                'event_type',
                'importance',
                'bias',
                'confidence',
                'card_score',
                'days_since_publish',
            ]
        )

    scoring_cfg = cfg.get('priority_scoring', {})
    weights = {**DEFAULT_CARD_WEIGHTS, **scoring_cfg.get('event_card_weights', {})}
    importance_map = {**DEFAULT_IMPORTANCE_MAP, **scoring_cfg.get('importance_map', {})}
    bias_map = {**DEFAULT_BIAS_MAP, **scoring_cfg.get('bias_map', {})}
    confidence_map = {**DEFAULT_CONFIDENCE_MAP, **scoring_cfg.get('confidence_map', {})}
    event_type_map = {**DEFAULT_EVENT_TYPE_MAP, **scoring_cfg.get('event_type_map', {})}

    cards = pd.DataFrame(event_cards).copy()
    cards['publish_date'] = _normalize_datetime(cards['publish_date'])
    cards['instrument'] = cards['instrument'].astype(str)
    if reference_date is None:
        reference_date = _reference_date(pd.DataFrame(), cards)

    lookback_days = int(cfg.get('announcements', {}).get('lookback_days', 7))
    cards['days_since_publish'] = (reference_date - cards['publish_date']).dt.days.fillna(lookback_days).clip(lower=0)
    freshness = 1 - (cards['days_since_publish'] / max(lookback_days, 1))
    cards['freshness_score'] = freshness.clip(lower=0, upper=1)

    card_counts = cards.groupby('instrument')['instrument'].transform('size')
    slots = int(scoring_cfg.get('top_event_slots', 3))
    cards['density_score'] = ((card_counts.clip(upper=slots) - 1) / max(slots - 1, 1)).clip(lower=0, upper=1)
    cards['importance_score'] = cards['importance'].map(importance_map).fillna(0.4)
    cards['bias_score'] = cards['bias'].map(bias_map).fillna(0.45)
    cards['confidence_score'] = cards['confidence'].map(confidence_map).fillna(0.3)
    cards['event_type_score'] = cards['event_type'].map(event_type_map).fillna(0.2)
    cards['signal_quality'] = [
        _signal_quality(title, event_type, bias, confidence)
        for title, event_type, bias, confidence in zip(
            cards['title'], cards['event_type'], cards['bias'], cards['confidence']
        )
    ]

    base_score = (
        cards['importance_score'] * weights['importance']
        + cards['bias_score'] * weights['bias']
        + cards['confidence_score'] * weights['confidence']
        + cards['freshness_score'] * weights['freshness']
        + cards['event_type_score'] * weights['event_type']
        + cards['density_score'] * weights['density']
    )
    cards['card_score'] = (base_score * cards['signal_quality']).round(6)
    cards['risk_card_score'] = 0.0
    return cards.sort_values(['instrument', 'card_score', 'publish_date'], ascending=[True, False, False]).reset_index(drop=True)


def build_priority_candidates(candidates: pd.DataFrame, event_cards: list[dict], cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    candidates = candidates.copy()
    candidates['instrument'] = candidates['instrument'].astype(str)
    candidates['datetime'] = _normalize_datetime(candidates['datetime'])
    candidates['quant_score_norm'] = _safe_minmax(candidates['score'].astype(float)).round(6)

    scored_cards = score_event_cards(event_cards, cfg, reference_date=_reference_date(candidates, pd.DataFrame(event_cards)))
    if scored_cards.empty:
        candidates['event_score'] = 0.0
        candidates['priority_score'] = candidates['quant_score_norm']
        candidates['priority_rank'] = candidates['priority_score'].rank(method='first', ascending=False).astype(int)
        candidates['top_event_type'] = ''
        candidates['top_event_title'] = ''
        candidates['top_event_summary'] = ''
        candidates['top_event_bias'] = ''
        candidates['top_event_importance'] = ''
        candidates['top_event_confidence'] = ''
        candidates['event_card_count'] = 0
        return candidates.sort_values('priority_rank').reset_index(drop=True), scored_cards

    scoring_cfg = cfg.get('priority_scoring', {})
    quant_weight = float(scoring_cfg.get('quant_weight', 0.55))
    event_weight = float(scoring_cfg.get('event_weight', 0.45))
    slots = int(scoring_cfg.get('top_event_slots', 3))

    scored_cards['event_rank'] = scored_cards.groupby('instrument').cumcount() + 1
    top_cards = scored_cards[scored_cards['event_rank'] <= slots].copy()
    top_cards['slot_weight'] = top_cards['event_rank'].map({1: 0.6, 2: 0.3, 3: 0.1}).fillna(0.0)

    rows = []
    for instrument, df in top_cards.groupby('instrument', sort=False):
        lead = df.iloc[0]
        rows.append(
            {
                'instrument': instrument,
                'event_score': float((df['card_score'] * df['slot_weight']).sum()),
                'event_card_count': int(len(df)),
                'top_event_type': lead.get('event_type', ''),
                'top_event_title': lead.get('title', ''),
                'top_event_summary': lead.get('summary', ''),
                'top_event_bias': lead.get('bias', ''),
                'top_event_importance': lead.get('importance', ''),
                'top_event_confidence': lead.get('confidence', ''),
            }
        )
    aggregated = pd.DataFrame(rows)

    merged = candidates.merge(aggregated, on='instrument', how='left')
    for col in ['event_score', 'event_card_count']:
        merged[col] = merged[col].fillna(0)
    for col in ['top_event_type', 'top_event_title', 'top_event_summary', 'top_event_bias', 'top_event_importance', 'top_event_confidence']:
        merged[col] = merged[col].fillna('')

    merged['priority_score'] = (merged['quant_score_norm'] * quant_weight + merged['event_score'] * event_weight).round(6)
    merged = merged.sort_values(['priority_score', 'score'], ascending=[False, False]).reset_index(drop=True)
    merged['priority_rank'] = merged.index + 1
    return merged, scored_cards


def build_risk_candidates(candidates: pd.DataFrame, scored_cards: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    risk_cfg = cfg.get('risk_scoring', {})
    weights = {**DEFAULT_RISK_CARD_WEIGHTS, **risk_cfg.get('event_card_weights', {})}
    importance_map = {**DEFAULT_IMPORTANCE_MAP, **risk_cfg.get('importance_map', {})}
    negativity_map = {**DEFAULT_NEGATIVITY_MAP, **risk_cfg.get('negativity_map', {})}
    confidence_map = {**DEFAULT_CONFIDENCE_MAP, **risk_cfg.get('confidence_map', {})}
    event_type_map = {**DEFAULT_RISK_EVENT_TYPE_MAP, **risk_cfg.get('event_type_map', {})}
    slots = int(risk_cfg.get('top_event_slots', 3))
    min_risk_score = float(risk_cfg.get('min_risk_score', 0.15))
    quant_weight = float(risk_cfg.get('quant_weight', 0.20))
    event_weight = float(risk_cfg.get('event_weight', 0.80))

    base = candidates.copy()
    if 'quant_score_norm' not in base.columns:
        base['quant_score_norm'] = _safe_minmax(base['score'].astype(float)).round(6)

    if scored_cards.empty:
        return pd.DataFrame(columns=['risk_rank']), scored_cards

    cards = scored_cards.copy()
    cards['risk_importance_score'] = cards['importance'].map(importance_map).fillna(0.4)
    cards['negativity_score'] = cards['bias'].map(negativity_map).fillna(0.0)
    cards['risk_confidence_score'] = cards['confidence'].map(confidence_map).fillna(0.3)
    cards['risk_event_type_score'] = cards['event_type'].map(event_type_map).fillna(0.1)
    cards['risk_gate'] = ((cards['bias'] == 'negative') | (cards['event_type'] == '风险事项')).astype(float)

    cards['risk_card_score'] = (
        cards['risk_importance_score'] * weights['importance']
        + cards['negativity_score'] * weights['negativity']
        + cards['freshness_score'] * weights['freshness']
        + cards['risk_confidence_score'] * weights['confidence']
        + cards['risk_event_type_score'] * weights['event_type']
    ) * cards['risk_gate']
    cards['risk_card_score'] = cards['risk_card_score'].round(6)

    active_risk_cards = cards[cards['risk_card_score'] > 0].copy()
    if active_risk_cards.empty:
        return pd.DataFrame(columns=['risk_rank']), cards

    active_risk_cards = active_risk_cards.sort_values(['instrument', 'risk_card_score', 'publish_date'], ascending=[True, False, False]).reset_index(drop=True)
    active_risk_cards['risk_event_rank'] = active_risk_cards.groupby('instrument').cumcount() + 1
    top_cards = active_risk_cards[active_risk_cards['risk_event_rank'] <= slots].copy()
    top_cards['slot_weight'] = top_cards['risk_event_rank'].map({1: 0.6, 2: 0.3, 3: 0.1}).fillna(0.0)

    rows = []
    for instrument, df in top_cards.groupby('instrument', sort=False):
        lead = df.iloc[0]
        rows.append(
            {
                'instrument': instrument,
                'risk_event_score': float((df['risk_card_score'] * df['slot_weight']).sum()),
                'risk_event_count': int(len(df)),
                'top_risk_event_type': lead.get('event_type', ''),
                'top_risk_title': lead.get('title', ''),
                'top_risk_summary': lead.get('summary', ''),
                'top_risk_bias': lead.get('bias', ''),
                'top_risk_importance': lead.get('importance', ''),
                'top_risk_confidence': lead.get('confidence', ''),
            }
        )
    aggregated = pd.DataFrame(rows)

    merged = base.merge(aggregated, on='instrument', how='inner')
    merged['risk_attention_score'] = (
        merged['risk_event_score'] * event_weight + merged['quant_score_norm'] * quant_weight
    ).round(6)
    merged = merged[merged['risk_attention_score'] >= min_risk_score].copy()
    merged = merged.sort_values(['risk_attention_score', 'risk_event_score', 'score'], ascending=[False, False, False]).reset_index(drop=True)
    merged['risk_rank'] = merged.index + 1
    return merged, cards


def format_priority_breakdown(row: Any) -> str:
    return (
        f"priority={getattr(row, 'priority_score', 0):.4f} "
        f"(quant={getattr(row, 'quant_score_norm', 0):.4f}, event={getattr(row, 'event_score', 0):.4f})"
    )


def format_risk_breakdown(row: Any) -> str:
    return (
        f"risk={getattr(row, 'risk_attention_score', 0):.4f} "
        f"(risk_event={getattr(row, 'risk_event_score', 0):.4f}, quant={getattr(row, 'quant_score_norm', 0):.4f})"
    )
