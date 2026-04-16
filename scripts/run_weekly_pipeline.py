from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.announcements import fetch_announcements_for_candidates
from ashare_platform.config import load_config
from ashare_platform.dashboard import write_dashboard_payload
from ashare_platform.io_utils import archive_output_batch, ensure_dir, write_json
from ashare_platform.priority import build_priority_candidates, build_risk_candidates
from ashare_platform.qlib_pipeline import build_feature_importance, build_training_artifacts, generate_topk_candidates, init_qlib
from ashare_platform.summarizer import summarize_announcement
from ashare_platform.watchlist import build_watchlists


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    processed_dir = ensure_dir(ROOT / cfg['paths']['processed_dir'])
    ensure_dir(ROOT / cfg['paths']['logs_dir'])
    archives_dir = ensure_dir(ROOT / cfg['paths'].get('archives_dir', 'data/archives'))

    provider_uri = init_qlib(cfg['qlib']['provider_uri'], cfg['qlib']['region'])
    print('provider_uri', provider_uri)
    model, train, valid, score = build_training_artifacts(cfg)
    print('train_rows', len(train), 'valid_rows', len(valid), 'historical_score_rows', len(score))

    importance = build_feature_importance(model)
    importance_path = processed_dir / 'feature_importance.csv'
    importance.to_csv(importance_path, index=False)
    print('feature_importance_path', importance_path)
    print(importance.head(10).to_string(index=False))

    topk = generate_topk_candidates(model, score, cfg)
    topk_path = outputs_dir / 'top30_candidates.csv'
    topk.to_csv(topk_path, index=False)
    print('topk_path', topk_path)
    print('candidate_source', topk['candidate_source'].iloc[0] if not topk.empty else 'none')
    print('candidate_date', topk['datetime'].max() if not topk.empty else 'none')

    announcements = fetch_announcements_for_candidates(topk, cfg)
    raw_path = outputs_dir / 'announcements_raw.json'
    write_json(raw_path, announcements)
    print('announcements_raw_path', raw_path)
    print('announcement_rows', len(announcements))

    event_cards = [summarize_announcement(item, cfg['llm']) for item in announcements]
    priority_candidates, scored_cards = build_priority_candidates(topk, event_cards, cfg)
    risk_candidates, scored_cards = build_risk_candidates(priority_candidates, scored_cards, cfg)
    scored_event_cards = scored_cards.to_dict(orient='records')

    events_path = outputs_dir / 'event_cards.json'
    write_json(events_path, scored_event_cards)
    print('events_path', events_path)

    priority_path = outputs_dir / 'priority_candidates.csv'
    priority_candidates.to_csv(priority_path, index=False)
    print('priority_path', priority_path)
    print(priority_candidates[['priority_rank', 'instrument', 'priority_score', 'quant_score_norm', 'event_score']].head(10).to_string(index=False))

    risk_path = outputs_dir / 'risk_candidates.csv'
    risk_candidates.to_csv(risk_path, index=False)
    print('risk_path', risk_path)
    if not risk_candidates.empty:
        print(risk_candidates[['risk_rank', 'instrument', 'risk_attention_score', 'risk_event_score', 'quant_score_norm']].head(10).to_string(index=False))
    else:
        print('risk_candidates empty')

    daily_text, weekly_text, risk_text = build_watchlists(priority_candidates, scored_event_cards, risk_candidates)
    daily_path = outputs_dir / 'daily_watchlist.md'
    weekly_path = outputs_dir / 'weekly_watchlist.md'
    risk_watchlist_path = outputs_dir / 'risk_watchlist.md'
    daily_path.write_text(daily_text, encoding='utf-8')
    weekly_path.write_text(weekly_text, encoding='utf-8')
    risk_watchlist_path.write_text(risk_text, encoding='utf-8')
    print('daily_path', daily_path)
    print('weekly_path', weekly_path)
    print('risk_watchlist_path', risk_watchlist_path)

    archive_dir = archive_output_batch(outputs_dir, archives_dir)
    print('archive_dir', archive_dir)

    dashboard_path = write_dashboard_payload(outputs_dir / 'dashboard_data.json')
    print('dashboard_path', dashboard_path)
    print(priority_candidates.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
