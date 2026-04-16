from __future__ import annotations

import json
import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.priority import build_priority_candidates, build_risk_candidates
from ashare_platform.watchlist import build_watchlists


def main() -> None:
    cfg = load_config()
    candidates = pd.read_csv(ROOT / 'data' / 'outputs' / 'top30_candidates.csv')
    with (ROOT / 'data' / 'outputs' / 'event_cards.json').open('r', encoding='utf-8') as f:
        cards = json.load(f)

    card_df = pd.DataFrame(cards)
    if 'card_score' not in card_df.columns:
        priority_candidates, scored_cards = build_priority_candidates(candidates, cards, cfg)
        cards = scored_cards.to_dict(orient='records')
        priority_candidates.to_csv(ROOT / 'data' / 'outputs' / 'priority_candidates.csv', index=False)
        with (ROOT / 'data' / 'outputs' / 'event_cards.json').open('w', encoding='utf-8') as f:
            json.dump(cards, f, ensure_ascii=False, indent=2, default=str)
    else:
        priority_path = ROOT / 'data' / 'outputs' / 'priority_candidates.csv'
        if priority_path.exists():
            priority_candidates = pd.read_csv(priority_path)
        else:
            priority_candidates, _ = build_priority_candidates(candidates, cards, cfg)
            priority_candidates.to_csv(priority_path, index=False)

    card_df = pd.DataFrame(cards)
    risk_candidates = build_risk_candidates(priority_candidates, card_df, cfg)
    risk_candidates.to_csv(ROOT / 'data' / 'outputs' / 'risk_candidates.csv', index=False)

    daily_text, weekly_text, risk_text = build_watchlists(priority_candidates, cards, risk_candidates)
    (ROOT / 'data' / 'outputs' / 'daily_watchlist.md').write_text(daily_text, encoding='utf-8')
    (ROOT / 'data' / 'outputs' / 'weekly_watchlist.md').write_text(weekly_text, encoding='utf-8')
    (ROOT / 'data' / 'outputs' / 'risk_watchlist.md').write_text(risk_text, encoding='utf-8')
    print('watchlists_built')


if __name__ == '__main__':
    main()
