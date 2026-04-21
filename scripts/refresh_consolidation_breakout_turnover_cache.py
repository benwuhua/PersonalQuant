from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.qlib_pipeline import fetch_top_a_share_codes_by_turnover


def main() -> int:
    codes = fetch_top_a_share_codes_by_turnover(1200)
    primary_cache_path = ROOT / 'data' / 'outputs' / 'consolidation_breakout_turnover_cache.json'
    legacy_cache_path = ROOT / 'data' / 'outputs' / 'wangji_turnover_cache.json'
    cache_path = primary_cache_path if primary_cache_path.exists() else legacy_cache_path
    payload_text = cache_path.read_text(encoding='utf-8')
    payload = json.loads(payload_text)
    primary_cache_path.write_text(payload_text, encoding='utf-8')
    print('cache_path', primary_cache_path)
    print('source', payload.get('source'))
    print('count', payload.get('count'))
    print('created_at', payload.get('created_at'))
    print('cache_first10', (payload.get('codes') or [])[:10])
    print('returned_first10', codes[:10])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
