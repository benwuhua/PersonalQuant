from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.announcements import fetch_announcements_for_candidates
from ashare_platform.config import load_config
from ashare_platform.io_utils import write_json


def main() -> None:
    cfg = load_config()
    candidates = pd.read_csv(ROOT / 'data' / 'outputs' / 'top30_candidates.csv')
    items = fetch_announcements_for_candidates(candidates, cfg)
    out_path = ROOT / 'data' / 'outputs' / 'announcements_raw.json'
    write_json(out_path, items)
    print('announcements_saved', out_path)
    print('rows', len(items))


if __name__ == '__main__':
    main()
