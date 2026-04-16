
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir
from ashare_platform.timeline import write_instrument_timeline


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('instrument')
    parser.add_argument('--limit', type=int, default=20)
    args = parser.parse_args()
    cfg = load_config()
    archives_dir = ensure_dir(ROOT / cfg['paths'].get('archives_dir', 'data/archives'))
    output_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir']) / 'timelines'
    paths = write_instrument_timeline(archives_dir, output_dir, args.instrument, limit=args.limit)
    for name, path in paths.items():
        print(f'{name} {path}')


if __name__ == '__main__':
    main()
