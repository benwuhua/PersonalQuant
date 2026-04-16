
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.archive_diff import write_archive_diff
from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir


def main() -> None:
    cfg = load_config()
    archives_dir = ensure_dir(ROOT / cfg['paths'].get('archives_dir', 'data/archives'))
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    paths = write_archive_diff(archives_dir, outputs_dir)
    for name, path in paths.items():
        print(f'{name} {path}')


if __name__ == '__main__':
    main()
