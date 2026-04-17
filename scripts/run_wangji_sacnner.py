from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir
from ashare_platform.wangji_scanner import run_wangji_sacnner, write_wangji_sacnner_outputs


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    df = run_wangji_sacnner(cfg)
    paths = write_wangji_sacnner_outputs(df, outputs_dir)
    print('wangji_sacnner_rows', len(df))
    print('wangji_sacnner_passed', int(df['pattern_passed'].sum()) if not df.empty else 0)
    print('wangji_sacnner_csv', paths['csv_path'])
    print('wangji_sacnner_report', paths['md_path'])
    if not df.empty:
        cols = ['scanner_rank', 'instrument', 'pattern_passed', 'breakout_ret', 'vol_ratio_5', 'pullback_ret_3d', 'close_range_10']
        print(df[cols].head(20).to_string(index=False))


if __name__ == '__main__':
    main()
