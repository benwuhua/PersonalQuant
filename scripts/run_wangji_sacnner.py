from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir
from ashare_platform.wangji_scanner import run_all_wangji_scanner_profiles, write_wangji_scanner_outputs


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    profile_frames = run_all_wangji_scanner_profiles(cfg)
    paths = write_wangji_scanner_outputs(profile_frames, outputs_dir)
    for profile_name, df in profile_frames.items():
        print(f'wangji_scanner_profile {profile_name}')
        print('rows', len(df))
        print('passed', int(df['pattern_passed'].sum()) if not df.empty else 0)
        print('csv', paths[profile_name]['csv_path'])
        print('report', paths[profile_name]['md_path'])
        if not df.empty:
            cols = ['scanner_rank', 'instrument', 'pattern_passed', 'rules_passed_count', 'breakout_ret', 'vol_ratio_5', 'pullback_ret_3d', 'close_range_10']
            print(df[cols].head(15).to_string(index=False))
            print('---')


if __name__ == '__main__':
    main()
