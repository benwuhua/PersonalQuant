from __future__ import annotations

import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.io_utils import ensure_dir, write_json
from ashare_platform.qlib_pipeline import (
    fetch_all_a_share_codes,
    fetch_csi300_constituents,
    fetch_recent_hist_for_code,
    normalize_stock_code_list,
)


def resolve_output_path(cfg: dict) -> Path:
    ext_cfg = cfg.get('historical_extension', {})
    output_path = Path(ext_cfg.get('path', 'data/processed/akshare_recent_history.csv.gz'))
    if not output_path.is_absolute():
        output_path = ROOT / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    return output_path


def resolve_meta_path(output_path: Path) -> Path:
    return output_path.with_suffix(output_path.suffix + '.meta.json')


def resolve_codes(cfg: dict) -> list[str]:
    ext_cfg = cfg.get('historical_extension', {})
    universe = str(ext_cfg.get('universe') or cfg.get('qlib', {}).get('universe', 'all')).lower()
    if universe in {'all', 'all_a'}:
        codes = fetch_all_a_share_codes()
    elif universe == 'csi300':
        codes = fetch_csi300_constituents()
    else:
        raise ValueError(f'unsupported historical_extension universe: {universe}')
    codes = normalize_stock_code_list(codes)
    return [code for code in codes if code[:1] in {'0', '3', '6'}]


def main() -> None:
    cfg = load_config()
    ext_cfg = cfg.get('historical_extension', {})
    requested_start_date = str(ext_cfg.get('start_date', '20200926')).replace('-', '')
    overlap_days = int(ext_cfg.get('overlap_days', 30))
    overlap_start_dt = datetime.strptime(requested_start_date, '%Y%m%d') - timedelta(days=overlap_days)
    start_date = overlap_start_dt.strftime('%Y%m%d')
    end_date = str(ext_cfg.get('end_date') or datetime.now().strftime('%Y%m%d')).replace('-', '')
    max_workers = int(ext_cfg.get('max_workers', cfg.get('live_data', {}).get('max_workers', 4)))
    output_path = resolve_output_path(cfg)
    meta_path = resolve_meta_path(output_path)
    codes = resolve_codes(cfg)

    frames: list[pd.DataFrame] = []
    failures: list[dict[str, str]] = []
    completed = 0

    print('output_path', output_path)
    print('meta_path', meta_path)
    print('requested_start_date', requested_start_date)
    print('fetch_start_date', start_date)
    print('end_date', end_date)
    print('codes', len(codes))
    print('max_workers', max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(fetch_recent_hist_for_code, code, start_date, end_date): code for code in codes}
        for future in as_completed(futures):
            code = futures[future]
            completed += 1
            try:
                frame = future.result()
            except Exception as exc:  # pragma: no cover - network variability
                failures.append({'code': code, 'error': repr(exc)})
                frame = pd.DataFrame()
            if frame is not None and not frame.empty:
                frames.append(frame)
            if completed % 100 == 0 or completed == len(codes):
                print('progress', completed, '/', len(codes), 'success_frames', len(frames), 'failures', len(failures))

    if not frames:
        raise RuntimeError('no historical extension data fetched')

    panel = pd.concat(frames, ignore_index=True)
    panel = panel.dropna(subset=['datetime', 'instrument', 'open', 'close', 'high', 'low', 'volume'])
    panel = panel.drop_duplicates(subset=['instrument', 'datetime'], keep='last').sort_values(['instrument', 'datetime']).reset_index(drop=True)
    panel.to_csv(output_path, index=False, compression='gzip')

    meta = {
        'generated_at': datetime.now().isoformat(timespec='seconds'),
        'requested_start_date': requested_start_date,
        'fetch_start_date': start_date,
        'end_date': end_date,
        'code_count': len(codes),
        'success_frames': len(frames),
        'failure_count': len(failures),
        'row_count': int(len(panel)),
        'instrument_count': int(panel['instrument'].nunique()),
        'min_datetime': str(panel['datetime'].min().date()),
        'max_datetime': str(panel['datetime'].max().date()),
        'failures_preview': failures[:50],
        'output_path': str(output_path),
    }
    write_json(meta_path, meta)

    print('row_count', len(panel))
    print('instrument_count', panel['instrument'].nunique())
    print('min_datetime', panel['datetime'].min())
    print('max_datetime', panel['datetime'].max())
    print('failure_count', len(failures))


if __name__ == '__main__':
    main()
