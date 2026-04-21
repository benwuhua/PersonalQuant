from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / 'src'
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ashare_platform.consolidation_breakout_scanner import DEFAULT_CALIBRATION_CASES, replay_calibration_case


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Replay consolidation-breakout-scanner calibration cases and summarize best windows')
    parser.add_argument('--profile', default='relax', choices=['relax'])
    parser.add_argument('--start-date', default='', help='Override all case start dates')
    parser.add_argument('--end-date', default='', help='Override all case end dates')
    return parser


def main() -> int:
    args = build_parser().parse_args()
    outputs_dir = ROOT / 'data' / 'outputs'
    outputs_dir.mkdir(parents=True, exist_ok=True)
    payload: list[dict] = []

    for case in DEFAULT_CALIBRATION_CASES:
        case_start = args.start_date or case.get('start_date') or '20240101'
        case_end = args.end_date or case.get('end_date') or None
        _, summary = replay_calibration_case(
            case['code'],
            profile_name=args.profile,
            start_date=case_start,
            end_date=case_end,
        )
        best_row = summary.pop('best_row', {})
        summary.update(
            {
                'instrument': case['instrument'],
                'label': case['label'],
                'best_pattern_passed': bool(best_row.get('pattern_passed', False)),
                'best_impulse_score': round(float(best_row.get('impulse_score', 0.0) or 0.0), 4),
                'best_digestion_score': round(float(best_row.get('digestion_score', 0.0) or 0.0), 4),
                'best_controlled_down_days': int(best_row.get('controlled_down_days', 0) or 0),
                'best_impulse_day_count': int(best_row.get('impulse_day_count', 0) or 0),
                'best_impulse_volume_day_count': int(best_row.get('impulse_volume_day_count', 0) or 0),
                'best_weekly_structure_score': round(float(best_row.get('weekly_structure_score', 0.0) or 0.0), 4),
                'best_second_leg_distance': round(float(best_row.get('second_leg_distance', 0.0) or 0.0), 6),
            }
        )
        payload.append(summary)
        print(json.dumps(summary, ensure_ascii=False))

    out_path = outputs_dir / f'consolidation-breakout-scanner_{args.profile}_calibration.json'
    legacy_path = outputs_dir / f'wangji-scanner_{args.profile}_calibration.json'
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    out_path.write_text(text, encoding='utf-8')
    legacy_path.write_text(text, encoding='utf-8')
    print(f'calibration_json {out_path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
