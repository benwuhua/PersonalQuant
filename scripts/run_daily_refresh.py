from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.announcements import fetch_announcements_for_candidates
from ashare_platform.archive_diff import write_archive_diff
from ashare_platform.config import load_config
from ashare_platform.dashboard import write_dashboard_payload
from ashare_platform.io_utils import archive_output_batch, ensure_dir, write_json
from ashare_platform.priority import build_priority_candidates, build_risk_candidates
from ashare_platform.qlib_pipeline import init_qlib, load_model_artifact, score_live_topk
from ashare_platform.quant_pipeline import write_quant_pipeline_snapshot
from ashare_platform.summarizer import summarize_announcement
from ashare_platform.timeline import write_instrument_timeline
from ashare_platform.validation import update_validation_artifacts
from ashare_platform.consolidation_breakout_scanner import run_all_consolidation_breakout_scanner_profiles, write_consolidation_breakout_scanner_outputs
from ashare_platform.watchlist import build_watchlists


MODEL_FILENAME = 'lightgbm_model.pkl'


def main() -> None:
    cfg = load_config()
    outputs_dir = ensure_dir(ROOT / cfg['paths']['outputs_dir'])
    processed_dir = ensure_dir(ROOT / cfg['paths']['processed_dir'])
    archives_dir = ensure_dir(ROOT / cfg['paths'].get('archives_dir', 'data/archives'))
    validation_dir = ensure_dir(ROOT / cfg['paths'].get('validation_dir', 'data/validation'))

    provider_uri = init_qlib(cfg['qlib']['provider_uri'], cfg['qlib']['region'])
    print('provider_uri', provider_uri)

    model_path = processed_dir / MODEL_FILENAME
    topk_path = outputs_dir / 'top30_candidates.csv'
    model = None
    if model_path.exists():
        model = load_model_artifact(model_path)
        print('model_artifact_path', model_path)
        topk = score_live_topk(model, cfg)
        topk.to_csv(topk_path, index=False)
        print('topk_path', topk_path)
        print('candidate_source', topk['candidate_source'].iloc[0] if not topk.empty else 'none')
        print('candidate_refresh_mode', 'live_model_refresh')
    else:
        if not topk_path.exists():
            raise FileNotFoundError(f'缺少模型产物 {model_path}，且没有可回退的 {topk_path}')
        topk = pd.read_csv(topk_path)
        print('model_artifact_missing', model_path)
        print('topk_path', topk_path)
        print('candidate_refresh_mode', 'fallback_existing_topk')

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

    risk_path = outputs_dir / 'risk_candidates.csv'
    risk_candidates.to_csv(risk_path, index=False)
    print('risk_path', risk_path)

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

    validation_paths = update_validation_artifacts(validation_dir, outputs_dir, topk, priority_candidates, risk_candidates)
    print('validation_records_path', validation_paths['records_path'])
    print('validation_report_path', validation_paths['report_path'])

    wangji_frames = run_all_consolidation_breakout_scanner_profiles(cfg)
    wangji_paths = write_consolidation_breakout_scanner_outputs(wangji_frames, outputs_dir)
    for profile_name, frame in wangji_frames.items():
        print(f'consolidation_breakout_scanner_{profile_name}_rows', len(frame))
        print(f'consolidation_breakout_scanner_{profile_name}_passed', int(frame['pattern_passed'].sum()) if not frame.empty else 0)
        print(f'consolidation_breakout_scanner_{profile_name}_report', wangji_paths[profile_name]['md_path'])

    archive_dir = archive_output_batch(outputs_dir, archives_dir, run_label='refresh_' + pd.Timestamp.now().strftime('%Y%m%d_%H%M%S'))
    print('archive_dir', archive_dir)

    archive_diff_paths = write_archive_diff(archives_dir, outputs_dir)
    print('archive_diff_path', archive_diff_paths['md_path'])

    timelines_dir = ensure_dir(outputs_dir / 'timelines')
    for instrument in priority_candidates['instrument'].head(5).astype(str).tolist():
        write_instrument_timeline(archives_dir, timelines_dir, instrument, limit=20)
    print('timelines_dir', timelines_dir)

    quant_pipeline_paths = write_quant_pipeline_snapshot(cfg, output_dir=outputs_dir, root=ROOT)
    print('quant_pipeline_snapshot_json', quant_pipeline_paths['json_path'])
    print('quant_pipeline_blueprint_md', quant_pipeline_paths['md_path'])

    dashboard_path = write_dashboard_payload(outputs_dir / 'dashboard_data.json')
    print('dashboard_path', dashboard_path)


if __name__ == '__main__':
    main()
