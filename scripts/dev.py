from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / 'src'
CONFIG_DIR = ROOT / 'config'
DEFAULT_CONFIG = CONFIG_DIR / 'config.yaml'
SAMPLE_CONFIG = CONFIG_DIR / 'config.sample.yaml'
LOCAL_CONFIG = CONFIG_DIR / 'config.local.yaml'


def _python() -> str:
    return sys.executable


def _env_with_config(config: str | None) -> dict[str, str]:
    env = os.environ.copy()
    chosen = (config or '').strip()
    if chosen:
        env['PERSONALQUANT_CONFIG'] = str(Path(chosen).expanduser().resolve())
    return env


def _run(command: list[str], *, config: str | None = None) -> int:
    env = _env_with_config(config)
    print('run:', ' '.join(command))
    if env.get('PERSONALQUANT_CONFIG'):
        print('config:', env['PERSONALQUANT_CONFIG'])
    return subprocess.run(command, cwd=ROOT, env=env, check=False).returncode


def cmd_init_config(_: argparse.Namespace) -> int:
    if LOCAL_CONFIG.exists():
        print(f'{LOCAL_CONFIG} already exists')
        return 0
    shutil.copy2(SAMPLE_CONFIG, LOCAL_CONFIG)
    print(f'created {LOCAL_CONFIG} from {SAMPLE_CONFIG}')
    return 0


def cmd_smoke(args: argparse.Namespace) -> int:
    code = _run([_python(), '-m', 'compileall', 'src', 'scripts'], config=args.config)
    if code != 0:
        return code

    smoke_code = (
        "import sys; from pathlib import Path; "
        "root = Path.cwd(); sys.path.insert(0, str(root / 'src')); "
        "import ashare_platform.announcements, ashare_platform.config, "
        "ashare_platform.dashboard, ashare_platform.fusion, ashare_platform.io_utils, "
        "ashare_platform.priority, ashare_platform.qlib_pipeline, ashare_platform.quant_pipeline, "
        "ashare_platform.summarizer, ashare_platform.watchlist, "
        "ashare_platform.consolidation_breakout_scanner; "
        "print('smoke ok')"
    )
    return _run([_python(), '-c', smoke_code], config=args.config)


def cmd_run(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_weekly_pipeline.py'], config=args.config)


def cmd_model_training(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_model_training_only.py'], config=args.config)


def cmd_daily_refresh(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_daily_refresh.py'], config=args.config)


def cmd_dashboard(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/build_dashboard_data.py'], config=args.config)


def cmd_validate(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_validation_update.py'], config=args.config)


def cmd_backtest(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_backtest_evaluation.py'], config=args.config)


def cmd_archive_diff(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/build_archive_diff.py'], config=args.config)


def cmd_timeline(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/build_instrument_timeline.py', args.instrument, '--limit', str(args.limit)], config=args.config)


def cmd_consolidation_breakout_scanner(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_consolidation_breakout_scanner.py'], config=args.config)


def cmd_wangji_scanner(args: argparse.Namespace) -> int:
    return cmd_consolidation_breakout_scanner(args)


def cmd_consolidation_breakout_refresh_cache(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/refresh_consolidation_breakout_turnover_cache.py'], config=args.config)


def cmd_wangji_refresh_cache(args: argparse.Namespace) -> int:
    return cmd_consolidation_breakout_refresh_cache(args)


def cmd_model_consolidation_breakout_fusion(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/build_model_consolidation_breakout_fusion.py'], config=args.config)


def cmd_model_wangji_fusion(args: argparse.Namespace) -> int:
    return cmd_model_consolidation_breakout_fusion(args)


def cmd_build_history_extension(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/build_training_history_extension.py'], config=args.config)


def cmd_quant_pipeline(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/build_quant_pipeline_snapshot.py'], config=args.config)


def cmd_consolidation_breakout_calibration(args: argparse.Namespace) -> int:
    command = [_python(), 'scripts/run_consolidation_breakout_calibration.py', '--profile', args.profile, '--start-date', args.start_date]
    if args.end_date:
        command.extend(['--end-date', args.end_date])
    return _run(command, config=args.config)


def cmd_wangji_calibration(args: argparse.Namespace) -> int:
    return cmd_consolidation_breakout_calibration(args)


def cmd_wangji_sacnner(args: argparse.Namespace) -> int:
    return cmd_wangji_scanner(args)


def cmd_cron_run(args: argparse.Namespace) -> int:
    env = _env_with_config(args.config)
    script_path = ROOT / 'scripts' / 'run_cron_workflow.sh'
    print('run:', script_path)
    if env.get('PERSONALQUANT_CONFIG'):
        print('config:', env['PERSONALQUANT_CONFIG'])
    return subprocess.run(['bash', str(script_path)], cwd=ROOT, env=env, check=False).returncode


def cmd_serve(args: argparse.Namespace) -> int:
    return _run(
        [_python(), 'scripts/serve_dashboard.py', '--host', args.host, '--port', str(args.port)],
        config=args.config,
    )


def cmd_clean_pyc(_: argparse.Namespace) -> int:
    removed = 0
    for cache_dir in ROOT.rglob('__pycache__'):
        if '.git' in cache_dir.parts:
            continue
        shutil.rmtree(cache_dir, ignore_errors=True)
        removed += 1
    for pyc_file in ROOT.rglob('*.pyc'):
        if '.git' in pyc_file.parts:
            continue
        try:
            pyc_file.unlink()
            removed += 1
        except FileNotFoundError:
            pass
    print(f'cleaned python caches/items: {removed}')
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Developer CLI for PersonalQuant')
    parser.add_argument('--config', help='Override config path via PERSONALQUANT_CONFIG')
    subparsers = parser.add_subparsers(dest='command', required=True)

    init_parser = subparsers.add_parser('init-config', help='Create config/config.local.yaml from sample if missing')
    init_parser.set_defaults(func=cmd_init_config)

    smoke_parser = subparsers.add_parser('smoke', help='Compile source and run import smoke test')
    smoke_parser.set_defaults(func=cmd_smoke)

    run_parser = subparsers.add_parser('run', help='Run the full weekly pipeline')
    run_parser.set_defaults(func=cmd_run)

    model_training_parser = subparsers.add_parser('model-training', help='Run the heavy training pipeline only and persist the model artifact')
    model_training_parser.set_defaults(func=cmd_model_training)

    daily_refresh_parser = subparsers.add_parser('daily-refresh', help='Refresh latest live data, announcements, scanners, watchlists, archives, and dashboard without retraining')
    daily_refresh_parser.set_defaults(func=cmd_daily_refresh)

    dashboard_parser = subparsers.add_parser('dashboard', help='Rebuild dashboard_data.json only')
    dashboard_parser.set_defaults(func=cmd_dashboard)

    validate_parser = subparsers.add_parser('validate', help='Update forward validation snapshots and reports')
    validate_parser.set_defaults(func=cmd_validate)

    backtest_parser = subparsers.add_parser('backtest', help='Run historical cross-sectional evaluation outputs')
    backtest_parser.set_defaults(func=cmd_backtest)

    archive_diff_parser = subparsers.add_parser('archive-diff', help='Build latest-vs-previous archive diff outputs')
    archive_diff_parser.set_defaults(func=cmd_archive_diff)

    timeline_parser = subparsers.add_parser('timeline', help='Build an instrument timeline from archives')
    timeline_parser.add_argument('instrument')
    timeline_parser.add_argument('--limit', type=int, default=20)
    timeline_parser.set_defaults(func=cmd_timeline)

    consolidation_breakout_parser = subparsers.add_parser('consolidation-breakout-scanner', help='Run consolidation breakout scanner with impulse/digestion scoring')
    consolidation_breakout_parser.set_defaults(func=cmd_consolidation_breakout_scanner)

    wangji_parser = subparsers.add_parser('wangji-scanner', help='Legacy alias for consolidation-breakout-scanner')
    wangji_parser.set_defaults(func=cmd_wangji_scanner)

    consolidation_breakout_cache_parser = subparsers.add_parser('consolidation-breakout-refresh-cache', help='Refresh local consolidation breakout turnover prefilter cache')
    consolidation_breakout_cache_parser.set_defaults(func=cmd_consolidation_breakout_refresh_cache)

    wangji_cache_parser = subparsers.add_parser('wangji-refresh-cache', help='Legacy alias for consolidation-breakout-refresh-cache')
    wangji_cache_parser.set_defaults(func=cmd_wangji_refresh_cache)

    fusion_parser = subparsers.add_parser('model-consolidation-breakout-fusion', help='Build fused ranking from model candidates and consolidation breakout scanner results')
    fusion_parser.set_defaults(func=cmd_model_consolidation_breakout_fusion)

    legacy_fusion_parser = subparsers.add_parser('model-wangji-fusion', help='Legacy alias for model-consolidation-breakout-fusion')
    legacy_fusion_parser.set_defaults(func=cmd_model_wangji_fusion)

    history_ext_parser = subparsers.add_parser('history-extension', help='Fetch AkShare recent history extension for model training')
    history_ext_parser.set_defaults(func=cmd_build_history_extension)

    quant_pipeline_parser = subparsers.add_parser('quant-pipeline', help='Build a complete quant pipeline blueprint snapshot from current project state')
    quant_pipeline_parser.set_defaults(func=cmd_quant_pipeline)

    consolidation_breakout_calibration_parser = subparsers.add_parser('consolidation-breakout-calibration', help='Replay calibration tickers and summarize best historical windows for consolidation breakout')
    consolidation_breakout_calibration_parser.add_argument('--profile', default='relax', choices=['relax'])
    consolidation_breakout_calibration_parser.add_argument('--start-date', default='20240101')
    consolidation_breakout_calibration_parser.add_argument('--end-date', default='')
    consolidation_breakout_calibration_parser.set_defaults(func=cmd_consolidation_breakout_calibration)

    wangji_calibration_parser = subparsers.add_parser('wangji-calibration', help='Legacy alias for consolidation-breakout-calibration')
    wangji_calibration_parser.add_argument('--profile', default='relax', choices=['relax'])
    wangji_calibration_parser.add_argument('--start-date', default='20240101')
    wangji_calibration_parser.add_argument('--end-date', default='')
    wangji_calibration_parser.set_defaults(func=cmd_wangji_calibration)

    wangji_legacy_parser = subparsers.add_parser('wangji-sacnner', help='Legacy alias for wangji-scanner')
    wangji_legacy_parser.set_defaults(func=cmd_wangji_sacnner)

    cron_parser = subparsers.add_parser('cron-run', help='Run the full scheduled workflow wrapper with log capture')
    cron_parser.set_defaults(func=cmd_cron_run)

    serve_parser = subparsers.add_parser('serve', help='Serve the local dashboard')
    serve_parser.add_argument('--host', default='127.0.0.1')
    serve_parser.add_argument('--port', type=int, default=8765)
    serve_parser.set_defaults(func=cmd_serve)

    clean_parser = subparsers.add_parser('clean-pyc', help='Remove Python bytecode caches')
    clean_parser.set_defaults(func=cmd_clean_pyc)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == '__main__':
    raise SystemExit(main())
