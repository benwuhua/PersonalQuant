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
        "ashare_platform.dashboard, ashare_platform.io_utils, "
        "ashare_platform.priority, ashare_platform.qlib_pipeline, "
        "ashare_platform.summarizer, ashare_platform.watchlist; "
        "print('smoke ok')"
    )
    return _run([_python(), '-c', smoke_code], config=args.config)


def cmd_run(args: argparse.Namespace) -> int:
    return _run([_python(), 'scripts/run_weekly_pipeline.py'], config=args.config)


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
