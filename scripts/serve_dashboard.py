from __future__ import annotations

import argparse
import functools
import http.server
import json
import socketserver
import sys
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / 'frontend'
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config  # noqa: E402
from ashare_platform.wangji_scanner import (  # noqa: E402
    build_wangji_scanner_report,
    normalize_profile_rules,
    run_wangji_scanner,
    summarize_wangji_scanner_run,
)

JOB_LOCK = threading.Lock()
SCAN_JOBS: dict[str, dict] = {}


def _job_payload(job_id: str) -> dict:
    with JOB_LOCK:
        job = dict(SCAN_JOBS.get(job_id, {}))
    if not job:
        return {}
    return job


def _update_job(job_id: str, **updates):
    with JOB_LOCK:
        if job_id not in SCAN_JOBS:
            return
        SCAN_JOBS[job_id].update(updates)
        SCAN_JOBS[job_id]['updated_at'] = time.time()


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in {'/', ''}:
            self.send_response(302)
            self.send_header('Location', '/frontend/')
            self.end_headers()
            return
        if parsed.path == '/api/wangji-scanner/status':
            self._handle_wangji_scanner_status(parsed)
            return
        return super().do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == '/api/wangji-scanner/run':
            self._handle_wangji_scanner_run(parsed)
            return
        self.send_error(404, 'Not found')

    def _handle_wangji_scanner_run(self, parsed):
        try:
            length = int(self.headers.get('Content-Length', '0'))
            body = self.rfile.read(length) if length > 0 else b'{}'
            payload = json.loads(body.decode('utf-8') or '{}')
            profile = str(payload.get('profile') or 'strict')
            params = payload.get('params') or {}
            cfg = load_config()
            rules = normalize_profile_rules(profile, params)
            job_id = uuid.uuid4().hex[:12]
            with JOB_LOCK:
                SCAN_JOBS[job_id] = {
                    'ok': True,
                    'job_id': job_id,
                    'profile': profile,
                    'status': 'queued',
                    'stage': 'queued',
                    'message': '任务已创建，准备开始',
                    'created_at': time.time(),
                    'updated_at': time.time(),
                }

            def progress(stage: str, message: str):
                _update_job(job_id, status='running', stage=stage, message=message)

            def worker():
                try:
                    progress('preparing', '正在准备扫描参数')
                    df = run_wangji_scanner(cfg, profile, overrides=rules, progress_callback=progress)
                    result = {
                        'ok': True,
                        'job_id': job_id,
                        'profile': profile,
                        'status': 'completed',
                        'stage': 'done',
                        'message': '候选生成完成',
                        'summary': summarize_wangji_scanner_run(df, profile, rules),
                        'report': build_wangji_scanner_report(df, profile, rules),
                        'rows': df.fillna('').to_dict(orient='records'),
                    }
                    _update_job(job_id, **result)
                except Exception as exc:
                    _update_job(job_id, ok=False, status='failed', stage='failed', message=str(exc), error=str(exc))

            threading.Thread(target=worker, daemon=True).start()
            self._send_json({'ok': True, 'job_id': job_id, 'profile': profile, 'status': 'queued'})
        except Exception as exc:
            self._send_json({'ok': False, 'error': str(exc)}, status=500)

    def _handle_wangji_scanner_status(self, parsed):
        query = parse_qs(parsed.query)
        job_id = (query.get('job_id') or [''])[0]
        if not job_id:
            self._send_json({'ok': False, 'error': 'missing job_id'}, status=400)
            return
        payload = _job_payload(job_id)
        if not payload:
            self._send_json({'ok': False, 'error': 'job not found'}, status=404)
            return
        self._send_json(payload)

    def _send_json(self, payload, status=200):
        content = json.dumps(payload, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(content)))
        self.end_headers()
        self.wfile.write(content)


class ThreadingTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


def main() -> None:
    parser = argparse.ArgumentParser(description='Serve the A-share research dashboard locally')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=8765)
    args = parser.parse_args()

    if not FRONTEND_DIR.exists():
        raise FileNotFoundError(f'frontend directory not found: {FRONTEND_DIR}')

    handler = functools.partial(DashboardHandler, directory=str(ROOT))
    with ThreadingTCPServer((args.host, args.port), handler) as httpd:
        print(f'dashboard_url http://{args.host}:{args.port}/frontend/')
        print(f'api_run_url http://{args.host}:{args.port}/api/wangji-scanner/run')
        print(f'api_status_url http://{args.host}:{args.port}/api/wangji-scanner/status?job_id=<id>')
        print(f'serving_root {ROOT}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nserver_stopped')
            sys.exit(0)


if __name__ == '__main__':
    main()
