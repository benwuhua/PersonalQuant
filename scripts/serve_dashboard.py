from __future__ import annotations

import argparse
import functools
import http.server
import json
import socketserver
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / 'frontend'
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.wangji_scanner import (  # noqa: E402
    build_wangji_scanner_report,
    normalize_profile_rules,
    run_wangji_scanner,
    summarize_wangji_scanner_run,
)


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path in {'/', ''}:
            self.send_response(302)
            self.send_header('Location', '/frontend/')
            self.end_headers()
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
            df = run_wangji_scanner(cfg, profile, overrides=rules)
            response = {
                'ok': True,
                'profile': profile,
                'summary': summarize_wangji_scanner_run(df, profile, rules),
                'report': build_wangji_scanner_report(df, profile, rules),
                'rows': df.fillna('').to_dict(orient='records'),
            }
            self._send_json(response)
        except Exception as exc:
            self._send_json({'ok': False, 'error': str(exc)}, status=500)

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
        print(f'api_url http://{args.host}:{args.port}/api/wangji-scanner/run')
        print(f'serving_root {ROOT}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nserver_stopped')
            sys.exit(0)


if __name__ == '__main__':
    main()
