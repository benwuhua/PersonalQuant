from __future__ import annotations

import argparse
import functools
import http.server
import socketserver
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
FRONTEND_DIR = ROOT / 'frontend'


class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path in {'/', ''}:
            self.send_response(302)
            self.send_header('Location', '/frontend/')
            self.end_headers()
            return
        return super().do_GET()


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
        print(f'serving_root {ROOT}')
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            print('\nserver_stopped')
            sys.exit(0)


if __name__ == '__main__':
    main()
