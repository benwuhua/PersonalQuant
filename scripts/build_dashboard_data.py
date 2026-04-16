from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.dashboard import write_dashboard_payload


if __name__ == '__main__':
    output_path = write_dashboard_payload()
    print(f'dashboard_data_path {output_path}')
