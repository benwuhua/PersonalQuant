from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.quant_pipeline import write_quant_pipeline_snapshot


if __name__ == '__main__':
    cfg = load_config()
    paths = write_quant_pipeline_snapshot(cfg)
    print(f"quant_pipeline_snapshot_json {paths['json_path']}")
    print(f"quant_pipeline_blueprint_md {paths['md_path']}")
    print(f"multitask_label_spec_json {paths['label_spec_path']}")
