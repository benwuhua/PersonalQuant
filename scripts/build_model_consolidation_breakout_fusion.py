from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from ashare_platform.config import load_config
from ashare_platform.fusion import build_model_consolidation_breakout_fusion


def main() -> None:
    cfg = load_config()
    outputs_dir = ROOT / 'data' / 'outputs'
    model_path = outputs_dir / 'top30_candidates.csv'
    scanner_path = outputs_dir / 'consolidation-breakout-scanner_relax_candidates.csv'
    fusion_path = outputs_dir / 'model_consolidation_breakout_fusion_candidates.csv'
    summary_path = outputs_dir / 'model_consolidation_breakout_fusion_summary.md'
    legacy_fusion_path = outputs_dir / 'model_wangji_fusion_candidates.csv'
    legacy_summary_path = outputs_dir / 'model_wangji_fusion_summary.md'

    model = pd.read_csv(model_path)
    scanner = pd.read_csv(scanner_path)
    fused = build_model_consolidation_breakout_fusion(model, scanner, cfg)
    fused.to_csv(fusion_path, index=False)
    fused.to_csv(legacy_fusion_path, index=False)

    lines = [
        '# model-consolidation-breakout-fusion',
        '',
        f'- model_candidates: {len(model)}',
        f'- consolidation_breakout_candidates: {len(scanner)}',
        f'- fused_rows: {len(fused)}',
        '',
        '## Top 15',
    ]
    for row in fused.head(15).itertuples(index=False):
        lines.append(
            f"- fusion_rank={row.fusion_rank} | {row.instrument} | fusion_score={row.fusion_score:.6f} | "
            f"model_rank={row.rank} | scanner_rank={row.scanner_rank} | family={row.pattern_family} | passed={row.pattern_passed}"
        )
    summary_text = '\n'.join(lines) + '\n'
    summary_path.write_text(summary_text, encoding='utf-8')
    legacy_summary_path.write_text(summary_text, encoding='utf-8')

    print('fusion_path', fusion_path)
    print('summary_path', summary_path)
    print(fused.head(15).to_string(index=False))


if __name__ == '__main__':
    main()
