from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_index_html_contains_multitask_label_panel_mount() -> None:
    html = (ROOT / 'frontend' / 'index.html').read_text(encoding='utf-8')

    assert '多任务标签层' in html
    assert 'id="multitaskLabelPanel"' in html
