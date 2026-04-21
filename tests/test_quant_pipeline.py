from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform.quant_pipeline import (
    build_multitask_label_spec,
    build_quant_pipeline_report,
    build_quant_pipeline_snapshot,
    write_quant_pipeline_snapshot,
)


def test_build_quant_pipeline_snapshot_marks_missing_artifacts_as_partial(tmp_path: Path) -> None:
    cfg = {
        'project': {'name': 'demo-workbench'},
        'qlib': {'universe': 'all', 'score_date': '2026-04-17'},
        'model': {'type': 'lightgbm'},
        'candidate_generation': {'mode': 'live_akshare'},
        'live_data': {
            'consolidation_breakout_universe': 'all_a',
            'consolidation_breakout_prefilter_mode': 'turnover_top_n',
            'consolidation_breakout_prefilter_top_n': 1200,
        },
    }
    (tmp_path / 'data' / 'outputs').mkdir(parents=True)
    (tmp_path / 'data' / 'processed').mkdir(parents=True)
    (tmp_path / 'data' / 'outputs' / 'top30_candidates.csv').write_text('instrument\nAAA\n', encoding='utf-8')

    snapshot = build_quant_pipeline_snapshot(cfg, root=tmp_path)

    assert snapshot['summary']['project_name'] == 'demo-workbench'
    assert snapshot['summary']['scanner_universe'] == 'all_a'
    universe_layer = next(layer for layer in snapshot['layers'] if layer['id'] == 'universe')
    assert universe_layer['status'] == 'partial'
    assert any(item['path'] == 'data/outputs/top30_candidates.csv' and item['exists'] for item in universe_layer['artifact_status']['items'])


def test_write_quant_pipeline_snapshot_writes_json_and_markdown(tmp_path: Path) -> None:
    cfg = {
        'project': {'name': 'demo-workbench'},
        'qlib': {'universe': 'all', 'score_date': '2026-04-17'},
        'model': {'type': 'lightgbm'},
        'candidate_generation': {'mode': 'live_akshare'},
        'live_data': {
            'consolidation_breakout_universe': 'all_a',
            'consolidation_breakout_prefilter_mode': 'turnover_top_n',
            'consolidation_breakout_prefilter_top_n': 1200,
        },
    }
    out_dir = tmp_path / 'outputs'

    paths = write_quant_pipeline_snapshot(cfg, output_dir=out_dir, root=tmp_path)

    assert paths['json_path'].exists()
    assert paths['md_path'].exists()
    payload = json.loads(paths['json_path'].read_text(encoding='utf-8'))
    assert payload['summary']['project_name'] == 'demo-workbench'
    report = paths['md_path'].read_text(encoding='utf-8')
    assert '# Complete Quant Pipeline Blueprint' in report
    assert '## 数据层 /' in report


def test_build_quant_pipeline_report_includes_layer_next_steps() -> None:
    snapshot = {
        'summary': {
            'project_name': 'demo',
            'qlib_universe': 'all',
            'model_type': 'lightgbm',
            'live_candidate_mode': 'live_akshare',
            'scanner_universe': 'all_a',
            'scanner_prefilter_mode': 'turnover_top_n',
            'scanner_prefilter_top_n': 1200,
        },
        'layers': [
            {
                'name': '数据层',
                'status': 'partial',
                'goal': '统一管理数据。',
                'existing_components': ['src/demo.py'],
                'artifact_status': {'items': [{'path': 'data/x.csv', 'exists': False}]},
                'next_steps': ['补充行业画像'],
            }
        ],
    }

    report = build_quant_pipeline_report(snapshot)

    assert '补充行业画像' in report
    assert '[missing] data/x.csv' in report


def test_build_multitask_label_spec_expands_return_hit_and_risk_tasks() -> None:
    cfg = {
        'qlib': {'label_horizon': 5},
        'quant_pipeline': {
            'label_tasks': {
                'return_horizons': [3, 5, 10],
                'hit_threshold': 0.03,
                'risk_threshold': -0.05,
                'event_windows': [1, 3],
            }
        },
    }

    spec = build_multitask_label_spec(cfg)

    task_ids = [task['id'] for task in spec['tasks']]
    assert task_ids == [
        'return_3d',
        'return_5d',
        'return_10d',
        'excess_return_3d',
        'excess_return_5d',
        'excess_return_10d',
        'hit_rate_5d',
        'downside_risk_5d',
        'event_alpha_1d',
        'event_alpha_3d',
    ]
    hit_task = next(task for task in spec['tasks'] if task['id'] == 'hit_rate_5d')
    assert hit_task['threshold'] == 0.03
    risk_task = next(task for task in spec['tasks'] if task['id'] == 'downside_risk_5d')
    assert risk_task['threshold'] == -0.05


def test_build_quant_pipeline_snapshot_embeds_multitask_label_layer_details(tmp_path: Path) -> None:
    cfg = {
        'project': {'name': 'demo-workbench'},
        'qlib': {'universe': 'all', 'score_date': '2026-04-17', 'label_horizon': 5},
        'model': {'type': 'lightgbm'},
        'candidate_generation': {'mode': 'live_akshare'},
        'live_data': {
            'consolidation_breakout_universe': 'all_a',
            'consolidation_breakout_prefilter_mode': 'turnover_top_n',
            'consolidation_breakout_prefilter_top_n': 1200,
        },
        'quant_pipeline': {
            'label_tasks': {
                'return_horizons': [5, 10],
                'event_windows': [1],
            }
        },
    }
    (tmp_path / 'data' / 'outputs').mkdir(parents=True)

    snapshot = build_quant_pipeline_snapshot(cfg, root=tmp_path)

    label_layer = next(layer for layer in snapshot['layers'] if layer['id'] == 'label')
    assert label_layer['label_spec']['primary_horizon'] == 5
    assert [task['id'] for task in label_layer['label_spec']['tasks']] == [
        'return_5d',
        'return_10d',
        'excess_return_5d',
        'excess_return_10d',
        'hit_rate_5d',
        'downside_risk_5d',
        'event_alpha_1d',
    ]


def test_write_quant_pipeline_snapshot_writes_multitask_label_spec_file(tmp_path: Path) -> None:
    cfg = {
        'project': {'name': 'demo-workbench'},
        'qlib': {'universe': 'all', 'score_date': '2026-04-17', 'label_horizon': 5},
        'model': {'type': 'lightgbm'},
        'candidate_generation': {'mode': 'live_akshare'},
        'live_data': {
            'consolidation_breakout_universe': 'all_a',
            'consolidation_breakout_prefilter_mode': 'turnover_top_n',
            'consolidation_breakout_prefilter_top_n': 1200,
        },
    }
    out_dir = tmp_path / 'data' / 'outputs'

    paths = write_quant_pipeline_snapshot(cfg, output_dir=out_dir, root=tmp_path)

    label_spec_path = out_dir / 'multitask_label_spec.json'
    assert label_spec_path.exists()
    payload = json.loads(label_spec_path.read_text(encoding='utf-8'))
    assert payload['primary_horizon'] == 5
    assert any(task['id'] == 'excess_return_5d' for task in payload['tasks'])

    snapshot = json.loads(paths['json_path'].read_text(encoding='utf-8'))
    label_layer = next(layer for layer in snapshot['layers'] if layer['id'] == 'label')
    assert any(
        item['path'] == 'data/outputs/multitask_label_spec.json' and item['exists']
        for item in label_layer['artifact_status']['items']
    )
