from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.ashare_platform import dashboard


def _write_sample_multitask_spec(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                'primary_horizon': 5,
                'return_horizons': [5, 10, 20],
                'hit_threshold': 0.03,
                'risk_threshold': -0.05,
                'event_windows': [1, 3, 5],
                'tasks': [
                    {'id': 'return_5d', 'group': 'return', 'label_key': 'future_ret_5', 'objective': '预测未来绝对收益率'},
                    {'id': 'return_10d', 'group': 'return', 'label_key': 'future_ret_10', 'objective': '预测未来绝对收益率'},
                    {'id': 'return_20d', 'group': 'return', 'label_key': 'future_ret_20', 'objective': '预测未来绝对收益率'},
                    {'id': 'excess_return_5d', 'group': 'excess_return', 'label_key': 'excess_ret_5', 'objective': '预测相对市场超额收益'},
                    {'id': 'excess_return_10d', 'group': 'excess_return', 'label_key': 'excess_ret_10', 'objective': '预测相对市场超额收益'},
                    {'id': 'excess_return_20d', 'group': 'excess_return', 'label_key': 'excess_ret_20', 'objective': '预测相对市场超额收益'},
                    {'id': 'hit_rate_5d', 'group': 'classification', 'label_key': 'hit_rate_5', 'objective': '判断未来窗口是否达到目标收益阈值'},
                    {'id': 'downside_risk_5d', 'group': 'risk', 'label_key': 'downside_risk_5', 'objective': '判断未来窗口是否触发风险阈值'},
                    {'id': 'event_alpha_1d', 'group': 'event', 'label_key': 'event_alpha_1', 'objective': '评估公告/事件驱动后的短窗超额表现'},
                    {'id': 'event_alpha_3d', 'group': 'event', 'label_key': 'event_alpha_3', 'objective': '评估公告/事件驱动后的短窗超额表现'},
                    {'id': 'event_alpha_5d', 'group': 'event', 'label_key': 'event_alpha_5', 'objective': '评估公告/事件驱动后的短窗超额表现'},
                ],
            },
            ensure_ascii=False,
        ),
        encoding='utf-8',
    )


def test_build_dashboard_payload_exposes_multitask_label_summary(monkeypatch, tmp_path: Path) -> None:
    outputs_dir = tmp_path / 'data' / 'outputs'
    archives_dir = tmp_path / 'data' / 'archives'
    timelines_dir = outputs_dir / 'timelines'
    outputs_dir.mkdir(parents=True)
    archives_dir.mkdir(parents=True)
    timelines_dir.mkdir(parents=True)

    _write_sample_multitask_spec(outputs_dir / 'multitask_label_spec.json')

    monkeypatch.setattr(dashboard, 'OUTPUTS_DIR', outputs_dir)
    monkeypatch.setattr(dashboard, 'ARCHIVES_DIR', archives_dir)
    monkeypatch.setattr(dashboard, 'TIMELINES_DIR', timelines_dir)

    payload = dashboard.build_dashboard_payload()

    assert payload['summary']['multitask_task_count'] == 11
    assert payload['summary']['label_primary_horizon'] == 5
    assert payload['summary']['label_return_horizons'] == '5 / 10 / 20'
    assert payload['summary']['label_event_windows'] == '1 / 3 / 5'
    assert payload['summary']['label_hit_threshold'] == 0.03
    assert payload['summary']['label_risk_threshold'] == -0.05
    assert payload['multitask_label_spec']['primary_horizon'] == 5


def test_build_dashboard_payload_groups_multitask_tasks_for_frontend_panel(monkeypatch, tmp_path: Path) -> None:
    outputs_dir = tmp_path / 'data' / 'outputs'
    archives_dir = tmp_path / 'data' / 'archives'
    timelines_dir = outputs_dir / 'timelines'
    outputs_dir.mkdir(parents=True)
    archives_dir.mkdir(parents=True)
    timelines_dir.mkdir(parents=True)
    _write_sample_multitask_spec(outputs_dir / 'multitask_label_spec.json')

    monkeypatch.setattr(dashboard, 'OUTPUTS_DIR', outputs_dir)
    monkeypatch.setattr(dashboard, 'ARCHIVES_DIR', archives_dir)
    monkeypatch.setattr(dashboard, 'TIMELINES_DIR', timelines_dir)

    payload = dashboard.build_dashboard_payload()

    overview = payload['multitask_label_overview']
    assert overview['task_count'] == 11
    assert overview['group_counts'] == {
        'return': 3,
        'excess_return': 3,
        'classification': 1,
        'risk': 1,
        'event': 3,
    }
    assert overview['task_ids_by_group']['event'] == ['event_alpha_1d', 'event_alpha_3d', 'event_alpha_5d']
    assert overview['task_rows'][0]['id'] == 'return_5d'
    assert overview['task_rows'][0]['label_key'] == 'future_ret_5'


def test_build_dashboard_payload_links_multitask_labels_to_training_and_validation(monkeypatch, tmp_path: Path) -> None:
    outputs_dir = tmp_path / 'data' / 'outputs'
    archives_dir = tmp_path / 'data' / 'archives'
    timelines_dir = outputs_dir / 'timelines'
    outputs_dir.mkdir(parents=True)
    archives_dir.mkdir(parents=True)
    timelines_dir.mkdir(parents=True)
    _write_sample_multitask_spec(outputs_dir / 'multitask_label_spec.json')
    (outputs_dir / 'model_validation_summary.json').write_text(
        json.dumps(
            {
                'generated_at': '2026-04-21T09:00:00',
                'fold_count': 4,
                'rank_ic_mean': 0.076636,
                'topk_avg_label_mean': 0.013935,
            },
            ensure_ascii=False,
        ),
        encoding='utf-8',
    )

    monkeypatch.setattr(dashboard, 'OUTPUTS_DIR', outputs_dir)
    monkeypatch.setattr(dashboard, 'ARCHIVES_DIR', archives_dir)
    monkeypatch.setattr(dashboard, 'TIMELINES_DIR', timelines_dir)

    payload = dashboard.build_dashboard_payload()

    linkage = payload['multitask_training_linkage']
    assert linkage['training_label_key'] == 'excess_ret_5_label'
    assert linkage['training_task_id'] == 'excess_return_5d'
    assert linkage['validation_metric_name'] == 'topk_avg_label_mean'
    assert linkage['validation_metric_label_key'] == 'excess_ret_5_label'
    assert linkage['fold_count'] == 4
    assert linkage['rank_ic_mean'] == 0.076636
    assert linkage['topk_avg_label_mean'] == 0.013935


def test_build_dashboard_payload_splits_multitask_tasks_by_pipeline_status(monkeypatch, tmp_path: Path) -> None:
    outputs_dir = tmp_path / 'data' / 'outputs'
    archives_dir = tmp_path / 'data' / 'archives'
    timelines_dir = outputs_dir / 'timelines'
    outputs_dir.mkdir(parents=True)
    archives_dir.mkdir(parents=True)
    timelines_dir.mkdir(parents=True)
    _write_sample_multitask_spec(outputs_dir / 'multitask_label_spec.json')
    (outputs_dir / 'model_validation_summary.json').write_text(
        json.dumps(
            {
                'generated_at': '2026-04-21T09:00:00',
                'fold_count': 4,
                'rank_ic_mean': 0.076636,
                'topk_avg_label_mean': 0.013935,
            },
            ensure_ascii=False,
        ),
        encoding='utf-8',
    )

    monkeypatch.setattr(dashboard, 'OUTPUTS_DIR', outputs_dir)
    monkeypatch.setattr(dashboard, 'ARCHIVES_DIR', archives_dir)
    monkeypatch.setattr(dashboard, 'TIMELINES_DIR', timelines_dir)

    payload = dashboard.build_dashboard_payload()

    status_view = payload['multitask_task_status']
    assert status_view['status_counts'] == {
        'training_primary': 1,
        'data_ready_not_training': 5,
        'auxiliary_diagnostic': 5,
    }
    task_status = {row['id']: row['pipeline_status'] for row in status_view['task_rows']}
    assert task_status['excess_return_5d'] == 'training_primary'
    assert task_status['return_10d'] == 'data_ready_not_training'
    assert task_status['excess_return_20d'] == 'data_ready_not_training'
    assert task_status['hit_rate_5d'] == 'auxiliary_diagnostic'
    assert task_status['event_alpha_3d'] == 'auxiliary_diagnostic'
