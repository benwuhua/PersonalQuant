from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def _unique_ints(values: list[Any]) -> list[int]:
    seen: set[int] = set()
    result: list[int] = []
    for value in values:
        ivalue = int(value)
        if ivalue not in seen:
            seen.add(ivalue)
            result.append(ivalue)
    return result


def build_multitask_label_spec(cfg: dict[str, Any]) -> dict[str, Any]:
    qlib_cfg = cfg.get('qlib', {})
    qp_cfg = cfg.get('quant_pipeline', {})
    label_cfg = qp_cfg.get('label_tasks', {})
    primary_horizon = int(qlib_cfg.get('label_horizon', 5) or 5)
    return_horizons = _unique_ints(label_cfg.get('return_horizons') or [primary_horizon])
    if primary_horizon not in return_horizons:
        return_horizons.append(primary_horizon)
        return_horizons = sorted(return_horizons)
    event_windows = _unique_ints(label_cfg.get('event_windows') or [1, 3])
    hit_threshold = float(label_cfg.get('hit_threshold', 0.02))
    risk_threshold = float(label_cfg.get('risk_threshold', -0.04))

    tasks: list[dict[str, Any]] = []
    for horizon in return_horizons:
        tasks.append(
            {
                'id': f'return_{horizon}d',
                'group': 'return',
                'label_key': f'future_ret_{horizon}',
                'horizon': horizon,
                'objective': '预测未来绝对收益率',
            }
        )
    for horizon in return_horizons:
        tasks.append(
            {
                'id': f'excess_return_{horizon}d',
                'group': 'excess_return',
                'label_key': f'excess_ret_{horizon}',
                'baseline': 'cross_section_market_mean',
                'horizon': horizon,
                'objective': '预测相对市场超额收益',
            }
        )
    tasks.append(
        {
            'id': f'hit_rate_{primary_horizon}d',
            'group': 'classification',
            'label_key': f'hit_rate_{primary_horizon}',
            'horizon': primary_horizon,
            'threshold': hit_threshold,
            'objective': '判断未来窗口是否达到目标收益阈值',
        }
    )
    tasks.append(
        {
            'id': f'downside_risk_{primary_horizon}d',
            'group': 'risk',
            'label_key': f'downside_risk_{primary_horizon}',
            'horizon': primary_horizon,
            'threshold': risk_threshold,
            'objective': '判断未来窗口是否触发风险阈值',
        }
    )
    for window in event_windows:
        tasks.append(
            {
                'id': f'event_alpha_{window}d',
                'group': 'event',
                'label_key': f'event_alpha_{window}',
                'horizon': window,
                'objective': '评估公告/事件驱动后的短窗超额表现',
            }
        )
    return {
        'primary_horizon': primary_horizon,
        'return_horizons': return_horizons,
        'hit_threshold': hit_threshold,
        'risk_threshold': risk_threshold,
        'event_windows': event_windows,
        'tasks': tasks,
    }


PIPELINE_LAYERS: list[dict[str, Any]] = [
    {
        'id': 'data',
        'name': '数据层',
        'goal': '统一管理日线行情、扩展历史、交易日历与多源事件数据。',
        'existing_components': [
            'src/ashare_platform/qlib_pipeline.py',
            'scripts/build_training_history_extension.py',
            'src/ashare_platform/announcements.py',
        ],
        'artifacts': [
            'data/processed/akshare_recent_history.csv.gz',
            'data/outputs/announcements_raw.json',
        ],
        'next_steps': [
            '补充行业/市值/风格画像数据表',
            '把公告、新闻、资金流整理成统一事件表',
        ],
    },
    {
        'id': 'universe',
        'name': '股票池层',
        'goal': '区分训练池、实盘候选池、规则扫描池与特殊观察池。',
        'existing_components': [
            'config/config.yaml: qlib.universe, live_data.*',
            'src/ashare_platform/consolidation_breakout_scanner.py',
        ],
        'artifacts': [
            'data/outputs/consolidation_breakout_turnover_cache.json',
            'data/outputs/top30_candidates.csv',
        ],
        'next_steps': [
            '把模型 live 池从 CSI300 扩到可配置的全市场/行业子池',
            '增加行业、中小盘、主题池切换',
        ],
    },
    {
        'id': 'feature',
        'name': '特征工程层',
        'goal': '同时支持 Qlib 量价因子、结构特征、事件特征与横截面相对特征。',
        'existing_components': [
            'src/ashare_platform/qlib_pipeline.py:add_derived_features',
            'src/ashare_platform/consolidation_breakout_scanner.py',
        ],
        'artifacts': [
            'data/processed/feature_importance.csv',
        ],
        'next_steps': [
            '增加行业/风格中性化特征',
            '把结构形态特征抽成可复用 factor block',
        ],
    },
    {
        'id': 'label',
        'name': '标签层',
        'goal': '支持收益、超额收益、命中率、风险回撤与事件收益等多任务标签。',
        'existing_components': [
            'src/ashare_platform/qlib_pipeline.py: label_horizon / future_ret_5',
        ],
        'artifacts': [
            'data/outputs/model_validation_summary.json',
            'data/outputs/multitask_label_spec.json',
        ],
        'next_steps': [
            '扩展多 horizon 标签与分类标签',
            '引入 event-driven label 与风险标签',
        ],
    },
    {
        'id': 'training',
        'name': '训练层',
        'goal': '管理 walk-forward 训练、实验配置、特征重要性与模型版本。',
        'existing_components': [
            'scripts/run_model_training_only.py',
            'src/ashare_platform/qlib_pipeline.py: build_training_artifacts',
        ],
        'artifacts': [
            'data/outputs/model_validation_summary.json',
            'data/processed/feature_importance.csv',
        ],
        'next_steps': [
            '增加实验 registry 与 model catalog',
            '增加训练配置快照与版本对比',
        ],
    },
    {
        'id': 'evaluation',
        'name': '评估层',
        'goal': '统一历史回测、walk-forward、前向验证、分层统计与归因。',
        'existing_components': [
            'src/ashare_platform/evaluation.py',
            'src/ashare_platform/validation.py',
            'src/ashare_platform/archive_diff.py',
            'src/ashare_platform/timeline.py',
        ],
        'artifacts': [
            'data/outputs/backtest_summary.json',
            'data/outputs/strategy_validation_summary.json',
            'data/outputs/archive_diff.json',
        ],
        'next_steps': [
            '增加按行业/市值/形态族的分组评估',
            '补充收益来源归因和失败样本归因',
        ],
    },
    {
        'id': 'signal',
        'name': '信号层',
        'goal': '把模型、盘整突破形态、事件卡片与风控信号统一到候选信号总线。',
        'existing_components': [
            'src/ashare_platform/fusion.py',
            'src/ashare_platform/priority.py',
            'src/ashare_platform/consolidation_breakout_scanner.py',
        ],
        'artifacts': [
            'data/outputs/model_consolidation_breakout_fusion_candidates.csv',
            'data/outputs/priority_candidates.csv',
            'data/outputs/risk_candidates.csv',
        ],
        'next_steps': [
            '加入 explainability: 各信号分量贡献',
            '接入事件 alpha 与风险拦截信号',
        ],
    },
    {
        'id': 'portfolio',
        'name': '组合构建层',
        'goal': '从候选信号生成可执行持仓建议、仓位与调仓列表。',
        'existing_components': [
            'src/ashare_platform/watchlist.py',
        ],
        'artifacts': [
            'data/outputs/daily_watchlist.md',
            'data/outputs/weekly_watchlist.md',
            'data/outputs/risk_watchlist.md',
        ],
        'next_steps': [
            '增加 position sizing 与持仓约束',
            '增加行业暴露与单票风险上限',
        ],
    },
    {
        'id': 'execution',
        'name': '执行/运营层',
        'goal': '把批处理、缓存刷新、归档、dashboard、cron 调度串成稳定运营流水线。',
        'existing_components': [
            'scripts/run_weekly_pipeline.py',
            'scripts/dev.py',
            'scripts/serve_dashboard.py',
        ],
        'artifacts': [
            'data/outputs/dashboard_data.json',
            'data/archives/',
        ],
        'next_steps': [
            '增加批次健康检查与失败告警',
            '增加阶段耗时与成功率监控',
        ],
    },
    {
        'id': 'research_os',
        'name': '投研工作台层',
        'goal': '把 dashboard、报告、timeline、diff、校准工具串成持续研究操作台。',
        'existing_components': [
            'frontend/',
            'src/ashare_platform/dashboard.py',
            'scripts/run_consolidation_breakout_calibration.py',
        ],
        'artifacts': [
            'data/outputs/dashboard_data.json',
            'data/outputs/consolidation-breakout-scanner_relax_report.md',
        ],
        'next_steps': [
            '把完整量化流水线状态接入 dashboard 顶层',
            '加入实验比较和策略阶段看板',
        ],
    },
]


def _artifact_status(root: Path, relative_paths: list[str]) -> dict[str, Any]:
    items = []
    ready_count = 0
    for rel in relative_paths:
        path = root / rel
        exists = path.exists()
        if exists:
            ready_count += 1
        items.append(
            {
                'path': rel,
                'exists': exists,
            }
        )
    return {
        'ready_count': ready_count,
        'total_count': len(relative_paths),
        'items': items,
    }


def build_quant_pipeline_snapshot(cfg: dict[str, Any], root: Path | None = None) -> dict[str, Any]:
    project_root = (root or Path(__file__).resolve().parents[2]).resolve()
    live_cfg = cfg.get('live_data', {})
    qlib_cfg = cfg.get('qlib', {})
    model_cfg = cfg.get('model', {})
    label_spec = build_multitask_label_spec(cfg)
    summary = {
        'project_name': cfg.get('project', {}).get('name', ''),
        'qlib_universe': qlib_cfg.get('universe', ''),
        'score_date': qlib_cfg.get('score_date', ''),
        'model_type': model_cfg.get('type', ''),
        'live_candidate_mode': cfg.get('candidate_generation', {}).get('mode', ''),
        'scanner_universe': live_cfg.get('consolidation_breakout_universe') or live_cfg.get('wangji_universe') or '',
        'scanner_prefilter_mode': live_cfg.get('consolidation_breakout_prefilter_mode') or live_cfg.get('wangji_prefilter_mode') or '',
        'scanner_prefilter_top_n': live_cfg.get('consolidation_breakout_prefilter_top_n') or live_cfg.get('wangji_prefilter_top_n') or 0,
        'label_task_count': len(label_spec.get('tasks', [])),
        'primary_label_horizon': label_spec.get('primary_horizon', 0),
    }
    layers = []
    for layer in PIPELINE_LAYERS:
        artifact_status = _artifact_status(project_root, layer['artifacts'])
        status = 'implemented' if artifact_status['ready_count'] == artifact_status['total_count'] else 'partial'
        layer_payload = {
            **layer,
            'status': status,
            'artifact_status': artifact_status,
        }
        if layer['id'] == 'label':
            layer_payload['label_spec'] = label_spec
        layers.append(layer_payload)
    return {
        'summary': summary,
        'layers': layers,
    }


def build_quant_pipeline_report(snapshot: dict[str, Any]) -> str:
    summary = snapshot.get('summary', {})
    lines = [
        '# Complete Quant Pipeline Blueprint',
        '',
        f"- project: {summary.get('project_name', 'n/a')}",
        f"- qlib_universe: {summary.get('qlib_universe', 'n/a')}",
        f"- model_type: {summary.get('model_type', 'n/a')}",
        f"- live_candidate_mode: {summary.get('live_candidate_mode', 'n/a')}",
        f"- scanner_universe: {summary.get('scanner_universe', 'n/a')}",
        f"- scanner_prefilter: {summary.get('scanner_prefilter_mode', 'n/a')} / top_n={summary.get('scanner_prefilter_top_n', 'n/a')}",
        f"- primary_label_horizon: {summary.get('primary_label_horizon', 'n/a')}",
        f"- label_task_count: {summary.get('label_task_count', 'n/a')}",
        '',
    ]
    for layer in snapshot.get('layers', []):
        lines.append(f"## {layer['name']} / {layer['status']}")
        lines.append(f"- goal: {layer['goal']}")
        lines.append('- existing_components:')
        for component in layer.get('existing_components', []):
            lines.append(f'  - {component}')
        lines.append('- artifacts:')
        for item in layer.get('artifact_status', {}).get('items', []):
            marker = 'ok' if item.get('exists') else 'missing'
            lines.append(f"  - [{marker}] {item.get('path', '')}")
        label_spec = layer.get('label_spec')
        if label_spec:
            lines.append('- label_tasks:')
            for task in label_spec.get('tasks', []):
                threshold = task.get('threshold')
                threshold_suffix = f", threshold={threshold}" if threshold is not None else ''
                lines.append(
                    f"  - {task.get('id', '')}: {task.get('objective', '')}"
                    f" (label_key={task.get('label_key', '')}, horizon={task.get('horizon', '')}{threshold_suffix})"
                )
        lines.append('- next_steps:')
        for step in layer.get('next_steps', []):
            lines.append(f'  - {step}')
        lines.append('')
    return '\n'.join(lines).rstrip() + '\n'


def write_quant_pipeline_snapshot(
    cfg: dict[str, Any],
    output_dir: Path | None = None,
    root: Path | None = None,
) -> dict[str, Path]:
    project_root = (root or Path(__file__).resolve().parents[2]).resolve()
    out_dir = (output_dir or (project_root / 'data' / 'outputs')).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    label_spec = build_multitask_label_spec(cfg)
    json_path = out_dir / 'quant_pipeline_snapshot.json'
    md_path = out_dir / 'quant_pipeline_blueprint.md'
    label_spec_path = out_dir / 'multitask_label_spec.json'
    label_spec_path.write_text(json.dumps(label_spec, ensure_ascii=False, indent=2), encoding='utf-8')
    snapshot = build_quant_pipeline_snapshot(cfg, root=project_root)
    json_path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding='utf-8')
    md_path.write_text(build_quant_pipeline_report(snapshot), encoding='utf-8')
    return {'json_path': json_path, 'md_path': md_path, 'label_spec_path': label_spec_path}
