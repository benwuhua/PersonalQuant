# Cron workflow

这个项目现在可以用一条固定脚本跑完整后端流程。

## What the cron run does

`bash scripts/run_cron_workflow.sh`

会依次完成：

1. 激活 `~/.venvs/qlib-activate.sh`
2. 执行 `python scripts/dev.py run`
3. 生成 / 更新：
   - Top30 候选池
   - priority / risk 候选池
   - event cards
   - daily / weekly / risk watchlists
   - forward validation 快照与报告
   - historical backtest 评估摘要
   - archive diff
   - top priority 时间线
   - dashboard_data.json
4. 把日志写到 `logs/cron/run_YYYYMMDD_HHMMSS.log`

## Manual run

```bash
cd /Users/ryan/.hermes/hermes-agent/projects/a_share_research_platform
bash scripts/run_cron_workflow.sh
```

如果要使用自定义配置：

```bash
cd /Users/ryan/.hermes/hermes-agent/projects/a_share_research_platform
PERSONALQUANT_CONFIG=config/config.local.yaml bash scripts/run_cron_workflow.sh
```

## Suggested schedule

默认建议用工作日早上跑一遍：

```cron
35 8 * * 1-5
```

这样你在开盘前后就能直接看：
- priority 多头优先池
- risk 风险观察池
- archive diff
- 前向验证和历史评估摘要

## Frontend relationship

前端本身不需要定时构建。

因为 `scripts/run_cron_workflow.sh` 最后已经会重建 `data/outputs/dashboard_data.json`，所以只要本地静态服务还开着，前端刷新页面就会读到最新结果。
