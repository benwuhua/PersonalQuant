# Sample Outputs

这个页面用当前本地跑出来的一次真实样例，帮助读代码前先理解：PersonalQuant 到底会产出什么。

注意：
- 下面内容来自一次本地 pipeline 结果快照
- `data/outputs/` 与 `data/archives/` 默认不提交到 git
- 这里保留的是“样例摘录”，目的是让人快速理解产品形态

## 1) Priority Candidates（多头优先池）

系统会把量价分和公告事件分合成 `priority_score`，输出一个适合人工复核的优先观察列表。

样例：

```text
priority_rank  instrument  priority_score  quant_score_norm  event_score  top_event_type  top_event_title
1              SH601618    0.863430        0.953081          0.753857     经营进展         中国中冶2026年1-3月新签合同情况简报
2              SH601916    0.751777        1.000000          0.448393     其他             浙商银行第七届监事会第十七次会议决议公告
3              SH601818    0.578065        0.532913          0.633250     分红回购         光大银行控股股东增持结果公告
4              SH600111    0.489516        0.247990          0.784715     业绩             北方稀土2026年第一季度业绩预增公告
5              SH600875    0.440643        0.586887          0.261900     风险事项         东方电气高级管理人员减持股份计划公告
```

你可以把这张表理解成：
- quant 层先做候选初筛
- event 层做催化/风险再排序
- 最终给出“今天先看谁”的顺序

## 2) Risk Candidates（风险观察池）

负面事件不会和多头观察池混在一起，而是单独输出 `risk_attention_score`。

样例：

```text
risk_rank  instrument  risk_attention_score  risk_event_score  top_risk_event_type  top_risk_title
1          SH600875    0.501377              0.480000          风险事项              东方电气高级管理人员减持股份计划公告
2          SH601872    0.412549              0.497143          风险事项              招商轮船持股5%以上股东减持股份计划公告
```

这张表更适合回答：
- 哪些票需要优先做风险复核？
- 哪些负面公告值得在强势股里也快速跟踪？

## 3) Daily Watchlist（每日观察清单）

除了结构化表格，系统还会生成适合直接阅读的 daily watchlist。

样例：

```text
- P1 | SH601618: priority=0.8634 (quant=0.9531, event=0.7539) | 经营进展 | 公告栏目：月度经营情况、重大合同；公告标题：中国中冶:中国中冶2026年1-3月新签合同情况简报 | 动作: 人工复核公告原文并结合盘面判断。
- P2 | SH601916: priority=0.7518 (quant=1.0000, event=0.4484) | 其他 | 公告栏目：监事会决议公告；公告标题：浙商银行:浙商银行股份有限公司第七届监事会第十七次会议决议公告 | 动作: 人工复核公告原文并结合盘面判断。
- P3 | SH601818: priority=0.5781 (quant=0.5329, event=0.6332) | 分红回购 | 公告栏目：股东/实际控制人股份增持；公告标题：光大银行:中国光大银行股份有限公司关于控股股东增持股份计划实施完毕暨增持结果的公告 | 动作: 人工复核公告原文并结合盘面判断。
```

它更像一个“当天直接可执行”的复核清单，而不是纯数据表。

## 4) Weekly Watchlist（周度深挖清单）

weekly watchlist 会把单票相关事件展开成更像研究备忘录的格式。

样例：

```text
## Priority 1 - SH601618
- raw_rank: 2
- raw_score: 0.0037
- priority_score: 0.8634
- quant_score_norm: 0.9531
- event_score: 0.7539
- event_card_count: 3
- top_event: [medium] 经营进展 / positive / confidence=high: 中国中冶:中国中冶2026年1-3月新签合同情况简报
- source: akshare_live_csi300
```

这个产物适合：
- 周末回顾
- 单票催化梳理
- 人工决定是否进入长期观察池

## 5) Event Cards（公告事件卡片）

每条公告会被整理成结构化卡片，典型字段包括：

- `instrument`
- `title`
- `event_type`
- `importance`
- `bias`
- `confidence`
- `summary`
- `card_score`
- `risk_card_score`
- `content_source`
- `content_quality_score`

这使得后续可以继续扩展：
- 单票事件时间线
- 历史批次对比
- 更细的事件过滤与再排序

## 6) Dashboard Data（前端快照）

前端静态页面统一读取：

```text
data/outputs/dashboard_data.json
```

其中包含：
- summary
- priority_candidates
- risk_candidates
- top30_candidates
- event_cards
- instrument_details
- recent_archives
- daily_watchlist
- weekly_watchlist
- risk_watchlist

这意味着前端不需要额外后端服务，就能直接浏览最近一次研究结果。

## 7) 一句话理解整个链路

```text
量价初筛 -> Top30 候选池 -> 抓最近 7 天公告 -> 生成事件卡片 -> priority/risk 双榜 -> daily/weekly watchlist -> dashboard 浏览
```

如果你第一次看这个仓库，建议顺序：
1. 先看这个页面
2. 再看 `README.md`
3. 再跑 `scripts/run_weekly_pipeline.py`
4. 最后打开本地 dashboard
