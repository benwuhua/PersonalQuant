个人 A 股投研操作台 1.0

目标输出：
1. 每周候选池 Top 30
2. 候选股最近 7 天公告事件卡片
3. 每日/每周观察清单
4. 本地可视化前端仪表盘

当前版本采用“先跑通、后增强”的思路：
- 用 Qlib 历史样本训练 baseline 模型
- 用 AkShare 拉取当前沪深300成分股最近行情并实时打分
- 选出当前 Top 30
- 通过东方财富公告接口抓最近 7 天公告
- 默认使用本地规则卡片生成；如果后续恢复可用 LLM 再切回真实模型
- 新增 final priority_score，把量价分、事件重要性、bias、confidence、公告新鲜度合成为最终观察优先级
- 新增 risk_attention_score，把负面事件单独拉成风险观察池
- 默认开启 PDF 正文摘录，把标题级公告尽量升级到正文级输入
- 新增本地 dashboard，直接浏览 priority / risk / 事件卡片 / 观察清单
- 生成 watchlist 供人工复核

当前量价 baseline 已升级为 v1.1 特征增强版：
- 短中期动量：1/5/10/20日收益率
- 均线结构：5/10/20日均价及偏离率
- 量能结构：5/10/20日均量及偏离率
- 波动特征：5日/20日波动率
- 日内与隔夜：intraday_ret, overnight_ret
- 区间位置：20日价格区间位置
- 量能异常：20日成交量 z-score

当前观察层已升级为 v1.5 priority + risk + body + dashboard 版：
- quant_score_norm：把量价分压到 0~1 区间，便于和事件层合成
- card_score：按单条公告计算事件分，包含 importance / bias / confidence / freshness / event_type / density
- event_score：对单只股票取前 3 张最重要事件卡，按 0.6/0.3/0.1 聚合
- priority_score：0.55 * quant_score_norm + 0.45 * event_score
- risk_card_score：只对负面 bias 或 风险事项事件生效
- risk_attention_score：0.80 * risk_event_score + 0.20 * quant_score_norm
- priority_rank：多头观察顺序
- risk_rank：风险复核顺序
- content_source：区分正文摘录(pdf_excerpt)还是标题回退(title_only)
- dashboard_data.json：前端统一读取的数据快照

目录结构：
- config/: 配置文件
- data/: 因子、公告、处理中间件、最终输出
- frontend/: 本地前端页面
- scripts/: 可直接运行的脚本
- src/ashare_platform/: 公共模块
- notebooks/: 预留给后续研究
- logs/: 运行日志
- docs/: 实施计划和后续扩展说明

快速开始：
1) source ~/.venvs/qlib-activate.sh
2) cd /Users/ryan/.hermes/hermes-agent/projects/a_share_research_platform
3) python scripts/run_weekly_pipeline.py
4) python scripts/serve_dashboard.py
5) 打开 http://127.0.0.1:8765

如果你只想单独重建前端数据：
- python scripts/build_dashboard_data.py

默认输出文件：
- data/outputs/top30_candidates.csv
- data/outputs/priority_candidates.csv
- data/outputs/risk_candidates.csv
- data/outputs/announcements_raw.json
- data/outputs/event_cards.json
- data/outputs/dashboard_data.json
- data/outputs/daily_watchlist.md
- data/outputs/weekly_watchlist.md
- data/outputs/risk_watchlist.md
- data/processed/feature_importance.csv

候选池说明：
- 默认 candidate_generation.mode=live_akshare
- 训练样本仍来自 Qlib cn_data_simple
- 打分对象已经切成“当前沪深300成分股最近行情”
- 如果要回退到历史样本打分，可把 mode 改成 historical_qlib
- 当前 score 本质上是模型对未来 5 日收益率的预测倾向值，rank 比绝对 score 更重要

前端 dashboard 说明：
- 纯本地静态前端，无需额外前端框架
- 默认读取 ../data/outputs/dashboard_data.json
- 支持四个视图：多头优先池 / 风险观察池 / 事件卡片 / 股票详情
- 支持搜索、事件类型筛选、观察清单切换
- 适合先快速浏览，再回到 csv/json 做深挖

正文抽取说明：
- 默认 announcements.download_pdf_text=true
- 会尝试抓取东方财富公告 PDF，并提取前若干页正文文本
- 当前默认截取上限 3000 字，优先服务事件识别而不是全文存档
- 如果 PDF 解析失败，会自动回退到标题+栏目名模式，不会中断整条流水线

priority_score 说明：
- 不是替代量价模型，而是把量价候选池做第二次排序
- 高 importance + 高 freshness + 正向 bias 的公告，会明显抬升优先级
- 风险事项默认仍会保留事件分，但由于 bias_map 里 negative=0，会压低最终优先级
- 通用低信号公告（董事会决议、H股公告、股东会通知等）会被额外降权，避免噪声顶到前排

risk_attention_score 说明：
- 不和多头观察池混排，而是单独形成风险复核清单
- 只有 negative bias 或 event_type=风险事项 的事件会进入风险评分
- 高 importance + 高 freshness + 明确负面 bias，会显著抬升风险关注度
- 用 quant_score_norm 做了轻量加权，表示“强势股上的负面事件也值得更快复核”

事件卡片说明：
- 当前默认 llm.provider=mock，原因是可用外部 LLM 额度/认证不可稳定依赖
- 代码里仍保留 openrouter 路径，后面有额度时可切回
- 如果要更强的文本质量，优先提升 PDF 正文提取长度与质量，而不是只改标题规则
- 另一个更适合的方案是：用 Hermes cron 读取本项目落盘后的公告与候选池，再复用 Hermes 自己的主模型/provider 生成高质量事件卡片
- 相关说明见：
  - docs/hermes_cron_event_cards_prompt.txt
  - docs/cron_usage.txt

注意：
- 目前不能直接把 GPT Codex 5.4 当作脚本内 provider 使用；当前环境没有可供项目脚本直接调用的 OpenAI/Codex API 凭据，而且此前 Codex refresh 返回 401。
- cron 方案能复用 Hermes 的 provider 配置，但如果 Hermes 自己的 openai-codex 认证仍失效，cron 也同样会失败。
- 现在系统仍然可以稳定跑通，只是事件卡片回到无成本规则版。
