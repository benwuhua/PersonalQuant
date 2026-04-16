# PersonalQuant

个人 A 股投研操作台 1.0。

这个项目的目标不是做一个花哨的大而全系统，而是先把一个能稳定日用的投研后台跑通，当前聚焦 4 个稳定输出：

1. 每周候选池 Top 30
2. 候选股最近 7 天公告事件卡片
3. 每日 / 每周观察清单
4. 本地可视化前端仪表盘

## 当前能力

- 用 Qlib 历史样本训练量价 baseline
- 用 AkShare 拉取当前沪深 300 成分股最近行情并实时打分
- 选出当前 Top 30 候选池
- 通过东方财富公告接口抓最近 7 天公告
- 生成事件卡片、priority_score、risk_attention_score
- 生成 daily / weekly / risk watchlist
- 提供本地静态 dashboard 浏览结果
- 对输出结果按批次归档，便于后续复盘与时间线分析

## 当前后端设计

### 量价层
- 模型：LightGBMRegressor
- 样本：Qlib `cn_data_simple`
- 候选生成：默认 `live_akshare`
- 当前 baseline 已升级为 v1.1 特征增强版，包含：
  - 1/5/10/20 日动量
  - 5/10/20 日均线结构及偏离率
  - 5/10/20 日量能结构及偏离率
  - 5/20 日波动率
  - intraday / overnight 因子
  - 20 日价格位置
  - 20 日成交量 z-score

### 事件层
- 默认对候选池抓最近 7 天公告
- 优先尝试 PDF 正文摘录，失败自动回退 `title_only`
- 已加入正文质量评分与低质量回退，避免乱码/空正文污染排序
- 事件层会生成：
  - `card_score`
  - `risk_card_score`
  - `event_score`
  - `priority_score`
  - `risk_attention_score`

### 排序层
- `priority_score = 0.55 * quant_score_norm + 0.45 * event_score`
- `risk_attention_score = 0.80 * risk_event_score + 0.20 * quant_score_norm`
- 同时维护：
  - 多头优先池
  - 风险观察池

## 项目结构

```text
config/                  配置文件
src/ashare_platform/     后端核心模块
scripts/                 可直接运行的脚本
frontend/                本地静态前端
notebooks/               研究预留目录
docs/                    方案、使用说明、扩展说明
data/
  announcements/         公告样本/输入
  factors/               预留因子目录
  outputs/               当前批次输出（默认忽略）
  processed/             处理中间产物（默认忽略）
  archives/              历史归档（默认忽略）
logs/                    运行日志（默认忽略）
```

## 环境准备

推荐运行环境：

- macOS Apple Silicon
- Python 3.11
- 已验证可用的 Qlib 虚拟环境：`~/.venvs/qlib`

最快启动方式：

```bash
source ~/.venvs/qlib-activate.sh
python -V
python -c "import qlib, akshare, lightgbm, pandas; print('env ok')"
```

如果你需要自行安装依赖：

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip setuptools wheel
pip install -r requirements.txt
```

更完整的环境说明见：`docs/environment_setup.md`

## 快速开始

```bash
cd /Users/ryan/.hermes/hermes-agent/projects/a_share_research_platform
source ~/.venvs/qlib-activate.sh
python scripts/run_weekly_pipeline.py
python scripts/serve_dashboard.py
```

浏览器打开：

```text
http://127.0.0.1:8765
```

## 常用脚本

### 1) 运行完整周度流水线

```bash
python scripts/run_weekly_pipeline.py
```

默认会完成：
- Qlib 训练与打分
- Top30 候选池生成
- 公告抓取与正文抽取
- 事件卡片生成
- priority / risk 候选池生成
- watchlist 输出
- dashboard 数据快照生成
- 当前批次归档

### 2) 单独重建 dashboard 数据

```bash
python scripts/build_dashboard_data.py
```

### 3) 启动本地 dashboard

```bash
python scripts/serve_dashboard.py
```

## 主要输出文件

运行后默认输出到 `data/outputs/`：

- `top30_candidates.csv`
- `priority_candidates.csv`
- `risk_candidates.csv`
- `announcements_raw.json`
- `event_cards.json`
- `dashboard_data.json`
- `daily_watchlist.md`
- `weekly_watchlist.md`
- `risk_watchlist.md`

处理中间产物：

- `data/processed/feature_importance.csv`

历史批次归档：

- `data/archives/run_YYYYMMDD_HHMMSS/`
- `data/archives/latest.json`

## 前端说明

当前前端是纯静态页面，不依赖 React / Vue，直接读取 `data/outputs/dashboard_data.json`。

支持：
- 多头优先池浏览
- 风险观察池浏览
- 事件卡片浏览
- 股票详情聚合
- 搜索与基础筛选
- recent_archives 数据展示入口

## 当前限制

- 默认 `llm.provider=mock`，事件摘要和分类仍以规则版为主
- 东方财富 PDF 正文抽取偶尔会出现空正文或低质量文本，当前已做安全回退
- 当前重点是“量价初筛 + 公告事件再排序”，不是高频策略或自动交易系统

## 相关文件

- 配置：`config/config.yaml`
- 周度主流程：`scripts/run_weekly_pipeline.py`
- 公告处理：`src/ashare_platform/announcements.py`
- 事件摘要：`src/ashare_platform/summarizer.py`
- 排序逻辑：`src/ashare_platform/priority.py`
- 仪表盘数据：`src/ashare_platform/dashboard.py`

## 后续方向

当前更有价值的增强方向：

1. 公告正文多源兜底
2. 单票事件时间线与历史批次对比
3. 事件分类器继续提纯
4. 横截面排序评估（如 rank IC / 分组回测）
