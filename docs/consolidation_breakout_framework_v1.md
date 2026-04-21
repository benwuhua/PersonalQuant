# 盘整突破策略框架 v1

这份文档的目标，是把当前 `wangji-scanner` 从“单一模板扫描器”升级成一个可扩展的“盘整突破策略框架”。

一句话定义：
- 不是只识别一种图形；
- 而是识别“处于盘整蓄势、且存在突破潜力”的不同形态家族；
- 再用统一特征、统一评分、分策略标签的方式输出候选池。


## 0. 当前 wangji 的定位

当前 `wangji-scanner` 的主干，本质上是：
- 平台整理
- 放量点火/突破
- 突破后缩量回踩 / 受控消化

也就是说，当前它最接近下面的策略家族：
1. 平台整理 + 放量突破
2. 缩量回踩 + 再放量上攻（只做到了前半段“缩量回踩/受控消化”，还没有把“二次上攻确认”单独建模）

它对以下形态只有弱覆盖或尚未显式建模：
3. 20/60/120 日新高突破
4. 高换手横盘后的再突破
5. 三角收敛 / 箱体突破

所以升级方向不是继续给 `wangji-scanner` 塞更多 if-else，而是把它抽象成：
- 共用盘整特征层
- 多个突破子策略
- 统一评分层


## 1. 框架总目标

框架输出要回答四个问题：
1. 这只票当前是不是处于“有效盘整”状态？
2. 它属于哪类盘整突破形态？
3. 它现在处于哪一阶段：盘整中 / 初突破 / 回踩确认 / 二次进攻？
4. 它的突破潜力和形态质量大概有多高？

因此最终输出不应只保留：
- `pattern_passed = True/False`

而应该扩展到：
- `pattern_family`
- `pattern_stage`
- `consolidation_score`
- `breakout_quality_score`
- `followthrough_score`
- `final_score`
- `entry_readiness`
- `risk_flags`


## 2. 时间分层定义

建议把所有盘整突破类策略统一拆成 4 个阶段：

A. 背景阶段 background
- 前期有一波上涨或至少有可辨识的价格重心上移
- 不是纯下降趋势末端的弱反抽

B. 盘整阶段 consolidation
- 振幅收敛 / 波动压缩 / 结构横盘
- 可以是平台、箱体、三角、旗形、高换手横盘等不同变体

C. 突破阶段 breakout
- 价格突破关键边界
- 同时伴随量能、换手、收盘质量等确认

D. 延续阶段 followthrough
- 突破后回踩不坏
- 或回踩后再放量上攻
- 或维持高位换手后的再突破

这四个阶段不是所有子策略都必须完全一致，但字段和评分应尽量共用。


## 3. 形态家族定义

### Family A：平台整理 + 放量突破
最接近当前 wangji 主干。

核心语义：
- 经过一段较低波动的平台整理
- 平台上沿被放量长阳/强阳突破
- 突破后不快速失守

典型适用：
- 强势股一波整理后的继续攻击
- 中低位横盘后第一次放量上攻

关键特征：
- 平台宽度窄
- 平台内部大阴大阳少
- 突破日涨幅明显、收盘靠近高位、量能放大

### Family B：缩量回踩 + 再放量上攻
这是当前 `wangji` 最值得升级的方向。

核心语义：
- 已经发生过一次有效点火/突破
- 随后出现受控缩量回踩或受控消化
- 再次放量向上，二次攻击成立

关键特征：
- 一次突破后的回撤有限
- 回踩期间量能明显低于点火日
- 结构没有破坏
- 第二次上攻日再次放量、收盘质量高

### Family C：20/60/120 日新高突破
核心语义：
- 关键不只是“新高”，而是“新高前经历了压缩或蓄势”
- 防止把纯追涨或末端加速都当作好信号

关键特征：
- 距离近 20/60/120 日新高较近
- 突破发生前振幅收缩或横盘
- 突破当天价格和量能质量足够高

### Family D：高换手横盘后的再突破
核心语义：
- 强势股在高换手环境下完成筹码交换
- 不是出货衰竭，而是高位蓄势后继续突破

关键特征：
- 横盘阶段换手率或成交额维持高位
- 价格没有明显塌陷
- 放量后能高位承接
- 再突破当天量价同步

### Family E：三角收敛 / 箱体突破
核心语义：
- 价格边界收敛或箱体震荡
- 随后突破关键边界

关键特征：
- 高点下移、低点上移（三角）
- 或箱体上下沿清晰（箱体）
- 突破必须有量价确认

注意：
- 这类形态不建议只靠肉眼定义，必须落到可计算特征上。


## 4. 共用特征层

以下特征建议作为所有盘整突破子策略共用底座。

### 4.1 背景趋势特征
- `drawdown_from_60d_high`
- `drawdown_from_120d_high`
- `ret_20`
- `ret_60`
- `close_ma20_ratio`
- `close_ma60_ratio`
- `close_ma120_ratio`
- `close_ma20_slope`
- `close_ma60_slope`
- `close_ma120_slope`

目的：
- 判断是强势整理、低位蓄势、还是下降中继

### 4.2 盘整压缩特征
- `base_range_10`
- `base_range_20`
- `base_range_30`
- `close_stability_10`
- `close_stability_20`
- `max_daily_abs_ret_10`
- `max_daily_abs_ret_20`
- `volatility_5`
- `volatility_20`
- `volatility_regime_20`

目的：
- 判断是否处于“压缩/收敛/横盘”状态

### 4.3 量能/换手特征
- `volume_ma5_ratio`
- `volume_ma10_ratio`
- `volume_ma20_ratio`
- `turnover_rate`（如有）
- `turnover_ma5`
- `turnover_ma20`
- `up_volume_ratio_10`
- `down_volume_ratio_10`
- `high_turnover_consolidation_score`

目的：
- 区分健康蓄势与出货震荡

### 4.4 突破质量特征
- `breakout_ret_1d`
- `breakout_ret_2d`
- `breakout_volume_ratio_1d`
- `breakout_volume_ratio_2d`
- `breakout_close_location`
- `ignition_day_strength`
- `confirmation_ret`
- `confirmation_vol_ratio`

目的：
- 判断突破当天及其确认质量

### 4.5 突破后延续 / 消化特征
- `pullback_ret_3d_like`
- `negative_pullback_days_3`
- `controlled_down_days_3`
- `pullback_avg_vol_ratio_3`
- `max_washout_vol_spike_3`
- `washout_direction_bias_3`
- `structure_break_days_3`
- `latest_close_vs_breakout_close`

目的：
- 判断突破后是否受控，是否具备二次进攻潜力

### 4.6 边界结构特征（新增建议）
这些是下一步为了做箱体/三角显式识别，建议新增的特征：
- `box_upper_20`：最近 20 日上沿
- `box_lower_20`：最近 20 日下沿
- `box_width_20`
- `box_touch_count_upper_20`
- `box_touch_count_lower_20`
- `triangle_upper_slope_20`
- `triangle_lower_slope_20`
- `range_compression_ratio_10_20`
- `pivot_high_density_20`
- `pivot_low_density_20`


## 5. 各家族最小可执行规则

先做最小可运行版本，不一开始就搞满配。

### A. 平台整理 + 放量突破
最小版本：
- 近 10-20 日收盘区间窄
- 整理期单日大波动少
- 突破日涨幅 >= 阈值
- 突破日量能 >= 前 5/10 日均量阈值
- 收盘突破平台上沿
- 突破日收盘位置靠近高位

### B. 缩量回踩 + 再放量上攻
最小版本：
- 先存在有效突破日
- 回踩 2-5 天总回撤受控
- 回踩期平均量能 < 突破日量能
- 回踩期结构未破坏
- 二次上攻日再次放量突破短期局部高点

### C. 多周期新高突破
最小版本：
- 当前 close 接近 20/60/120 日最高价
- 突破前 5-10 天波动压缩
- 突破当天价格和量能质量达标

### D. 高换手横盘后再突破
最小版本：
- 横盘阶段换手率高于基准
- 横盘区间宽度受控
- 没有连续结构性下破
- 再突破日量价共振

### E. 三角/箱体突破
最小版本：
- 上边界与下边界可稳定估计
- 区间宽度/斜率满足收敛或箱体定义
- 突破日收盘越过关键边界并放量


## 6. 统一评分建议

不要把所有分支都强行做成同一个 pass/fail 模板。
推荐统一评分、分家族阈值。

### 6.1 共用分数
- `background_score`：背景趋势质量
- `consolidation_score`：盘整质量
- `breakout_quality_score`：突破质量
- `followthrough_score`：突破后承接/延续质量
- `liquidity_score`：流动性与换手质量

### 6.2 总分
建议：
- `final_score = 0.20 * background_score + 0.30 * consolidation_score + 0.30 * breakout_quality_score + 0.20 * followthrough_score`

如果有高换手策略分支，可加：
- `final_score = final_score * 0.9 + 0.1 * liquidity_score`

### 6.3 分阶段输出
- `pattern_stage = setup | breakout | digestion | second_leg`

这样好处是：
- 没到二次上攻也能先进入观察池
- 不必等所有条件都满足才有信号


## 7. 输出字段设计

最终候选表建议至少输出：
- `instrument`
- `signal_date`
- `pattern_family`
- `pattern_stage`
- `pattern_passed`
- `recent_ignition_prepass`
- `background_score`
- `consolidation_score`
- `breakout_quality_score`
- `followthrough_score`
- `final_score`
- `scanner_rank`
- `breakout_date`
- `breakout_ret`
- `breakout_volume_ratio`
- `pullback_ret_3d`
- `pullback_avg_vol_ratio`
- `structure_break_days`
- `risk_flags`
- `fail_reasons`

对于前端展示，建议最少支持：
- 按 `pattern_family` 切换
- 按 `pattern_stage` 切换
- 按 `pass / prepass / observe` 切换


## 8. 与现有 wangji 的兼容关系

建议兼容方式如下：
- 保留 `wangji-scanner` 这个名字，作为框架主入口
- 当前逻辑迁移成：`family = platform_breakout`
- 当前 `recent_ignition_prepass` 保留，作为 `stage = breakout/digestion` 的观察分支
- 后续新家族逐步接入：
  - `second_leg_breakout`
  - `new_high_breakout`
  - `high_turnover_rebreakout`
  - `triangle_box_breakout`

这样不会推翻现在已经可跑的代码，也不会一下子大改所有前端和输出结构。


## 9. 推荐实施顺序

### Phase 1
先把现有 wangji 抽象成框架字段，不改大逻辑：
- 新增 `pattern_family`
- 新增 `pattern_stage`
- 新增 `background_score / consolidation_score / breakout_quality_score / followthrough_score`
- 让现有逻辑对应 `platform_breakout`

### Phase 2
补 Family B：缩量回踩 + 再放量上攻
这是最接近当前代码、性价比最高的升级

### Phase 3
补 Family C：20/60/120 日新高突破
这个跟现有特征体系兼容度很高，改造成本也不高

### Phase 4
补 Family D：高换手横盘后的再突破
需要换手率/成交额语义更清晰

### Phase 5
补 Family E：三角/箱体突破
这一类最需要新增边界几何特征，放最后做最稳


## 10. 核心判断

如果只保留一句话，这个 v1 框架的核心思想是：
- `wangji` 不再是“唯一形态模板”；
- 它应该升级为“盘整突破候选识别框架”；
- 当前模板只是其中一个子策略：`platform_breakout`。
