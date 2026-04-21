# Consolidation Breakout Framework v1 Implementation Plan

> For Hermes: implement this plan incrementally; keep current wangji runnable at every step.

目标：
- 把当前 `wangji-scanner` 从单一模板，升级为“盘整突破策略框架”的 v1
- 第一阶段不推翻现有逻辑，只做抽象、字段补齐、兼容输出

技术策略：
- 复用现有 `consolidation_breakout_scanner.py` 的 live panel / breakout / digestion 逻辑
- 先把当前模板映射为 `platform_breakout`
- 再逐步增加新的 family，而不是一次性重写

---

## Task 1：补框架字段，但不改当前 pass 逻辑

目标：
- 先让现有 `wangji` 拥有“框架字段”，但保持当前结果基本可复现

文件：
- Modify: `src/ashare_platform/consolidation_breakout_scanner.py`
- Test: `tests/test_consolidation_breakout_scanner_calibration.py`

步骤：
1. 在单票评估输出里新增字段：
   - `pattern_family='platform_breakout'`
   - `pattern_stage`
   - `background_score`
   - `consolidation_score`
   - `breakout_quality_score`
   - `followthrough_score`
   - `final_score`
2. `pattern_stage` 最小映射：
   - 未突破但进入观察：`setup`
   - 刚突破且样本不足：`breakout`
   - 突破后受控消化：`digestion`
   - 正式通过且结构延续：`second_leg`
3. 保留原有：
   - `pattern_passed`
   - `recent_ignition_prepass`
   - `shape_score`
4. 新旧字段并存，避免前端立即断裂
5. 跑现有测试，确保当前 exemplar 行为不被破坏

验收：
- 输出 CSV 新增这些字段
- 原有 pass/prepass 逻辑不大幅漂移

---

## Task 2：把 shape_score 拆成可解释子分数

目标：
- 让当前打分从黑箱单分，变成可解释的分项分数

文件：
- Modify: `src/ashare_platform/consolidation_breakout_scanner.py`
- Test: `tests/test_consolidation_breakout_scanner_calibration.py`

步骤：
1. 从现有规则和数值里构造：
   - `background_score`
   - `consolidation_score`
   - `breakout_quality_score`
   - `followthrough_score`
2. 先允许是 heuristic 加权，不要求一次做到最优
3. 用这 4 个子分数合成：
   - `final_score`
4. `shape_score` 暂时保留为兼容字段，可令其等于 `final_score` 或近似映射

验收：
- 每只票至少能看到 4 个分项分数
- 用户能理解为什么这票高分/低分

---

## Task 3：正式引入 family 注册机制

目标：
- 不再把所有逻辑糊在一个函数里

文件：
- Modify: `src/ashare_platform/consolidation_breakout_scanner.py`

步骤：
1. 增加 family 注册结构，例如：
   - `FAMILY_CONFIGS`
   - `evaluate_platform_breakout(...)`
2. 当前主逻辑迁移到 `platform_breakout`
3. 主入口统一返回：
   - `pattern_family`
   - `family_passed`
   - `family_rank_score`
4. 外部总入口仍叫 `wangji-scanner`

验收：
- 当前 family 只有一个，但结构上已经支持多个 family

---

## Task 4：新增 Family B：second_leg_breakout

目标：
- 落地“缩量回踩 + 再放量上攻”

文件：
- Modify: `src/ashare_platform/consolidation_breakout_scanner.py`
- Add/Modify tests: `tests/test_consolidation_breakout_scanner_calibration.py`

步骤：
1. 定义最小规则：
   - 存在首次突破/点火
   - 回踩 2-5 天
   - 回踩期平均量能低于点火日
   - 结构未破坏
   - 二次上攻日放量并突破回踩局部高点
2. 先输出：
   - `second_leg_breakout_triggered`
   - `second_leg_breakout_date`
3. 与 `platform_breakout` 并行输出
4. 不急着并入同一个 pass 阈值，先观察结果

验收：
- 能筛出一批“二次进攻型”候选
- 不破坏当前 platform_breakout 逻辑

---

## Task 5：新增 Family C：new_high_breakout

目标：
- 落地 20/60/120 日新高突破策略

文件：
- Modify: `src/ashare_platform/consolidation_breakout_scanner.py`

步骤：
1. 增加新高类特征：
   - `dist_to_high_20`
   - `dist_to_high_60`
   - `dist_to_high_120`
   - `is_new_high_20`
   - `is_new_high_60`
   - `is_new_high_120`
2. 增加“新高前压缩”条件
3. 输出：
   - `new_high_breakout_triggered`
   - `new_high_window`

验收：
- 可以和 platform_breakout 区分开
- 不把纯末端追高全塞进来

---

## Task 6：前端最小支持 family / stage 切换

目标：
- 让框架升级不是只停在后端 CSV

文件：
- Modify: dashboard payload builder
- Modify: frontend dashboard files

步骤：
1. 增加 `pattern_family` 和 `pattern_stage` 到 payload
2. 在 wangji 面板增加：
   - family 筛选
   - stage 筛选
3. 表格中展示 4 个子分数的简版列

验收：
- 前端能按 family 看票
- 用户能看到当前票处于 setup/breakout/digestion/second_leg 哪个阶段

---

## Task 7：补一版 explainability 输出

目标：
- 让用户不只看到结论，还看到“为什么”

文件：
- Modify: `src/ashare_platform/consolidation_breakout_scanner.py`
- Modify: dashboard payload builder

步骤：
1. 为每只票增加：
   - `top_positive_factors`
   - `top_negative_factors`
   - `family_fail_reasons`
2. 前端支持展开查看

验收：
- 用户能快速知道：
   - 是哪一段分数高
   - 卡在哪个阶段/哪条规则

---

## 建议实施顺序总结

先做：
1. 框架字段
2. 子分数
3. family 注册机制

再做：
4. second_leg_breakout
5. new_high_breakout

最后再考虑：
6. 高换手横盘后再突破
7. 三角 / 箱体突破

原因：
- 前 5 步和当前 wangji 兼容性最高
- 后两类需要更多新增几何/换手特征，复杂度更高
