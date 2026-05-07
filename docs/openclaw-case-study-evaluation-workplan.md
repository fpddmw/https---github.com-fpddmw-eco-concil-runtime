# OpenClaw 真实案例议会能力评测工作计划

## 1. 文档定位

本文是独立工作计划，不作为全局总计划，也不要求其他计划同时执行。

本文取代“先固定展示流程”的思路。跨区域烟霾 / PM2.5 议题只作为真实案例评测场景，用来测试 OpenClaw 的议会能力、状态治理、证据不足处理和报告约束。只有在该案例真实运行过、暴露的问题被修复、关键轨迹可复核之后，才允许把其中一条成功轨迹沉淀为可回放展示材料。

核心原则：

`先评测议会能力，再修复系统问题，最后沉淀可回放轨迹。`

## 2. 评测目标

本计划评测 OpenClaw 是否能在真实复杂议题中完成：

1. 多源证据进入 DB-backed signal plane。
2. investigator 基于 item-level evidence refs 提交 finding 和 evidence bundle。
3. challenger 识别本地源、时间窗、站点代表性、气象背景不足和过度归因风险。
4. readiness opinion 在证据不足时阻止直接 freeze/report。
5. follow-up round 从 open challenges、board tasks、evidence gaps 中自然生成。
6. relation helper 输出只作为候选线索，不能直接成为报告结论。
7. report basis freeze 只消费被 council/reporting object 承接过的依据。

## 3. 非目标

本计划不做：

1. 预设固定调查流程或固定结论。
2. 把 PM2.5 / FIRMS 场景写成专用 skill 或专用捷径。
3. 为展示效果绕过 runtime approval、role policy、receipt、ledger 或 DB 恢复边界。
4. 在未真实运行前编写“成功展示”脚本。
5. HYSPLIT、WRF-Chem、Gaussian plume、化学传输模型、污染源解析、健康风险或合规裁决。

## 4. 输入原则

案例输入应区分三类材料：

1. `raw case inputs`
   - 真实或半真实 public、formal、environment、fire、weather 输入。
   - 用于探索运行，不承诺每次都能完整成功。
2. `evaluation fixtures`
   - 从真实运行中抽取的最小可复核输入。
   - 用于重放失败、验证修复和保护回归。
3. `replay artifacts`
   - 只有在真实运行通过后，才从一条成功轨迹中抽取。
   - 用于论文展示或 smoke test，不作为默认调查流程。

## 5. 工作阶段

### P0：案例问题定义

1. 选择议题、区域、时间窗和调查问题。
2. 写出 explicit verification scope。
3. 明确禁止表述，包括传播证明、污染源确认、排除本地源。

验收：

1. investigation question 不预设结论。
2. verification scope 包含 receptor、candidate source、study period、lag window、spatial rule 和 excluded inferences。

### P1：探索运行

1. 使用真实或半真实输入初始化 run/round。
2. 通过 runtime-governed path 执行 fetch/import、normalize、query 和 council writes。
3. 记录系统卡点、错误、缺失对象、过度输出和人工介入点。

验收：

1. 运行日志能定位 receipt、ledger、approval refs。
2. 每个失败点都有 DB/artifact 状态说明。
3. 不因为展示需要手工改写 council outcome。

### P2：问题归类

将探索运行暴露的问题归入独立修复面：

1. DB-only recovery 问题。
2. runtime-governed execution 问题。
3. schema/migration 问题。
4. module boundary 或维护性问题。
5. optional-analysis overclaim 或 report-basis 绕路问题。
6. 缺少 CI/targeted regression 的问题。

验收：

1. 每个问题都有所属工作计划和最小复现。
2. 不为单个 PM2.5 案例引入硬编码捷径。

### P3：修复后再运行

1. 在完成相关代码修复后重新运行同一案例。
2. 验证 challenger、readiness、follow-up round、relation packet、report basis 的行为是否自然出现。
3. 记录仍无法通过的残余风险。

验收：

1. 证据不足时系统能自然进入 needs-more-data 或 withheld。
2. 证据可承接时，report basis 只引用 DB-backed council/reporting object。
3. helper cue 不直接进入报告正文。

### P4：沉淀可回放轨迹

只有 P1-P3 完成后，才执行本阶段：

1. 从成功运行中抽取最小 replay fixture。
2. 编写 replay/smoke 命令。
3. 固定关键验收点，而不是固定调查结论。
4. 标注该 replay 仅用于回归和展示。

验收：

1. 一条命令可重放关键轨迹。
2. replay 失败能暴露 runtime、DB、relation、reporting 的真实回归。
3. 文档明确说明 replay 不是系统默认调查剧本。

## 6. 与当前基线的关系

本计划现在建立在已收口的 runtime/DB/schema/module 重构基线上。执行真实案例时应参考：

1. `docs/openclaw-project-overview.md`
   - 确认系统定位、DB-first 工作流和能力边界。
2. `docs/openclaw-refactor-overall-notes.md`
   - 确认工程护栏、helper 降权和论文展示口径。
3. `docs/openclaw-ci-quality-gates-workplan.md`
   - 使用现有 targeted gate 和 full gate 防止回归。
4. `docs/openclaw-skills-refactor-checklist-v2.md`
   - 确认 skill 分层、原子能力边界和 optional-analysis 降权原则。

## 7. 最终产物

本计划最终产物不是一个预先写死的展示剧本，而是：

1. 一份真实案例运行记录。
2. 一组从真实问题抽取的 regression fixtures。
3. 一批已归档的问题和对应修复。
4. 一条可回放的成功轨迹。
5. 一份明确边界的论文/展示说明。
