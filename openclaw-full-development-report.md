# OpenClaw 全量开发规划报告

## 1. 文档角色

本文件统一吸收此前分散在以下 3 份顶层文档中的重复内容：

- 当前完成度与现状判断
- 分阶段交付顺序
- 生产化推进路径与准入条件

当前顶层文档分工收敛为：

1. `openclaw-first-refactor-blueprint.md`
   - 唯一架构基线
   - 回答“系统最终应该长成什么样”
2. `openclaw-full-development-report.md`
   - 唯一全量状态、差距审计与开发路线文档
   - 回答“现在做到哪里、离最终交付还差什么、下一步怎么做”
3. `openclaw-collaboration-status.md`
   - 轻量状态入口页
4. `openclaw-skill-phase-plan.md`
   - 轻量交付入口页
5. `openclaw-production-development-plan.md`
   - 轻量生产化入口页

也就是说，架构判断留在蓝图里，执行层判断统一收敛到本报告，不再让 3 份计划/状态文档重复叙述同一件事。

## 2. 当前系统状态

### 2.1 当前主架构

当前仓库已经明确转向 skill-first：

- `skills/` 承担业务能力
- `eco-concil-runtime/` 只保留最小运行时内核
- `eco-concil-runtime(abandoned)/` 仅作为 legacy 参考，不再作为活跃实现面

当前主链已经不再是 legacy runtime 的大一统 stage pipeline，而是：

`mission/input -> raw artifact -> normalize -> signal plane -> candidate/evidence -> board -> readiness/promotion -> reporting/decision/publication`

### 2.2 当前已交付能力面

当前仓库在本批次之后共有 41 个活跃 skill，覆盖如下能力面：

1. source-specific normalize
2. shared query / lookup
3. candidate extraction / audit
4. evidence bridge / scope / coverage
5. board working-state / summary / brief
6. investigation / readiness / promotion
7. reporting / decision / final publication
8. orchestration planning
9. mission scaffold / prepare-round / import execution ingress

### 2.3 当前最小 runtime 内核

当前活跃 runtime 只保留：

- manifest
- round cursor
- registry
- receipt / ledger
- contract-aware preflight / postflight baseline
- promotion gate
- phase-2 controller
- supervisor state

它已经能完成 planner-backed 的 phase-2 preview，但仍然不是 full board-driven、agent-decided orchestration runtime。

### 2.4 本批次新增的关键闭环

本批次补入了此前最缺的 ingress 面：

1. `eco-scaffold-mission-run`
   - 从 mission contract 写入 `mission.json`
   - 生成 round task 文件
   - 初始化 board 上的 hypothesis working state
2. `eco-prepare-round`
   - 从 mission + round tasks 生成最小 `fetch_plan_<round_id>.json`
   - 明确哪些本地 artifact import 会进入哪些 normalizer
3. `eco-import-fetch-execution`
   - 消费 fetch plan
   - 复制 raw artifact 到当前 run 的 raw 存储
   - 调起现有 normalizer skill 写入 signal plane

这意味着当前仓库不再只依赖测试里的 `seed_analysis_chain(...)` 直接向 signal plane 注数，而是具备了最小可审计的：

`mission -> prepare -> fetch-plan -> import execution -> normalize`

入口闭环。

## 3. 代码与最终交付目标的差距审计

### 3.1 已经补齐的核心主链

以下能力已经具备：

- raw artifact 进入 canonical signal plane
- canonical signal 进入 claim / observation candidates
- candidates 进入 evidence linking / scope / coverage
- evidence 进入 board working-state、summary、brief
- board 进入 next actions、probe、readiness、promotion
- promotion 进入 reporting handoff、expert report、council decision、final publication
- runtime 具备最小审计、registry、contract baseline、phase-2 planner-backed preview
- mission contract 现在可以通过 prepare / import execution 接回 normalize 主链

### 3.2 当前仍然缺失的能力

当前距离最终交付，仍然还缺 4 组能力。

#### A. 真实外部 orchestration 仍未补齐

当前 ingress 是“本地 artifact import 驱动的最小 contract 闭环”，而不是完整的远程抓取执行面。

还缺：

- 真正的 external fetcher skill
- source-family / layer governance 的活跃执行面
- 真实 fetch command 调度与失败恢复
- 非本地 fixture 的 mission-driven collection

#### B. archive / history context 仍未接回主链

还缺：

- case library 回接
- history context 注入 moderator / investigator 工作流
- richer simulation / benchmark surface
- 跨轮次对照与历史证据复用

#### C. runtime hardening 仍处于 baseline

虽然 runtime 已经有 contract-aware baseline，但还缺：

- full permission-aware / sandboxed execution
- 更完整的 side-effect allow/deny enforcement
- distributed-safe coordination
- partial rerun / retry / recovery 语义
- structured observability 与 production-facing failure surface

#### D. 生产化准入面仍未到位

还缺：

- shadow test playbook
- pilot runbook
- rollback / operator approval 流程
- 真实任务域验收标准

## 4. 对“最终交付”的更准确判断

当前系统已经不是“只有技能雏形”的状态，也不是“还停在 normalize demo”的状态。

更准确的表述是：

- 已经具备 skill-first 主链
- 已经具备从 normalize 一路走到 final publication 的正式产物链
- 已经具备最小 runtime kernel 与 phase-2 planner-backed preview
- 已经具备最小 ingress contract loop
- 但仍处于 pre-production integration 阶段

当前不能宣称的内容包括：

- full board-driven runtime 已完成
- 真正远程 fetch execution 已完成
- 生产级 sandbox / permission boundary 已完成
- 分布式或跨主机控制面已完成

## 5. 全量开发路线

### 5.1 已完成阶段

已完成的工作面可以按能力块理解，而不再按旧文档重复拆散：

1. N1/N2/N3 数据主链：normalize、query、lookup、candidate、evidence
2. C 层 board working-state：note、hypothesis、challenge、task、summary、brief
3. D 层 investigation / promotion：next actions、probe、readiness、promotion basis
4. E 层 reporting / decision：handoff、expert report、decision、final publication
5. F 层 control-plane baseline：board lock、registry metadata、ledger hardening、contract-aware execution baseline、planner-backed phase-2 preview
6. ingress 最小闭环：mission scaffold、prepare-round、fetch-plan、import execution

### 5.2 后续建议按 4 个工作流推进

#### 工作流 1：真实 external orchestration

目标：把当前“本地 artifact import”推进成“真实 mission-driven collection”。

交付顺序建议：

1. source-family / layer selection contract
2. real fetcher skill 对接
3. fetch execution snapshot / retry / overwrite guard
4. remote dependency failure handling

完成标志：真实任务不再依赖本地 fixture 文件，就能从 mission 进入 signal plane。

#### 工作流 2：archive / history context / simulation

目标：恢复跨轮次、跨案例和 richer benchmark 能力。

交付顺序建议：

1. archive query surface
2. history context assembly
3. benchmark / simulation presets
4. cross-round comparison regression

完成标志：至少两类真实任务能稳定复用历史上下文，并且 benchmark 有独立回归面。

#### 工作流 3：runtime hardening

目标：把当前“可运行”补成“可控”。

交付顺序建议：

1. stricter path / side-effect governance
2. sandbox / permission boundary
3. retry / partial rerun / recovery 语义
4. structured logs / observability
5. operator-facing failure summary

完成标志：失败可定位、可阻断越界 side effect、可安全重跑。

#### 工作流 4：shadow test 与 pilot

目标：把系统从集成态推进到受控发布态。

交付顺序建议：

1. shadow test runbook
2. approval / rollback 机制
3. task-domain 限定与验收标准
4. pilot retrospective 模板

完成标志：至少两轮 shadow test 和一轮 pilot 可被人工审阅并回放。

## 6. 推荐的下一批编码重点

在本批次完成 ingress 最小闭环后，下一批不应再继续把业务判断塞回 runtime，也不应继续只优化 phase-2 controller 内部排队。

更合适的优先级是：

1. 把当前 local artifact import 扩成真实 external fetch execution
2. 接回 archive / history context
3. 继续做 runtime hardening，尤其是 permission boundary 与 recovery

## 7. 文档收敛后的使用方式

如果要判断：

- 架构是否正确：看 `openclaw-first-refactor-blueprint.md`
- 当前做到哪里、还差什么、整体路线是什么：看 `openclaw-full-development-report.md`
- 只想快速进入状态页：看 `openclaw-collaboration-status.md`
- 只想快速进入交付页：看 `openclaw-skill-phase-plan.md`
- 只想快速进入生产化页：看 `openclaw-production-development-plan.md`

这样顶层文档不再互相重写，而是形成“一个蓝图 + 一个总报告 + 三个轻入口页”的结构。