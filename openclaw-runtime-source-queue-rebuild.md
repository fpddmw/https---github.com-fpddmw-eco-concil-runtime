# Runtime Source-Queue 重建设计

## 1. 目标

把 runtime source-queue 重建成第二编排面，而不是继续维护一个 import-only 过渡层。

这条流程必须满足三条原则：

1. 共享同一层 skills。
2. 受控、可回放、可审计。
3. 不重新把业务推理塞回 runtime。

## 2. 当前 active queue 的问题

- prepare-round 只会从 mission.artifact_imports 生成最小 import plan。
- import-fetch-execution 只会复制本地文件并调用 normalizer。
- queue 里没有 active source selection、selected_sources、family_plans、layer_plans、source_decisions、effective_constraints。
- detached fetch skills 还没有 live execution 接线。

## 3. 需要从 abandoned runtime 选择性回收的内容

### 3.1 输入快照与变更检测

回收目标：prepare 后冻结输入，execution 前校验。

来源：

1. eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration_prepare.py
2. eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/fetch_plan_builder.py

活跃版应保留：

- mission / tasks / source selections 的 snapshot
- prepare 到 execute 之间的 drift 检测

### 3.2 source-selection 治理对象

回收目标：selected_sources 不再只是平铺列表，而是完整治理对象。

来源：

1. eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/governance.py
2. eco-concil-runtime(abandoned)/src/eco_council_runtime/controller/policy.py
3. eco-concil-runtime(abandoned)/src/eco_council_runtime/contract.py

活跃版应保留字段：

- status
- selected_sources
- family_plans
- layer_plans
- source_decisions
- override_requests
- allowed_sources
- evidence_requirements

### 3.3 fetch-plan 组装骨架

回收目标：fetch plan 从“本地 import 列表”升级为“角色选择后的受控 step graph”。

来源：

1. eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/fetch_plan_builder.py
2. eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/step_synthesis.py

活跃版应保留：

- policy_profile
- effective_constraints
- role summaries
- step depends_on
- artifact capture mode
- source-specific execution args

### 3.4 policy 与约束摘要

回收目标：queue 在 prepare 阶段就明确约束，不等到执行时才补救。

来源：

1. eco-concil-runtime(abandoned)/src/eco_council_runtime/controller/policy.py
2. eco-concil-runtime(abandoned)/src/eco_council_runtime/contract.py

活跃版应保留：

- policy_profile_summary
- effective_constraints
- role_source_governance
- allowed_sources_for_role

## 4. 明确不回收的内容

下面这些属于旧架构负担，不应回接到 active runtime：

1. stage-heavy supervisor lifecycle
2. operator outbox / prompt 驱动状态机
3. packet-heavy source-selection 文书链
4. 让 controller 代替 agent 做主要调查判断的流程

## 5. 新 active target model

```text
mission
  -> source requests / selections
  -> governed fetch_plan
  -> fetch_execution
  -> normalize
  -> analysis queue
  -> readiness / promotion
  -> reporting / archive
```

其中：

- OpenClaw 模式可以动态生成 source requests。
- runtime source-queue 模式可以走 operator 或 scenario 提供的 source selections。
- 两者都落到同一套 queue contract 和 skill surface。

## 6. 具体实现清单

### 6.1 已在本轮先落地

1. runtime registry 为所有活跃 skills 导出 source_queue_profile。
2. source_queue_profile 区分 bridge、direct、advisory 三类 queue 角色。

### 6.2 接下来要做的 active modules

1. kernel/source_queue_contract.py
   - 活跃版 source selection 和 fetch plan schema helper
2. kernel/source_queue_selection.py
   - selected_sources、family_plans、layer_plans、source_decisions 组装与校验
3. kernel/source_queue_planner.py
   - 用 policy、tasks、source selection 合成 step graph
4. skills/eco-prepare-round
   - 从 import-only planner 升级为 source-selection aware planner
5. skills/eco-import-fetch-execution
   - 支持 step_kind=import 和 future step_kind=detached-fetch
6. tests
   - 覆盖 selection drift、policy summary、step depends_on、mixed import/fetch queue

## 7. 当前批次的验收定义

本批次不追求 production-ready queue，只追求把重建边界和实现切面定清楚：

1. 哪些 abandoned 能回收
2. 哪些不能回收
3. active runtime 先补哪些模块
4. 所有 skills 如何进入统一 queue profile
