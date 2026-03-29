# Runtime Source-Queue 路线全量开发计划

## 1. 路线定位

这条路线是第二编排面，不是主智能体面。

它解决的是：

1. 固定场景执行
2. benchmark / replay / nightly run
3. 治理约束
4. 输入冻结
5. ledger / gate / archive
6. 可复现的批处理调查

它不解决的是：

1. 开放式调查推理
2. 多 agent 辩论与挑战
3. 动态工具组合策略
4. 主动探索式取证

这些属于 OpenClaw multi-agent 路线。

## 2. 当前代码状态

### 2.1 已经可运行的主链

当前 runtime route 已具备一条闭环主链：

1. `eco-scaffold-mission-run`
   - 写入 `mission.json`
   - 写入 `round_tasks`
   - 初始化 board
2. `eco-prepare-round`
   - 根据 mission / tasks / source selection 生成 role 级治理对象
   - 写入 `source_selection_<role>_<round_id>.json`
   - 写入 `fetch_plan_<round_id>.json`
3. `eco-import-fetch-execution`
   - 执行 `import`
   - 执行 `detached-fetch`
   - 调用 normalizer
4. normalize / analysis chain
   - signal plane、claim、observation、scope、coverage
5. `eco-plan-round-orchestration`
   - 生成 phase-2 orchestration plan
6. `controller`
   - 执行 planner-backed phase-2 queue
7. `supervisor`
   - 物化 promotion / reporting 准入状态
8. archive / history
   - case library
   - signal corpus
   - history context

### 2.2 已经落实的关键治理能力

当前已完成：

1. `source-selection governance`
2. `family_plans / layer_plans / source_decisions`
3. `prior-round family memory`
4. `fetch plan input snapshot`
5. `drift detection`
6. `detached fetch execution policy`
7. `allow_side_effects`
8. `registry + source_queue_profile`

### 2.3 当前还不完整的部分

当前还没有完成的不是“能不能跑”，而是“能不能稳定长期运转”：

1. detached fetch 仍以本地脚本 / 受控子进程为主，缺少更真实的 credential / admission / sandbox 方案
2. controller / supervisor 虽可用，但仍偏轻量，距离生产控制面还有差距
3. history retrieval 仍是规则打分，不是更强的调查型检索
4. operator 面还很薄，缺少系统化 replay / resume / benchmark tooling
5. archive 与 nightly / benchmark 的编排还没有统一 runbook

## 3. 路线完成定义

只有下面五个条件都满足，才能说 runtime route 完成：

1. 固定场景能稳定从 mission 跑到 archive
2. queue governance、drift detection、execution policy 都能阻断违规执行
3. replay / benchmark / nightly run 有统一入口
4. 所有关键状态物都有 ledger、gate、archive 对应物
5. route 明确保持“治理执行面”，不再重新吞回业务推理

## 4. 开发原则

1. 只增强 runtime 的治理、执行、存储、回放能力。
2. 不把调查判断塞回 controller。
3. 不再引入第二套业务逻辑实现。
4. 能做成 skill 的能力，优先做成 skill。
5. runtime 内核只承载：
   - contract
   - execution
   - gate
   - ledger
   - registry
   - cursor / manifest

## 5. 全量开发阶段

### 阶段 R0: 基线锁定

状态：`已完成`

目标：

1. 保证现有 preview 主链始终可运行
2. 让 regression 成为后续所有改动的前置条件

当前已有产物：

1. `kernel/cli.py`
2. `kernel/controller.py`
3. `kernel/supervisor.py`
4. `tests/test_runtime_kernel.py`
5. `tests/test_orchestration_ingress_workflow.py`
6. `tests/test_reporting_workflow.py`

完成判据：

1. 全量测试持续通过
2. ingress 到 supervisor 的链路不回退

### 阶段 R1: source governance 收口

状态：`已完成`

目标：

1. 让 source selection 成为完整治理对象
2. 把 prior-round family memory 收进 active kernel

当前已有产物：

1. `kernel/source_queue_contract.py`
2. `kernel/source_queue_selection.py`
3. `kernel/source_queue_history.py`
4. `tests/test_source_queue_governance.py`
5. `tests/test_source_queue_family_memory.py`

后续只保留维护性工作：

1. 收紧 schema
2. 补更细的 negative tests
3. 补 role-specific override 规则

### 阶段 R2: fetch plan 与执行边界收口

状态：`已完成第一版`

目标：

1. 让 `fetch_plan` 成为可审计 step graph
2. 让 detached fetch 具备独立治理边界

当前已有产物：

1. `kernel/source_queue_planner.py`
2. `kernel/source_queue_execution.py`
3. `skills/eco-prepare-round`
4. `skills/eco-import-fetch-execution`
5. `tests/test_source_queue_rebuild.py`

下一步要补的不是更多字段，而是更硬的执行控制：

1. credential 注入策略
2. side-effect 白名单细化
3. timeout / retry / failure class 标准化
4. detached fetch artifact quarantine 规则

完成判据：

1. 所有 fetch step 都能在 plan 中声明执行政策
2. 所有执行失败都有结构化状态和重试语义
3. 所有 raw artifacts 都有统一落盘与引用方式

### 阶段 R3: phase-2 控制面硬化

状态：`部分完成`

目标：

1. 让 planner / controller / supervisor 的边界固定
2. 让 queue mode 拥有稳定的 phase-2 运行面

当前已有产物：

1. `eco-plan-round-orchestration`
2. `controller.py`
3. `supervisor.py`
4. `tests/test_orchestration_planner_workflow.py`
5. `tests/test_supervisor_simulation_regression.py`

仍需完成的工作：

1. 为 controller 引入明确的 stage contract，而不是只靠 skill sequence
2. 为 supervisor 增加更强的 freeze / promote / hold 分类
3. 把 `show-run-state` 提升为更可靠的 round 运维入口
4. 补 controller 失败恢复与 resume 策略

完成判据：

1. phase-2 每一步都有明确输入物、输出物、失败语义
2. resume 一次中断 round 不需要人工猜测状态
3. operator 可以仅通过 runtime artifacts 判断 round posture

### 阶段 R4: archive / history 纳入正式 runtime 流程

状态：`技能已完成，调度未完全收口`

目标：

1. 让 archive / history 不再是“有技能但靠手工调用”
2. 让 post-round closure 成为 runtime route 的标准尾部

当前已有产物：

1. `eco-archive-case-library`
2. `eco-archive-signal-corpus`
3. `eco-query-case-library`
4. `eco-query-signal-corpus`
5. `eco-materialize-history-context`
6. `tests/test_archive_history_workflow.py`

仍需完成的工作：

1. 规定 archive write 的标准触发时机
2. 把 history context 纳入 replay / next-round bootstrap
3. 明确 archive 失败是否阻塞 round close
4. 为 nightly / benchmark 场景增加 archive compaction 策略

完成判据：

1. round close 时 archive 行为稳定、可预期
2. next round 可以稳定读取历史而无需人工拼装

### 阶段 R5: replay / benchmark / nightly tooling

状态：`未开始`

目标：

1. 让 runtime route 真正承担第二编排面职责
2. 让它能用于回放、基准对比、固定数据集验证

必须实现：

1. benchmark run manifest
2. replay command
3. scenario fixture contract
4. stable output comparison
5. per-skill / per-round timing and failure summary

建议新增能力：

1. round template
2. regression corpus
3. archive snapshot pinning
4. diffable summary export

完成判据：

1. 能基于同一 scenario 重复跑出稳定 artifacts
2. benchmark 结果能自动比较并暴露回归

### 阶段 R6: 生产化执行边界

状态：`未开始`

目标：

1. 把当前 preview route 提升为可长期运行的受控系统

必须实现：

1. permission model
2. sandbox boundary
3. approval / admission policy
4. rollback / retry / dead-letter policy
5. operator runbook
6. alert / observability surface

完成判据：

1. 任何 step 的副作用边界都能提前判断
2. 任何失败类型都有确定处理策略
3. operator 不依赖读代码就能处理常见故障

## 6. 近期编码顺序

接下来应按下面顺序推进：

1. 完成 R3
2. 把 R4 调度化
3. 落 R5 replay / benchmark tooling
4. 最后做 R6 生产化边界

不要反过来做。  
如果先做 R6，会把一条尚未稳定的路线过早固化。

## 7. 当前建议的具体 backlog

### Backlog A: runtime 控制面

1. 给 controller 增加 round resume contract
2. 给 supervisor 增加更清晰的 terminal states
3. 给 CLI 增加 replay / rerun / inspect 子命令

### Backlog B: detached fetch 生产化

1. 把 detached fetch 的执行输入从 ad hoc argv 收敛成正式 request contract
2. 增加 credential mount / env policy
3. 增加 quarantine / checksum / provenance 扩展

### Backlog C: archive / history

1. 定义 round close 后的 archive pipeline
2. 定义 next-round bootstrap 如何读取 history context
3. 给 history retrieval 增加更强检索策略的替换点

### Backlog D: benchmark / replay

1. 定义 scenario fixture 目录协议
2. 定义 canonical output comparison 规则
3. 输出 per-run regression summary

## 8. 路线风险

当前这条路线最大的风险不是“代码太少”，而是“职责滑坡”：

1. controller 再次吞回业务推理
2. queue 为了兼容 agent 场景而变成第二套 agent 系统
3. history retrieval 被误当成强调查能力本身
4. 生产化工作过早开始，导致结构僵化

必须持续抵抗这四个风险。

## 9. 路线完成后的定位

这条路线完成后，它应该是：

1. 可靠的 benchmark / replay / governed batch surface
2. OpenClaw agent 路线的受控执行后盾
3. 审计、存储、回放和固定流程验证的基础设施

它不应该成为：

1. 主调查智能体
2. controller-heavy 的第二套业务系统
3. 依赖规则堆叠取代 agent 判断的“伪智能”路线
