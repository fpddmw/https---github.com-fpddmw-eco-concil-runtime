# OpenClaw-First 总蓝图与当前状态

## 1. 项目最终目标

项目只保留一个目标形态：一层共享 skill surface，两条编排路线。

1. `OpenClaw multi-agent`
   - 主路径
   - 负责开放式调查、假设竞争、challenge、falsification、动态取证、跨轮协作
2. `runtime source-queue`
   - 第二编排面
   - 负责固定场景、回放、benchmark、nightly run、受控批处理、审计复现

两条路线都调用同一套 `skills/eco-*`。  
runtime 不再承担新的业务推理，只负责治理、不变式、执行封装、账本、归档和状态物化。

## 2. 当前代码真实状态

### 2.1 已经完成的事实

1. 旧版 legacy runtime 已删除，迁移阶段结束。
2. 当前运行时代码已经收敛到 `eco-concil-runtime/src/eco_council_runtime/kernel/`。
3. 当前技能层已经形成统一 skill surface。
4. runtime registry 能扫描全部活跃 skills，并输出机器可读的 `source_queue_profile`。
5. runtime route 已经具备从 ingress 到 archive/history 的可运行主链。

### 2.2 当前代码结构

```text
skills/
  eco-.../

eco-concil-runtime/
  src/eco_council_runtime/
    kernel/

tests/
```

当前已经不存在单独的 `adapters/openclaw/`、`board/`、`storage/` 子包。  
这说明现在的代码现实是：

1. `runtime route` 已经有实装内核。
2. `OpenClaw route` 还没有独立 runtime 实现层。
3. 第二条路线目前仍停留在 skill 元数据、handoff 提示和 advisory planner 级别。

### 2.3 当前能力面

当前 registry 扫描结果：

1. 活跃 skills 总数：`46`
2. queue profile 已覆盖：`46 / 46`
3. `core_queue_default` skills：`30`
4. profile 分类：
   - `bridge`: `3`
   - `direct`: `29`
   - `advisory`: `14`

当前已经实装的 runtime kernel 关键模块：

1. `registry.py`
   - skill 扫描、frontmatter 解析、agent metadata 读取、queue profile 导出
2. `source_queue_contract.py`
   - source-queue 合同对象与通用 helper
3. `source_queue_selection.py`
   - source selection 组装与校验
4. `source_queue_history.py`
   - prior-round family memory
5. `source_queue_planner.py`
   - fetch plan 组装、input snapshot、drift detection
6. `source_queue_execution.py`
   - detached fetch 边界与执行 helper
7. `controller.py`
   - planner-backed phase-2 runtime route
8. `supervisor.py`
   - promotion / reporting 准入态物化
9. `cli.py`
   - `init-run`、`run-skill`、`preflight-skill`、`run-phase2-round`、`supervise-round`、`show-run-state`

### 2.4 当前最重要的实现结论

`runtime source-queue` 不是空壳，而是已经能跑。  
`OpenClaw multi-agent` 不是成型主链，而是还没落地。

这两个判断必须固定住，后续文档和开发计划都不能再写反。

## 3. 两条路线的现状判断

### 3.1 Route A: runtime source-queue

当前已经具备：

1. `eco-scaffold-mission-run`
2. `eco-prepare-round`
3. `eco-import-fetch-execution`
4. normalize / analysis / board / readiness / reporting skills
5. planner-backed `controller`
6. `supervisor`
7. archive / query / history context

因此这条路线的状态是：

1. 已有可运行主链
2. 可做演示、回放、基准样例
3. 尚未完成生产化
4. 不应继续扩张为业务推理中心

### 3.2 Route B: OpenClaw multi-agent

当前只具备以下“前置条件”，还不具备真正主链：

1. `eco-scaffold-mission-run` 支持 `orchestration_mode=openclaw-agent`
2. `eco-plan-round-orchestration` 支持 `planner_mode=agent-advisory`
3. skills 普遍带有 agent metadata 与 queue profile
4. 已有 board/query/lookup/history/reporting 类 skills 可供 agent 调用

但仍然缺失：

1. OpenClaw adapter
2. managed skill projection
3. role workspace
4. turn loop
5. multi-agent 协作协议
6. agent audit / ledger / replay 语义
7. 从 agent source request 到 runtime queue bridge 的正式接线

因此这条路线的状态是：

1. 战略主路径
2. 实际上尚未开始主实现
3. 不能再被文档描述成“快完成了”

## 4. 当前测试状态

当前测试目录覆盖：

1. `test_orchestration_ingress_workflow.py`
2. `test_source_queue_rebuild.py`
3. `test_source_queue_governance.py`
4. `test_source_queue_family_memory.py`
5. `test_runtime_source_queue_profiles.py`
6. `test_runtime_kernel.py`
7. `test_analysis_workflow.py`
8. `test_board_workflow.py`
9. `test_investigation_workflow.py`
10. `test_reporting_workflow.py`
11. `test_reporting_publish_workflow.py`
12. `test_archive_history_workflow.py`
13. `test_orchestration_planner_workflow.py`
14. `test_signal_plane_workflow.py`
15. `test_supervisor_simulation_regression.py`

当前全量回归结果：

1. `python3 -m unittest discover -s tests -q`
2. `Ran 53 tests`
3. `OK`

所以当前仓库不是“概念稿”，而是一个已经可以持续回归的 active codebase。

## 5. 已经固定的架构边界

下面这些边界不应再摇摆：

1. 只保留一层 shared skills，不再维护两套 skill 体系。
2. OpenClaw multi-agent 是主路径，runtime source-queue 是第二编排面。
3. runtime 不替代 agent 做主要调查判断。
4. board、history、query、lookup、reporting 都应该表现为 skill，而不是 controller 内部特判逻辑。
5. source selection、fetch plan、execution policy、ledger、archive 属于 runtime 强项。
6. 假设竞争、挑战、推翻、补证策略属于 agent 强项。
7. 生产化约束要后置到两条路线都跑通之后。

## 6. 当前优先级判断

当前项目的最优先事项不是“再写更多 skill”，而是下面三件事：

1. 把 runtime route 从“能跑”推进到“边界稳定、可复现、可回放”。
2. 真正启动 OpenClaw multi-agent 主实现，而不是继续停留在 handoff 文档层。
3. 把 agent 与 runtime 的桥接面收敛为明确合同：
   - source request
   - governed fetch queue
   - normalized signal plane
   - board / history / reporting handoff

## 7. 文档约定

根目录只保留三份文档：

1. 本文档 `openclaw-first-refactor-blueprint.md`
   - 负责统一目标形态、当前代码现实、边界、优先级
2. `openclaw-runtime-mode-development-flow.md`
   - 负责 Route A: runtime source-queue 的全量开发计划
3. `openclaw-skill-phase-plan.md`
   - 负责 Route B: OpenClaw multi-agent 的全量开发计划

以后不再恢复阶段性碎片文档。  
新增文档前，必须先判断能否并入这三份之一。
