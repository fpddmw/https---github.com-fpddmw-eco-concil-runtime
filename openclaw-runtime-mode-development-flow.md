# Runtime-Mode 开发流程与旧版迁移判据

## 1. 文档定位

这份文档是当前 runtime-mode 的执行版开发流程。

已有的 [openclaw-runtime-source-queue-rebuild.md](openclaw-runtime-source-queue-rebuild.md) 方向是合理的，但它主要解决“为什么要重建”和“重建什么”，还不够回答下面四个问题：

1. 当前代码实际已经到哪一步。
2. runtime-mode 的完整开发顺序是什么。
3. 旧版 `eco-concil-runtime(abandoned)` 哪些该迁、哪些该放弃。
4. 到什么条件下可以彻底删除旧版仓库。

这份文档专门补这四件事。

近期 `2-3` 次对话内的 legacy runtime 退役任务板见 [openclaw-legacy-runtime-retirement-sprint.md](openclaw-legacy-runtime-retirement-sprint.md)。

最终删除结论与证据链见 [openclaw-legacy-runtime-final-audit.md](openclaw-legacy-runtime-final-audit.md)。

## 2. 当前 runtime-mode 的真实状态

当前 runtime-mode 已经不是空壳。它已经具备一条可运行的受控主链：

1. `eco-scaffold-mission-run`
   - 写入 `mission.json`
   - 写入 `investigation/round_tasks_<round_id>.json`
   - 写入初始 `board`
2. `eco-prepare-round`
   - 读取 mission 与 round tasks
   - 生成 `source_selection_<role>_<round_id>.json`
   - 生成 `fetch_plan_<round_id>.json`
3. `eco-import-fetch-execution`
   - 执行 `import` step
   - 执行 `detached-fetch` step
   - 调用对应 normalize skill
4. 分析技能组
   - claim / observation 提取、聚类、链接、scope、coverage
5. runtime kernel
   - `controller` 负责 phase-2 planner-backed 执行
   - `supervisor` 负责 promotion / reporting 准入状态

当前已经存在的关键代码位置：

- `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_contract.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_selection.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_planner.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/supervisor.py`
- `skills/eco-scaffold-mission-run`
- `skills/eco-prepare-round`
- `skills/eco-import-fetch-execution`

当前已经覆盖到的直接测试面：

- `tests/test_orchestration_ingress_workflow.py`
- `tests/test_source_queue_rebuild.py`
- `tests/test_source_queue_governance.py`
- `tests/test_source_queue_family_memory.py`
- `tests/test_runtime_source_queue_profiles.py`
- `tests/test_runtime_kernel.py`
- `tests/test_archive_history_workflow.py`
- `tests/test_supervisor_simulation_regression.py`

结论：
runtime-mode 当前已经是“可运行的受控 preview 主链”，但还不是 production-ready source-queue，也不是最终 OpenClaw agent-native 主执行面。

## 3. runtime-mode 的职责边界

runtime-mode 不是用来替代 OpenClaw 推理的。

它的职责应该收敛为：

1. 固化受控输入边界
2. 冻结 source selection / fetch plan
3. 执行可审计的数据引入与 normalize
4. 记录 ledger、gate、promotion、handoff
5. 为 OpenClaw mode 提供同一套 skill surface 的第二编排面

runtime-mode 不应该再承担：

1. 角色级主要调查判断
2. 复杂 prompt / outbox 状态机
3. packet-heavy 的人工文书流
4. 用 controller 代替 agent 做实质性推理

## 4. 完整开发流程

### 阶段 A：固定当前主链

目标：让现有 preview 主链成为稳定基线。

必须保持稳定的链路：

1. scaffold mission
2. prepare round
3. import / detached fetch execution
4. normalize
5. analysis chain
6. planner-backed phase-2
7. supervisor / reporting handoff

验收：

- mixed import + detached-fetch 可运行
- drift detection 生效
- controller / supervisor artifacts 可回放
- 测试保持通过

### 阶段 B：补齐 source-selection 治理对象

目标：让 `source_selection` 不只是一个 `selected_sources` 列表，而是可审计治理对象。

应具备的字段与能力：

- `status`
- `selected_sources`
- `family_plans`
- `layer_plans`
- `source_decisions`
- `override_requests`
- `allowed_sources`
- `evidence_requirements`
- 与 mission governance 一致的校验

这一步是旧版最值得回收的一小块能力，因为它能加强可控性与审计性，但不会把旧 `controller` 的大体量依赖一起迁回来。

### 阶段 C：补齐 fetch-plan 骨架

目标：让 `fetch_plan` 成为受控 step graph，而不是简单的 import 列表。

应具备：

- `policy_profile`
- `effective_constraints`
- role 级 source summary
- `depends_on`
- `step_kind=import`
- `step_kind=detached-fetch`
- artifact capture 约定
- input snapshot 与 drift detection

注意：
这里回收的是“step graph 思路”和“快照校验”，不是旧版 `step_synthesis.py` 那种大而全命令工厂。

### 阶段 D：把真实 detached fetch 接回主链

目标：从“mission 里内嵌本地 `fetch_argv`”升级到真正可治理的 detached fetch 接线。

应具备：

1. fetch request 与执行参数分离
2. side-effect / permission 边界可声明
3. 输出 artifact capture 规则统一
4. 失败后可审计、可重放

这一步完成前，runtime-mode 仍只能说是“可展示主链”，不能说生产级 source queue 完成。

### 阶段 E：补齐 runtime-mode 的控制面

目标：让 runtime-mode 成为真正的第二编排面，而不是只停留在 ingress。

应具备：

1. registry 为 skill 输出统一 queue profile
2. planner / controller / supervisor 之间边界稳定
3. reporting / promotion / archive 均可被 queue 调度
4. operator 只做准入与 override，不做推理

### 阶段 F：最后再做生产化约束

目标：增强可控性，而不是提前加重 runtime 架构。

包括：

1. execution policy
2. sandbox / permission boundary
3. admission checks
4. retry / timeout / failure classes
5. 更完整的 ledger 与 replay

## 5. 旧版 `eco-concil-runtime(abandoned)` 的取舍原则

### 5.1 直接保留概念，但在新 kernel 中重写

这些内容有价值，但应该以“小模块重写”的方式进入当前 runtime：

1. `application/orchestration/fetch_plan_builder.py`
   - 保留：input snapshot、plan 组装思路
   - 放弃：旧版路径体系与旧 controller 依赖
2. `application/orchestration/governance.py`
   - 保留：source-selection 治理校验
   - 放弃：对旧 contract bridge 的耦合
3. `controller/policy.py`
   - 保留：policy summary、allowed sources、evidence requirements、family memory 等治理摘要
   - 放弃：围绕旧 run-dir 结构和旧 packet 的适配层
4. `application/orchestration_prepare.py`
   - 保留：prepare 阶段的 artifact materialization 思路
   - 放弃：面向旧 OpenClaw prompt/outbox 的输出物

### 5.2 只迁移“小而硬”的能力，不迁整层

应该优先迁移的内容：

1. source-selection 结构校验
2. fetch-plan 输入快照与 drift detection
3. role / family / layer 级治理摘要
4. prior-round family memory
5. detached fetch step metadata

不应该整体迁移的内容：

1. `controller/` 作为中心枢纽的大部分模块
2. `supervisor` 的 stage-heavy lifecycle
3. prompt / outbox / response 目录体系
4. root facade CLI 兼容壳
5. packet-heavy 人工协作文书流

### 5.3 可以彻底放弃的旧内容

下面这些不是“以后再迁”，而是应当明确废弃：

1. 旧版 `orchestrate.py` / `supervisor.py` 的兼容 facade 设计
2. 依赖人工回填 JSON 的 agent prompt 文档链
3. 把 OpenClaw 限制成固定表单填写器的工作流
4. controller 内部继续堆积业务逻辑的结构方向

## 6. 当前建议的迁移顺序

按优先级应当这样推进：

1. 先把 `source-selection / fetch-plan` 这条小链收干净
2. 再把 detached fetch 的治理和执行边界补齐
3. 再补 prior-round memory 与 archive/history 对 runtime-mode 的回接
4. 最后再处理生产化控制面

不要反过来做。否则会继续把过渡态固化成新包袱。

## 7. 删除旧版仓库的判据

只有在下面四类条件都满足后，才能删除 `eco-concil-runtime(abandoned)`：

### 7.1 功能判据

下面这些能力已经在当前仓库里有明确归宿：

1. source-selection governance
2. fetch-plan input snapshot / drift detection
3. detached fetch 执行骨架
4. phase-2 controller / supervisor 基线
5. archive / history context 主链回接

### 7.2 结构判据

当前活跃代码不再依赖旧版路径或旧版模块语义：

1. 新代码不再参考旧 `controller/` 组织方式扩展
2. 新 runtime-mode 文档已经替代旧 runtime 架构说明
3. 旧版只剩归档参考，不再承担“待迁移运行逻辑”

### 7.3 测试判据

至少覆盖：

1. mixed import + detached-fetch
2. source-selection governance 校验
3. input drift detection
4. planner-backed controller / supervisor
5. archive / history context 回接

### 7.4 文档判据

必须能在不打开旧仓库的前提下回答：

1. runtime-mode 如何运行
2. source-selection 如何治理
3. fetch-plan 如何生成与冻结
4. 哪些旧设计被放弃，为什么

## 8. 现在能不能删旧版仓库

结论：现在可以删。

但这个“可以删”有一个前提语义：

1. 指的是可以从当前工作目录删除 `eco-concil-runtime(abandoned)`。
2. 不是说旧设计从此毫无参考价值，而是说这些参考价值已经不应该再以“并列活跃代码目录”的形式存在。

当前之所以可以删除，是因为：

1. source-selection governance、family memory、fetch-plan snapshot、detached fetch 边界都已有 active 归宿。
2. archive / query / history context 主链已经由当前技能组闭环。
3. active 代码、skills、tests 不再直接依赖旧目录。
4. 删除判据已有文档与测试证据链支撑。

更实际的判断标准已经从“旧版还有没有可看代码”变成：

当旧版只剩历史对照价值，而不再承担待迁移执行逻辑时，就应当删除。

## 9. 当前执行建议

legacy runtime 退役完成后，下一批工作不应继续围绕旧目录迁移，而应转向 active 架构本身：

1. 把 runtime-mode 从“受控 preview 主链”继续推进到更稳的生产化边界。
2. 把当前 skill surface 进一步做成 OpenClaw agent-first 的执行面，而不是 planner/controller-first。
3. 加强 history retrieval、admission policy、permission boundary、ledger replay 等 active 能力。

换句话说，后续重点已经不再是“旧版还能抄什么”，而是“active runtime 还缺什么”。
