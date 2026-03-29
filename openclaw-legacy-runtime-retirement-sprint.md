# Legacy Runtime 退役冲刺计划

## 1. 目标

目标不是继续长期维护 `eco-concil-runtime(abandoned)`，而是在未来 `2-3` 次对话内完成以下三件事：

1. 把旧版仍有价值的 runtime 代码迁入当前 active kernel。
2. 明确旧版每一块代码的参考价值：保留概念、重写迁移、还是直接放弃。
3. 形成“可以删除旧版仓库”的最终判据与证据链。

这份文档是本轮冲刺的持久化任务板。

## 2. 三轮任务划分

### 第 1 轮：治理记忆迁移

状态：`已完成`

目标：

1. 把旧版 `controller/policy.py` 里的 `prior-round family memory` 迁入 active kernel。
2. 让 `source_selection` 与 `fetch_plan` 直接暴露这份治理记忆。
3. 补测试，确认旧逻辑已经不再只存在于 abandoned 目录里。

本轮交付物：

1. `kernel/source_queue_history.py`
2. `source_selection.family_memory`
3. `fetch_plan.roles[*].family_memory`
4. 对应测试

完成标志：

- `prepare-round` 生成的选择对象能看到 prior-round family memory
- `fetch_plan` 角色摘要能看到相同记忆
- 全量测试通过

当前结果：

1. 已迁入 active kernel
2. 已接入 `eco-prepare-round`
3. 已补测试并通过全量回归

### 第 2 轮：detached fetch 治理边界迁移

状态：`下一轮主任务`

目标：

1. 把旧版 `application/orchestration/fetch_plan_builder.py` 与 `step_synthesis.py` 中仍有价值的 detached fetch 约束迁入 active kernel。
2. 把 `eco-import-fetch-execution` 从“脚本内自行解释 fetch step”收敛为“调用 kernel 治理 helper”。
3. 明确 fetch request、execution policy、artifact capture、side effects 的受控边界。

计划交付物：

1. `kernel` 中的 detached fetch request / execution helper
2. `eco-import-fetch-execution` 对新 helper 的接线
3. mixed import + detached fetch 更细粒度测试

完成标志：

- detached fetch 的执行参数和治理边界不再散落在 skill 脚本里
- 旧版 fetch-plan / step-synthesis 的剩余参考价值被大幅压缩

### 第 3 轮：最终退役审计

状态：`待开始`

目标：

1. 收口 archive / history context 与 runtime-mode 的最后边界。
2. 对旧版目录做最终 keep / rewrite / drop 结论。
3. 输出“可删除旧版仓库”的最终判断。

计划交付物：

1. 最终 legacy reference map
2. 删除前检查表
3. 如条件满足，明确写出“旧版仓库可删”

完成标志：

- 旧版只剩历史参考价值，不再是开发依赖
- 删除判据有文档和测试支撑

## 3. 旧代码参考价值分级

### A. 高价值，必须迁入 active kernel

1. `eco-concil-runtime(abandoned)/src/eco_council_runtime/controller/policy.py`
   - 价值：policy summary、allowed sources、evidence requirements、family memory
   - 处理：拆散后迁入 `kernel/source_queue_*`
   - 当前状态：`部分已迁`
2. `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/fetch_plan_builder.py`
   - 价值：input snapshot、fetch plan 组装思路
   - 处理：保留骨架，不回收旧依赖
   - 当前状态：`部分已迁`
3. `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/governance.py`
   - 价值：source-selection 治理校验
   - 处理：迁入当前 selection validation
   - 当前状态：`已大部迁入`

### B. 有参考价值，但只保留概念

1. `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration_prepare.py`
   - 价值：prepare 阶段 artifact materialization 思路
   - 处理：只参考，不回收 prompt/outbox 输出
2. `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/orchestration/step_synthesis.py`
   - 价值：step graph 思路、source-specific execution args
   - 处理：择要迁入 detached fetch helper，不整文件复用

### C. 应明确放弃

1. 旧版根部 facade CLI
2. stage-heavy supervisor lifecycle
3. operator outbox / prompt 文书链
4. packet-heavy source-selection 文书流
5. controller 代替 agent 推理的流程

## 4. 当前判断

现在还不能删除 `eco-concil-runtime(abandoned)`。

但只要三轮冲刺按计划完成，旧版就应当从“运行逻辑来源”退化为“纯历史参考”。届时应优先删除，而不是继续拖着它并行存在。
