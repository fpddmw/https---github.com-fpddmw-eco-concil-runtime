# Legacy Runtime 最终退役审计

## 1. 最终结论

结论：当前 active runtime 已经不再依赖 `eco-concil-runtime(abandoned)` 作为运行逻辑来源。

从“当前仓库能否继续开发、测试、运行、演示 runtime-mode 主链”这个角度看，旧目录现在已经可以删除。它不再是迁移中的执行依赖，只剩历史参考价值。

更准确地说：

1. 现在可以删除的是工作目录中的 `eco-concil-runtime(abandoned)` 并列目录。
2. 如果你仍想保留历史对照，应该依赖 `git history`、单独标签或压缩归档，而不是继续把旧目录留在主工作树里并行参与开发。

## 2. 证据链

### 2.1 运行时代码已与旧目录解耦

对 `eco-concil-runtime`、`skills`、`tests` 的扫描没有发现 active 代码直接引用 `eco-concil-runtime(abandoned)` 或 `abandoned` 目录语义。

这意味着当前运行链路不是“边运行边抄旧仓库”，而是已经在 active tree 中自洽。

### 2.2 source-queue 的旧版高价值能力已完成回收

旧版最值得回收的三类能力已经进入 active kernel：

1. `source-selection` 治理对象与校验
   - active 归宿：`kernel/source_queue_contract.py`、`kernel/source_queue_selection.py`
2. prior-round family memory
   - active 归宿：`kernel/source_queue_history.py`
3. detached fetch 执行边界与治理元数据
   - active 归宿：`kernel/source_queue_planner.py`、`kernel/source_queue_execution.py`

对应 skill 接线也已收口到 active tree：

1. `skills/eco-prepare-round`
2. `skills/eco-import-fetch-execution`

### 2.3 archive / history context 主链已经由当前技能组接管

archive / history 这条链不再依赖旧 runtime 模块，而是由当前仓库的技能组直接实现：

1. `skills/eco-archive-signal-corpus`
   - 将当前 run 的 `normalized_signals` 归档进跨轮次 signal corpus SQLite。
2. `skills/eco-archive-case-library`
   - 将当前 run 的结论、brief、开放问题、证据引用压缩进 case library SQLite。
3. `skills/eco-query-case-library`
   - 对 case library 做结构化检索与打分。
4. `skills/eco-query-signal-corpus`
   - 对 signal corpus 做结构化检索与打分。
5. `skills/eco-materialize-history-context`
   - 基于当前 round 的 mission、board、scope、signals 生成 history query，再调用上面两个 query skill，最后输出检索快照和 markdown history context。

这条链的关键特征是：

1. 数据边界在当前 run 目录和 `../archives/*.sqlite`。
2. 查询与回接逻辑全部在 active skill 脚本中。
3. 没有旧版 controller / application 模块参与执行。

### 2.4 删除判据已有测试支撑

当前至少已有下面几组测试覆盖 legacy 退役所需的关键面：

1. `tests/test_source_queue_governance.py`
   - 锁定 `source-selection` 治理对象与校验。
2. `tests/test_source_queue_family_memory.py`
   - 锁定 prior-round family memory 的生成与 fetch-plan 回接。
3. `tests/test_source_queue_rebuild.py`
   - 锁定 mixed import + detached fetch、execution policy、retry metadata、drift detection。
4. `tests/test_archive_history_workflow.py`
   - 锁定 archive / query / history context 的严格 runtime 主链。
5. `tests/test_runtime_kernel.py`、`tests/test_orchestration_ingress_workflow.py`、`tests/test_supervisor_simulation_regression.py`
   - 说明 controller / supervisor / kernel CLI 的 active baseline 已存在。

## 3. 旧目录最终分类

### 3.1 已经吸收进 active kernel，可视为迁移完成

1. `controller/policy.py`
   - 已吸收的价值：policy summary、allowed sources、evidence requirements、family memory。
   - 处理结论：`rewrite-and-absorbed`
2. `application/orchestration/governance.py`
   - 已吸收的价值：selection 治理校验、family/layer/source 决策约束。
   - 处理结论：`rewrite-and-absorbed`
3. `application/orchestration/fetch_plan_builder.py`
   - 已吸收的价值：input snapshot、plan 组装思路、role summary。
   - 处理结论：`rewrite-and-absorbed`
4. `application/orchestration/step_synthesis.py`
   - 已吸收的价值：step graph 思路、detached fetch 元数据、artifact capture 边界。
   - 处理结论：`rewrite-and-absorbed`

### 3.2 只保留概念，不再继续做代码级迁移

1. `application/orchestration_prepare.py`
   - 保留 prepare 阶段 materialization 思路。
   - 不再回收其旧 prompt/outbox 产物。
2. `application/investigation/history_context.py`
   - 历史上下文的职责已经被当前 archive/query/history skills 接管。
   - 旧实现只保留概念参考。
3. `application/reporting/*`
   - 旧版对 readiness / promotion / publication 的拆法可作为历史对照，但不再作为 active 迁移清单。

### 3.3 应明确放弃，不再复用

1. 根部 facade CLI 与兼容入口。
2. `controller/` 作为中心化大控制器的大部分模块。
3. stage-heavy supervisor lifecycle。
4. operator outbox / prompt 文书链。
5. packet-heavy source-selection 文书流。
6. 把 OpenClaw 限制成表单填写机器的控制逻辑。
7. 旧目录中的 `__pycache__`、重复包装层与过渡性 glue code。

## 4. 删除前检查表

- [x] active 代码不再直接引用 `eco-concil-runtime(abandoned)`
- [x] source-selection governance 已在 active kernel 中有明确归宿
- [x] prior-round family memory 已在 active kernel 中有明确归宿
- [x] detached fetch governance / execution helper 已在 active kernel 中有明确归宿
- [x] archive / query / history context 已在当前技能组闭环
- [x] controller / supervisor / kernel CLI 在 active tree 中可运行
- [x] runtime-mode 开发流与旧版取舍判据已有独立文档
- [ ] 可选：若你还想保留人工对照样本，先打 tag 或导出压缩归档

前七项已经满足，因此“能不能删旧目录”的答案已经是“能”。

## 5. 删除后仍然存在的风险

删除旧目录并不意味着 runtime-mode 已经彻底完成，也不意味着 OpenClaw 自由度问题已经解决。剩余风险主要在 active 架构本身：

1. history retrieval 仍主要基于结构化匹配加词项重叠，离更强的调查型检索还很远。
2. `eco-materialize-history-context` 目前通过子进程调用 query skill，契约稳定，但还不是统一 kernel invocation。
3. detached fetch 已有治理边界，但生产级权限、沙箱和 admission policy 还可以继续加强。
4. source-queue 现在是“受控 preview 主链”，还不是最终的 OpenClaw agent-native 执行面。

这些都属于 active roadmap，而不是保留旧目录的理由。

## 6. 后续建议

1. 删除 `eco-concil-runtime(abandoned)` 工作目录。
2. 保留本审计文档、`openclaw-runtime-mode-development-flow.md` 和 `openclaw-legacy-runtime-retirement-sprint.md` 作为迁移结论。
3. 后续开发重心从“继续搬旧代码”切换到“把 active skills/runtime 做成真正的 OpenClaw agent-first 执行面”。
