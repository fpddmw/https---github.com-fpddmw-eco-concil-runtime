# OpenClaw 双流程与 Skill 适配评估

## 1. 总结结论

当前仓库应当保留两套编排流程，但这两套流程必须共享同一层 skill surface：

1. OpenClaw 多 agent 协作流程
2. runtime source-queue / queue-driven 流程

这里的关键原则不是“双实现各写一套能力”，而是：

- skill 与 OpenClaw 解耦
- skill 与 runtime controller 解耦
- 编排层可以有两套，但业务能力层只保留一套 skill

换句话说，正确结构应当是：

- 同一套 skills
- 两种 orchestration mode
- 一个共享 board 和 signal plane
- 一套共享 receipt / artifact / contract 约定

## 2. 对 runtime source queue 完成度的判断

结论：当前活跃 runtime 的 source queue 还不接近完整，不能诚实地说“即将可以投产”。

原因如下：

1. 当前活跃的 `eco-prepare-round` 只会从 `mission.json` 里的 `artifact_imports` 生成最小 import plan，它并不负责 live source selection。
2. 当前活跃的 `eco-prepare-round` 只内置了 6 个 source skill 的固定映射，覆盖面仍然很窄。
3. 当前活跃的 `eco-import-fetch-execution` 只做本地文件复制和 normalizer 调用，并不执行 detached fetch skills，也不处理真实 credential / network / remote failure surface。
4. richer 的 source-selection、family_plans、layer_plans、selected_sources 治理逻辑目前只存在于 `eco-concil-runtime(abandoned)`，并没有回接到活跃 runtime。
5. 当前测试能证明的是“最小 ingress contract loop 可运行”，不能证明“runtime queue 模式已具备真实生产 source orchestration”。

因此，当前 runtime queue 模式的状态更准确的描述是：

- 已有可运行的最小 ingress/import baseline
- 还没有完整的生产级 source queue / external collection 面
- 不能在本轮对话里被直接收尾成 production-ready runtime

## 3. 双流程保留策略

建议保留两套流程，但职责要明确分开：

### 3.1 OpenClaw 多 agent 协作流程

适合：

- 开放式调查
- 多角色协同
- 动态 challenge / falsification
- 调查路径由 agent 自主决定

特点：

- agent 主导 skill 调用
- board delta 是核心协作对象
- source selection 可以由 expert role 动态决定
- runtime 主要提供治理与审计边界

### 3.2 runtime source-queue 流程

适合：

- 受控批处理
- 固定场景模板
- benchmark / replay / nightly runs
- 对输入与 source coverage 有较强先验约束的任务

特点：

- runtime 产出或接收 source queue
- fetch / normalize / analysis 执行顺序更稳定
- 更适合 deterministic replay 与 operator 审核
- 不应承担开放式调查的主要工作模式

### 3.3 技术原则

无论哪条流程，skills 都应保持：

1. 原子能力独立
2. 输入输出 envelope 稳定
3. board_handoff / artifact_refs / canonical_ids 一致
4. 不把业务判断硬编码进 orchestration wrapper

## 4. 当前仓库 46 个 skill 的双流程适配评估

判定标签：

- `双流程就绪`：已经适合同时被 OpenClaw agent 和 runtime queue 调用。
- `双流程可用，但 runtime 优先`：本质可复用，但当前更偏 runtime mode；要在 agent mode 里发挥价值，还需要 wrapper 或流程降级。
- `仅 runtime 过渡态`：当前设计明显绑定 runtime ingest / queue，后续应收缩成桥接工具，而不是长期核心 skill。

| Skill | 能力组 | OpenClaw 多 agent | runtime queue | 结论 | 说明 |
|---|---|---|---|---|---|
| eco-scaffold-mission-run | ingress | 可用 | 可用 | 双流程可用，但 runtime 优先 | moderator 也可调用，但当前更像 run 初始化器 |
| eco-prepare-round | ingress | 受限 | 可用 | 仅 runtime 过渡态 | 依赖 mission.artifact_imports，本质是最小 import plan builder |
| eco-import-fetch-execution | ingress | 受限 | 可用 | 仅 runtime 过渡态 | 只处理本地 artifact import，不是 agent-native fetch 执行 |
| eco-plan-round-orchestration | orchestration | 可用 | 可用 | 双流程可用，但 runtime 优先 | 在 agent mode 里应退化为 advisory planner，而非强制队列 |
| eco-normalize-gdelt-doc-public-signals | normalize | 可用 | 可用 | 双流程就绪 | 原子 normalize skill |
| eco-normalize-youtube-video-public-signals | normalize | 可用 | 可用 | 双流程就绪 | 原子 normalize skill |
| eco-normalize-bluesky-cascade-public-signals | normalize | 可用 | 可用 | 双流程就绪 | 原子 normalize skill |
| eco-normalize-openaq-observation-signals | normalize | 可用 | 可用 | 双流程就绪 | 原子 normalize skill |
| eco-normalize-airnow-observation-signals | normalize | 可用 | 可用 | 双流程就绪 | 原子 normalize skill |
| eco-normalize-open-meteo-historical-signals | normalize | 可用 | 可用 | 双流程就绪 | 原子 normalize skill |
| eco-query-public-signals | query | 可用 | 可用 | 双流程就绪 | 典型 agent retrieval skill |
| eco-query-environment-signals | query | 可用 | 可用 | 双流程就绪 | 典型 agent retrieval skill |
| eco-lookup-normalized-signal | lookup | 可用 | 可用 | 双流程就绪 | 典型 agent lookup skill |
| eco-lookup-raw-record | lookup | 可用 | 可用 | 双流程就绪 | 法证与 challenge 关键 skill |
| eco-extract-claim-candidates | analysis | 可用 | 可用 | 双流程就绪 | 原子分析能力 |
| eco-extract-observation-candidates | analysis | 可用 | 可用 | 双流程就绪 | 原子分析能力 |
| eco-cluster-claim-candidates | analysis | 可用 | 可用 | 双流程就绪 | 原子分析能力 |
| eco-merge-observation-candidates | analysis | 可用 | 可用 | 双流程就绪 | 原子分析能力 |
| eco-derive-claim-scope | analysis | 可用 | 可用 | 双流程就绪 | 原子分析能力 |
| eco-derive-observation-scope | analysis | 可用 | 可用 | 双流程就绪 | 原子分析能力 |
| eco-link-claims-to-observations | evidence | 可用 | 可用 | 双流程就绪 | 原子 evidence skill |
| eco-score-evidence-coverage | audit | 可用 | 可用 | 双流程就绪 | 既可 queue 调用，也适合 agent 主动查询 readiness |
| eco-build-normalization-audit | audit | 可用 | 可用 | 双流程就绪 | board-facing 审计 skill |
| eco-post-board-note | board | 可用 | 可用 | 双流程就绪 | 多 agent 协作核心 skill |
| eco-read-board-delta | board | 可用 | 可用 | 双流程就绪 | 多 agent 协作核心 skill |
| eco-update-hypothesis-status | board | 可用 | 可用 | 双流程就绪 | 多 agent 协作核心 skill |
| eco-open-challenge-ticket | board | 可用 | 可用 | 双流程就绪 | 多 agent 协作核心 skill |
| eco-close-challenge-ticket | board | 可用 | 可用 | 双流程就绪 | 多 agent 协作核心 skill |
| eco-claim-board-task | board | 可用 | 可用 | 双流程就绪 | 多 agent 协作核心 skill |
| eco-summarize-board-state | board | 可用 | 可用 | 双流程就绪 | board snapshot skill |
| eco-materialize-board-brief | board | 可用 | 可用 | 双流程就绪 | handoff artifact skill |
| eco-propose-next-actions | investigate | 可用 | 可用 | 双流程就绪 | agent 或 runtime 都可触发 |
| eco-open-falsification-probe | investigate | 可用 | 可用 | 双流程就绪 | challenger / moderator 均可触发 |
| eco-summarize-round-readiness | investigate | 可用 | 可用 | 双流程就绪 | readiness gate skill |
| eco-promote-evidence-basis | promotion | 可用 | 可用 | 双流程就绪 | 可由 moderator 或 runtime gate 触发 |
| eco-materialize-reporting-handoff | reporting | 可用 | 可用 | 双流程就绪 | 原子 handoff skill |
| eco-draft-expert-report | reporting | 可用 | 可用 | 双流程就绪 | role agent 特别适合直接调用 |
| eco-publish-expert-report | reporting | 可用 | 可用 | 双流程就绪 | 原子 publish skill |
| eco-draft-council-decision | reporting | 可用 | 可用 | 双流程就绪 | moderator 特别适合直接调用 |
| eco-publish-council-decision | reporting | 可用 | 可用 | 双流程就绪 | 原子 publish skill |
| eco-materialize-final-publication | reporting | 可用 | 可用 | 双流程就绪 | 原子 publish aggregation skill |
| eco-query-case-library | archive | 可用 | 可用 | 双流程就绪 | archivist 典型 retrieval skill |
| eco-query-signal-corpus | archive | 可用 | 可用 | 双流程就绪 | archivist 典型 retrieval skill |
| eco-materialize-history-context | archive | 可用 | 可用 | 双流程就绪 | archivist / moderator 典型 skill |
| eco-archive-case-library | archive | 可用 | 可用 | 双流程就绪 | 归档导入 skill |
| eco-archive-signal-corpus | archive | 可用 | 可用 | 双流程就绪 | 归档导入 skill |

## 5. 评估结果汇总

按当前状态汇总：

1. `双流程就绪`：42 个
2. `双流程可用，但 runtime 优先`：2 个
3. `仅 runtime 过渡态`：2 个

也就是说，当前仓库绝大多数 skill 本身已经满足“双流程共享”的要求。真正没有完成的，不是 skill 层，而是 orchestration layer。

## 6. 对 runtime 是否应在本轮继续“收尾”的判断

结论：不应把本轮目标定义成“把 runtime 剩余部分全部推进完毕”，因为这会建立在一个错误前提上：即当前 runtime source queue 已经接近 production-ready。

这个前提不成立。

当前 runtime 还缺的不是零散小尾巴，而是关键结构件：

1. active source-selection / selected_sources / family_plans / layer_plans 面
2. detached fetch skills 的 live execution 接线
3. credential / remote failure / overwrite guard 体系
4. benchmark 与真实任务验收
5. operator-facing source-governance 审核面

这些都不是本轮顺手补几个函数就能诚实收尾的东西。

## 7. 正确的后续顺序

如果你要保留两套生产流程，正确顺序应当是：

1. 先把 skills 彻底从 orchestration 中解耦，并确认双流程共享 skill surface。
2. 先落地 OpenClaw 多 agent 基础框架。
3. 再回头把 runtime source-queue 模式补成第二套受控生产流程。
4. 最后在两套流程之上统一审计、权限和 operator 控制面。

## 8. 下一批建议的具体编码目标

建议按下面顺序继续：

1. 新增 active `adapters/openclaw/`，完成两仓 skill 的统一 projection / refresh。
2. provision 五个 role agent workspace，并建立 board-driven turn loop。
3. 保留 runtime mode，但把它明确定位为第二编排面，而不是唯一主路径。
4. 从 `eco-concil-runtime(abandoned)` 中有选择地回收 source-selection、fetch-plan governance、step synthesis 逻辑，重建 active runtime queue mode。
5. 最后再补 production admission 所需的 sandbox、approval、rollback 和 runbook。
