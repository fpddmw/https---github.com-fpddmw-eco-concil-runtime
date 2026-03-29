# OpenClaw Agent 松绑评估

## 1. 结论

当前项目的 `runtime route` 已经基本完成它应该承担的职责：受控执行、审计、回放、benchmark、archive、operator surface。

目前真正限制 agent 的，主要不是 runtime 的审计能力，而是上层流程把很多“分析工具”写成了“强制通道”：

1. phase-2 被固定成严格顺序和固定 skill 绑定。
2. source ingress 被固定成一次性冻结的 fetch plan。
3. signal -> candidate -> cluster / merge -> link -> coverage 这条链压缩过早，且默认是唯一正路。
4. readiness / next-actions / history retrieval 被过早做成单一结论，而不是供 agent 参考的分析物。

因此，问题不在于“是否把更多规则改写成 skill”。  
如果只是把现有强规则搬进 skill，而 controller / planner 仍强制这些 skill 的顺序和产物，那 OpenClaw 仍然只是表单填写机器。

真正的目标应该是：

1. 保留 runtime 的治理能力。
2. 把多数分析 skill 从“强制路径”降级为“可调用工具”。
3. 让 agent 能直接访问原始数据、归一化数据、历史库和查询面。
4. 只在高风险边界保留硬 gate，例如写边界、外部副作用、promotion / publication。

## 2. 应保留的硬约束

这些不应当被“松绑”，否则系统会失去可审计性和可运维性。

1. `runtime admission`
   `eco-concil-runtime/src/eco_council_runtime/kernel/operations.py`
   负责 side effect、sandbox root、timeout / retry、approval、dead-letter。
2. `ledger / receipt / dead-letter / operator surface`
   `eco-concil-runtime/src/eco_council_runtime/kernel/executor.py`
   `eco-concil-runtime/src/eco_council_runtime/kernel/operations.py`
   这些是审计闭环的核心。
3. `input snapshot / drift detection`
   `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_planner.py`
   这对 replay / benchmark / nightly 是必须的。
4. `archive / history persistence`
   `eco-concil-runtime/src/eco_council_runtime/kernel/post_round.py`
   历史归档必须稳定，不应交给 agent 自由发挥。
5. `publication boundary gate`
   promotion / reporting 前的最终 gate 仍应保留，只是不应提前侵入调查内循环。

## 3. 当前限制 Agent 的主要流程

### 3.1 Source Queue / Ingress

关键文件：

1. `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_contract.py`
2. `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_selection.py`
3. `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_planner.py`
4. `eco-concil-runtime/src/eco_council_runtime/kernel/source_queue_execution.py`
5. `skills/eco-prepare-round/scripts/eco_prepare_round.py`
6. `skills/eco-import-fetch-execution/scripts/eco_import_fetch_execution.py`

当前限制点：

1. source catalog 是固定表，且 role 只有 `sociologist` / `environmentalist` 两类。
2. mission / tasks / source selection 会被冻结成单一 `fetch_plan_<round_id>.json`。
3. `ensure_fetch_plan_inputs_match(...)` 要求 prepare 后输入不能漂移，否则整条链直接拒绝执行。
4. detached-fetch 只能执行预先写进 plan 的 `argv / cwd / artifact_capture / execution_policy`。
5. 这让 agent 很难在调查中途临时提出“再查一个源”“换一个 query”“补抓一批历史片段”。

问题本质：

这里的设计非常适合 governed batch run，但不适合探索式调查。

建议修改：

1. 保留当前 `fetch_plan` 模式，作为 strict replay / benchmark / nightly 模式。
2. 新增 appendable / branchable 的 ingress 模式，让 agent 能在 run 内追加新 fetch request。
3. 让 `source selection` 从“最终白名单”降级为“优先建议列表”；严格白名单只在 benchmark 模式启用。
4. 保留 `declared_side_effects / requested_side_effect_approvals`、sandbox、dead-letter，不放开执行边界。
5. 把“能不能新增一条调查取数动作”交给 agent，把“这条动作是否越权”交给 runtime。

### 3.2 Phase-2 Controller Flow

关键文件：

1. `eco-concil-runtime/src/eco_council_runtime/kernel/phase2_contract.py`
2. `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
3. `eco-concil-runtime/src/eco_council_runtime/kernel/supervisor.py`
4. `skills/eco-plan-round-orchestration/scripts/eco_plan_round_orchestration.py`

当前限制点：

1. `PHASE2_STAGE_DEFINITIONS` 把 stage 名、依赖关系、期望 skill 名写死。
2. `validate_skill_stage(...)` 要求某个 stage 只能运行唯一 skill。
3. `validate_stage_sequence(...)` 要求固定先后顺序。
4. `eco-plan-round-orchestration` 依据 board counts、next actions、readiness 等固定规则决定是否插入 falsification-probe stage。
5. controller / supervisor 把 round 推入一条非常硬的 planner-backed queue。

问题本质：

这不是“agent planning”，而是“先用规则做计划，再让 agent 去执行规定动作”。

建议修改：

1. 用 capability contract 取代 exact stage contract。
   例如 `summarize-board`、`inspect-history`、`open-probe`、`prepare-promotion`、`publish-report`。
2. 允许同一 capability 由多个 skill 实现，允许 agent 重复调用、跳过、回退。
3. 允许 agent 自己提出 plan graph；runtime 只校验输入物、输出物、权限和副作用。
4. 只在 `promotion / publication` 边界保留严格 gate。
5. `eco-plan-round-orchestration` 保留为“建议型 planner skill”，不再是唯一入口。

### 3.3 Analysis Compression Chain

关键文件与当前机理：

1. `skills/eco-extract-claim-candidates/scripts/eco_extract_claim_candidates.py`
   通过 `claim_type_from_text(...)` 和 `semantic_fingerprint(...)` 先把 public signals 压成 claim candidates。
2. `skills/eco-cluster-claim-candidates/scripts/eco_cluster_claim_candidates.py`
   再按 `claim_type + semantic_fingerprint` 继续聚类。
3. `skills/eco-extract-observation-candidates/scripts/eco_extract_observation_candidates.py`
   按 `source_skill | metric | rounded_point` 聚合 environment signals。
4. `skills/eco-merge-observation-candidates/scripts/eco_merge_observation_candidates.py`
   再按 `metric + point_bucket + time_bucket` 合并 observation candidates。
5. `skills/eco-link-claims-to-observations/scripts/eco_link_claims_to_observations.py`
   用固定的 `metric preference / time score / intensity relation` 算 link。
6. `skills/eco-score-evidence-coverage/scripts/eco_score_evidence_coverage.py`
   用固定加权分数推 `coverage_score` 和 `readiness`。

问题本质：

1. 压缩发生得太早。
2. agent 往往先拿到“规则压缩后的对象”，而不是原始证据面。
3. 一旦 claim / observation / link 的形成方式被固定，agent 就很难探索替代解释。
4. 这些 skill 更像 materialized view builder，却被当成唯一推理骨架。

建议修改：

1. 把这条链改成“可选分析工具链”，而不是必经主链。
2. 任何压缩 skill 都必须保留可回溯的 parent ids、artifact refs、query basis。
3. agent 可以选择：
   - 直接查 raw record
   - 直接查 normalized signal
   - 调 claim/observation 压缩 skill
   - 调 link / coverage score skill
   - 完全跳过某一步，自行构造解释
4. board / promotion 最终引用的应是“证据包 + 推理说明”，而不是某个压缩 skill 的唯一结论。

### 3.4 Retrieval / Memory / Readiness

关键文件：

1. `skills/eco-materialize-history-context/scripts/eco_materialize_history_context.py`
2. `skills/eco-materialize-board-brief/scripts/eco_materialize_board_brief.py`
3. `skills/eco-propose-next-actions/scripts/eco_propose_next_actions.py`
4. `skills/eco-summarize-round-readiness/scripts/eco_summarize_round_readiness.py`

当前限制点：

1. `eco-materialize-history-context` 由当前 board summary、brief、readiness、next actions、probes、promotion 等对象自动生成 query。
2. 它还内置 `MAX_CASES = 3`、`MAX_EXCERPTS_PER_CASE = 2`、`MAX_SIGNALS = 4` 等上限。
3. `eco-materialize-board-brief` 根据 board counts 给出固定 next moves。
4. `eco-propose-next-actions` 按固定权重表对 action 排序。
5. `eco-summarize-round-readiness` 用固定计数与阈值直接输出 `blocked / needs-more-data / ready`。

问题本质：

这些对象原本应该是“辅助判断的分析摘要”，现在却在充当“下一步只能怎么做”的隐性控制器。

建议修改：

1. history retrieval 拆成原子 skill：
   - build-history-query
   - query-case-library
   - query-signal-corpus
   - compose-history-context
2. 让 agent 能直接请求更深 retrieval，而不是永远被固定上限截断。
3. `board-brief / next-actions / round-readiness` 改成 advisory artifacts。
4. promotion gate 可以读取这些 artifacts，但不应被它们单独绑定。

## 4. 哪些 Skills 最需要改造

### 4.1 建议保留但降级为 advisory 的 skills

1. `eco-materialize-board-brief`
   保留为沟通与汇报产物。
   不再要求它必须位于 `next-actions` 前。
2. `eco-propose-next-actions`
   保留为启发式建议器。
   agent 可以采纳、改写或完全忽略。
3. `eco-summarize-round-readiness`
   保留为 gate 输入之一。
   不再作为调查内循环的硬阻断器。
4. `eco-plan-round-orchestration`
   保留为 plan proposal skill。
   不再强制 controller 只能执行它给出的单一队列。

### 4.2 建议从“主链必经”改为“可选透镜”的 skills

1. `eco-extract-claim-candidates`
2. `eco-cluster-claim-candidates`
3. `eco-extract-observation-candidates`
4. `eco-merge-observation-candidates`
5. `eco-link-claims-to-observations`
6. `eco-score-evidence-coverage`

这些 skill 适合做：

1. 压缩视图
2. ranking 视图
3. consistency check
4. challenge seed

它们不适合做：

1. 唯一事实通道
2. 唯一 board candidate 入口
3. 唯一 promotion 依据

### 4.3 应优先增强为“直接工作面”的 skills

当前仓库里已经有一些更接近 agent 原生工作方式的 skill，应当成为主工作面：

1. `eco-query-public-signals`
2. `eco-query-environment-signals`
3. `eco-lookup-normalized-signal`
4. `eco-lookup-raw-record`
5. `eco-query-case-library`
6. `eco-query-signal-corpus`

建议：

1. agent 默认先使用 query / lookup 类 skill 看原始证据面。
2. candidate / cluster / coverage skill 在需要压缩、排序、汇报时再调用。
3. 所有 summary / brief / readiness 结果都必须能回跳到 query / lookup 层。

## 5. 如何让它们变成分析工具而不是枷锁

建议把后续设计强制遵守下面五条规则。

### 规则 1：所有 summary skill 都必须是可跳过、可重复、可替换的

只要一个 skill 生成的是摘要、排序、聚类、readiness、brief，它就不应成为强制唯一路径。

### 规则 2：原始层和归一化层必须始终可直达

agent 任何时候都应该能：

1. 查 raw artifact
2. 查 normalized signal
3. 查 archive case
4. 查 signal corpus

否则所有上层对象都会变成不透明黑盒。

### 规则 3：压缩对象只能是 view，不应是唯一 truth

claim candidate、cluster、merged observation、coverage，本质上都只是中间视图。

它们必须保留：

1. parent ids
2. artifact refs
3. query basis
4. sampling / grouping basis

这样 agent 才能质疑它们、拆开它们、重建它们。

### 规则 4：gate 只出现在高风险边界

适合保留 hard gate 的位置：

1. 外部副作用执行
2. 写共享状态
3. destructive write
4. promotion
5. publication

不适合保留 hard gate 的位置：

1. 是否先做 brief
2. 是否必须先聚类再匹配
3. 是否必须先得出 readiness 才能继续调查

### 规则 5：runtime 管边界，agent 管调查

runtime 应管：

1. contract
2. execution
3. audit
4. archive
5. replay
6. budgets

agent 应管：

1. 调查顺序
2. 假设分支
3. 证据组合
4. 何时求证、何时挑战、何时收敛

## 6. 推荐改造顺序

### 阶段 A：先解除“强制顺序”而不破坏 runtime

1. 将 `eco-plan-round-orchestration`、`eco-materialize-board-brief`、`eco-propose-next-actions`、`eco-summarize-round-readiness` 从 mandatory chain 降级为 advisory chain。
2. 将 `phase2_contract.py` 从 exact stage contract 改成 capability contract。
3. controller 只校验 capability 输入输出与权限，不再要求 exact skill。

### 阶段 B：把 raw / normalized / archive 查询提升为主工作面

1. 强化 query / lookup 类 skill。
2. 让 agent 默认先查数据，再决定是否调用 summary / cluster / coverage。
3. 允许 agent 在 run 内追加 fetch / retrieval 请求，但继续受 runtime admission 约束。

### 阶段 C：把压缩 skill 改造成 materialized views

1. claim / observation / link / coverage skill 继续存在。
2. 但它们产出的对象被定义为 analysis views，而不是唯一事实层。
3. board / promotion 可以引用这些 view，但必须保留回溯链路。

### 阶段 D：最终形成 OpenClaw-native 工作流

理想工作流应是：

1. agent 读取 mission、board、history、raw / normalized data。
2. agent 自主决定下一批 query、fetch、lookup、challenge、probe。
3. runtime 只审查执行边界与副作用。
4. agent 可在调查中多次调用 summary / clustering / readiness 工具，但不被其绑定。
5. 只有在 promotion / reporting 边界，系统才要求冻结一份可审计 evidence package。

## 7. 对当前项目的直接判断

如果目标是 `2026-04-01` 前拿出“已经有稳定 runtime 成果”的中期汇报材料，那么当前 runtime 线已经足够成立。

如果目标是“真正发挥 OpenClaw 多 agent 调查能力”，那么下一阶段最重要的工作不再是继续增强 runtime，而是：

1. 拆掉 phase-2 的强顺序绑定。
2. 停止把 candidate / cluster / coverage 链当成唯一主链。
3. 让 query / lookup / raw access 成为主调查入口。
4. 让 summary / readiness / action planning 全部退回 advisory 地位。

这是“让能力变成工具”的关键分界线。
