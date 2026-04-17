# OpenClaw 彻底重构执行清单

## 1. 使用方式

本清单不是“兼容性维护目录”，而是彻底重构的执行清单。

使用原则：

1. 只要旧实现阻碍目标架构，就改写或删除。
2. 只要兼容层开始变成长期依赖，就判定为失败。
3. 每一项完成都必须减少旧链真实职责，而不是只新增一条平行路径。

## 2. 硬迁移规则

### 2.1 可以 breaking change

本轮允许：

1. 改 schema
2. 改 query surface
3. 改 skill 输出
4. 删旧 artifact wrapper
5. 重写测试

### 2.2 不以稳定性和兼容性为目标

本轮不接受以下说法：

1. “先保留旧链，之后再看要不要删”
2. “先不动 kernel，外面再包一层”
3. “先让 artifact 继续工作，等后面再 DB-native”

### 2.3 一次只允许一个 canonical

每类对象只能有一个真实 canonical shape。

允许存在：

1. 迁移期兼容视图
2. export-only envelope
3. fallback-only heuristic object

不允许存在：

1. 两套长期并行 canonical
2. 旧 wrapper 和新对象都被当成真实状态源

### 2.4 任何兼容层都必须带删除条件

每个兼容层必须明确：

1. 保留理由
2. 退出条件
3. 删除时机

### 2.5 Batch 1 当前状态

- `已完成` deliberation canonical contract registry、council object query surface、reporting DB 恢复链。
- `已完成` `next-action / probe / readiness-assessment / promotion-basis` 主存储入口 canonical 化与 fallback source 显式标注。
- `已完成` `eco-propose-next-actions` 对 `proposal` 的优先消费。
- `已完成` `eco-summarize-round-readiness` 对 `readiness-opinion` 的优先消费。
- `已完成` phase-2 controller / post-round 的 DB-first 控制读取。

### 2.6 Batch 2 当前状态

- `已完成` `eco-promote-evidence-basis` 对 `proposal / readiness-opinion` 的 judgement 吸收，并产出 `supporting_* / rejected_* / council_input_counts`。
- `已完成` `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-publish-council-decision / eco-materialize-final-publication` 对 trace 链字段的显式透传。
- `已完成` canonical `decision-trace` 写库、查询与 final publication 暴露。
- `已完成` `tests/test_decision_trace_workflow.py`，覆盖 ready/hold 两类 decision trace 工作流。

### 2.7 Batch 3 当前状态

- `已完成` `eco-open-falsification-probe` 对 council proposal 的直接消费，probe 打开不再必须依赖 `next_actions` wrapper。
- `已完成` proposal-first probe candidate 合并逻辑；存在 proposal 时优先于 DB-backed heuristic action。
- `已完成` canonical probe 对 `decision_source / provenance / lineage / source_ids` 的显式继承。
- `已完成` `tests/test_council_autonomy_flow.py` 中的 council-driven probe autonomy 回归。

### 2.8 Batch 4 当前状态

- `已完成` `board_proposal_support.py`，board judgement 现在直接消费 DB 中的 council proposal，并统一生成 canonical judgement metadata。
- `已完成` `hypothesis_cards / challenge_tickets / board_tasks` 的 `decision_source / evidence_refs_json / source_ids_json / provenance_json / lineage_json` 落库与迁移。
- `已完成` `[重写]` `eco-open-challenge-ticket / eco-close-challenge-ticket / eco-update-hypothesis-status / eco-claim-board-task` 的 proposal-first 执行路径。
- `已完成` `hypothesis / challenge / board-task` canonical contract 与 `query-council-objects` 查询面。
- `已完成` proposal-only board workflow 回归，覆盖 hypothesis update、challenge open、challenge close、board task claim，并断言 DB 列与 `raw_json` judgement metadata。
- `已完成` 本地大回归 `75` 项通过，board proposal-first 改造未击穿 council / reporting / runtime 主链。

## 3. Work Package 0: 冻结旧错误增长

- `[ ]` 冻结旧 `claim -> coverage -> readiness` 主链的功能扩张
- `[ ]` 冻结 kernel 内新增 domain policy
- `[ ]` 给 legacy 模块标明迁移状态
- `[ ]` 禁止新增依赖 summary artifact 的流程控制逻辑

## 4. Work Package 1: Canonical contracts 与 DB schema

### 4.1 Signal plane

- `[ ]` 建立 `formal-comment-signal`
- `[ ]` 建立 `public-discourse-signal`
- `[ ]` 建立 `environment-observation-signal`
- `[ ]` 每类 signal 都带 provenance、artifact refs、source metadata

### 4.2 Analysis plane

- `[ ]` 建立 `issue-cluster`
- `[ ]` 建立 `stance-group`
- `[ ]` 建立 `concern-facet`
- `[ ]` 建立 `actor-profile`
- `[ ]` 建立 `evidence-citation-type`
- `[ ]` 建立 `verifiability-assessment`
- `[ ]` 建立 `verification-route`
- `[ ]` 建立 `formal-public-link`
- `[ ]` 建立 `representation-gap`
- `[ ]` 建立 `diffusion-edge`
- `[ ]` 建立 `controversy-map`

### 4.3 Deliberation plane

- `[x]` 建立 `hypothesis`
- `[x]` 建立 `challenge`
- `[x]` 建立 `board-task`
- `[x]` 建立 `proposal`
- `[x]` 建立 `next-action`
- `[x]` 建立 `probe`
- `[x]` 建立 `readiness-opinion`
- `[x]` 建立 `readiness-assessment`
- `[x]` 建立 `promotion-basis`
- `[x]` 建立 `decision-trace`

### 4.4 通用要求

- `[ ]` 每个关键对象支持 item-level query
- `[ ]` 每个关键对象有 ID、provenance、evidence refs、lineage、decision source
- `[ ]` phase-2 对象不再只作为整包 snapshot 存在

## 5. Work Package 2: Signal plane 重构

- `[ ]` 停止把 formal comments 仅作为 generic public signal 写入系统
- `[ ]` 为 formal comments 增加 docket / agency / submitter / stance / concern / citation / route 维度
- `[ ]` 保留 formal/public/environment 三类输入的 source-specific provenance
- `[ ]` 为 typed signals 提供统一 query surface

## 6. Work Package 3: Analysis plane 改写为 controversy chain

### 6.1 重写旧主链 skills

- `[ ]` `[重写]` `eco-extract-claim-candidates`
- `[ ]` `[重写]` `eco-cluster-claim-candidates`
- `[ ]` `[重写]` `eco-derive-claim-scope`

### 6.2 新增 controversy 主链 skills

- `[ ]` `[新增 canonical]` `eco-extract-issue-candidates`
- `[ ]` `[新增 canonical]` `eco-cluster-issue-candidates`
- `[ ]` `[新增 canonical]` `eco-extract-stance-candidates`
- `[ ]` `[新增 canonical]` `eco-extract-concern-facets`
- `[ ]` `[新增 canonical]` `eco-extract-actor-profiles`
- `[ ]` `[新增 canonical]` `eco-extract-evidence-citation-types`
- `[ ]` `[新增 canonical]` `eco-link-formal-comments-to-public-discourse`
- `[ ]` `[新增 canonical]` `eco-identify-representation-gaps`
- `[ ]` `[新增 canonical]` `eco-detect-cross-platform-diffusion`
- `[ ]` `[新增 canonical]` `eco-classify-claim-verifiability`
- `[ ]` `[新增 canonical]` `eco-route-verification-lane`
- `[ ]` `[新增 canonical]` `eco-materialize-controversy-map`

### 6.3 强约束

- `[ ]` 每个 extractor 输出 `confidence`
- `[ ]` 每个 extractor 输出 `rationale`
- `[ ]` 每个 extractor 输出 `provenance`
- `[ ]` heuristic 输出显式标记 `decision_source = heuristic-fallback`
- `[ ]` 旧 claim 输出只保留为兼容视图或 fallback，不再是 canonical 主轴

## 7. Work Package 4: Deliberation plane 与 council objects

### 7.1 重写 phase-2 skills

- `[x]` `[重写]` `eco-propose-next-actions`
- `[x]` `[重写]` `eco-open-falsification-probe`
- `[x]` `[重写]` `eco-summarize-round-readiness`
- `[x]` `[重写]` `eco-promote-evidence-basis`

### 7.2 重写 board skills

- `[x]` `[重写]` `eco-claim-board-task`
- `[x]` `[重写]` `eco-open-challenge-ticket`
- `[x]` `[重写]` `eco-close-challenge-ticket`
- `[x]` `[重写]` `eco-update-hypothesis-status`

### 7.3 结构性要求

- `[ ]` `next-action` 可锚定 `issue / route / gap / actor / proposal`
- `[x]` `probe` 可由 agent proposal 或 policy fallback 生成
- `[x]` `readiness-assessment` 能表达多 agent 分歧
- `[x]` `promotion-basis` 冻结的是 controversy judgement，而不是只冻结 coverages
- `[x]` `decision-trace` 记录采纳了哪个 proposal、拒绝了哪些 proposal、理由是什么
- `[x]` `hypothesis / challenge / board-task` DB 行与 `raw_json` 已显式承载 `decision_source / evidence_refs / source_ids / provenance / lineage`

## 8. Work Package 5: 建立 agent council loop

### 8.1 当前状态

- `[x]` `openclaw-agent` 轮次进入 phase-2 时，controller 与 agent entry 现在都会先尝试 `direct-council-advisory` compiler，只有 direct council inputs 不足或 compiler 失败时才回退 `agent-advisory` planner skill。
- `[x]` advisory plan 已存在时会直接采用；advisory 物化失败时才会回退 `planner-backed` phase-2。
- `[x]` controller 状态现在显式记录 `plan_source / planning_attempts / agent_advisory_plan_path`，agent 路径与 fallback 路径不再混在一条隐式 planner 语义里。
- `[x]` `eco-plan-round-orchestration` 在 `agent-advisory` 模式下，若 DB 中已存在直接 `proposal / readiness-opinion`，现在可以跳过 `next-actions` 重算，直接产出 `probe -> readiness` 或 `readiness-only` 队列。
- `[x]` advisory plan 现在会显式暴露 `direct_council_queue / next_actions_stage_skipped / council_input_counts`，能区分“由 council inputs 直接驱动的 advisory”与“仍依赖 wrapper/action snapshot 的 advisory”。
- `[x]` `eco-concil-runtime/src/eco_council_runtime/phase2_direct_advisory.py` 已接入主链，能把 DB 中的 `proposal / readiness-opinion / probe` 直接编译为 advisory queue，并把 `plan_source = direct-council-advisory` 写入 advisory artifact、controller 状态与 planning attempts。

- `[x]` 定义 `proposal contract`
- `[x]` 定义 `challenge contract`
- `[x]` 定义 `readiness opinion contract`
- `[x]` 定义 `decision trace contract`
- `[x]` 允许多个 agent 对同一问题提交相互冲突的 judgement
- `[ ]` runtime 默认执行 agent proposal，而不是替 agent 先算出结论
- `[ ]` heuristic 只在 proposal 缺失、失败或审计模式下触发

## 9. Work Package 6: Runtime kernel 收边界

### 9.0 当前状态

- `[x]` `controller.py` 已把 `openclaw-agent` 轮次改成 `direct-council-advisory -> agent-advisory -> runtime-planner` 的三级回退链；`runtime planner` 不再是默认入口。
- `[x]` phase-2 controller artifact 与 round-controller ledger 事件现在都会暴露 `plan_source`，controller 已能区分 `direct-council-advisory / agent-advisory / runtime-planner`。
- `[ ]` `controller.py` 仍强依赖固定 stage contract 与固定 gate/post-gate 序列，尚未退化为真正的 generic execution queue runner。
- `[ ]` `investigation_planning.py` 与 readiness/promotion fallback 仍保留较多 heuristic 主语义，kernel 边界尚未收干净。

### 9.1 必须收缩或迁出的模块

- `[ ]` 收缩或迁出 `eco-concil-runtime/src/eco_council_runtime/kernel/phase2_contract.py`
- `[ ]` 收缩或迁出 `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
- `[ ]` 迁出 `eco-concil-runtime/src/eco_council_runtime/kernel/investigation_planning.py`
- `[ ]` 重写 `eco-concil-runtime/src/eco_council_runtime/kernel/agent_entry.py`

### 9.2 必须保留在 kernel 的职责

- `[ ]` admission / capability / side-effect governance
- `[ ]` execution / retry / receipt
- `[ ]` ledger / replay / audit
- `[ ]` persistence / query surface
- `[ ]` operator-visible health

### 9.3 不再允许留在 kernel 的职责

- `[ ]` readiness 主判断逻辑
- `[ ]` promotion 主判断逻辑
- `[ ]` controversy scoring formula
- `[ ]` fixed phase policy
- `[ ]` 默认议会编排假设

## 10. Work Package 7: Reporting / publication 重建

- `[x]` `[重写]` `eco-summarize-board-state`
- `[x]` `[重写]` `eco-materialize-board-brief`
- `[x]` `[重写]` `eco-materialize-reporting-handoff`
- `[x]` `[重写]` `eco-draft-council-decision`
- `[x]` `[重写]` `eco-draft-expert-report`
- `[x]` `[重写]` `eco-publish-council-decision`
- `[x]` `[重写]` `eco-publish-expert-report`
- `[x]` `[重写]` `eco-materialize-final-publication`
- `[x]` board summary / brief 只作为 DB 导出物存在
- `[x]` reporting / publication 默认从 canonical DB 对象物化

## 11. Work Package 8: Verification lane 降级为 optional lane

- `[ ]` `[降级为 optional lane]` `eco-extract-observation-candidates`
- `[ ]` `[降级为 optional lane]` `eco-merge-observation-candidates`
- `[ ]` `[降级为 optional lane]` `eco-derive-observation-scope`
- `[ ]` `[降级为 optional lane]` `eco-link-claims-to-observations`
- `[ ]` `[降级为 optional lane]` `eco-score-evidence-coverage`
- `[ ]` observation chain 只在 verifiability + route 明确允许时触发
- `[ ]` readiness 默认不再围绕 coverage 公式展开

## 12. Work Package 9: 删除兼容债

- `[ ]` 删除“formal comments 只是 generic public signal”的长期假设
- `[ ]` 删除“next_actions / probes / readiness 只以 artifact wrapper 存在”的长期假设
- `[ ]` 删除“coverage 是默认主链”的长期假设
- `[ ]` 删除“board / reporting 依赖 summary artifact 才能推进”的长期假设
- `[ ]` 删除“kernel 默认承载新增 domain policy”的长期假设
- `[ ]` 删除“旧 envelope 可以无限期作为 canonical 输出”的长期假设

## 13. Work Package 10: 测试与 benchmark 改写

- `[ ]` 删除或重写固化旧 coverage-first 语义的测试
- `[x]` 新增 DB-only recovery tests
- `[x]` 新增 agent proposal-driven round tests
- `[x]` 新增 board canonical query-surface tests
- `[ ]` 新增 kernel boundary tests
- `[ ]` 新增 optional verification lane tests
- `[ ]` 准备争议型政策 case
- `[ ]` 准备混合型争议 case
- `[ ]` 准备可核实事件 case

## 14. 硬完成检查表

- `[ ]` canonical signal / analysis / deliberation 对象已经定义并落库
- `[ ]` formal comments 已成为一等结构化输入
- `[x]` `hypothesis / challenge / board-task / proposal / next-action / probe / readiness-opinion / readiness-assessment / promotion-basis / decision-trace` 已可 item-level 查询
- `[ ]` 删除 `board_summary / board_brief / next_actions / probes / readiness` artifact 后，round 仍可继续
- `[ ]` 主链默认输出已不再是 `claim-observation-link-coverage`
- `[ ]` observation matching 只在明确可核实时触发
- `[ ]` agent proposal 已带 `rationale / confidence / evidence refs / provenance`
- `[ ]` heuristic 已降为 fallback，并带显式 trace
- `[x]` reporting / publication 默认从 DB canonical 对象物化
- `[ ]` kernel 已不再承载 readiness / promotion / controversy judgement 的主语义
- `[ ]` 至少一个争议型政策 case、一个混合型争议 case、一个可核实事件 case 稳定通过新验收
