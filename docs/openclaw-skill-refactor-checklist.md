# OpenClaw 下一阶段迁移与清债清单

## 1. 使用方式

本清单不再只是“改哪些 skill”的目录，而是下一阶段的迁移与清债约束。

阅读方法：

1. 先看 `openclaw-project-overview.md`
2. 再看 `openclaw-next-phase-development-plan.md`
3. 最后用本清单执行迁移、替换、降级与删除兼容债

## 2. 状态约定

1. `[保留底座]`
   - 保留，但不扩大职责。
2. `[重写]`
   - 保留 skill 名称或入口，但重写语义与 canonical 输出。
3. `[降级为 optional lane]`
   - 不再作为默认主链。
4. `[新增 canonical]`
   - 必须补上的新对象或新 skill。
5. `[删除兼容债]`
   - 当前为了兼容保留，但下一阶段必须移除或降为 export-only。

## 3. 迁移原则

### 3.1 Canonical object 优先于兼容 envelope

下一阶段允许暂时保留兼容 envelope，但不允许继续把它当成真实 schema。

### 3.2 Database 优先于 artifact

所有 council-critical 对象必须先写 DB，再导出 artifact。

### 3.3 Heuristic 只能做 fallback

启发式实现可以保留，但只能作为 bootstrap、fallback、audit 或 guardrail。

### 3.4 Runtime 不再继续吸收 domain policy

任何新的领域流程语义，优先落到 canonical object、workflow layer 或 council policy，不应默认写进 runtime kernel。

## 4. 必须保留的底座

### 4.1 Runtime 与 round 编排

以下能力保留，但不应继续扩大 kernel 边界：

1. `[保留底座]` `eco-scaffold-mission-run`
2. `[保留底座]` `eco-open-investigation-round`
3. `[保留底座]` `eco-prepare-round`
4. `[保留底座]` `eco-import-fetch-execution`
5. `[保留底座]` runtime query / lookup / replay / archive 相关 CLI

约束：

1. runtime 负责治理、执行、持久化、查询。
2. runtime 不负责长期承载新的争议判断公式。

### 4.2 数据抓取与查询底座

以下能力保留为输入与回查底座：

1. `[保留底座]` public fetchers
2. `[保留底座]` environment fetchers
3. `[保留底座]` query / lookup / corpus / case library / history / archive

这些能力不是当前主矛盾，不应在下一阶段被误删。

## 5. 必须建立或硬化的 canonical contract

### 5.1 Signal plane

下一阶段必须明确以下一等输入对象：

1. `[新增 canonical]` `public-discourse-signal`
2. `[新增 canonical]` `formal-comment-signal`
3. `[新增 canonical]` `environment-observation-signal`

最低要求：

1. formal comments 不再只是 generic public signal。
2. formal comment 至少要有 docket / agency / submitter / issue / stance / concern / citation / route 相关字段或派生对象。

### 5.2 Analysis plane

下一阶段必须把主分析对象收束到以下 canonical kinds：

1. `[新增 canonical]` `issue-cluster`
2. `[新增 canonical]` `stance-group`
3. `[新增 canonical]` `concern-facet`
4. `[新增 canonical]` `actor-profile`
5. `[新增 canonical]` `evidence-citation-type`
6. `[新增 canonical]` `verifiability-assessment`
7. `[新增 canonical]` `verification-route`
8. `[新增 canonical]` `formal-public-link`
9. `[新增 canonical]` `representation-gap`
10. `[新增 canonical]` `diffusion-edge`
11. `[新增 canonical]` `controversy-map`

约束：

1. 每个对象必须有 ID、provenance、evidence refs、lineage。
2. 不允许只靠 artifact wrapper 表达对象存在。

### 5.3 Deliberation plane

下一阶段必须把以下对象变成真正可查询的议会对象：

1. `[新增 canonical]` `next-action`
2. `[新增 canonical]` `probe`
3. `[新增 canonical]` `readiness-assessment`
4. `[新增 canonical]` `promotion-basis`

并继续保留并升级：

1. `[重写]` `hypothesis`
2. `[重写]` `challenge`
3. `[重写]` `board-task`

约束：

1. `next_actions / probes / readiness / promotion_basis` 不能继续只是整包 snapshot。
2. board 关键对象要能锚定 issue / route / gap / actor 等新争议对象。

## 6. 需要重写的主链技能

### 6.1 从 claim 主链迁移到争议主链

以下 skill 不应继续按旧语义运作：

1. `[重写]` `eco-extract-claim-candidates`
   - 方向：从 claim 抽取改为 issue / stance / concern 的 bootstrap extractor。
   - 约束：旧 claim 输出只能作为兼容视图或 fallback。
2. `[重写]` `eco-cluster-claim-candidates`
   - 方向：从 lexical cluster 改为 issue-cluster materialization。
3. `[重写]` `eco-derive-claim-scope`
   - 方向：从轻量 location/tag 猜测改为 verifiability / dispute / route assessment。

### 6.2 必须新增或完成的新主链技能

以下能力必须存在，并输出 canonical 对象：

1. `[新增 canonical]` `eco-extract-issue-candidates`
2. `[新增 canonical]` `eco-cluster-issue-candidates`
3. `[新增 canonical]` `eco-extract-stance-candidates`
4. `[新增 canonical]` `eco-extract-concern-facets`
5. `[新增 canonical]` `eco-extract-actor-profiles`
6. `[新增 canonical]` `eco-extract-evidence-citation-types`
7. `[新增 canonical]` `eco-link-formal-comments-to-public-discourse`
8. `[新增 canonical]` `eco-identify-representation-gaps`
9. `[新增 canonical]` `eco-detect-cross-platform-diffusion`
10. `[新增 canonical]` `eco-classify-claim-verifiability`
11. `[新增 canonical]` `eco-route-verification-lane`
12. `[新增 canonical]` `eco-materialize-controversy-map`

要求：

1. 每个 extractor 或 materializer 都要输出 `confidence`、`rationale`、`provenance`。
2. heuristic 版本必须显式标记 `decision_source = heuristic-fallback`。

## 7. 需要重写的议会与 phase-2 技能

以下技能必须改成“先写 canonical DB 对象，再导出 artifact”：

1. `[重写]` `eco-propose-next-actions`
   - 不再主要围绕补 coverage；要基于争议结构缺口、route、challenge 与 agent proposal 形成 `next-action` 对象。
2. `[重写]` `eco-open-falsification-probe`
   - 不再只从固定 action kind 映射 probe type；要支持 agent- or policy-backed probe proposal。
3. `[重写]` `eco-summarize-round-readiness`
   - 不再主要靠 coverage / open count 公式；要形成结构化 `readiness-assessment`，允许记录分歧与理由。
4. `[重写]` `eco-claim-board-task`
   - 必须能基于 issue / gap / route / probe 生成 board task。
5. `[重写]` `eco-open-challenge-ticket`
   - 必须能对 issue cluster、actor profile、diffusion edge、route judgement 提 challenge。
6. `[重写]` `eco-close-challenge-ticket`
   - 必须能记录 challenge resolution 的依据与 decision trace。
7. `[重写]` `eco-plan-round-orchestration`
   - 不再默认把 runtime 阶段逻辑编码成固定 skill 链；应更多承担 policy assembly，而不是实质判断。
8. `[重写]` `eco-promote-evidence-basis`
   - 必须升级为 `promotion-basis`，冻结争议对象与理由，而不是只冻结 coverages。

## 8. 需要重写的 board / reporting / publication 技能

以下技能必须改成消费新对象，而不是继续消费旧 coverage 主线：

1. `[重写]` `eco-summarize-board-state`
2. `[重写]` `eco-materialize-board-brief`
3. `[重写]` `eco-materialize-reporting-handoff`
4. `[重写]` `eco-draft-council-decision`
5. `[重写]` `eco-draft-expert-report`
6. `[重写]` `eco-publish-council-decision`
7. `[重写]` `eco-publish-expert-report`
8. `[重写]` `eco-materialize-final-publication`

约束：

1. `board summary / board brief` 必须是 DB 导出物，不再是流程控制前提。
2. reporting / publication 默认从 canonical DB 对象重新物化。

## 9. 需要降级为 optional verification lane 的技能

以下技能保留，但不再是默认主线：

1. `[降级为 optional lane]` `eco-extract-observation-candidates`
2. `[降级为 optional lane]` `eco-merge-observation-candidates`
3. `[降级为 optional lane]` `eco-derive-observation-scope`
4. `[降级为 optional lane]` `eco-link-claims-to-observations`
5. `[降级为 optional lane]` `eco-score-evidence-coverage`

约束：

1. 这些技能只在 `verifiability + route` 明确允许时触发。
2. 它们不再构成全局 readiness 的默认主轴。

## 10. 必须删除的兼容债

以下内容不能再被写成长期合理状态：

1. `[删除兼容债]` formal comments 仅作为 generic public signal 进入系统
2. `[删除兼容债]` `next_actions / probes / readiness` 只以 artifact wrapper 或 snapshot 整包存在
3. `[删除兼容债]` coverage 作为默认主链与默认 readiness 主轴
4. `[删除兼容债]` board/reporting 依赖 summary artifact 才能继续
5. `[删除兼容债]` runtime kernel 默认承载新增的 domain policy
6. `[删除兼容债]` 无限期保留旧 envelope 作为 canonical 输出

这意味着兼容层只能是迁移期措施，必须带有明确移除条件。

## 11. 暂不优先的事项

以下事项重要，但不是下一阶段主矛盾：

1. `[保留底座]` 更复杂的长生命周期 multi-session runtime
2. `[保留底座]` 更细的 auth / hard isolation
3. `[保留底座]` 新一轮数据源扩张
4. `[保留底座]` publication 样式继续打磨

注意：

1. 这些事项是优先级后移，不是方向替代。
2. 它们不能取代对 autonomy、kernel、contracts、DB-native 的修正。

## 12. 硬完成检查表

- `[ ]` canonical signal / analysis / deliberation 对象已经定义并落库
- `[ ]` formal comments 已成为一等结构化输入，而不是 generic text signal
- `[ ]` `next-action / probe / readiness-assessment / promotion-basis` 已可 item-level 查询
- `[ ]` `board summary / board brief / next_actions / probes / readiness` 在 artifact 删除后仍可从 DB 重建
- `[ ]` 主链默认输出已不再是 `claim-observation-link-coverage`
- `[ ]` observation matching 只在明确可核实时触发
- `[ ]` agent proposal 已带 `rationale / confidence / evidence refs / provenance`
- `[ ]` heuristic 已降为 fallback，并带显式 trace
- `[ ]` 至少一个争议型 case 和一个可核实事件 case 稳定跑通
