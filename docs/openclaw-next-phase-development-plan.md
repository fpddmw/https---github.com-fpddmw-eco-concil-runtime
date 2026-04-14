# OpenClaw 下一阶段开发规划

## 1. 规划目标

下一阶段不是继续补完旧蓝图，而是同时修正三类问题：

1. `研究问题`
   - 从“事件式核实”转向“环境争议地图 + 调查分诊”。
2. `系统结构`
   - 从“边界过宽的 workflow kernel”转向“最小 runtime kernel + 议会策略层”。
3. `状态与契约`
   - 从“DB-first 但仍依赖 artifact handoff”转向“DB-native council state + export-only artifacts”。

这一阶段的总目标应写成：

`把 OpenClaw 推进为一个受治理但高自主、DB-native 的环境争议议会系统。`

## 2. 本阶段必须解决的五个问题

### 2.1 Agent 自主权不足

本阶段必须显式提高 agent 在议会中的实质作用。

目标不是取消治理，而是重分工：

1. runtime 负责治理、执行与审计。
2. agent 负责争议判断、提案、挑战与调查分诊。

### 2.2 Runtime kernel 边界过宽

本阶段必须把 kernel 边界收窄到：

1. lifecycle
2. governance
3. execution
4. persistence
5. query / audit

phase-specific 的规则、controller 语义、争议判断逻辑，不应继续无上限堆进 kernel。

### 2.3 规则链压过了议会判断

本阶段必须把 heuristic 的角色从“主判断源”降为：

1. bootstrap
2. fallback
3. audit
4. guardrail

如果某条流程主要还是靠固定公式保证运行，就不能把它称为议会自主判断。

### 2.4 数据契约不够硬

本阶段必须把 formal comments、争议对象、phase-2 对象都收束成明确的 canonical contract，而不是继续靠 envelope 兼容字段长期维持。

### 2.5 议会流程还不够 DB-native

本阶段必须让 round 推进在 artifact 缺失时仍能成立，尤其是：

1. `next-action`
2. `probe`
3. `readiness`
4. `promotion-basis`
5. `board summary / brief` 的恢复链

## 3. 本阶段的硬设计决议

下一阶段规划必须遵守以下设计决议；这些不是建议，而是约束。

### 3.1 Council judgement belongs to agents

议会中的实质判断默认属于 agent council，不属于 runtime kernel。

必须成立的边界：

1. runtime 不负责替议会算出唯一“正确动作”。
2. agent 必须能提交带理由的 proposal / challenge / readiness opinion。
3. 规则链只能在 agent 没有给出合格产物时兜底。

### 3.2 Runtime kernel must be minimal

kernel 只保留：

1. admission / capability / side-effect policy
2. execution / scheduling / retry / receipt
3. ledger / replay / audit
4. persistence / query surface
5. operator-visible health

以下内容必须逐步移出 kernel：

1. phase-specific scoring formula
2. 争议对象语义
3. readiness 主判断逻辑
4. promotion 实质判断逻辑

### 3.3 Database is the canonical state source

所有 council-critical 对象都必须先写 DB，再导出 artifact。

不允许长期保留的状态模式：

1. artifact 是唯一可恢复状态源
2. snapshot 只能整包读，不能 item-level 查询
3. reporting 只能消费 handoff 文件，不能从 DB 重新物化

### 3.4 Formal comments are first-class structured inputs

formal comments 不能继续只是 generic public signal。

它们至少要拥有一等的结构化字段或派生对象，覆盖：

1. docket / agency / submitter 维度
2. issue
3. stance
4. concern
5. citation type
6. procedural vs empirical distinction

### 3.5 Verification is an optional lane

observation matching 不再是默认主链。

只有在明确满足可核实条件时才进入 verification lane；否则默认停留在争议理解、formal/public linkage、representation gap、diffusion 或 board deliberation。

## 4. 本阶段不追求的内容

以下内容不作为主目标：

1. 泛化的环境政策评估平台
2. 再扩一批数据源以追求覆盖面
3. 更复杂的 publication 包装
4. 只做对外叙事、不改底层架构的“多 agent”包装
5. 完整通用的长生命周期 multi-session agent platform

但这里要明确区分两件事：

1. 本阶段不追求一个通用的、长期常驻的 agent platform。
2. 本阶段仍然必须提升 round 内的议会自主性。

## 5. 分批路线

### Batch 0: 架构决议与验收基线

目标：

1. 把目标架构写成明确约束，而不是口头方向。
2. 定义 DB-native、agent autonomy、kernel boundary 的验收标准。

本批产物：

1. 主说明文档
2. 下一阶段开发规划
3. 迁移与清债清单
4. 一组硬验收测试清单

完成标志：

1. 文档中明确区分当前系统与目标系统。
2. “runtime 负责什么、agent 负责什么、DB 负责什么”被写成硬边界。

### Batch 1: Canonical contract 与 query surface 收束

目标：

1. 先把对象和契约定义清楚，再改 skill。
2. 让 formal/public/environment 与 phase-2 对象都有明确 canonical shape。

本批核心对象：

1. `formal-comment-signal`
2. `public-discourse-signal`
3. `environment-observation-signal`
4. `issue-cluster`
5. `stance-group`
6. `concern-facet`
7. `actor-profile`
8. `evidence-citation-type`
9. `verifiability-assessment`
10. `verification-route`
11. `formal-public-link`
12. `representation-gap`
13. `diffusion-edge`
14. `next-action`
15. `probe`
16. `readiness-assessment`
17. `promotion-basis`

本批要求：

1. 每个 canonical 对象都有 ID、provenance、evidence refs、lineage、decision source。
2. phase-2 对象不再只作为 JSON wrapper 存在。
3. 至少为关键对象提供 item-level query surface。

完成标志：

1. 不依赖旧 coverage envelope 也能解释主分析链。
2. phase-2 关键对象已可从 DB 查询。

### Batch 2: Runtime kernel 收边界

目标：

1. 把 domain workflow 语义从 kernel 中拆出来。
2. 让 kernel 回到最小治理/执行内核。

本批核心调整：

1. 把 phase-specific scoring 和 stage policy 从 kernel 下沉到 council policy 或 workflow layer。
2. 把 readiness、promotion、board posture 的领域语义从 runtime helper 中解耦。
3. 明确哪些模块属于 `kernel`，哪些属于 `policy / workflow / domain reasoning`。

完成标志：

1. 代码和文档都能清楚说明 kernel 的最小职责。
2. 新增领域流程不再需要默认修改 kernel 内部。

### Batch 3: 提升议会自主性

目标：

1. 让 agent 在共享状态上产出实质 deliberation。
2. 让 runtime 从“替议会做判断”退回“治理与执行”。

本批核心能力：

1. `proposal contract`
   - agent proposal 必须带 `rationale / confidence / evidence refs / provenance`。
2. `challenge contract`
   - challenger 的输出必须是结构化 challenge，而不是只调用固定关闭/开启动作。
3. `readiness opinion contract`
   - readiness 要能表示不同 agent 的判断分歧，而不是只有单一系统分数。
4. `decision trace`
   - 记录最后采用哪条 proposal、拒绝了哪些 proposal、原因是什么。

完成标志：

1. 至少一轮 round 能由 agent 提出下一步动作与 readiness 判断。
2. heuristic 只在 proposal 缺失或失败时兜底。

### Batch 4: Public-side 主分析链去规则主导化

目标：

1. 替换当前“规则驱动的 claim 流水线”。
2. 建立以 issue / stance / concern / actor / citation / diffusion 为核心的主链。

本批核心能力：

1. issue 抽取与 issue cluster
2. stance 抽取
3. concern 抽取
4. actor 识别
5. citation type 抽取
6. formal/public linkage
7. diffusion detection
8. verifiability routing

本批要求：

1. 每个 extractor 都输出 `confidence` 与 `rationale`。
2. heuristic 版本只能作为 fallback，并显式标记 `decision_source = heuristic-fallback`。
3. formal comments 与开放平台文本要进入同一争议结构，但保持 source-specific provenance。

完成标志：

1. 输出不再主要是 `claim cluster`，而是争议地图。
2. formal comments 不再只是 generic text 信号。

### Batch 5: DB-native phase-2 与 board/reporting 迁移

目标：

1. 让 deliberation、phase-2、reporting 都围绕 canonical DB 对象推进。
2. 让 artifact 退回导出物地位。

本批核心调整：

1. `next-action / probe / readiness / promotion-basis` 先写 DB，再导出 artifact。
2. `board summary / board brief` 从 DB 物化，不再参与流程控制。
3. reporting / handoff / decision / report / publication 默认从 DB 读取 canonical 对象。
4. challenge / board task / hypothesis 都要能直接锚定新 controversy 对象。

完成标志：

1. 删除中间 artifact 后，round 仍能继续推进。
2. board 和 reporting 默认消费新对象而不是旧 coverage 主线。

### Batch 6: Optional verification lane 与 benchmark

目标：

1. 保留 observation chain，但彻底降为按需支路。
2. 用新的 benchmark 证明系统方向已经改变。

本批核心调整：

1. verifiability 先于 observation matching。
2. 只有 empirical issue 才进入 verification lane。
3. 至少准备两类 case：
   - `争议型政策 case`
   - `可核实事件 case`

完成标志：

1. 系统能明确区分“争议理解”和“经验核实”。
2. 至少一个 case 能证明 OpenClaw 已不只是事件核实器。

## 6. 实施顺序上的硬约束

本阶段必须遵守以下顺序：

1. 先定架构边界，再继续扩 skill。
   - 不允许在边界不清的情况下继续把 domain logic 堆进 kernel。
2. 先定 canonical contract，再改 skill 输出。
   - 不允许长期依赖兼容 envelope 作为真实 schema。
3. 先让 phase-2 对象 DB-native，再改 reporting。
   - 不允许 reporting 继续把 artifact handoff 当真实状态源。
4. 先提升议会自主性，再讨论更复杂的 agent platform。
   - 不允许用更大的 runtime 包装掩盖 agent 实质作用不足。
5. 先完成 routing，再决定 verification lane 的保留方式。
   - 不允许默认把所有问题继续送进 observation coverage。

## 7. 本阶段的硬验收标准

下一阶段至少应满足以下标准，才算方向修正完成：

1. `DB-only recovery`
   - 删除 `next_actions / probes / readiness / board_summary / board_brief` 等中间 artifact 后，round 仍可恢复与继续。
2. `queryable phase-2 objects`
   - `next-action / probe / readiness-assessment / promotion-basis` 可 item-level 查询。
3. `first-class formal comments`
   - formal comments 能参与 issue / stance / concern / citation / route 分析，而不是只作为 generic public signal。
4. `autonomous council loop`
   - 至少一轮 deliberation 由 agent proposal 驱动，runtime 只做治理与记录。
5. `heuristic demotion`
   - heuristic 不再是默认主判断路径；每次 fallback 都有显式 trace。
6. `kernel boundary clarity`
   - 文档、模块与测试能一致证明 kernel 已收边界。
7. `optional verification`
   - observation matching 只在明确可核实时触发。

## 8. 推荐 demo / benchmark 类型

建议至少准备三类 case：

1. `争议型政策 case`
   - 体现 formal/public linkage、representation gap、程序争议。
2. `混合型争议 case`
   - 同时包含 empirical issue 与 representation / trust issue。
3. `可核实事件 case`
   - 证明 verification lane 仍然有价值，但只是可选支路。

这样才能清楚展示：

1. OpenClaw 不只是核实器。
2. OpenClaw 也不是放弃核实。
3. OpenClaw 的核心能力是争议理解后的分诊。
