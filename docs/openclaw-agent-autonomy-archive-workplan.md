# OpenClaw Agent 自主调查与归档复用工作计划

## 1. 文档定位

本文是当前唯一后续开发计划，替代已收口的动态调查规划计划。

上一阶段已经完成的基线能力包括：开放型 mission 可进入 scoping、mission 语义已声明为用户请求 envelope、moderator 可提交 investigation plan / scope / round brief / evidence request、agent entry gate 已暴露 finding / evidence bundle / hypothesis / proposal / challenge / readiness 等写入面、`open-investigation-round` 和 `materialize-context-packet` 已进入可用技能面。

本计划聚焦最近 NYC smoke transport-chain run 暴露的新问题：调查类 agent 的 source acquisition 自由度不足，finding 没有自然升级为 hypothesis / evidence bundle / next-round 验证议程，多 round lifecycle 没有在存在后续调查路线时继续推进，archive/history 机制没有在 checkpoint 或 closeout 中真正复用历史信号。

## 2. 不可违反的治理边界

1. `runtime` 是议会框架、权限执行层、审计层和归档层，不是 agent。
2. `moderator` 是议会组织者，但不替 investigator 写事实结论。
3. investigator 自主选择 evidence source、自主组合证据、自主决定证据采信和限制。
4. challenger 的 challenge 必须能形成调查压力，而不是只成为风险记录。
5. skill 只提供证据、检索结果、可复现派生对象和 provenance，不输出推荐结论、权重、排序或采信分数。
6. 任何新对象都必须保持薄 envelope：身份、目标引用、证据引用、参数、rationale、provenance、状态。
7. archive/history 只能提供历史线索和可复用 evidence refs，不能把历史案例结论自动套到当前 run。
8. source catalog 和 access policy 只能表达“某角色允许使用哪些能力”，不能再表达“runtime 替 agent 选中了哪些信源”。

## 3. 已确认问题

### 3.1 Source acquisition 仍被 runtime 管得过多

当前 `source_selection_*` 仍主要由 mission/source queue 预先决定。scoping mission 下 `intent_selected_sources()` 返回空，导致 investigator 可能无法自然启动任何本来合法的信源检索。

问题不是某一个 provider 缺失，也不是只需要补 GDELT。正确方向是简化并删除冗余 source-selection 机制：runtime 保留 allowed source surface、权限校验、side-effect gate、receipt 和 ledger；agent 从 evidence request、finding、challenge 或自身判断出发，自主选择合法 source 并编写请求参数。

### 3.2 Evidence request 到 fetch/normalize 的桥不完整

agent 能提交“需要什么信息”的 evidence request，但缺少一个标准对象表达：

1. 我准备调用哪个 source skill。
2. 我准备用什么 query/window/region/参数。
3. 该 source 如何回应哪个 evidence request 或 challenge。
4. 需要哪些 side-effect / operator approval。

### 3.3 Finding 没有自然升级成议会议程

调查 agent 已能提交 finding。本轮 environmental investigator 也提交了 substantive finding。

但后续没有自然进入：

1. evidence bundle
2. provisional hypothesis
3. council proposal
4. challenge-specific falsification probe
5. next round focus

这说明能力存在，但 agent uptake 工作流不顺。

### 3.4 Multi-round lifecycle 过早停止

本轮有 open challenge、open board tasks、not-ready readiness，但 moderator 没有请求 `open-investigation-round`。

需要让 round continuation 成为 moderator 的自然治理动作：只要存在后续调查或验证路线，moderator 就应主持下一轮并携带 unresolved refs，而不是在没有 report-ready、没有 no-actionable-path 说明的情况下停下。

challenger 复核轮次是另一类 round：它可以由 challenge 触发，但不应成为普通后续调查轮的唯一入口。正常调查发现、未完成 evidence request、待验证 finding、待成型 hypothesis、source acquisition gap 都可以推动下一轮。

### 3.5 Skill approval 链路不顺

challenger 尝试 `open-falsification-probe` 时，preflight 正确阻断了未审批执行，但系统没有自然生成可由 runtime-operator 审批的 `skill-approval-request`。

需要补齐从 agent proposal / helper need 到 approval request 的桥。

### 3.6 Archive/history 没有实际复用

本地已经有历史 NYC run 的 normalized signals，但当前 transport-chain run 没有生成 shared archives：

1. 没有 `eco_signal_corpus.sqlite`
2. 没有 `eco_case_library.sqlite`
3. 没有 `signal_corpus_import_*.json`
4. 没有 `case_library_import_*.json`

本轮 receipt-level fetch 没有进入 `normalized_signals`，因此也无法被 `archive-signal-corpus` 复用。

### 3.7 CLI 和状态摘要仍不够面向实际议会操作

需要更直接的命令面帮助 operator 和 moderator 看见：

1. available source surfaces and source acquisition intents
2. pending skill approvals
3. open challenges
4. findings not yet bundled
5. evidence gaps not yet carried to next round
6. archive checkpoint status

### 3.8 Role-local tool contract 不够顺手

本轮多个 dead letters 来自 agent 对命令形态和参数的自然误用，而不是调查判断错误：

1. fetch skills 的内部子命令形态不够显眼。
2. provider 的 historical / NRT 范围差异没有在 agent surface 中足够清楚。
3. query 参数和实际 DB schema / CLI contract 不一致或提示不足。
4. hyphenated option 与 underscore option 的易错点没有被 command template 吸收。

这类问题会削弱 agent 自主调查能力，因为 agent 会把精力花在猜 CLI，而不是调查和议会表达。

### 3.9 Receipt-level evidence 到 normalized query 闭环不完整

本轮 environmental investigator 成功产生了 runtime receipts 和 finding，但这些证据没有自然进入 `normalized_signals` 和稳定 query surface。结果是 evidence refs 可审计，但后续 agent 不能方便地用统一 signal query 复查、筛选和打包。

需要明确：任何 governed fetch 或 artifact import 成功后，都应向 agent 暴露下一步 normalize/query command hint；如果某 provider 暂不支持 normalize，也应显式标注为 receipt-only evidence，而不是让 agent 反复试错。

## 4. 实施计划

### P0：常驻文档与术语收口

目标：

1. 更新 `openclaw-project-overview.md`，把已完成的动态 scoping / round brief / context packet 基线写入常驻说明。
2. 更新 `openclaw-refactor-overall-notes.md`，把当前新增开发计划切换为本文。
3. 更新 `openclaw-skills-refactor-checklist-v2.md`，明确 skills 只提供原子能力；source acquisition proposal 是议会对象，不是 skill 推荐系统。
4. 删除过时的 `openclaw-dynamic-investigation-planning-workplan.md`。

验收：

1. docs 中不再引用已删除计划。
2. 常驻文档不再声称 dynamic planning 仍完全缺失。
3. 文档继续显式区分 runtime、runtime-operator、moderator、council agents。

### P1：Source acquisition 简化与薄提案对象

目标：

1. 删除或降级 `source_selection_*` 作为 agent 调查入口的地位。
2. 保留 source catalog / role access policy 作为权限面，而不是议程面。
3. 允许 investigator 直接使用任何本角色合法 source skill，并自主编写 query/window/region/参数。
4. 仅在需要跨 agent 协调、operator approval、跨轮承接或显式议会记录时，提交薄 `source-acquisition-proposal`。

新增或扩展：

1. canonical object：`source-acquisition-proposal`
2. CLI/skill：`submit-source-acquisition-proposal`
3. query surface：按 `author_role`、`target_evidence_request_id`、`source_skill`、`status` 查询。

最小字段：

1. `proposal_id`
2. `run_id`
3. `round_id`
4. `author_role`
5. `target_kind`
6. `target_id`
7. `source_skill`
8. `query_parameters`
9. `declared_side_effects`
10. `requested_side_effect_approvals`
11. `rationale`
12. `provenance`
13. `status=proposed|approved-for-execution|executed|withdrawn|rejected`

该对象不是所有 fetch 的强制前置条件。普通合法 source fetch 只需要通过 role permission、side-effect policy 和 runtime receipt；proposal 用于需要被议会看见或需要审批的取证路线。

禁止字段：

1. `score`
2. `rank`
3. `weight`
4. `priority`
5. `recommended_conclusion`
6. `recommended_source_rank`

验收：

1. social investigator 可基于 evidence request 自主选择任何合法 public/formal/social source。
2. environmental investigator 可基于 scope/challenge 自主选择任何合法 environment/fire/weather source。
3. 新增或既有 source provider 都走同一机制，不为 GDELT、AirNow、Open-Meteo、FIRMS 等写特例。
4. runtime 只校验 role permission、source skill 存在、side-effect approval 是否满足。
5. scoping mode 不再因为 `selected_sources=[]` 阻断 agent 自主取证。

### P2：Agent uptake bridge

目标是把已有 finding 变成可讨论的议会议程，而不是停留在 receipt-level summary。

新增封装或 hints：

1. `finding -> evidence-bundle`
2. `finding/evidence-bundle -> hypothesis`
3. `hypothesis/challenge -> falsification probe request`
4. `finding/gap/challenge -> next round focus refs`

验收：

1. agent entry gate 明确展示这些 follow-up commands。
2. finding 如果有 evidence refs，可直接作为 provisional hypothesis 的依据。
3. hypothesis 仍由 agent 显式提交，不由 runtime 自动生成。
4. challenger 可针对 hypothesis 或 evidence bundle 打开 challenge。

### P3：Round liveness 与自然 continuation

目标：

1. 只要存在后续调查或验证路线，moderator 就主持下一轮。
2. 下一轮携带 unresolved refs，但不把调查方向硬编码成剧本。
3. 普通 continuation round 与 challenger review / falsification round 分开表达。

实现方向：

1. 在 board/status 摘要中显式列出未解决对象：open evidence requests、unbundled findings、pending source acquisition intents、open tasks、active hypotheses、not-ready readiness、open challenges。
2. moderator 根据这些对象判断是否存在后续调查路线；存在则请求 `open-investigation-round`。
3. 若 moderator 不继续，必须显式记录 `no-actionable-path`、`human-paused`、`out-of-scope` 或 `report-ready`，不能静默停下。
4. `open-investigation-round` 消费 target refs、challenge id、context packet id、round mode。
5. 新 round 的 brief 只作为 coordination hint。

验收：

1. round-001 scoping 后可打开 round-002 acquisition/verification。
2. round-002 可以读取 round-001 findings/challenges/tasks。
3. runtime 不因 brief 外发现拒绝 agent 输出。
4. 存在可行动的后续调查路线时，流程不会以 coordination freeze point 静默停住。

### P4：Skill approval request 衔接

目标：

1. agent 需要 approval-gated helper 时，可以形成正式 approval request。
2. runtime-operator 有可审批对象，而不是只看到 preflight block。

实现方向：

1. source acquisition intent、source acquisition proposal 或 helper proposal 可生成 `request-skill-approval` command hint。
2. preflight block payload 指向“如何生成 approval request”。
3. operator status surface 显示 pending approvals 和 blocked helper intents。

验收：

1. challenger 的 `open-falsification-probe` 可从 proposal/request 进入 approval。
2. approval 后执行必须带 `skill_approval_request_id`。
3. 未审批执行继续被 admission gate 阻断。

### P5：Checkpoint archive 与 history reuse

分两层推进。

第一层：normalized signal / case checkpoint。

1. 允许非 terminal round 运行 `archive-signal-corpus` checkpoint。
2. 允许 board/finding/challenge/readiness 的 partial case checkpoint。
3. `materialize-history-context` 在新 run / 新 round 开始时检索历史 archive。
4. 没有 normalized signals 时给出明确 gap，而不是静默成功。

第二层：raw receipt cache 暂缓。

1. 原始 fetch cache 需要参数 hash、provider version、过期策略和许可边界。
2. 目前先不作为 P5 必须项。
3. 若后续接入，只作为 artifact reuse hint，不作为证据采信。

验收：

1. 历史 run 的 normalized public/formal/environment/fire/weather signals 可被写入 shared archive。
2. 新 run 能查询 archive，拿到历史信号线索和 evidence refs。
3. archive context 不自动替当前 run 生成 conclusion。

### P6：Status / CLI 收口

新增或扩展可读状态：

1. `show-council-status`
2. `show-source-surfaces`
3. `show-source-acquisition-intents`
4. `show-open-challenges`
5. `show-unbundled-findings`
6. `show-archive-status`

Role-local command templates：

1. fetch command template 必须展示真实可执行形态，不让 agent 猜子命令。
2. provider 模式必须标明 historical / NRT / archive / live 的可用时间范围。
3. query command template 必须只暴露当前 CLI 支持的参数，并提供 schema-aligned filters。
4. 常见 hyphen / underscore 参数误用应由 parser alias 或模板统一吸收。
5. fetch receipt 返回值应包含 next normalize / query hints；无法 normalize 时明确标注 `receipt-only`。

验收：

1. operator 能快速判断下一步是 approval、open round、normalize、archive 还是报告 gate。
2. 命令输出不包含调查建议排序。
3. 输出只展示对象状态、refs、缺口和可执行命令 hint。
4. agent 不需要通过失败 dead letter 来发现标准 fetch/normalize/query 命令形态。
5. receipt-level evidence 能自然进入 normalized query 闭环，或被明确标注为暂时不能归一化。

### P7：真实案例回归

使用 NYC smoke transport-chain mission 重新验证：

1. mission 不提加拿大。
2. investigator 不依赖 runtime-selected source，也不会被 `selected_sources=[]` 卡死。
3. social investigator 应能自主选择合法 public/formal/social source；GDELT 只是可选 source family 之一。
4. environmental investigator 应能继续验证 upwind fire/source geometry 或提出其他环境验证路线。
5. 只要存在后续调查路线，moderator 应主持 `round-001 -> round-002`。
6. challenger challenge 可触发独立复核/反证路线，但不是开启 round-002 的唯一理由。
7. 若仍不 report-ready，应有明确 evidence gaps、open challenges、next-round route 或 no-actionable-path 说明，以及 archive checkpoint。

## 5. 非目标

1. 不实现自动源区归因模型。
2. 不把任何 public/news/social/formal/environment signal 当成事实真相。
3. 不让 runtime 自动决定 source 优先级或 source 是否“应该被选中”。
4. 不引入 evidence score、source weight、ranked source list。
5. 不要求每个 run 都进入 final publication。
6. 不为了 NYC 案例硬编码 Canada、Quebec、wildfire 等调查方向。

## 6. 建议批次

第一批：

1. P0 文档收口。
2. P1 source-selection 简化/删除和 source acquisition 最小 CLI。
3. P4 approval request hint。

第二批：

1. P2 finding-to-hypothesis/evidence-bundle command hints。
2. P3 round liveness 和自然 continuation。
3. 对 NYC transport-chain run 做 round-002 验证。

第三批：

1. P5 checkpoint archive。
2. P6 status CLI。
3. 历史 NYC run archive import 与 history context 回归。

## 7. 当前交付进展

已完成的补充收口：

1. receipt-level evidence 已在 operator status / realcase 回归中显式区分可归一化证据与 `receipt-only` evidence，避免 agent 在暂不支持 normalize 的 provider 上反复试错。
2. next-actions 运行面已从 `ranked_actions` 收口为 `actions`，artifact locator 同步改为 `$.actions`；runtime 不再产出排序队列字段。
3. action fallback 语义已改为候选/兜底来源，不再用 `heuristic_action_count`、`heuristic-fallback` 或旧 wrapper provenance 名称作为公开运行面。
4. probe、readiness、round opening、orchestration、archive/history、report-basis 等 downstream skills 均改为读取 `actions`，由 agent 自主组合、采信和追问证据。
5. 全量 `python3 -m unittest discover tests` 已通过，当前回归覆盖 342 个测试。
