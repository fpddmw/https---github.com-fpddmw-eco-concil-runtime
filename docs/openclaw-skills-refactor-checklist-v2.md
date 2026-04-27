# OpenClaw Skills 重构清单 V2

## 1. 文档定位

本清单覆盖当前仓库内全部 `88` 个 skills，并按新目标架构重新分类：

1. 哪些 skill 保留
2. 哪些 skill 废弃
3. 哪些 skill 需要拆分重写
4. 哪些 skill 虽可保留，但必须降级为 optional / audited helper

分类统计：

1. `16` 个 fetch skills
2. `16` 个 normalize skills
3. `21` 个 analysis / heuristic skills
4. `19` 个 board / council / phase-2 skills
5. `16` 个 query / history / reporting skills

## 2. 判定原则

### 2.1 保留

适用于：

1. 原子性较好
2. 对调查闭环有直接价值
3. 不强迫 agent 进入单一 ontology

### 2.2 保留并修改

适用于：

1. 方向正确
2. 当前输出过厚或权限边界不清
3. 需要瘦身、去耦合、加权限声明

### 2.3 降级为可选

适用于：

1. 存在明显规则化 / 启发式
2. 可能对某些题目有帮助
3. 但不应进入默认调查链

### 2.4 拆分重写

适用于：

1. 非原子
2. 混合了 orchestrator、heuristic、export 多重职责
3. 需要拆成更小、更可审计的 skill

### 2.5 废弃

适用于：

1. 核心目标错误
2. 强迫 claim-matching / route ontology
3. 与“政策研究报告”目标直接冲突

## 3. 高优先级审计与拆分对象

### 3.1 必须先暂停默认使用的启发式 skill

以下 skill 在你审计前不应进入默认链：

1. `extract-claim-candidates`
2. `cluster-claim-candidates`
3. `derive-claim-scope`
4. `classify-claim-verifiability`
5. `route-verification-lane`
6. `extract-issue-candidates`
7. `cluster-issue-candidates`
8. `extract-stance-candidates`
9. `extract-concern-facets`
10. `extract-actor-profiles`
11. `extract-evidence-citation-types`
12. `materialize-controversy-map`
13. `link-claims-to-observations`
14. `score-evidence-coverage`
15. `link-formal-comments-to-public-discourse`
16. `identify-representation-gaps`
17. `detect-cross-platform-diffusion`
18. `propose-next-actions`
19. `open-falsification-probe`
20. `summarize-round-readiness`
21. `promote-evidence-basis`

### 3.2 最需要拆分的非原子 skill

1. `fetch-openaq`
2. `normalize-fetch-execution`
3. `build-normalization-audit`
4. `plan-round-orchestration`
5. `propose-next-actions`
6. `summarize-round-readiness`
7. `promote-evidence-basis`
8. `materialize-reporting-handoff`
9. `draft-council-decision`
10. `materialize-final-publication`

## 4. 全量 skill 清单

说明：

1. `规则/审计` 列中：
   - `无 / 否` = 无显式启发式规则，不需要你做规则审计
   - `弱 / 是` = 有轻度规则或映射，需要你确认
   - `强 / 是（强制）` = 明确启发式/规则系统，必须由你审计
2. `原子性` 列中：
   - `原子` = 基本做一件事
   - `半原子` = 主职责清晰，但夹带辅助逻辑
   - `非原子` = 明显承担多重职责，必须拆分

## 4.1 Fetch Skills（16）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `fetch-airnow-hourly-observations` | 保留并修改 | 无 / 否 | 原子 | 保留官方数据抓取；补地区模板、数据质量标签、参数预设。 |
| `fetch-bluesky-cascade` | 保留并修改 | 弱 / 是 | 半原子 | 保留；明确平台样本偏差说明；去掉“board-ready”暗示。 |
| `fetch-gdelt-doc-search` | 保留并修改 | 弱 / 是 | 原子 | 保留；把 query 模板与主题扩展配置化；审计 GDELT 代表性。 |
| `fetch-gdelt-events` | 保留并修改 | 弱 / 是 | 原子 | 保留；只做导出抓取，不承担后续解释。 |
| `fetch-gdelt-gkg` | 保留并修改 | 弱 / 是 | 原子 | 保留；标明 source bias 和 coverage gap。 |
| `fetch-gdelt-mentions` | 保留并修改 | 弱 / 是 | 原子 | 保留；标明媒体选择偏差。 |
| `fetch-nasa-firms-fire` | 保留并修改 | 无 / 否 | 原子 | 保留；作为火点背景数据源，不承担结论。 |
| `fetch-open-meteo-air-quality` | 保留并修改 | 无 / 否 | 原子 | 保留；明确 modeled background，不等同地面实测。 |
| `fetch-open-meteo-flood` | 保留并修改 | 无 / 否 | 原子 | 保留；用于水文背景和情景研究。 |
| `fetch-open-meteo-historical` | 保留并修改 | 无 / 否 | 原子 | 保留；作为物理背景数据源。 |
| `fetch-openaq` | 拆分重写 | 无 / 否 | 非原子 | 拆成 metadata discovery、measurement fetch、archive backfill 三个 skill。 |
| `fetch-regulationsgov-comment-detail` | 保留并修改 | 无 / 否 | 原子 | 保留；作为 formal record 深度抓取。 |
| `fetch-regulationsgov-comments` | 保留并修改 | 无 / 否 | 原子 | 保留；作为正式评论抓取主入口。 |
| `fetch-usgs-water-iv` | 保留并修改 | 无 / 否 | 原子 | 保留；对水库/河流/洪水议题是关键技能。 |
| `fetch-youtube-comments` | 保留并修改 | 弱 / 是 | 原子 | 保留；强调平台偏差、spam 清理策略和时窗代表性。 |
| `fetch-youtube-video-search` | 保留并修改 | 弱 / 是 | 半原子 | 保留；应把 discovery ranking 逻辑显式暴露为可审计配置。 |

## 4.2 Normalize Skills（16）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `normalize-airnow-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 schema mapping、单位、时间、provenance；移除 board hints。 |
| `normalize-bluesky-cascade-public-signals` | 保留并修改 | 弱 / 是 | 半原子 | 只保留 signal normalization；线程结构解释留给上层 skill。 |
| `normalize-gdelt-doc-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 article rows 归一化。 |
| `normalize-gdelt-events-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 row normalization；不附会社会语义。 |
| `normalize-gdelt-gkg-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 metadata mapping。 |
| `normalize-gdelt-mentions-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 mention rows 归一化。 |
| `normalize-nasa-firms-fire-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 只做火点 signal normalization。 |
| `normalize-open-meteo-air-quality-signals` | 保留并修改 | 无 / 否 | 原子 | 保持 modeled signal 属性，不伪装成观测。 |
| `normalize-open-meteo-flood-signals` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `normalize-open-meteo-historical-signals` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `normalize-openaq-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 保留；强化质量标记和 provider provenance。 |
| `normalize-regulationsgov-comment-detail-public-signals` | 拆分重写 | 弱 / 是 | 非原子 | 只保留 formal signal 归一化；提交者/议题/立场抽取拆到 optional parser。 |
| `normalize-regulationsgov-comments-public-signals` | 拆分重写 | 弱 / 是 | 非原子 | 同上；禁止在 normalizer 内做 route/stance judgement。 |
| `normalize-usgs-water-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 保留；为水利类研究关键。 |
| `normalize-youtube-comments-public-signals` | 保留并修改 | 弱 / 是 | 半原子 | 保留；去掉下游 board hint 语义。 |
| `normalize-youtube-video-public-signals` | 保留并修改 | 无 / 否 | 原子 | 保留。 |

## 4.3 Analysis / Heuristic Skills（21）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `build-normalization-audit` | 废弃 | 强 / 是（强制） | 非原子 | 不再作为 board-facing moderation skill；改为 operator QA export 或删除。 |
| `extract-claim-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 可保留为 public narrative seed extractor，但不得进入默认链。 |
| `cluster-claim-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 可保留为 narrative clustering helper。 |
| `derive-claim-scope` | 降级为可选 | 强 / 是（强制） | 半原子 | 不应主导议题边界；仅作辅助标签。 |
| `classify-claim-verifiability` | 降级为可选 | 强 / 是（强制） | 半原子 | 不再作为 mandatory router；仅作 optional evidence triage。 |
| `route-verification-lane` | 降级为可选 | 强 / 是（强制） | 半原子 | 不能再预设“问题必须被路由到某条 lane”。 |
| `extract-issue-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 仅作 optional issue surface builder。 |
| `cluster-issue-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 同上；不得成为调查主干。 |
| `extract-stance-candidates` | 降级为可选 | 强 / 是（强制） | 原子 | 可保留为可审计 typed decomposition helper。 |
| `extract-concern-facets` | 降级为可选 | 强 / 是（强制） | 原子 | 同上。 |
| `extract-actor-profiles` | 降级为可选 | 强 / 是（强制） | 原子 | 同上。 |
| `extract-evidence-citation-types` | 降级为可选 | 强 / 是（强制） | 原子 | 同上。 |
| `materialize-controversy-map` | 降级为可选 | 强 / 是（强制） | 非原子 | 改为 report-support export，不再作为 canonical 主视图。 |
| `extract-observation-candidates` | 拆分重写 | 强 / 是（强制） | 半原子 | 改造成环境证据聚合 helper，而不是 claim-matching 前置。 |
| `merge-observation-candidates` | 拆分重写 | 强 / 是（强制） | 半原子 | 改造成 region/metric aggregation helper。 |
| `derive-observation-scope` | 拆分重写 | 强 / 是（强制） | 半原子 | 改造成 metadata tagging helper，不再服务 claim matching。 |
| `link-claims-to-observations` | 废弃 | 强 / 是（强制） | 非原子 | 当前“社会-物理匹配”主轴不符合目标；需彻底移除现形态。 |
| `score-evidence-coverage` | 拆分重写 | 强 / 是（强制） | 非原子 | 改成通用 `evidence sufficiency review`，不再依赖 claim-observation link。 |
| `link-formal-comments-to-public-discourse` | 保留并修改 | 强 / 是（强制） | 半原子 | 可保留为 optional formal/public alignment helper。 |
| `identify-representation-gaps` | 保留并修改 | 强 / 是（强制） | 半原子 | 可保留为 optional participation / representation audit helper。 |
| `detect-cross-platform-diffusion` | 保留并修改 | 强 / 是（强制） | 半原子 | 可保留为 optional discourse dynamics helper。 |

## 4.4 Board / Council / Phase-2 Skills（19）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `scaffold-mission-run` | 保留并修改 | 无 / 否 | 半原子 | 保留；改成最小 run bootstrap，不预设分析链。 |
| `prepare-round` | 保留并修改 | 弱 / 是 | 半原子 | 保留；只编 source plan 和 capability checks，不决定研究方法。 |
| `normalize-fetch-execution` | 保留并修改 | 无 / 否 | 非原子 | 保留执行器角色；拆出 fetch queue runner、normalizer runner、execution receipt。 |
| `open-investigation-round` | 保留并修改 | 无 / 否 | 半原子 | 保留 moderator-only；只能消费已批准 transition request。 |
| `query-board-delta` | 保留 | 无 / 否 | 原子 | 保留；作为多 agent 共享状态读面。 |
| `post-board-note` | 保留并修改 | 无 / 否 | 原子 | 保留，但只做 human-readable note，不承载 canonical judgement。 |
| `update-hypothesis-status` | 保留并修改 | 无 / 否 | 半原子 | 保留；建议改名/改义为 evidence-backed hypothesis/finding update。 |
| `open-challenge-ticket` | 保留 | 无 / 否 | 原子 | 保留；仅 challenger/moderator 可写。 |
| `close-challenge-ticket` | 保留 | 无 / 否 | 原子 | 保留。 |
| `claim-board-task` | 保留并修改 | 无 / 否 | 原子 | 保留；限制为 moderator / task owner。 |
| `submit-council-proposal` | 保留并修改 | 无 / 否 | 原子 | 保留；扩展为通用 structured finding/proposal submission。 |
| `submit-readiness-opinion` | 保留并修改 | 无 / 否 | 原子 | 保留；只在 moderator 请求阶段推进前使用。 |
| `summarize-board-state` | 保留并修改 | 无 / 否 | 原子 | 保留为 derived export。 |
| `materialize-board-brief` | 保留并修改 | 无 / 否 | 原子 | 保留为 human handoff export。 |
| `plan-round-orchestration` | 拆分重写 | 强 / 是（强制） | 非原子 | 现形态应退出 kernel 主链；如保留，只能是 moderator 可选 advisory skill。 |
| `propose-next-actions` | 降级为可选 | 强 / 是（强制） | 非原子 | 可保留为 moderator advisory，不再是默认 phase owner。 |
| `open-falsification-probe` | 保留并修改 | 强 / 是（强制） | 半原子 | 保留为 challenger tool；从 controller mandatory stage 移出。 |
| `summarize-round-readiness` | 降级为可选 | 强 / 是（强制） | 非原子 | 可保留为 moderator aid，但 readiness 正式推进应靠 moderator request + operator approval。 |
| `promote-evidence-basis` | 保留并修改 | 强 / 是（强制） | 非原子 | 保留为 `freeze-report-basis` 类 skill，但不再自动主导 promotion 语义。 |

## 4.5 Query / History / Reporting Skills（16）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `query-public-signals` | 保留 | 无 / 否 | 原子 | 保留。 |
| `query-formal-signals` | 保留 | 无 / 否 | 原子 | 保留。 |
| `query-environment-signals` | 保留 | 无 / 否 | 原子 | 保留。 |
| `query-normalized-signal` | 保留 | 无 / 否 | 原子 | 保留。 |
| `query-raw-record` | 保留 | 无 / 否 | 原子 | 保留。 |
| `archive-signal-corpus` | 保留并修改 | 无 / 否 | 半原子 | 保留为跨项目信号资产；注意权限与脱敏。 |
| `query-signal-corpus` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `archive-case-library` | 保留并修改 | 无 / 否 | 半原子 | 保留；调整 case schema 以支持政策研究而非 claim matching。 |
| `query-case-library` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `materialize-history-context` | 保留并修改 | 弱 / 是 | 半原子 | 保留为 optional retrieval helper。 |
| `materialize-reporting-handoff` | 拆分重写 | 弱 / 是 | 非原子 | 拆成 evidence packet、decision packet、report packet 三个 skill。 |
| `draft-council-decision` | 保留并修改 | 弱 / 是 | 非原子 | 改成 decision memo drafter；减少对 phase-2 字段的耦合。 |
| `draft-expert-report` | 保留并修改 | 弱 / 是 | 半原子 | 改成章节化 report drafting skill，支持多角色 section 输出。 |
| `publish-expert-report` | 保留并修改 | 无 / 否 | 原子 | 保留；增加 operator confirmation / overwrite policy。 |
| `publish-council-decision` | 保留并修改 | 无 / 否 | 原子 | 保留；只发布经审批的 decision memo。 |
| `materialize-final-publication` | 保留并修改 | 弱 / 是 | 非原子 | 改成 final report assembler，不再只拼 council/reporting 产物。 |

## 5. 为新目标必须补充的 skill 层面

## 5.1 问题定义与研究设计

建议新增：

1. `define-study-brief`
2. `define-decision-question`
3. `define-region-baseline`
4. `request-phase-transition`

## 5.2 政策与区域事实采集

建议新增：

1. `fetch-eia-documents`
2. `fetch-planning-policy-documents`
3. `fetch-land-use-and-demography`
4. `fetch-complaint-and-enforcement-records`
5. `fetch-local-media-corpus`
6. `fetch-basin-and-hydrology-profile`

## 5.3 结构化归一化

建议新增：

1. `normalize-eia-documents`
2. `normalize-policy-documents`
3. `normalize-land-use-and-demography`
4. `normalize-complaint-records`
5. `normalize-project-alternative-documents`

## 5.4 研究分析

建议新增：

1. `build-evidence-bundle`
2. `build-stakeholder-map`
3. `build-causal-hypotheses`
4. `evaluate-policy-options`
5. `build-risk-register`
6. `build-uncertainty-register`
7. `run-scenario-comparison`
8. `build-implementation-checklist`
9. `build-communication-plan`

## 5.5 模拟与推演

建议新增，但必须强标 `scenario/simulation`：

1. `simulate-public-response-scenarios`
2. `simulate-policy-rollout-risks`
3. `run-hydrology-sensitivity-scenarios`

这些 skill 全部需要你审计假设与参数。

## 5.6 报告与格式规范

建议新增：

1. `draft-decision-maker-report`
2. `draft-report-section`
3. `assemble-citation-index`
4. `format-report-to-template`
5. `qa-report-evidence-trace`
6. `qa-report-language-and-claims`

## 6. 最终结论

如果以“为决策者提供环保政策研究报告”为目标：

1. fetch / normalize / query 技能大多保留。
2. 当前中层 claim-route-coverage-controversy 链必须整体降级为 optional。
3. 当前 phase-2 orchestrator / readiness / promotion 主链必须退出默认控制权。
4. reporting 技能要从“议会结论封装”升级为“研究报告生产线”。
5. 需要新增一整层政策研究、替代方案比较、风险与不确定性、格式规范技能。

## 7. Skills 侧总体工作规划

本节是进入 skills 侧重构后的全链路推进计划，覆盖从启动盘点到最终交付。第 `5` 节列出的新增 skill 属于候选增强能力，不作为本轮硬性验收指标；本轮硬性验收只看现有 skill 是否完成边界收缩、默认链去启发式、调查闭环 DB-native、报告 evidence basis 可审计。

### 7.1 推进原则

1. 先修默认链与权限边界，再拆分或新增 skill。
2. investigator 的主闭环必须是 `fetch -> normalize -> query -> finding / evidence bundle / proposal`，不能退回“分析 skill 自动替议会给结论”。
3. `moderator` 只通过 transition request 推进阶段；runtime kernel 只做权限、审批、审计、回放、DB 一致性。
4. 强规则化 / 启发式 skill 必须继续走 `skill_approval_requests -> approval -> consumption`，并在输出中保留 `decision_source / rule_version / evidence_refs / provenance`。
5. artifact 只作为 export / handoff；抓取数据、跨轮状态、报告证据 basis 必须能从 DB 恢复。
6. 任何 simulation / scenario skill 必须显式标记假设、参数来源与输出类型，不得混作事实证据。

### 7.2 WP0：基线盘点与冻结线

目标：在动代码前冻结现状，避免旧 claim-route-coverage 主链继续扩散。

任务：

1. 对 `88` 个 skill 生成当前 registry snapshot，核对 `skill_layer / allowed_roles / requires_operator_approval / write_scope / output_object_kinds`。
2. 逐个审阅 `SKILL.md` 与脚本实际读写，标记“文档语义旧、代码语义新”或“代码仍旧耦合”的差异。
3. 建立 skill 审计台账：rule id、触发条件、例外条件、偏差、代表性风险、样例输入输出、版本和审计状态。
4. 暂停把第 `3.1` 节列出的启发式 skill 放入任何默认 investigator / moderator entry sequence。

交付物：

1. 更新后的 skill 分类矩阵。
2. 启发式规则审计台账初版。
3. 默认链冻结说明。

硬验收：

1. `skill_registry.py` 与实际 `skills/` 目录数量一致。
2. 所有 optional-analysis 执行都需要持久化 approval request。
3. 文档和 operator surface 不再暗示未审计规则可默认执行。

### 7.3 WP1：默认链去启发式与 source queue 语义清理

目标：让 runtime / source queue / agent entry 只暴露能力面，不再暗示固定调查路线。

任务：

1. 清理 `source_queue_profile.py` 中仍容易被误读为默认主链的 `planned-step / core_queue_default / downstream_hints` 文案，尤其是 claim、route、coverage、readiness、promotion 相关项。
2. 确认 `phase2_agent_entry_profile.py` 的 recommended skills 默认保持空或 capability-only。
3. 将 `plan-round-orchestration / propose-next-actions / summarize-round-readiness` 明确降为 moderator 可选 advisory，且必须有审批记录。
4. 更新相关 `SKILL.md`，删除 `board-ready`、默认 promotion、默认 coverage gate 等旧表达。

交付物：

1. source queue profile 清理补丁。
2. agent entry / runbook 文案回归。
3. 旧默认链语义检索清单。

硬验收：

1. 默认 agent entry 不推荐 claim-route-coverage 链。
2. controller 不因 source queue profile 自动生成议程。
3. 未审批 optional-analysis 仍被 preflight 阻断。

### 7.4 WP2：fetch / normalize 原子化与 DB 落库加固

目标：保留数据采集能力，但让 fetch 和 normalize 不承担研究判断。

任务：

1. 将 `fetch-openaq` 拆成 metadata discovery、measurement fetch、archive backfill 三个更原子的技能或内部子命令。
2. 拆分 `normalize-fetch-execution` 的 queue runner、normalizer runner、execution receipt 职责，避免它成为隐性 orchestrator。
3. 重写 formal normalizer：`normalize-regulationsgov-comments-public-signals` 与 detail normalizer 只做 formal signal normalization；提交者、issue、stance、concern、route typing 留给 optional parser / analysis。
4. 为 fetch / normalize 输出补齐 source provenance、数据质量标签、时空范围、coverage limitation。

交付物：

1. 拆分后的 fetch / import / normalize 技能或清晰子命令。
2. signal plane DB 写入与 raw artifact 引用一致性测试。
3. 数据质量与 source bias 字段说明。

硬验收：

1. fetch 只写 raw artifact / receipt，不写研究结论。
2. normalize 只写 normalized signals / index，不写 board judgement。
3. 删除中间 export 后，raw / normalized 状态仍可从 DB 查询和重建。

### 7.5 WP3：query 与 investigator 提交闭环

目标：让 investigator 能独立完成调查闭环，而不是只读现成状态再交 proposal。

任务：

1. 加固 `query-public-signals / query-formal-signals / query-environment-signals / lookup` 的 item-level evidence refs 返回。
2. 将 `submit-finding-record / submit-evidence-bundle / post-discussion-message` 作为 investigator 默认写入面。
3. 调整 `submit-council-proposal` 文档和示例，让 proposal 建立在 finding / evidence bundle 上，而不是直接替代调查记录。
4. 为 challenger 补充 review comment / challenge 与 evidence bundle 的交叉引用约定。

交付物：

1. investigator role runbook。
2. finding / evidence bundle / proposal 的最小闭环测试。
3. query 返回 evidence basis 的示例 fixture。

硬验收：

1. investigator 可以从 DB query 结果提交 finding 和 evidence bundle。
2. proposal / readiness opinion 不是唯一跨 agent 讨论对象。
3. finding / evidence bundle 可被 moderator 和 report-editor item-level 查询。

### 7.6 WP4：启发式 analysis skill 审计与降级

目标：保留有用的派生分析，但禁止它们默认决定研究方向。

任务：

1. 对 claim / issue / stance / concern / actor / citation / route / coverage / representation / diffusion 等规则建立版本化审计记录。
2. 将 `link-claims-to-observations` 从现形态废弃或隔离到 legacy optional helper，不作为新报告 basis 的默认输入。
3. 将 `score-evidence-coverage` 改造成通用 `evidence sufficiency review`，不再依赖 claim-observation link。
4. 将 observation candidate 系列改造成 region / metric / time-window evidence aggregation helper。
5. 确保 optional-analysis 输出都带 `decision_source = audited-rule | heuristic-fallback | scenario` 等可回溯标记。

交付物：

1. 规则审计台账完整版。
2. optional-analysis 输出契约更新。
3. legacy / deprecated skill 清单。

硬验收：

1. 未审计规则不能执行到默认链。
2. 每个启发式输出都能追溯到规则版本和审批记录。
3. 报告 evidence basis 不默认依赖 claim-observation matching。

### 7.7 WP5：board / council / transition skill 收边界

目标：让 moderator 成为唯一阶段推进者，board skill 只写结构化调查状态。

任务：

1. 将 `open-investigation-round / promote-evidence-basis / close-round` 继续绑定 transition request 与 operator approval。
2. 将 `promote-evidence-basis` 改名或改义为 `freeze-report-basis` 类技能，强调冻结 DB evidence basis 而不是裁决研究结论。
3. 将 `update-hypothesis-status` 改成 evidence-backed finding / hypothesis update，不接受无 evidence refs 的状态变更。
4. 将 `post-board-note` 明确降为 human-readable note，不承载 canonical judgement。

交付物：

1. board/council skill 文档与 CLI 示例更新。
2. transition request 端到端回归。
3. evidence-backed board mutation 测试。

硬验收：

1. 只有 moderator 可请求阶段推进。
2. operator approval 才能 commit transition。
3. runtime 不生成默认调查结论。

### 7.8 WP6：reporting 重建为决策者报告生产线

目标：把 reporting 从“议会状态封装”升级为政策研究报告生产线。

任务：

1. 拆分 `materialize-reporting-handoff` 为 evidence packet、decision packet、report packet 或等价清晰职责。
2. 将 `draft-council-decision` 改成 decision memo drafter，不再绑死 phase-2 字段。
3. 将 `draft-expert-report` 改成章节化 report drafting skill，读取冻结 evidence basis 与 report section drafts。
4. 将 `materialize-final-publication` 改为 final report assembler，必须输出 evidence index、不确定性、剩余争议、建议措施。
5. publish / finalize 若继续要求人工确认，应扩展 skill approval 或新增 publish transition kind。

交付物：

1. reporting plane object 与 export 重建回归。
2. decision-maker report 模板。
3. report evidence trace QA。

硬验收：

1. 报告 evidence basis 来自 DB canonical objects。
2. 删除 reporting export 后可从 DB 重建。
3. 报告不退化为“某条 claim 是否被打脸”。

### 7.9 WP7：场景 fixture、回归与最终交付

目标：用真实工作流证明新 skills 侧架构可用。

任务：

1. 准备一个政策争议 case、一个混合型舆情/正式记录 case、一个可核实经验事件 case。
2. 覆盖 fetch / normalize / query / finding / evidence bundle / proposal / challenge / transition / report 的端到端路径。
3. 增加 artifact 删除后的 DB-only recovery 测试。
4. 增加 optional-analysis 审批、消费、不可复用测试。
5. 更新 operator runbook、agent role runbook、迁移说明和残留风险清单。

交付物：

1. 最小 targeted 回归集合。
2. 三类 case fixture。
3. 最终迁移报告。

硬验收：

1. 三类 case 均能稳定生成可审计 evidence basis。
2. optional heuristic 不进入默认主链。
3. runtime/kernel 不持有 domain judgement。
4. report-editor 能在冻结 basis 上生成决策者报告。

### 7.10 候选新增能力的处理方式

第 `5` 节中的新增 skill 建议按以下方式处理：

1. 先作为 backlog 和能力缺口，不作为本轮硬性验收指标。
2. 只有当现有 skill 无法支撑端到端 case 时，才提升为本轮必要项。
3. 任何新增 fetch / normalize skill 必须先定义 DB plane、provenance、quality flags。
4. 任何新增 analysis / simulation skill 必须先完成规则或假设审计。
5. 任何新增 reporting skill 必须先说明读取哪些 frozen evidence basis，不得直接读取未冻结调查状态给结论。

## 8. 本轮硬验收口径

skills 侧重构完成至少应满足：

1. 现有 `88` 个 skill 的 registry、文档、脚本职责一致。
2. 默认链不依赖未审计 heuristic。
3. investigator 可完成 `fetch -> normalize -> query -> finding / evidence bundle`。
4. moderator 是唯一阶段推进请求者。
5. operator approval 可查询、可回放、可消费，且不可重复使用。
6. 报告证据 basis 全部来自 DB canonical objects。
7. 删除中间 artifact 后，核心研究状态和报告导出仍可从 DB 重建。
8. 输出报告面向政策决策，包含证据来源、时空范围、关键发现、替代方案、风险、不确定性、建议和引用索引。

## 9. 2026-04-27 Session 收口回写

- 已完成：
  - 已阅读并对照 `openclaw-project-overview.md`、`openclaw-refactor-overall-notes.md`、`openclaw-runtime-kernel-agent-refactor-checklist.md`、本 v2 清单和旧 `openclaw-skill-refactor-checklist.md`。
  - 已抽查 `skill_registry / access_policy / skill_approvals / governance / executor / agent_entry / source_queue_profile`，确认 runtime/kernel 侧已有 role allowlist、optional-analysis 持久化审批、transition request 与 DB-first control surface。
  - 已抽查代表性 skill 文档：fetch/import、optional orchestration、claim extractor、proposal submission、reporting handoff，确认 skills 侧仍存在文档与旧语义残留。
  - 已在本文档补充 skills 侧从启动到最终交付的总体工作规划、硬验收口径，以及候选新增能力“不作为硬验收指标”的处理方式。

- 未完成：
  - 本轮只做文档规划补充，尚未改动 skill 代码、`SKILL.md`、registry 或测试。
  - 尚未生成完整规则审计台账。
  - 尚未清理 `source_queue_profile.py` 和各 `SKILL.md` 中仍容易暗示旧默认链的文案。

- 新发现的问题：
  - 多个 `SKILL.md` 仍引用 `docs/openclaw-next-phase-development-plan.md`，但当前仓库根目录未发现该文件；后续需要补文档或改引用入口。
  - `source_queue_profile.py` 对外虽已不导出 `core_queue_default`，内部仍大量使用 `planned-step / core_queue_default / downstream_hints` 描述 claim、route、coverage、readiness、promotion 链，存在被误读为默认调查链的风险。
  - 部分 skill 文档仍使用 `board-ready`、`claim-observation`、`coverage readiness`、`promotion-stage artifact` 等旧目标表达，需要先做文档级语义清理，再做代码拆分。
  - reporting handoff 文档仍强调读取 promotion/readiness/supervisor artifact；代码侧已具备 DB wrapper 和 orphaned artifact 识别，文档需要同步为 DB-first/export-only 口径。

- 是否影响后续计划：
  - 不阻塞 skills 侧重构；这些发现应作为 WP0/WP1 的首批输入。
  - 下一步建议优先清理默认链语义与 skill 文档，再进入 fetch/normalize 拆分和 optional-analysis 审计，避免在旧文案基础上继续实现新功能。

## 10. 2026-04-27 Skills Batch 1 代码交付回写

- 已完成：
  - 完成 WP0/WP1 第一批代码落地：`source_queue_profile.py` 已从旧 `planned-step / core_queue_default / downstream_hints` 语义改成 capability / advisory / transition surface；所有 profile 的 `downstream_hints` 现在为空，`default_chain_eligible=false`，optional-analysis 统一标记为 `approval-gated-runtime-surface`。
  - `plan-round-orchestration / propose-next-actions / summarize-round-readiness / link-claims-to-observations / score-evidence-coverage` 等高风险 heuristic 不再通过 source queue 暗示默认主链，只能作为审批后的 optional advisory / legacy helper。
  - agent-entry operator surface 已补齐 skill approval 查询、请求、批准、拒绝、消费和 approved optional-analysis run command template。
  - 修复 approved optional-analysis command template：`--skill-approval-request-id` 现在由 `skill_command_hint / run_skill_command` 插入到 `--` 之前，避免被误传给 skill 脚本。
  - 更新代表性高风险 `SKILL.md` 与 agent prompt：orchestration、next actions、readiness、reporting handoff、claim extraction、claim-observation link、coverage scoring 均改成 DB-backed / optional / approval-required 口径，并移除当前文档中发现的旧 `openclaw-next-phase-development-plan.md` 引用。
  - 新增 `docs/openclaw-skill-rule-audit-ledger.md`，作为第一版 heuristic 规则审计台账和 freeze line；当前状态均为 `default-frozen / approval-required / audit-pending`，未标记任何规则为审计通过。
  - registry 复核结果：当前 `skills/` 目录与 registry 均为 `88` 个 skill；`32` 个 skill 声明 `requires_operator_approval`；source queue summary 覆盖 `88/88`。

- 未完成：
  - 尚未逐个审完全部 `SKILL.md` 与脚本实际读写；本批只处理第一批默认链风险最高的 source queue、agent entry surface 和代表性文档。
  - 规则审计台账仍是 freeze-line 初版；尚未补每条规则的完整样例、偏差量化和人工审计结论。
  - `fetch-openaq`、`normalize-fetch-execution`、formal normalizer 拆分仍属于 WP2，未在本批实施。
  - reporting 生产线重构仍属于 WP6；本批只把 reporting handoff 文档改成 DB-first/export-only 口径。

- 新发现的问题：
  - operator runbook 之前的 approved optional-analysis 示例把 `--skill-approval-request-id` 追加在 skill args 之后，实际会被 `--` 分隔后误传给 skill 脚本；本批已修复并补回归。
  - `source_queue_profile.py` 原本对外不导出 `core_queue_default`，但 profile 输出仍通过 `planned-step` 和非空 `downstream_hints` 保留链式暗示；本批已清空输出并加回归。
  - 仓库中仍有其他 normalizer / query / archive 文档残留 `board-ready`、旧 coverage/promotion 表述；这些不再位于默认入口，但需要在 WP2-WP4 继续清理。

- 是否影响后续计划：
  - 不阻塞后续计划；WP2 可以在“默认链已冻结、optional-analysis 已审批化、operator surface 可见审批链”的基础上继续推进 fetch/normalize 原子化。
  - 后续任何新增 source queue 或 agent-entry surface 都必须保持 `default_chain_eligible=false`，并且不得重新暴露 claim-route-coverage 链式 `downstream_hints`。
  - 后续规则审计若要把某 heuristic 从 `audit-pending` 改成可用，必须更新 `docs/openclaw-skill-rule-audit-ledger.md` 并保留审批/消费记录。

- 测试：
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate tests.test_skill_approval_workflow`
  - 结果：`14` 项通过。

## 11. 2026-04-27 Skills Batch 2 代码交付回写

- 已完成：
  - 启动 WP2 的 formal normalizer 收缩：`normalize-regulationsgov-comments-public-signals` 与 `normalize-regulationsgov-comment-detail-public-signals` 已移除 `build_formal_signal_semantics()` 调用，不再在 normalizer 内派生 `submitter_type / issue_labels / stance_hint / concern_facets / evidence_citation_types / route_hint`。
  - 两个 Regulations.gov normalizer 现在只做 provider-field mapping：保留 `docket_id / agency_id / comment_on_id / submitter_name / provider dates / validation / artifact_sha256 / source_provenance`，并写入 `decision_source=provider-field-normalization`、`normalization_scope=provider-fields-only`、`typed_metadata_status=not-derived-by-normalizer`。
  - formal normalized rows 增加 source/data-quality flags，例如 `formal-record`、`provider-field-normalized`、`comment-detail`、`missing-docket-id`、`missing-agency-id`、`missing-comment-text`、`missing-submitter-name`。
  - `query-formal-signals` 文档与 agent prompt 已改成 provider fields + optional typed metadata 口径；query skill 不推导缺失 typed metadata。
  - `tests/test_signal_plane_workflow.py` 已改为验证 typed metadata 不由 normalizer 产生：typed filters 在未跑 optional parser/analysis 时返回 0，`normalized_signal_index` 只保留 provider-field index 与 `decision_source`。
  - `link-formal-comments-to-public-discourse` 仍可在 optional-analysis 自己的文本规则内 fallback 生成 formal/public issue linkage；启发式留在 approval-gated analysis skill，而不是 normalizer。

- 未完成：
  - 尚未新增独立 formal-only optional parser skill；当前 typed formal/public linkage 仍由既有 optional-analysis skill 内部推导。
  - 尚未拆分 `fetch-openaq` 为 metadata discovery / measurement fetch / archive backfill。
  - 尚未拆分 `normalize-fetch-execution` 的 queue runner / normalizer runner / execution receipt 职责。
  - 其他 fetch / normalize skill 的 source provenance、quality flags、coverage limitation 字段尚未逐一补齐。

- 新发现的问题：
  - `query-formal-signals` 历史文档暗示 formal normalizer 默认提供 typed formal metadata；这与 WP2 新边界冲突，本批已修正文档与测试预期。
  - `normalized_signal_index` 仍保留 typed metadata field 名称作为通用索引能力；本批没有删除这些字段，因为后续 optional parser / analysis skill 仍可能写入并查询这些字段。关键变化是 normalizer 不再写这些 typed 字段。

- 是否影响后续计划：
  - 不阻塞后续 WP2；下一步可继续处理 `fetch-openaq` 拆分或 `normalize-fetch-execution` 职责拆分。
  - 对后续 parser/analysis 的约束是：如果要重新产生 submitter type、issue、stance、concern、citation、route，必须作为 optional-analysis 或独立 parser 输出，并进入 `docs/openclaw-skill-rule-audit-ledger.md` 的规则审计与 approval consumption 链。

- 测试：
  - 已运行：`.venv/bin/python -m unittest tests.test_signal_plane_workflow tests.test_formal_public_workflow`
  - 结果：`8` 项通过。

## 12. 2026-04-27 Skills Batch 3 / WP2 代码交付回写

- 已完成：
  - 完成 `fetch-openaq` 原子化：保留兼容 `fetch` 路由，同时新增明确的 `fetch-metadata`、`fetch-measurements`、`fetch-archive-backfill` 三个子命令；三者均输出 `fetch_contract`，包含 `source_provenance / data_quality / temporal_scope / spatial_scope / coverage_limitations / research_judgement=none`。
  - 完成 `normalize-fetch-execution` 职责拆分：代码路径已拆为 `queue_runner`、`normalizer_runner`、`execution_receipt` 三个组件；执行 snapshot 和 skill 返回值均暴露 `execution_components`，每个 raw queue status 都带 `fetch_contract`，不再暗示 claim extraction、observation extraction、coverage scoring、readiness 或 promotion 链。
  - 完成 normalized signal 元数据加固：`signal_plane_normalizer.enrich_signal_metadata_fields()` 会在写库前补齐最小 `source_provenance / data_quality / temporal_scope / spatial_scope / coverage_limitations`，并显式标记 `research_judgement=none`。
  - 已把 OpenAQ normalizer 改为读取新 `fetch_contract` envelope，保留 provider/station/metric/timestamp/coordinate 原始证据，不推导 exposure、readiness、policy conclusion。
  - 已清理 normalize 类 `SKILL.md` 与 agent prompt 中的 `board-ready`、默认 claim extraction、默认 observation extraction 表述；normalize handoff 的 suggested skills 仅保留 query surface。
  - 已补回归：OpenAQ 三个子命令的 raw fetch contract、import execution component boundary、queue raw fetch contract、删除 `import_execution` export 与 raw artifact 后仍可通过 DB query / raw lookup 恢复 normalized/raw record。

- 未完成：
  - WP2 代码项本批已收口；未新增 formal-only optional parser skill，因为 WP2 的要求是把 issue/stance/concern/route typing 从 normalizer 移出，后续若新增 parser 应归入 WP4 审计与 approval 链。
  - 全仓库 query / reporting / archive 文档中仍可能有旧 coverage/promotion 词汇；这些属于 WP3-WP6 的后续清理面，不再属于 fetch/normalize 原子化主项。

- 新发现的问题：
  - 部分 query skill 的 `board_handoff.suggested_next_skills` 仍可能包含 optional extraction helper；它不影响 WP2 的 fetch/normalize 边界，但 WP3/WP4 需要继续改成 investigator evidence-bundle/finding 闭环或 approval-gated optional-analysis 口径。
  - 若未来要让 standalone normalizer 的 metadata 字段进入 `normalized_signal_index`，需要统一改它们的 local `insert_signals()` 去调用 shared `replace_signal_index_rows()`；当前 WP2 只要求 DB row 内保留 provenance/quality/limitation，不依赖这些字段做 indexed query。

- 是否影响后续计划：
  - 不阻塞 WP3；investigator 后续可直接从 DB query surfaces 读取带 provenance/quality/limitation 的 signal rows，再提交 finding / evidence bundle。
  - WP4 需要继续审计任何重新引入 typed formal parser、claim/observation extraction 或 evidence sufficiency scoring 的规则版本，不能把本批 query-only handoff 重新扩成默认启发式主链。

- 测试：
  - 已运行：`.venv/bin/python -m unittest tests.test_source_queue_rebuild tests.test_migrated_source_runtime_integration tests.test_signal_plane_workflow`
  - 结果：`21` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_formal_public_workflow tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`
  - 结果：`11` 项通过。

## 13. 2026-04-27 Skills 命名规范化回写

- 已完成：
  - 取消全部 skill 的 `eco-` 项目前缀；当前 `88` 个 skill 目录、frontmatter `name`、agent prompt、runtime registry/source queue contract、测试 fixture 均改为无项目前缀命名。
  - 按作用层级统一前缀：抓取层使用 `fetch-*`，归一化层使用 `normalize-*`，查询层使用 `query-*`；OpenAQ 内部子命令同步改为 `fetch-metadata / fetch-measurements / fetch-archive-backfill`。
  - 主脚本文件统一为 `<skill_name.replace("-", "_")>.py`，registry 新增校验测试，避免再次出现目录名、frontmatter、script path 不一致。
  - ingress 回归改为先提交 readiness opinion 并 materialize readiness，再批准 promotion transition；没有恢复缺少 readiness 的默认推进语义。

- 未完成：
  - 未重命名 runtime 包名、仓库目录、数据库文件名中的 `eco`，这些不是 skill id，本批不处理。

- 新发现的问题：
  - 旧 ingress 测试仍隐含“无 readiness 也可 promoted”的兼容假设；本批已按当前治理规则修正测试流程。
  - 文档归档区仍保留若干历史项目路径 `eco-concil-runtime/...`，属于项目路径引用，不影响 skill 命名规范。

- 是否影响后续计划：
  - 后续所有新增 skill 应直接使用层级前缀，不再加项目前缀；source queue / registry 测试会拦截 `eco-` skill id。
  - 对 WP3/WP4 无阻塞；只要求后续文档和计划沿用新命名。

- 测试：
  - 已运行：`.venv/bin/python -m py_compile $(find skills -path '*/scripts/*.py' -maxdepth 3 | sort)`
  - 结果：通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_source_queue_rebuild tests.test_migrated_source_runtime_integration tests.test_source_queue_governance tests.test_source_queue_family_memory tests.test_orchestration_ingress_workflow tests.test_agent_entry_gate tests.test_signal_plane_workflow tests.test_formal_public_workflow`
  - 结果：`43` 项通过。
