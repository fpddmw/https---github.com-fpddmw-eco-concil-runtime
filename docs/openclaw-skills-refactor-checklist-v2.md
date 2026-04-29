# OpenClaw Skills 重构清单 V2

## 1. 文档定位

本清单最初覆盖仓库内原始盘点的 `88` 个 skills，并按新目标架构重新分类：

1. 哪些 skill 保留
2. 哪些 skill 废弃
3. 哪些 skill 需要拆分重写
4. 哪些 skill 虽可保留，但必须降级为 optional / audited helper

分类统计（原始盘点口径）：

1. `16` 个 fetch skills
2. `16` 个 normalize skills
3. `21` 个 analysis / heuristic skills
4. `19` 个 board / council / phase-2 skills
5. `16` 个 query / history / reporting skills

验收补记：截至 2026-04-29 最新 active registry，旧 claim / observation / route / coverage 系列入口已删除并由 successor optional-analysis helper 替换，且 `build-normalization-audit` 已在破坏性清理中删除；当前 `skills/` 目录与 registry 均为 `79` 个 active skills。下文第 `18`、`21`、`24`、`26` 和最新验收回写为当前状态准线；第 `4` 节全量表保留原始盘点和迁移判定语境。

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
21. `freeze-report-basis`

### 3.2 最需要拆分的非原子 skill

1. `fetch-openaq`
2. `normalize-fetch-execution`
3. `build-normalization-audit`
4. `plan-round-orchestration`
5. `propose-next-actions`
6. `summarize-round-readiness`
7. `freeze-report-basis`
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
| `freeze-report-basis` | 保留并修改 | 强 / 是（强制） | 非原子 | 保留为 `freeze-report-basis` 类 skill，但不再自动主导 report basis 语义。 |

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
3. 当前 phase-2 orchestrator / readiness / report basis 主链必须退出默认控制权。
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

### 7.2 baseline-freeze track：基线盘点与冻结线

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

### 7.3 default-chain cleanup track：默认链去启发式与 source queue 语义清理

目标：让 runtime / source queue / agent entry 只暴露能力面，不再暗示固定调查路线。

任务：

1. 清理 `source_queue_profile.py` 中仍容易被误读为默认主链的 `planned-step / core_queue_default / downstream_hints` 文案，尤其是 claim、route、coverage、readiness、report basis 相关项。
2. 确认 `phase2_agent_entry_profile.py` 的 recommended skills 默认保持空或 capability-only。
3. 将 `plan-round-orchestration / propose-next-actions / summarize-round-readiness` 明确降为 moderator 可选 advisory，且必须有审批记录。
4. 更新相关 `SKILL.md`，删除 `board-ready`、默认 report basis、默认 coverage gate 等旧表达。

交付物：

1. source queue profile 清理补丁。
2. agent entry / runbook 文案回归。
3. 旧默认链语义检索清单。

硬验收：

1. 默认 agent entry 不推荐 claim-route-coverage 链。
2. controller 不因 source queue profile 自动生成议程。
3. 未审批 optional-analysis 仍被 preflight 阻断。

### 7.4 fetch/normalize hardening track：fetch / normalize 原子化与 DB 落库加固

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

### 7.5 query/investigator-submission track：query 与 investigator 提交闭环

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

### 7.6 optional-analysis helper governance：启发式 analysis skill 审计与降级

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

### 7.7 board/council boundary track：board / council / transition skill 收边界

目标：让 moderator 成为唯一阶段推进者，board skill 只写结构化调查状态。

任务：

1. 将 `open-investigation-round / freeze-report-basis / close-round` 继续绑定 transition request 与 operator approval。
2. 将 `freeze-report-basis` 改名或改义为 `freeze-report-basis` 类技能，强调冻结 DB evidence basis 而不是裁决研究结论。
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

### 7.8 decision-maker reporting pipeline track：reporting 重建为决策者报告生产线

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

### 7.9 policy-research fixture/regression track：场景 fixture、回归与最终交付

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
  - 多个 `SKILL.md` 当时仍引用已不存在的旧规划入口；后续批次已改为清理旧引用，不再补单独规划文档。
  - `source_queue_profile.py` 对外虽已不导出 `core_queue_default`，内部仍大量使用 `planned-step / core_queue_default / downstream_hints` 描述 claim、route、coverage、readiness、report basis 链，存在被误读为默认调查链的风险。
  - 部分 skill 文档仍使用 `board-ready`、`claim-observation`、`coverage readiness`、`report basis-stage artifact` 等旧目标表达，需要先做文档级语义清理，再做代码拆分。
  - reporting handoff 文档仍强调读取 report basis/readiness/supervisor artifact；代码侧已具备 DB wrapper 和 orphaned artifact 识别，文档需要同步为 DB-first/export-only 口径。

- 是否影响后续计划：
  - 不阻塞 skills 侧重构；这些发现应作为 baseline and default-chain cleanup tracks 的首批输入。
  - 下一步建议优先清理默认链语义与 skill 文档，再进入 fetch/normalize 拆分和 optional-analysis 审计，避免在旧文案基础上继续实现新功能。

## 10. 2026-04-27 Skills Batch 1 代码交付回写

- 已完成：
  - 完成 baseline and default-chain cleanup tracks 第一批代码落地：`source_queue_profile.py` 已从旧 `planned-step / core_queue_default / downstream_hints` 语义改成 capability / advisory / transition surface；所有 profile 的 `downstream_hints` 现在为空，`default_chain_eligible=false`，optional-analysis 统一标记为 `approval-gated-runtime-surface`。
  - `plan-round-orchestration / propose-next-actions / summarize-round-readiness / link-claims-to-observations / score-evidence-coverage` 等高风险 heuristic 不再通过 source queue 暗示默认主链，只能作为审批后的 optional advisory / legacy helper。
  - agent-entry operator surface 已补齐 skill approval 查询、请求、批准、拒绝、消费和 approved optional-analysis run command template。
  - 修复 approved optional-analysis command template：`--skill-approval-request-id` 现在由 `skill_command_hint / run_skill_command` 插入到 `--` 之前，避免被误传给 skill 脚本。
  - 更新代表性高风险 `SKILL.md` 与 agent prompt：orchestration、next actions、readiness、reporting handoff、claim extraction、claim-observation link、coverage scoring 均改成 DB-backed / optional / approval-required 口径，并移除当前文档中发现的旧规划入口引用。
  - 已形成第一版 heuristic 规则审计 freeze line；当前状态均为 `default-frozen / approval-required / audit-pending`，未标记任何规则为审计通过。后续规则审计记录统一收敛到 `docs/openclaw-optional-analysis-skills-refactor-workplan.md`。
  - registry 复核结果：当前 `skills/` 目录与 registry 均为 `88` 个 skill；`32` 个 skill 声明 `requires_operator_approval`；source queue summary 覆盖 `88/88`。

- 未完成：
  - 尚未逐个审完全部 `SKILL.md` 与脚本实际读写；本批只处理第一批默认链风险最高的 source queue、agent entry surface 和代表性文档。
  - 规则审计台账仍是 freeze-line 初版；尚未补每条规则的完整样例、偏差量化和人工审计结论。
  - `fetch-openaq`、`normalize-fetch-execution`、formal normalizer 拆分仍属于 fetch/normalize hardening track，未在本批实施。
  - reporting 生产线重构仍属于 decision-maker reporting pipeline track；本批只把 reporting handoff 文档改成 DB-first/export-only 口径。

- 新发现的问题：
  - operator runbook 之前的 approved optional-analysis 示例把 `--skill-approval-request-id` 追加在 skill args 之后，实际会被 `--` 分隔后误传给 skill 脚本；本批已修复并补回归。
  - `source_queue_profile.py` 原本对外不导出 `core_queue_default`，但 profile 输出仍通过 `planned-step` 和非空 `downstream_hints` 保留链式暗示；本批已清空输出并加回归。
  - 仓库中仍有其他 normalizer / query / archive 文档残留 `board-ready`、旧 coverage/report basis 表述；这些不再位于默认入口，但需要在 fetch/normalize through optional-analysis governance tracks 继续清理。

- 是否影响后续计划：
  - 不阻塞后续计划；fetch/normalize hardening track 可以在“默认链已冻结、optional-analysis 已审批化、operator surface 可见审批链”的基础上继续推进 fetch/normalize 原子化。
  - 后续任何新增 source queue 或 agent-entry surface 都必须保持 `default_chain_eligible=false`，并且不得重新暴露 claim-route-coverage 链式 `downstream_hints`。
  - 后续规则审计若要把某 heuristic 从 `audit-pending` 改成可用，必须更新 `docs/openclaw-optional-analysis-skills-refactor-workplan.md` 中的 freeze line / audit records，并保留审批/消费记录。

- 测试：
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate tests.test_skill_approval_workflow`
  - 结果：`14` 项通过。

## 11. 2026-04-27 Skills Batch 2 代码交付回写

- 已完成：
  - 启动 fetch/normalize hardening track 的 formal normalizer 收缩：`normalize-regulationsgov-comments-public-signals` 与 `normalize-regulationsgov-comment-detail-public-signals` 已移除 `build_formal_signal_semantics()` 调用，不再在 normalizer 内派生 `submitter_type / issue_labels / stance_hint / concern_facets / evidence_citation_types / route_hint`。
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
  - `query-formal-signals` 历史文档暗示 formal normalizer 默认提供 typed formal metadata；这与 fetch/normalize hardening track 新边界冲突，本批已修正文档与测试预期。
  - `normalized_signal_index` 仍保留 typed metadata field 名称作为通用索引能力；本批没有删除这些字段，因为后续 optional parser / analysis skill 仍可能写入并查询这些字段。关键变化是 normalizer 不再写这些 typed 字段。

- 是否影响后续计划：
  - 不阻塞后续 fetch/normalize hardening track；下一步可继续处理 `fetch-openaq` 拆分或 `normalize-fetch-execution` 职责拆分。
  - 对后续 parser/analysis 的约束是：如果要重新产生 submitter type、issue、stance、concern、citation、route，必须作为 optional-analysis 或独立 parser 输出，并进入 `docs/openclaw-optional-analysis-skills-refactor-workplan.md` 的规则审计与 approval consumption 链。

- 测试：
  - 已运行：`.venv/bin/python -m unittest tests.test_signal_plane_workflow tests.test_formal_public_workflow`
  - 结果：`8` 项通过。

## 12. 2026-04-27 Skills Batch 3 / fetch/normalize hardening track 代码交付回写

- 已完成：
  - 完成 `fetch-openaq` 原子化：保留兼容 `fetch` 路由，同时新增明确的 `fetch-metadata`、`fetch-measurements`、`fetch-archive-backfill` 三个子命令；三者均输出 `fetch_contract`，包含 `source_provenance / data_quality / temporal_scope / spatial_scope / coverage_limitations / research_judgement=none`。
  - 完成 `normalize-fetch-execution` 职责拆分：代码路径已拆为 `queue_runner`、`normalizer_runner`、`execution_receipt` 三个组件；执行 snapshot 和 skill 返回值均暴露 `execution_components`，每个 raw queue status 都带 `fetch_contract`，不再暗示 claim extraction、observation extraction、coverage scoring、readiness 或 report basis 链。
  - 完成 normalized signal 元数据加固：`signal_plane_normalizer.enrich_signal_metadata_fields()` 会在写库前补齐最小 `source_provenance / data_quality / temporal_scope / spatial_scope / coverage_limitations`，并显式标记 `research_judgement=none`。
  - 已把 OpenAQ normalizer 改为读取新 `fetch_contract` envelope，保留 provider/station/metric/timestamp/coordinate 原始证据，不推导 exposure、readiness、policy conclusion。
  - 已清理 normalize 类 `SKILL.md` 与 agent prompt 中的 `board-ready`、默认 claim extraction、默认 observation extraction 表述；normalize handoff 的 suggested skills 仅保留 query surface。
  - 已补回归：OpenAQ 三个子命令的 raw fetch contract、import execution component boundary、queue raw fetch contract、删除 `import_execution` export 与 raw artifact 后仍可通过 DB query / raw lookup 恢复 normalized/raw record。

- 未完成：
  - fetch/normalize hardening track 代码项本批已收口；未新增 formal-only optional parser skill，因为 fetch/normalize hardening track 的要求是把 issue/stance/concern/route typing 从 normalizer 移出，后续若新增 parser 应归入 optional-analysis helper governance 审计与 approval 链。
  - 全仓库 query / reporting / archive 文档中仍可能有旧 coverage/report basis 词汇；这些属于 query through reporting tracks 的后续清理面，不再属于 fetch/normalize 原子化主项。

- 新发现的问题：
  - 部分 query skill 的 `board_handoff.suggested_next_skills` 仍可能包含 optional extraction helper；它不影响 fetch/normalize hardening track 的 fetch/normalize 边界，但 query/investigator-submission and optional-analysis governance tracks 需要继续改成 investigator evidence-bundle/finding 闭环或 approval-gated optional-analysis 口径。
  - 若未来要让 standalone normalizer 的 metadata 字段进入 `normalized_signal_index`，需要统一改它们的 local `insert_signals()` 去调用 shared `replace_signal_index_rows()`；当前 fetch/normalize hardening track 只要求 DB row 内保留 provenance/quality/limitation，不依赖这些字段做 indexed query。

- 是否影响后续计划：
  - 不阻塞 query/investigator-submission track；investigator 后续可直接从 DB query surfaces 读取带 provenance/quality/limitation 的 signal rows，再提交 finding / evidence bundle。
  - optional-analysis helper governance 需要继续审计任何重新引入 typed formal parser、claim/observation extraction 或 evidence sufficiency scoring 的规则版本，不能把本批 query-only handoff 重新扩成默认启发式主链。

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
  - ingress 回归改为先提交 readiness opinion 并 materialize readiness，再批准 report-basis transition；没有恢复缺少 readiness 的默认推进语义。

- 未完成：
  - 未重命名 runtime 包名、仓库目录、数据库文件名中的 `eco`，这些不是 skill id，本批不处理。

- 新发现的问题：
  - 旧 ingress 测试仍隐含“无 readiness 也可 frozen”的兼容假设；本批已按当前治理规则修正测试流程。
  - 旧历史文档曾保留若干项目路径 `eco-concil-runtime/...`；这些属于历史路径引用，不影响 skill 命名规范，也不再作为当前文档入口保留。

- 是否影响后续计划：
  - 后续所有新增 skill 应直接使用层级前缀，不再加项目前缀；source queue / registry 测试会拦截 `eco-` skill id。
  - 对 query/investigator-submission and optional-analysis governance tracks 无阻塞；只要求后续文档和计划沿用新命名。

- 测试：
  - 已运行：`.venv/bin/python -m py_compile $(find skills -path '*/scripts/*.py' -maxdepth 3 | sort)`
  - 结果：通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_source_queue_rebuild tests.test_migrated_source_runtime_integration tests.test_source_queue_governance tests.test_source_queue_family_memory tests.test_orchestration_ingress_workflow tests.test_agent_entry_gate tests.test_signal_plane_workflow tests.test_formal_public_workflow`
  - 结果：`43` 项通过。

## 14. 2026-04-27 Skills Batch 4 / query/investigator-submission track 代码交付回写

- 已完成：
  - 完成 query/investigator-submission track query evidence basis 加固：`query-public-signals / query-formal-signals / query-environment-signals / query-normalized-signal / query-raw-record` 的每条结果现在都返回 item-level `evidence_refs` 与 `evidence_basis`，包含 canonical object kind、signal id、artifact ref、source provenance、data quality、temporal/spatial scope、coverage limitations。
  - query skill 的 `board_handoff.suggested_next_skills` 已从默认 optional extraction/linkage helper 改为 `query-normalized-signal / query-raw-record / submit-finding-record / submit-evidence-bundle / post-discussion-message`，不再把 claim extraction、observation extraction、formal-public linkage 或 representation gap analysis 暗示为默认 investigator 下一步。
  - investigator runbook 的必要约定已收敛进本文档，不再单独保留：默认闭环为 `fetch / normalize -> query / lookup -> finding -> evidence bundle -> optional proposal`，query 结果必须携带 item-level `evidence_refs` 与 `evidence_basis`，proposal 必须通过 `--response-to-id` / `--lineage-id` 锚定 finding 或 evidence bundle，challenger review 必须引用 evidence bundle 或具体证据 refs。
  - 补齐 `post-review-comment` kernel direct write 命令：接入 CLI、既有 access policy、ledger、artifact audit、`review-comment` canonical DB 表与 `query-council-objects` 查询面。
  - agent entry/operator surface 已新增 `query_review_comments_command` 与 `post_review_comment_command_template`，`submit_council_proposal_command_template` 也加入 `--response-to-id <finding_or_bundle_id>` 和 `--lineage-id <finding_or_bundle_id>`，使 proposal 默认锚定 finding/evidence bundle basis。
  - `open-challenge-ticket` 已支持 `--evidence-bundle-id`，challenge ticket 会把 `evidence-bundle:<id>` 写入 evidence refs，并在 lineage/source ids/raw payload 中保留 bundle id，满足 challenger 对 evidence bundle 的交叉引用约定。
  - 更新 query / lookup / proposal / challenge 相关 `SKILL.md` 与 agent prompt，移除本批触达路径中的 `board-ready`、旧 phase plan 引用和 proposal-first 口径。
  - 新增最小闭环回归：从 DB query 结果提交 finding、evidence bundle、proposal、review comment、challenge ticket，并验证 moderator/report-editor 可通过 `query-council-objects` item-level 查询这些对象。

- 未完成：
  - query/investigator-submission track 范围内没有新的功能性遗留；proposal/readiness opinion 仍保留为有效 deliberation 对象，但已不是 investigator 默认调查记录。
  - 仍未清理全仓库所有 reporting/archive 文档中的旧 coverage/report basis 表述；这些属于 optional-analysis through reporting tracks 的后续清理面。
  - `post-review-comment` 当前是 kernel direct command，不是独立 skill 目录；本批选择复用已存在的 canonical `review-comment` 表与 kernel direct write 模式，未新增第 89 个 skill。

- 新发现的问题：
  - `review-comment` 的 schema、contract 与 access policy 已存在，但 CLI 与 agent-entry surface 之前没有暴露该写入口；这会让 challenger 只能借 `discussion-message` 或 `challenge` 表达 review。本批已补齐。
  - query skill 之前虽然有顶层 `artifact_refs`，但单条 `results[]` 没有稳定的 item-level evidence basis，investigator 从结果到 finding/evidence bundle 需要隐式拼 ref。本批已统一补齐。
  - `open-challenge-ticket` 之前只能通过 `linked_artifact_ref` 表达证据交叉引用，不能显式锚定 evidence bundle；本批已增加 bundle id 引用。

- 是否影响后续计划：
  - 不阻塞 optional-analysis helper governance。相反，optional-analysis helper governance 可以建立在“默认 investigator loop 不调用 optional heuristic、所有 optional-analysis 仍需 approval、finding/evidence bundle 已成为一等调查记录”的前提上继续做规则审计与降级。
  - 后续新增 query surface 必须继续返回 item-level `evidence_refs` 与 `evidence_basis`；新增 proposal/challenge/review 路径也应优先引用 finding/evidence bundle，而不是绕过调查记录直接写 judgement。

- 测试：
  - 已运行：`.venv/bin/python -m unittest tests.test_signal_plane_workflow tests.test_council_submission_workflow tests.test_agent_entry_gate`
  - 结果：`18` 项通过。

## 15. 2026-04-29 Skills Batch 5 / board/council boundary track 代码交付回写

- 已完成：
  - `update-hypothesis-status` 已改成 evidence-backed board mutation：本次调用、已存在 hypothesis 或已接受 proposal 必须提供至少一个 evidence ref；无 evidence ref 时返回 `status=blocked`，不写 canonical hypothesis。
  - `post-board-note` 已降为 human-readable board note：输出显式标记 `canonical_judgement=false`，`board_handoff.suggested_next_skills` 清空，不再把 note 暗示成 finding、readiness opinion 或 report basis。
  - `materialize-board-brief` 已降为 human-readable export：删除自动 `Immediate Next Moves` 生成，改为只展示 open board item counts，并清空 handoff next-skill 建议。
  - `freeze-report-basis` 保留兼容 skill id，但语义已明确为 `freeze-report-basis`：输出新增 `basis_object_kind=report-basis-freeze`、`transition_semantics=freeze-report-basis`、`report_basis_selection_mode=freeze-report-basis-v1`，并清空 report basis handoff 的默认下一步建议。
  - `skill_registry.py` 已同步 board/council boundary track 边界：`update-hypothesis-status` 输入面包含 finding / evidence-bundle / proposal，`freeze-report-basis` 输出面包含 `report-basis-freeze`。
  - 所有直接调用 `update-hypothesis-status` 的 workflow 测试夹具已显式传入 evidence ref；新增无 evidence ref 被阻断的 board mutation 回归。

- 未完成：
  - `freeze-report-basis` 的 skill id、transition kind、DB 表名仍保留历史 `report basis` 命名，以避免本批扩大为 schema/CLI 迁移；本批完成的是语义收缩和输出标记。
  - publish/finalize 仍未统一纳入独立 publish transition kind；这仍属于 decision-maker reporting pipeline 审批模型范围。
  - `post-board-note` 仍写入 deliberation board note 表和 JSON export；它只是明确不再承载 canonical judgement。

- 新发现的问题：
  - benchmark replay 中 artifact drift 数量会随 board brief/report-basis 输出语义变化增加；测试已改为断言关键 `orchestration_plan` drift 存在，而不是固定 drift 数量为 `1`。
  - 历史测试中很多 hypothesis 更新只传 `linked_claim_id`，没有 evidence ref；这些 fixture 反映旧“board judgement 可裸写”的习惯，本批已全部改为显式 evidence-backed。
  - `freeze-report-basis` 仍有兼容字段 `report_basis_status / selected_coverages / basis_selection_mode`，后续若要彻底改名为 `freeze-report-basis`，需要单独迁移 reporting、benchmark、runtime state surface 和历史 artifact export。

- 是否影响后续计划：
  - 不阻塞 decision-maker reporting pipeline track；相反，reporting 可以在 `report-basis-freeze` 语义上继续拆分 evidence packet / decision packet / report packet。
  - 后续新增 board mutation 必须延续本批约束：有 canonical judgement 的写入必须引用 finding / evidence bundle / proposal / evidence ref；human-readable export 不得携带默认 next-action 或 phase advice。
  - 若后续要重命名 `freeze-report-basis`，应作为 breaking schema/CLI migration 单独推进，不应在 reporting 重构中隐式完成。

- 测试：
  - 已运行：`.venv/bin/python -m py_compile skills/update-hypothesis-status/scripts/update_hypothesis_status.py skills/post-board-note/scripts/post_board_note.py skills/materialize-board-brief/scripts/materialize_board_brief.py skills/freeze-report-basis/scripts/freeze_report_basis.py eco-concil-runtime/src/eco_council_runtime/kernel/skill_registry.py`
  - 结果：通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_board_workflow tests.test_investigation_workflow tests.test_runtime_kernel`
  - 结果：`66` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_benchmark_replay_workflow`
  - 结果：`2` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_archive_history_workflow tests.test_benchmark_replay_workflow tests.test_board_workflow tests.test_council_autonomy_flow tests.test_council_query_surface tests.test_decision_trace_workflow tests.test_investigation_workflow tests.test_orchestration_ingress_workflow tests.test_orchestration_planner_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_reporting_workflow tests.test_runtime_kernel tests.test_supervisor_simulation_regression`
  - 结果：`125` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`
  - 结果：`10` 项通过。
  - 已运行：`git diff --check`
  - 结果：通过。

## 16. 2026-04-29 Skills Batch 6 / decision-maker reporting pipeline track 代码交付回写

- 已完成：
  - `materialize-reporting-handoff` 已从单一 handoff 封装改为显式 packet 化输出：`evidence_packet / decision_packet / report_packet`，并在 canonical reporting handoff raw JSON 中保留 `evidence_index / uncertainty_register / residual_disputes / policy_recommendations`。
  - `draft-council-decision` 已改为 decision memo drafter：优先消费 handoff `decision_packet`，输出 `decision_packet / memo_sections`，不再把 reporting posture 重新解释成 phase-2 结论。
  - `draft-expert-report` 已改为章节化 role report drafter：读取 handoff `report_packet` 与 DB `report-section-draft` rows，输出 `report_sections / section_draft_refs / evidence_index / uncertainty_register / residual_disputes / policy_recommendations`。
  - `materialize-final-publication` 已改为 decision-maker report assembler：输出 `decision_maker_report`，并显式包含证据索引、引用索引、风险与不确定性、剩余争议、建议措施和 audit refs。
  - reporting skills 的 `board_handoff.suggested_next_skills` 已移除 `propose-next-actions / open-falsification-probe / post-board-note` 这类旧默认建议；hold path 只提示 DB council/reporting basis 写入面，例如 finding、evidence bundle、proposal、readiness opinion。
  - `canonical_contracts.py / deliberation_plane.py / skill_registry.py` 已同步 decision-maker reporting pipeline track 字段与 reporting 输入面；publish/finalize 继续通过 `requires_operator_approval=True` 的 skill approval 链执行，不新增本批 schema 级 publish transition。
  - 触达 reporting skills 的 `SKILL.md` 与 agent prompt 已同步为 DB evidence basis、packet、decision-maker report、operator approval 口径。

- 未完成：
  - 未把 `freeze-report-basis`、`report_basis_status`、`report_basis_path` 等历史命名做 schema/CLI 级改名；本批仍沿用既有 DB 表和 wrapper，只在 reporting 输出中明确 `report-basis-freeze` 与 packet 语义。
  - 未新增独立 `materialize-evidence-packet / materialize-decision-packet / materialize-report-packet` 三个 skill；本批选择在现有 `materialize-reporting-handoff` 内形成等价清晰职责，避免扩大 skill id 与 registry 迁移。
  - 未重写 `sociologist / environmentalist` 旧 role id；本批只把 role report 章节内容改成 public-discourse/community-impact 与 environmental-evidence/risk 口径。

- 新发现的问题：
  - 当前 ready-round fixture 仍可能没有 DB `finding-record` 或 `evidence-bundle`，因此 `key_findings` 可以为空；final report 会把这标成 report-basis gap，而不是从 coverage/helper 结果伪造 finding。
  - final publication 过去只有 `evidence-index` section；本批增加显式 `citation-index`，否则决策者报告的引用索引要求不够清楚。
  - reporting 仍需保留 `coverage_source=missing-coverage` 等兼容 trace 字段，原因是 reporting contract 还复用历史 D1 trace shape；本批未做 trace contract 改名迁移。

- 是否影响后续计划：
  - 不阻塞 policy-research fixture/regression track。相反，policy-research fixture/regression track 可以直接用 `evidence_packet -> decision_packet -> report_packet -> decision_maker_report` 做端到端 case fixture。
  - 后续若要彻底消除 report basis 命名债，应单独做 breaking DB/schema/query migration，不应在 report assembler 中隐式改名。
  - 后续 case fixture 应补 DB-backed `finding-record / evidence-bundle / report-section-draft`，让 final report 的 key findings 和 recommendations 来自明确 report basis。

- 测试：
  - 已运行：`.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/canonical_contracts.py eco-concil-runtime/src/eco_council_runtime/kernel/deliberation_plane.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py skills/draft-council-decision/scripts/draft_council_decision.py skills/draft-expert-report/scripts/draft_expert_report.py skills/publish-expert-report/scripts/publish_expert_report.py skills/publish-council-decision/scripts/publish_council_decision.py skills/materialize-final-publication/scripts/materialize_final_publication.py`
  - 结果：通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface`
  - 结果：`21` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_reporting_publish_workflow.ReportingPublishWorkflowTests.test_final_publication_ready_round_collects_reports_and_decision`
  - 结果：`1` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate`
  - 结果：`10` 项通过。

## 17. 2026-04-29 Skills Batch 7 / policy-research fixture/regression track 代码交付回写

- 已完成：
  - 新增 `tests/test_policy_research_case_fixtures.py`，落地三类 policy research case fixture：政策争议 case、舆情/正式记录混合 case、可核实经验事件 case。
  - 三类 fixture 均走本地 fetch/import queue、normalize、query、finding、evidence bundle、challenger review/challenge、moderator transition request、operator approval、reporting handoff、decision draft、role report、decision publish、final publication 路径。
  - 新增 `submit_report_basis_records(...)` 测试辅助，让 policy research fixture 明确补 `finding-record / evidence-bundle / report-section-draft`，final report 的 key findings 和 evidence index 来自 DB canonical objects，而不是 optional-analysis helper cue。
  - 修复 `materialize-reporting-handoff` 对 finding basis 的读取：handoff 现在用 canonical query kind `finding` 读取 DB rows，并在 reporting evidence index 中继续以 `finding-record` basis role 暴露，避免 ready round 有 finding 但 `key_findings` 为空。
  - Policy research fixture 中 `summarize-round-readiness` 不再由测试 helper 隐式调用，而是走 `request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id`，并断言 consumption control object 存在。
  - `tests/test_skill_approval_workflow.py` 已补“已消费 skill approval request 不可复用”的回归；optional-analysis 审批链覆盖 request、approval、consumption、reuse block。
  - `scaffold-mission-run` 生成的 source task 文案已去掉 `claim-candidates / observation-candidates / corroborate-or-contradict` 表述，改成 investigator query、finding、evidence-bundle 口径。
  - Policy research case 在删除 `reporting_handoff / council_decision / expert_report / report_basis_freeze / supervisor_state` 导出物后，仍能从 DB 恢复并生成 `decision-maker-environmental-policy-report`。

- 未完成：
  - `formal_signal_semantics.py` 仍未物理拆成 versioned taxonomy family records；本批没有推进 taxonomy schema 迁移。
  - `analysis_plane.py` 的历史 analysis kind / query object 命名仍未迁移；本批只修 report basis query alias 与 policy-research fixture/regression track e2e fixture。
  - 本批没有把 `freeze-report-basis / report_basis_status` 等历史命名做 breaking CLI/schema 改名。
  - 没有运行全仓所有测试；只运行 policy research case fixture、reporting、approval、source queue、agent entry 的直接相关最小集合。

- 新发现的问题：
  - `materialize-reporting-handoff` 之前查询 `finding-record`，但 council canonical query surface 实际支持 `finding`；这会让 DB finding 不能进入 handoff `key_findings`，本批已修正并由 policy research fixture 覆盖。
  - `scaffold-mission-run` 的任务文案还残留旧 claim/observation 目标表达；虽然不支配 runtime 行为，但会误导 operator/agent runbook，本批已改为政策研究调查闭环表述。
  - Policy research fixture 证明 report basis 可以由 direct DB write surface 承接；后续不应通过恢复 coverage/helper 输出填充 final report finding。
  - 当前 reporting-ready 判定仍依赖 approved readiness summary；这不应被理解为 optional-analysis 默认主链，而应保留为 moderator 请求、operator 审批、一次性消费的阶段推进依据。

- 是否影响后续计划：
  - 不阻塞整批重构交付；policy research fixture 已提供最终 targeted regression 与三类 case fixture，可作为整批交付验收入口。
  - 后续整批交付前仍应单列残留风险：taxonomy family records、analysis kind 命名迁移、report basis 命名债、以及 full regression 未覆盖面。
  - 若后续新增 case fixture，应沿用本批模式：helper cue 只能作为 audit/appendix，报告正文 finding 必须由 DB `finding / evidence-bundle / report-section-draft / proposal / readiness` basis 承接。

- 测试：
  - 已运行：`.venv/bin/python -m py_compile tests/_workflow_support.py tests/test_policy_research_case_fixtures.py tests/test_skill_approval_workflow.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py skills/scaffold-mission-run/scripts/scaffold_mission_run.py`
  - 结果：通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_policy_research_case_fixtures -v`
  - 结果：`1` 项通过，覆盖 `3` 个 subTest case。
  - 已运行：`.venv/bin/python -m unittest tests.test_skill_approval_workflow tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate -v`
  - 结果：`15` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_source_queue_rebuild -v`
  - 结果：`8` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface -v`
  - 结果：`21` 项通过。
  - 已运行：`git diff --check`
  - 结果：通过。

## 18. 2026-04-29 Skills Batch 8 / final acceptance hardening track 最终验收硬化回写

- 已完成：
  - `analysis_plane.py` 已为 analysis kind 增加 `analysis_kind_governance` 元数据；高风险旧对象如 `evidence-coverage / claim-observation-link / observation-candidate / merged-observation / formal-public-link / representation-gap / diffusion-edge` 被显式标记为 `legacy-frozen-compatibility-query-only`。
  - analysis query surface 在零结果时也会返回请求 kind 的治理元数据，明确 `default_chain_eligible=false`、`phase_gate_eligible=false`、`report_basis_eligible=false`、`requires_explicit_approval=true`，并列出报告使用必须经由 DB `finding-record / evidence-bundle / proposal / review-comment / report-section-draft`。
  - `formal_signal_semantics.py` 已补 versioned taxonomy family records：issue、concern、citation、stance、submitter type、route hint 均带 `formal-public-taxonomy-freeze-2026-04-29`、approval/audit refs、candidate-only 语义和不可作为 phase/report basis 的标记。
  - `skill_registry.py` 已把 `apply-approved-formal-public-taxonomy` 的 optional-analysis helper governance 同步到同一 taxonomy freeze version。
  - 默认 agent entry 的 role capability surface 已移除旧 analysis query commands；agent 默认入口保留 DB query、finding/evidence-bundle/proposal/readiness 写入面，以及 optional-analysis approval/run templates，不再把 frozen analysis kind 暴露为默认角色工作入口。
  - `open-investigation-round` fallback task 已从 `claim-candidates / observation-candidates` 输出改为 `public-discourse-evidence / environment-evidence`，并会把历史 source task 中的旧 output kind 自动改写为新 evidence 口径。
  - 新增 final guardrail 回归：覆盖 legacy analysis kind 治理元数据、formal/public taxonomy family freeze line、agent entry 无默认 analysis commands、open-round fallback 无旧 output kind。

- 未完成：
  - 该历史项已被第 24 节覆盖；旧 promotion CLI/schema/DB 命名已删除，当前 `report_basis_*` 是新架构字段。
  - 未删除 `build-normalization-audit` 中用于读取历史 claim/observation candidate result set 的兼容参数；该 skill 仍是 operator QA optional-analysis，需审批后执行，不进入默认主链。
  - 未物理删除 `analysis_objects.py / canonical_contracts.py` 中旧 canonical object contract；本批将其冻结为兼容查询/审计面，而不是在最终验收前做破坏性 schema 删除。
  - 未运行全仓所有测试；已运行最终验收直接相关的 targeted regression。

- 新发现的问题：
  - `open-investigation-round` 仍残留旧 `claim-candidates / observation-candidates` fallback output，这是阶段入口层面的旧语义泄漏；本批已修复并补回归。
  - 默认 agent entry 中旧 analysis kind 查询命令会把 frozen helper surface 暗示为默认角色入口；本批已移除，optional-analysis 只能通过审批模板进入。
  - analysis kind 命名债短期内不宜通过删除 query kind 解决；否则会破坏历史 DB replay/query。当前收口方式是保留可查询兼容面，同时让 query surface 自带冻结治理标记。

- 是否影响后续计划：
  - 不阻塞整批重构验收。Final guardrail 已把 policy research fixture 中的两个主要阻塞项（taxonomy family records、analysis kind 治理标记）收口为可测试边界。
  - 后续若推进彻底命名迁移，应作为单独 breaking migration：包含 DB schema、CLI 参数、historical replay、benchmark artifact、reporting trace 字段的统一迁移。
  - 最终交付说明中应把剩余项表述为非阻塞命名/兼容债，而不是 runtime/kernel/agent 默认链路风险。

- 测试：
  - 已运行：`.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/kernel/analysis_plane.py eco-concil-runtime/src/eco_council_runtime/formal_signal_semantics.py eco-concil-runtime/src/eco_council_runtime/kernel/skill_registry.py eco-concil-runtime/src/eco_council_runtime/phase2_agent_entry_profile.py skills/open-investigation-round/scripts/open_investigation_round.py skills/scaffold-mission-run/scripts/scaffold_mission_run.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py tests/test_optional_analysis_guardrails.py tests/test_agent_entry_gate.py tests/test_board_workflow.py tests/test_policy_research_case_fixtures.py tests/test_skill_approval_workflow.py`
  - 结果：通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails -v`
  - 结果：`19` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_board_workflow.BoardWorkflowTests.test_open_investigation_round_preserves_prior_round_and_carries_state_from_db tests.test_board_workflow.BoardWorkflowTests.test_open_investigation_round_fallback_uses_shared_source_role_catalog tests.test_board_workflow.BoardWorkflowTests.test_open_investigation_round_reads_db_backed_actions_when_export_is_missing -v`
  - 结果：`3` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_lists_no_legacy_claim_cluster_result_sets_after_successor_helpers tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_queries_no_legacy_claim_cluster_items_after_successor_helpers tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_does_not_inline_legacy_controversy_map_fallback tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_does_not_inline_legacy_issue_cluster_fallback tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_analysis_query_reports_invalid_analysis_kind -v`
  - 结果：`5` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_policy_research_case_fixtures -v`
  - 结果：`1` 项通过，覆盖 `3` 个 subTest case。
  - 已运行：`.venv/bin/python -m unittest tests.test_skill_approval_workflow tests.test_source_queue_rebuild -v`
  - 结果：`13` 项通过。
  - 已运行：`.venv/bin/python -m unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface -v`
  - 结果：`21` 项通过。
  - 已运行：`git diff --check`
  - 结果：通过。

## 19. 2026-04-29 验收审阅回写

- 已完成：
  - 复核 board/reporting/case-fixture/final-acceptance tracks 最新代码后，skills 侧硬验收通过：默认 investigator loop 不调用 optional-analysis helper；query 结果可形成 item-level evidence basis；finding/evidence bundle/report-section-draft 是报告正文 basis；helper cue 默认只能作为 approval-gated advisory/audit surface。
  - `materialize-reporting-handoff` 已确认通过 DB canonical kind `finding` 恢复 finding basis，并在 evidence index 中以 `finding-record` 暴露。
  - 三类 policy research case fixture 已证明 `fetch/import -> normalize -> query -> finding -> evidence bundle -> review/challenge -> transition approval -> decision-maker report` 路径可运行。
  - agent entry 默认 capability surface 已不再暴露 legacy analysis query commands。

- 未完成：
  - 未做历史 skill id / analysis kind / report basis trace 字段的 breaking rename。
  - `build-normalization-audit` 等 operator QA 兼容参数仍存在，但属于 approval-gated optional-analysis。
  - freeze line 仍是 `audit-pending`，不是完整人工审计记录。
  - 未运行全仓 discover。

- 新发现的问题：
  - 旧 checklist/workplan 中仍有未勾选的“物理删除/彻底改名/完整审计”类条目；这些不影响默认链验收，但应从“硬功能缺口”改列为后续迁移债。
  - `freeze-report-basis` 的兼容命名和内部 legacy helper 函数容易造成误读；当前测试表明它不再作为 runtime 研究判断来源，但后续应单独清理。

- 是否影响后续计划：
  - 不阻塞本轮 skills 重构验收。
  - 后续新增 skill 必须延续本轮约束：默认链只做 fetch/normalize/query/DB write；optional helper 必须带 approval、audit、evidence refs、lineage/provenance 与 report-basis 非直通标记。

- 本次验收实际运行：
  - `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_skill_approval_workflow tests.test_source_queue_rebuild tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_runtime_kernel tests.test_board_workflow -v`
  - 结果：`111` 项通过。
  - `git diff --check`：通过。

## 26. 2026-04-29 破坏性清理补记：旧兼容 skill / advisory 接口删除

- 已完成：
  - 删除 active `build-normalization-audit` skill、脚本、agent yaml、registry policy 和 source queue profile；该 skill 不再作为 operator QA 兼容入口保留。
  - 删除 direct council advisory compiler 与测试；agent entry/controller/planning profile 不再支持 direct advisory materialization。
  - 删除 `--refresh-advisory-plan`、`agent_advisory_plan_*`、`agent-advisory / advisory-only` planner mode。
  - 删除 `kernel/investigation_planning.py`、`phase2_fallback_planning.py`、`kernel/phase2_contract.py` 兼容门面。
  - `plan-round-orchestration` 改为 queue-owned runtime plan，`direct_council_queue` 字段改为 `council_proposal_queue`。
  - 当前 active skills 为 `79` 个，optional-analysis active helper 为 `16` 个。

- 未完成：
  - 旧 analysis kind / canonical contract / query object 命名尚未完成物理迁移。
  - 内部 helper 共享模块 `phase2_fallback_*` 仍需后续重命名或拆分。

- 新发现的问题：
  - `build-normalization-audit` 曾被记录为“可审批保留”，但它本身仍读取旧 claim/observation candidate 兼容面；按本次要求已删除。
  - advisory 旧接口跨 CLI、artifact、planning mode、controller source、state surface 多处残留。

- 是否影响后续计划：
  - 影响外部旧调用和历史测试；失败不应通过恢复旧入口解决。
  - 后续计划应聚焦 DB/query/schema 命名迁移与内部 module rename。

- 本次实际运行：
  - `.venv/bin/python -m unittest tests.test_investigation_contracts tests.test_agent_entry_gate tests.test_orchestration_planner_workflow tests.test_runtime_kernel -v`：`58` 项通过。

## 20. 2026-04-29 测试命名清理回写

- 已完成：
  - 将旧编号命名的 optional-analysis guardrail 测试模块改为 `tests/test_optional_analysis_guardrails.py`。
  - 将旧编号命名的 case fixture 测试模块改为 `tests/test_policy_research_case_fixtures.py`。
  - 清理测试函数、测试类、共享 helper 和 fixture provenance 中的 `policy-research/final-acceptance/optional-analysis successor` 阶段编号命名，改为 `optional-analysis / policy-research / successor-helper / research-issue` 等功能命名。
  - 同步本文档和 optional-analysis helper governance workplan 中的测试模块路径与精确测试函数名。

- 未完成：
  - 本节原先未覆盖 runtime contract breaking rename；该项已在第 21 节收口。

- 新发现的问题：
  - 旧测试 helper 名称会把阶段编号误当成行为语义；测试代码已不再用该类编号命名测试行为。

- 是否影响后续计划：
  - 不影响既有功能；后续新增测试应以行为/能力命名，不再用 work package 编号命名模块、类、函数或 fixture。

- 本次实际运行：
  - `.venv/bin/python -m py_compile tests/_workflow_support.py tests/test_optional_analysis_guardrails.py tests/test_policy_research_case_fixtures.py tests/test_analysis_workflow.py tests/test_runtime_kernel.py tests/test_investigation_workflow.py tests/test_board_workflow.py tests/test_reporting_workflow.py tests/test_reporting_publish_workflow.py tests/test_reporting_query_surface.py tests/test_decision_trace_workflow.py tests/test_archive_history_workflow.py tests/test_benchmark_replay_workflow.py tests/test_council_autonomy_flow.py tests/test_orchestration_ingress_workflow.py tests/test_orchestration_planner_workflow.py tests/test_supervisor_simulation_regression.py`：通过。
  - `.venv/bin/python -m unittest tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_lists_no_legacy_claim_cluster_result_sets_after_successor_helpers tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_queries_no_legacy_claim_cluster_items_after_successor_helpers tests.test_analysis_workflow.AnalysisWorkflowTests.test_successor_analysis_chain_materializes_db_backed_surfaces tests.test_investigation_workflow -v`：`22` 项通过。
  - import smoke：`16` 个受影响测试模块可正常 import。
  - `git diff --check`：通过。

## 21. 2026-04-29 runtime / skill / docs 命名同步回写

- 已完成：
  - `eco_council_runtime.optional_analysis_helpers` 取代旧 helper 模块名，skill wrapper import 已全部同步。
  - runtime contract 字段改为 `helper_governance / helper_destination`，registry 常量、freeze line、schema version、warning code 和 tests 断言均已同步为功能命名。
  - optional-analysis skill 描述、agent short description 和本文档引用已移除阶段编号命名。
  - 旧 workplan 文件已重命名为 `docs/openclaw-optional-analysis-skills-refactor-workplan.md`。

- 未完成：
  - 未运行全仓 discover；本批只准备运行直接相关 targeted regression。
  - 旧 analysis kind / report basis CLI/schema 的业务命名迁移仍是后续 breaking migration。

- 新发现的问题：
  - runtime API 若继续保留编号字段，会和测试/skill 文档的新功能命名不一致；本批已作为 breaking rename 处理。
  - 外部旧 artifact consumer 需要迁移到 `helper_governance`。

- 是否影响后续计划：
  - 不阻塞当前验收；后续计划应把剩余命名债限定在 analysis kind 与 report basis contract 迁移，不再把 helper governance 归为未改项。

- 本次实际运行：
  - `.venv/bin/python -m py_compile eco-concil-runtime/src/eco_council_runtime/analysis_objects.py eco-concil-runtime/src/eco_council_runtime/optional_analysis_helpers.py eco-concil-runtime/src/eco_council_runtime/kernel/skill_registry.py eco-concil-runtime/src/eco_council_runtime/phase2_fallback_context.py eco-concil-runtime/src/eco_council_runtime/formal_signal_semantics.py skills/aggregate-environment-evidence/scripts/aggregate_environment_evidence.py skills/review-fact-check-evidence-scope/scripts/review_fact_check_evidence_scope.py skills/discover-discourse-issues/scripts/discover_discourse_issues.py skills/suggest-evidence-lanes/scripts/suggest_evidence_lanes.py skills/materialize-research-issue-surface/scripts/materialize_research_issue_surface.py skills/project-research-issue-views/scripts/project_research_issue_views.py skills/export-research-issue-map/scripts/export_research_issue_map.py skills/apply-approved-formal-public-taxonomy/scripts/apply_approved_formal_public_taxonomy.py skills/compare-formal-public-footprints/scripts/compare_formal_public_footprints.py skills/identify-representation-audit-cues/scripts/identify_representation_audit_cues.py skills/detect-temporal-cooccurrence-cues/scripts/detect_temporal_cooccurrence_cues.py skills/review-evidence-sufficiency/scripts/review_evidence_sufficiency.py skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py tests/test_optional_analysis_guardrails.py tests/test_analysis_workflow.py tests/test_formal_public_workflow.py tests/test_policy_research_case_fixtures.py tests/test_skill_approval_workflow.py`：通过。
  - `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_skill_approval_workflow tests.test_source_queue_rebuild tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_runtime_kernel tests.test_board_workflow tests.test_analysis_workflow tests.test_formal_public_workflow -v`：`117` 项通过。
  - `rg` 扫描 阶段编号样式、旧 helper module 和旧 metadata 字段：无残留命中。
  - `git diff --check`：通过。

## 22. 2026-04-29 残留风险一次性收尾验收回写

- 已完成：
  - 公开 skill 目录从 `freeze report basis-evidence-basis` 收敛为 `freeze-report-basis`，执行脚本同步为 `freeze_report_basis.py`；transition constant 同步为 `TRANSITION_KIND_FREEZE_REPORT_BASIS`。
  - 将 report basis 与 runtime control 聚合面拆开：deliberation plane 保留 `report-basis-freeze`，runtime/control plane 改为 `runtime-control-freeze`，避免 canonical registry / query surface 撞名。
  - `deliberation_plane.py` 中 runtime control 聚合入口已改为 `store_runtime_control_freeze_record / load_runtime_control_freeze_record`；`store_report_basis_freeze_record / load_report_basis_freeze_record` 只负责 DB-backed report basis。
  - council resolution 入口从 `phase2_report_basis_resolution.py` 改为 `phase2_report_basis_resolution.py`，并新增 `report_basis_resolution_* / report_basis_status` 镜像字段；历史 `report_basis_*` 字段保留为 DB/replay/reporting 兼容字段。
  - `skill_registry.py` 已去除 `report-basis-freeze` 重复 object kind，并把 reporting handoff 输入面显式区分为 `report-basis-freeze / runtime-control-freeze / finding / evidence-bundle / proposal / readiness-opinion`。
  - 修复全量 discover 前置问题：canonical contract expected set 同步 runtime/deliberation/reporting 新对象；progress dashboard 与 milestone package 测试不再依赖缺失的 archive doc。

- 未完成：
  - 未做 `report_basis_status / report_basis_gate_path / report-basis-gate` 等历史 DB/CLI/stage 字段的破坏性改名；这些仍作为兼容字段存在，但已不再代表 runtime 默认调查结论。
  - 未物理删除 frozen legacy analysis kind；它们继续作为 query-only/audit-only 兼容面，并受 optional-analysis approval/freeze metadata 约束。

- 新发现的问题：
  - `report-basis-freeze` 曾同时代表 report evidence basis 与 runtime control freeze，导致 canonical registry 键覆盖和 `deliberation_plane.py` 同名函数覆盖风险；本批已拆成 `report-basis-freeze` 与 `runtime-control-freeze`。
  - 旧 `phase2_report_basis_resolution` 模块名会把 council judgement 误读为 kernel report basis decision；本批已改成 report-basis resolution 入口。
  - 根目录请求中提到的旧独立 skills workplan 文件当前工作区不存在；对应内容已在 `docs/openclaw-optional-analysis-skills-refactor-workplan.md` 继续维护。

- 是否影响后续计划：
  - 不阻塞当前重构验收；默认链仍是 fetch/import -> normalize -> query -> DB finding/evidence/proposal/review -> moderator transition -> reporting。
  - 后续若要彻底清除 `report_basis_*` 字段，应作为单独 breaking DB/CLI/replay migration，而不是在 runtime kernel 或 reporting skill 中隐式改名。

- 本次实际运行：
  - `.venv/bin/python -m py_compile ...`：核心 runtime、query、transition、skill 与测试模块通过。
  - `.venv/bin/python -m unittest tests.test_canonical_contracts tests.test_control_query_surface tests.test_phase2_state_surfaces -v`：`13` 项通过。
  - `.venv/bin/python -m unittest tests.test_decision_trace_workflow tests.test_investigation_workflow tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_phase2_gate_handlers -v`：`33` 项通过。
  - `.venv/bin/python -m unittest discover -s tests -v`：`235` 项通过，用时 `420.703s`。

## 23. 2026-04-29 report-basis gate 命名债收尾回写

- 已完成：
  - 默认 phase-2 gate stage / handler 从 `report-basis-gate` 切到 `report-basis-gate`；planner、direct advisory、transition executor、controller、supervisor、benchmark 与 state surfaces 同步使用新 stage。
  - 新增 `apply-report-basis-gate` CLI 与 `report_basis_gate_path` 默认 artifact；后续第 24 节已删除旧 promotion 兼容入口。
  - runtime/control/reporting payload 增加 `report_basis_status / report_basis_gate_status / report_basis_freeze_allowed / report_basis_source / report_basis_path / report_basis_resolution_*` 镜像字段，旧 `report_basis_*` 字段继续双写以支持既有 DB/replay。
  - `query-control-objects` 支持 `--report-basis-status`，并映射到底层兼容 DB column。

- 未完成：
  - 该项已被第 24 节覆盖：旧 promotion DB/schema/replay 命名已删除，当前 `report_basis_status` 是新架构字段。
  - 未物理删除 frozen legacy analysis/query 兼容面；该项仍需独立 DB replay / migration 计划。

- 新发现的问题：
  - 旧路径测试残留已同步为新路径；后续第 24 节已删除旧 loader fallback。
  - 一个中途编辑错误曾把 gate alias 变量插入 `empty_round_state()`；已修正并用 full discover 验证。

- 是否影响后续计划：
  - 不阻塞后续计划；默认语义已从 report basis gate 收敛到 report-basis freeze gate。
  - 后续如要清除 `report_basis_*`，应作为 explicit DB/schema/replay breaking migration，而不是业务逻辑重构的一部分。

- 本次实际运行：
  - `.venv/bin/python -m py_compile ...`：本批 runtime、gate、control/reporting surface 与相关 skill 脚本通过。
  - `.venv/bin/python -m unittest tests.test_phase2_gate_handlers tests.test_phase2_contracts tests.test_direct_council_advisory tests.test_orchestration_planner_workflow tests.test_control_query_surface tests.test_phase2_state_surfaces tests.test_runtime_kernel tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_supervisor_simulation_regression -v`：`84` 项通过。
  - `.venv/bin/python -m unittest tests.test_decision_trace_workflow -v`：`4` 项通过。
  - `.venv/bin/python -m unittest discover -s tests -v`：`235` 项通过，用时 `216.605s`。

## 24. 2026-04-29 report-basis-only 破坏性收尾回写

- 已完成：
  - 按“数据库无历史价值、以最新架构为准”的原则，移除 `promotion-gate / apply-promotion-gate / promotion_gate_* / promotion_status / promote_allowed / promotion_path` 等旧兼容入口和字段。
  - 默认 report basis artifact 改为 `run_dir/report_basis/frozen_report_basis_<round_id>.json`；runtime gate artifact 只使用 `runtime/report_basis_gate_<round>.json`。
  - DB schema、control query、council submission、readiness opinion、reporting handoff、decision/publication、archive/history context 与测试夹具同步到 `report_basis_*` 命名。
  - report-basis gate 的 withheld 状态统一为 `report-basis-freeze-withheld`，ready 状态使用 `frozen`。

- 未完成：
  - optional-analysis 中仍有 `legacy` 审计/冻结术语；这些属于“旧 heuristic 标记为不可默认进入主链”的治理口径，不再作为旧 concil 数据兼容层处理。

- 新发现的问题：
  - 批量改名会误伤 Python 变量名中的自然语言片段；已通过全仓 `compileall` 和目标回归修正。
  - 旧测试断言仍期待 `freeze-withheld`；已同步为 `report-basis-freeze-withheld`。

- 是否影响后续计划：
  - 不阻塞。report-basis 主链已按新架构收口，不再保留旧 promotion 兼容语义。

- 本次实际运行：
  - `.venv/bin/python -m compileall -q eco-concil-runtime/src skills tests`：通过。
  - `.venv/bin/python -m unittest tests.test_phase2_gate_handlers tests.test_phase2_contracts tests.test_runtime_kernel tests.test_control_query_surface tests.test_phase2_state_surfaces tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_decision_trace_workflow tests.test_council_submission_workflow tests.test_supervisor_simulation_regression -v`：`84` 项通过，用时 `81.445s`。
  - `.venv/bin/python -m unittest discover -s tests -v`：`235` 项通过，用时 `222.805s`。

## 25. 2026-04-29 验收审阅补充：agent entry 旧 analysis 命令收口

- 已完成：
  - 本次按最终验收口径复核 `phase2_agent_entry_profile.py / kernel/agent_entry.py / source_queue_profile.py / skill_registry.py / skill_approvals.py / reporting handoff / policy research fixture`。
  - 修正默认 agent entry operator surface 中残留的 `claim-cluster` analysis 查询模板：`list_claim_cluster_result_sets_command` 与 `query_claim_cluster_items_command_template` 不再由默认 entry/operator view 暴露。
  - `tests/test_agent_entry_gate.py` 已改为防回归断言：默认 operator surface 不包含旧 claim-cluster 命令，也不包含 `claim-cluster` 命令文本。
  - 复核确认 active `skills/` 与 registry 均为 `79` 个 skill；optional-analysis 为 `16` 个，其中均需 approval/freeze metadata。

- 未完成：
  - optional-analysis freeze line 仍是 `audit-pending`，不是完整人工审计批准记录。
  - frozen legacy analysis kind / query object 仍作为 compatibility query / replay surface 保留，未做物理删除。
  - 本次补充未运行全仓 discover；运行的是覆盖本补丁和最终验收主路径的 targeted regression。

- 新发现的问题：
  - 早前文档已写明“默认 agent entry 不再暴露旧 analysis query commands”，但代码默认 operator surface 仍残留 `claim-cluster` 查询模板；该问题已在本次修复。
  - 第 `1` 节原始 `88` 个 skill 盘点容易和当前 active registry `79` 个 skill 混读；已在文档顶部补充当前口径说明。

- 是否影响后续计划：
  - 不阻塞本轮 skills 重构验收；修复后默认入口、source queue 和 optional-analysis approval 链与最终架构一致。
  - 后续新增 agent entry/operator command 时，不得默认暴露 frozen legacy analysis query surface；如需访问，只能通过显式审计/回放/兼容查询语境。

- 本次实际运行：
  - `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_skill_approval_workflow tests.test_source_queue_rebuild tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_runtime_kernel tests.test_board_workflow -v`
  - 结果：`111` 项通过。
  - `git diff --check`：通过。
