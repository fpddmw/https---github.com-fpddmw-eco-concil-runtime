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

1. `eco-extract-claim-candidates`
2. `eco-cluster-claim-candidates`
3. `eco-derive-claim-scope`
4. `eco-classify-claim-verifiability`
5. `eco-route-verification-lane`
6. `eco-extract-issue-candidates`
7. `eco-cluster-issue-candidates`
8. `eco-extract-stance-candidates`
9. `eco-extract-concern-facets`
10. `eco-extract-actor-profiles`
11. `eco-extract-evidence-citation-types`
12. `eco-materialize-controversy-map`
13. `eco-link-claims-to-observations`
14. `eco-score-evidence-coverage`
15. `eco-link-formal-comments-to-public-discourse`
16. `eco-identify-representation-gaps`
17. `eco-detect-cross-platform-diffusion`
18. `eco-propose-next-actions`
19. `eco-open-falsification-probe`
20. `eco-summarize-round-readiness`
21. `eco-promote-evidence-basis`

### 3.2 最需要拆分的非原子 skill

1. `openaq-data-fetch`
2. `eco-import-fetch-execution`
3. `eco-build-normalization-audit`
4. `eco-plan-round-orchestration`
5. `eco-propose-next-actions`
6. `eco-summarize-round-readiness`
7. `eco-promote-evidence-basis`
8. `eco-materialize-reporting-handoff`
9. `eco-draft-council-decision`
10. `eco-materialize-final-publication`

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
| `airnow-hourly-obs-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留官方数据抓取；补地区模板、数据质量标签、参数预设。 |
| `bluesky-cascade-fetch` | 保留并修改 | 弱 / 是 | 半原子 | 保留；明确平台样本偏差说明；去掉“board-ready”暗示。 |
| `gdelt-doc-search` | 保留并修改 | 弱 / 是 | 原子 | 保留；把 query 模板与主题扩展配置化；审计 GDELT 代表性。 |
| `gdelt-events-fetch` | 保留并修改 | 弱 / 是 | 原子 | 保留；只做导出抓取，不承担后续解释。 |
| `gdelt-gkg-fetch` | 保留并修改 | 弱 / 是 | 原子 | 保留；标明 source bias 和 coverage gap。 |
| `gdelt-mentions-fetch` | 保留并修改 | 弱 / 是 | 原子 | 保留；标明媒体选择偏差。 |
| `nasa-firms-fire-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；作为火点背景数据源，不承担结论。 |
| `open-meteo-air-quality-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；明确 modeled background，不等同地面实测。 |
| `open-meteo-flood-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；用于水文背景和情景研究。 |
| `open-meteo-historical-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；作为物理背景数据源。 |
| `openaq-data-fetch` | 拆分重写 | 无 / 否 | 非原子 | 拆成 metadata discovery、measurement fetch、archive backfill 三个 skill。 |
| `regulationsgov-comment-detail-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；作为 formal record 深度抓取。 |
| `regulationsgov-comments-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；作为正式评论抓取主入口。 |
| `usgs-water-iv-fetch` | 保留并修改 | 无 / 否 | 原子 | 保留；对水库/河流/洪水议题是关键技能。 |
| `youtube-comments-fetch` | 保留并修改 | 弱 / 是 | 原子 | 保留；强调平台偏差、spam 清理策略和时窗代表性。 |
| `youtube-video-search` | 保留并修改 | 弱 / 是 | 半原子 | 保留；应把 discovery ranking 逻辑显式暴露为可审计配置。 |

## 4.2 Normalize Skills（16）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `eco-normalize-airnow-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 schema mapping、单位、时间、provenance；移除 board hints。 |
| `eco-normalize-bluesky-cascade-public-signals` | 保留并修改 | 弱 / 是 | 半原子 | 只保留 signal normalization；线程结构解释留给上层 skill。 |
| `eco-normalize-gdelt-doc-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 article rows 归一化。 |
| `eco-normalize-gdelt-events-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 row normalization；不附会社会语义。 |
| `eco-normalize-gdelt-gkg-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 metadata mapping。 |
| `eco-normalize-gdelt-mentions-public-signals` | 保留并修改 | 无 / 否 | 原子 | 只做 mention rows 归一化。 |
| `eco-normalize-nasa-firms-fire-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 只做火点 signal normalization。 |
| `eco-normalize-open-meteo-air-quality-signals` | 保留并修改 | 无 / 否 | 原子 | 保持 modeled signal 属性，不伪装成观测。 |
| `eco-normalize-open-meteo-flood-signals` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `eco-normalize-open-meteo-historical-signals` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `eco-normalize-openaq-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 保留；强化质量标记和 provider provenance。 |
| `eco-normalize-regulationsgov-comment-detail-public-signals` | 拆分重写 | 弱 / 是 | 非原子 | 只保留 formal signal 归一化；提交者/议题/立场抽取拆到 optional parser。 |
| `eco-normalize-regulationsgov-comments-public-signals` | 拆分重写 | 弱 / 是 | 非原子 | 同上；禁止在 normalizer 内做 route/stance judgement。 |
| `eco-normalize-usgs-water-observation-signals` | 保留并修改 | 无 / 否 | 原子 | 保留；为水利类研究关键。 |
| `eco-normalize-youtube-comments-public-signals` | 保留并修改 | 弱 / 是 | 半原子 | 保留；去掉下游 board hint 语义。 |
| `eco-normalize-youtube-video-public-signals` | 保留并修改 | 无 / 否 | 原子 | 保留。 |

## 4.3 Analysis / Heuristic Skills（21）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `eco-build-normalization-audit` | 废弃 | 强 / 是（强制） | 非原子 | 不再作为 board-facing moderation skill；改为 operator QA export 或删除。 |
| `eco-extract-claim-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 可保留为 public narrative seed extractor，但不得进入默认链。 |
| `eco-cluster-claim-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 可保留为 narrative clustering helper。 |
| `eco-derive-claim-scope` | 降级为可选 | 强 / 是（强制） | 半原子 | 不应主导议题边界；仅作辅助标签。 |
| `eco-classify-claim-verifiability` | 降级为可选 | 强 / 是（强制） | 半原子 | 不再作为 mandatory router；仅作 optional evidence triage。 |
| `eco-route-verification-lane` | 降级为可选 | 强 / 是（强制） | 半原子 | 不能再预设“问题必须被路由到某条 lane”。 |
| `eco-extract-issue-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 仅作 optional issue surface builder。 |
| `eco-cluster-issue-candidates` | 降级为可选 | 强 / 是（强制） | 半原子 | 同上；不得成为调查主干。 |
| `eco-extract-stance-candidates` | 降级为可选 | 强 / 是（强制） | 原子 | 可保留为可审计 typed decomposition helper。 |
| `eco-extract-concern-facets` | 降级为可选 | 强 / 是（强制） | 原子 | 同上。 |
| `eco-extract-actor-profiles` | 降级为可选 | 强 / 是（强制） | 原子 | 同上。 |
| `eco-extract-evidence-citation-types` | 降级为可选 | 强 / 是（强制） | 原子 | 同上。 |
| `eco-materialize-controversy-map` | 降级为可选 | 强 / 是（强制） | 非原子 | 改为 report-support export，不再作为 canonical 主视图。 |
| `eco-extract-observation-candidates` | 拆分重写 | 强 / 是（强制） | 半原子 | 改造成环境证据聚合 helper，而不是 claim-matching 前置。 |
| `eco-merge-observation-candidates` | 拆分重写 | 强 / 是（强制） | 半原子 | 改造成 region/metric aggregation helper。 |
| `eco-derive-observation-scope` | 拆分重写 | 强 / 是（强制） | 半原子 | 改造成 metadata tagging helper，不再服务 claim matching。 |
| `eco-link-claims-to-observations` | 废弃 | 强 / 是（强制） | 非原子 | 当前“社会-物理匹配”主轴不符合目标；需彻底移除现形态。 |
| `eco-score-evidence-coverage` | 拆分重写 | 强 / 是（强制） | 非原子 | 改成通用 `evidence sufficiency review`，不再依赖 claim-observation link。 |
| `eco-link-formal-comments-to-public-discourse` | 保留并修改 | 强 / 是（强制） | 半原子 | 可保留为 optional formal/public alignment helper。 |
| `eco-identify-representation-gaps` | 保留并修改 | 强 / 是（强制） | 半原子 | 可保留为 optional participation / representation audit helper。 |
| `eco-detect-cross-platform-diffusion` | 保留并修改 | 强 / 是（强制） | 半原子 | 可保留为 optional discourse dynamics helper。 |

## 4.4 Board / Council / Phase-2 Skills（19）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `eco-scaffold-mission-run` | 保留并修改 | 无 / 否 | 半原子 | 保留；改成最小 run bootstrap，不预设分析链。 |
| `eco-prepare-round` | 保留并修改 | 弱 / 是 | 半原子 | 保留；只编 source plan 和 capability checks，不决定研究方法。 |
| `eco-import-fetch-execution` | 保留并修改 | 无 / 否 | 非原子 | 保留执行器角色；拆出 fetch queue runner、normalizer runner、execution receipt。 |
| `eco-open-investigation-round` | 保留并修改 | 无 / 否 | 半原子 | 保留 moderator-only；只能消费已批准 transition request。 |
| `eco-read-board-delta` | 保留 | 无 / 否 | 原子 | 保留；作为多 agent 共享状态读面。 |
| `eco-post-board-note` | 保留并修改 | 无 / 否 | 原子 | 保留，但只做 human-readable note，不承载 canonical judgement。 |
| `eco-update-hypothesis-status` | 保留并修改 | 无 / 否 | 半原子 | 保留；建议改名/改义为 evidence-backed hypothesis/finding update。 |
| `eco-open-challenge-ticket` | 保留 | 无 / 否 | 原子 | 保留；仅 challenger/moderator 可写。 |
| `eco-close-challenge-ticket` | 保留 | 无 / 否 | 原子 | 保留。 |
| `eco-claim-board-task` | 保留并修改 | 无 / 否 | 原子 | 保留；限制为 moderator / task owner。 |
| `eco-submit-council-proposal` | 保留并修改 | 无 / 否 | 原子 | 保留；扩展为通用 structured finding/proposal submission。 |
| `eco-submit-readiness-opinion` | 保留并修改 | 无 / 否 | 原子 | 保留；只在 moderator 请求阶段推进前使用。 |
| `eco-summarize-board-state` | 保留并修改 | 无 / 否 | 原子 | 保留为 derived export。 |
| `eco-materialize-board-brief` | 保留并修改 | 无 / 否 | 原子 | 保留为 human handoff export。 |
| `eco-plan-round-orchestration` | 拆分重写 | 强 / 是（强制） | 非原子 | 现形态应退出 kernel 主链；如保留，只能是 moderator 可选 advisory skill。 |
| `eco-propose-next-actions` | 降级为可选 | 强 / 是（强制） | 非原子 | 可保留为 moderator advisory，不再是默认 phase owner。 |
| `eco-open-falsification-probe` | 保留并修改 | 强 / 是（强制） | 半原子 | 保留为 challenger tool；从 controller mandatory stage 移出。 |
| `eco-summarize-round-readiness` | 降级为可选 | 强 / 是（强制） | 非原子 | 可保留为 moderator aid，但 readiness 正式推进应靠 moderator request + operator approval。 |
| `eco-promote-evidence-basis` | 保留并修改 | 强 / 是（强制） | 非原子 | 保留为 `freeze-report-basis` 类 skill，但不再自动主导 promotion 语义。 |

## 4.5 Query / History / Reporting Skills（16）

| Skill | 去留 | 规则/审计 | 原子性 | 重构方向 |
| --- | --- | --- | --- | --- |
| `eco-query-public-signals` | 保留 | 无 / 否 | 原子 | 保留。 |
| `eco-query-formal-signals` | 保留 | 无 / 否 | 原子 | 保留。 |
| `eco-query-environment-signals` | 保留 | 无 / 否 | 原子 | 保留。 |
| `eco-lookup-normalized-signal` | 保留 | 无 / 否 | 原子 | 保留。 |
| `eco-lookup-raw-record` | 保留 | 无 / 否 | 原子 | 保留。 |
| `eco-archive-signal-corpus` | 保留并修改 | 无 / 否 | 半原子 | 保留为跨项目信号资产；注意权限与脱敏。 |
| `eco-query-signal-corpus` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `eco-archive-case-library` | 保留并修改 | 无 / 否 | 半原子 | 保留；调整 case schema 以支持政策研究而非 claim matching。 |
| `eco-query-case-library` | 保留并修改 | 无 / 否 | 原子 | 保留。 |
| `eco-materialize-history-context` | 保留并修改 | 弱 / 是 | 半原子 | 保留为 optional retrieval helper。 |
| `eco-materialize-reporting-handoff` | 拆分重写 | 弱 / 是 | 非原子 | 拆成 evidence packet、decision packet、report packet 三个 skill。 |
| `eco-draft-council-decision` | 保留并修改 | 弱 / 是 | 非原子 | 改成 decision memo drafter；减少对 phase-2 字段的耦合。 |
| `eco-draft-expert-report` | 保留并修改 | 弱 / 是 | 半原子 | 改成章节化 report drafting skill，支持多角色 section 输出。 |
| `eco-publish-expert-report` | 保留并修改 | 无 / 否 | 原子 | 保留；增加 operator confirmation / overwrite policy。 |
| `eco-publish-council-decision` | 保留并修改 | 无 / 否 | 原子 | 保留；只发布经审批的 decision memo。 |
| `eco-materialize-final-publication` | 保留并修改 | 弱 / 是 | 非原子 | 改成 final report assembler，不再只拼 council/reporting 产物。 |

## 5. 为新目标必须补充的 skill 层面

## 5.1 问题定义与研究设计

建议新增：

1. `eco-define-study-brief`
2. `eco-define-decision-question`
3. `eco-define-region-baseline`
4. `eco-request-phase-transition`

## 5.2 政策与区域事实采集

建议新增：

1. `eco-fetch-eia-documents`
2. `eco-fetch-planning-policy-documents`
3. `eco-fetch-land-use-and-demography`
4. `eco-fetch-complaint-and-enforcement-records`
5. `eco-fetch-local-media-corpus`
6. `eco-fetch-basin-and-hydrology-profile`

## 5.3 结构化归一化

建议新增：

1. `eco-normalize-eia-documents`
2. `eco-normalize-policy-documents`
3. `eco-normalize-land-use-and-demography`
4. `eco-normalize-complaint-records`
5. `eco-normalize-project-alternative-documents`

## 5.4 研究分析

建议新增：

1. `eco-build-evidence-bundle`
2. `eco-build-stakeholder-map`
3. `eco-build-causal-hypotheses`
4. `eco-evaluate-policy-options`
5. `eco-build-risk-register`
6. `eco-build-uncertainty-register`
7. `eco-run-scenario-comparison`
8. `eco-build-implementation-checklist`
9. `eco-build-communication-plan`

## 5.5 模拟与推演

建议新增，但必须强标 `scenario/simulation`：

1. `eco-simulate-public-response-scenarios`
2. `eco-simulate-policy-rollout-risks`
3. `eco-run-hydrology-sensitivity-scenarios`

这些 skill 全部需要你审计假设与参数。

## 5.6 报告与格式规范

建议新增：

1. `eco-draft-decision-maker-report`
2. `eco-draft-report-section`
3. `eco-assemble-citation-index`
4. `eco-format-report-to-template`
5. `eco-qa-report-evidence-trace`
6. `eco-qa-report-language-and-claims`

## 6. 最终结论

如果以“为决策者提供环保政策研究报告”为目标：

1. fetch / normalize / query 技能大多保留。
2. 当前中层 claim-route-coverage-controversy 链必须整体降级为 optional。
3. 当前 phase-2 orchestrator / readiness / promotion 主链必须退出默认控制权。
4. reporting 技能要从“议会结论封装”升级为“研究报告生产线”。
5. 需要新增一整层政策研究、替代方案比较、风险与不确定性、格式规范技能。
