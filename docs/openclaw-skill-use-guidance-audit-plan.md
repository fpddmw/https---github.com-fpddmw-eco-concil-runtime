# OpenClaw Skill 使用认知与提示词审计计划

## 1. 文档定位

本文记录对全部 OpenClaw skills 的系统性提示词和文档审计计划。

它不是新的 runtime contract，不是 source 排序系统，也不是让 agents 填写
更多表单的计划。它的目标是把 skill 的能力边界、常见误用、空结果语义和
同族 follow-up 路径写清楚，让 agents 能在保持自主调查权的前提下更正确地
使用工具。

相关基线文档：

- `docs/openclaw-source-family-workflows.md`
- `docs/openclaw-agent-autonomy-archive-workplan.md`
- `docs/openclaw-skills-refactor-checklist-v2.md`

## 2. 背景问题

NYC smoke 真实案例 run 暴露了一个通用问题：agent 可能误解 skill 的能力
边界，使用错误参数或过窄查询，然后自信地向议会报告“查不到数据”或
“无法组合证据”。

典型例子：

1. `fetch-nasa-firms-fire` 被用 NRT product 查询 2023 历史窗口，导致空结果。
   正确做法应先检查 product availability，并对历史窗口优先考虑可用 SP
   product。
2. `fetch-gdelt-doc-search` 被当成官方记录或 GDELT 全量能力入口。事实上 DOC
   Search 是文章/网页检索层，不等于 Events、Mentions、GKG 三张主表。
3. YouTube、Regulations.gov、OpenAQ 等 family 都存在 search/list/metadata
   到 detail/comment/measurement 的多层链路。初步检索为空或很窄，不应直接
   变成“没有数据”。
4. normalize/query/review/reporting 类 skill 的空结果或 advisory 输出也可能被
   agent 误读为事实缺席、证据不足或 ready/not-ready 结论。

## 3. 治理边界

本计划必须遵守以下边界：

1. 不引入 source 权重、评分、排序或固定议程。
2. 不要求每次 skill 调用都填写厚表单。
3. 不让 runtime 替 agent 选择信源、组合证据或判断采信。
4. 不让 helper skill 成为事实裁判或 readiness 裁判。
5. 不把弱报告作为过早放弃调查的出口。
6. 不把历史案例结论自动套用到当前 run。

正确方向是：在 agent 最容易看到的位置提供清晰、短促、可执行的认知提示。

## 4. 统一提示原则

所有 skill 文档和 agent-facing prompt 应遵守以下原则：

1. Skill 是能力表面，不是真相裁判。
2. Zero / failed / blocked / no-op / receipt-only 是尝试结果，不是现实中证据
   不存在的证明。
3. 在声称“查不到”或“无法组合”前，agent 应说明 skill、参数、覆盖范围和未
   尝试路线。
4. 如果 evidence need 仍然 live，应先考虑 revised query、preflight、metadata、
   availability、同族 follow-up skill 或替代 provider。
5. 如果停止继续调查，应由 agent 或 moderator 明确记录 source-limit rationale
   和报告边界。

建议的轻量表达模板：

```text
Under <skill> with <query/window/bbox/provider-mode>, this attempt returned
<zero/failed/receipt-only>. This does not rule out <untried routes>; next I will
<revise/switch/ask moderator/bound the claim>.
```

这不是表单要求，而是负面判断前的一句话调查纪律。

## 5. 当前已完成状态

第一批高风险路径已经完成更新：

1. `agents/openai.yaml` 默认提示词已更新：
   - `fetch-airnow-hourly-observations`
   - `fetch-bluesky-cascade`
   - `fetch-nasa-firms-fire`
   - `fetch-gdelt-doc-search`
   - `fetch-gdelt-events`
   - `fetch-gdelt-mentions`
   - `fetch-gdelt-gkg`
   - `fetch-open-meteo-air-quality`
   - `fetch-open-meteo-historical`
   - `fetch-open-meteo-flood`
   - `fetch-youtube-video-search`
   - `fetch-youtube-comments`
   - `fetch-regulationsgov-comments`
   - `fetch-regulationsgov-comment-detail`
   - `fetch-openaq`
   - `fetch-usgs-water-iv`
2. `SKILL.md` 已增加 `Agent Reasoning Guide`：
   - AirNow
   - Bluesky
   - GDELT export skills
   - NASA FIRMS
   - all normalize skills
   - Open-Meteo historical / air-quality / flood
   - archive/history skills
   - optional-analysis helper skills
   - reporting packet/draft/publish/finalization skills
   - all query skills
   - YouTube search/comments
   - Regulations.gov comments/detail
   - OpenAQ
   - USGS Water IV
   - `normalize-fetch-execution`
   - `review-evidence-sufficiency`
   - `summarize-round-readiness`
3. runtime 生成层已经存在 `skill_use_discipline` 和 fetch skill 的
   `skill_use_card`，新 agent workspace 可看到轻量的 skill 使用纪律。
4. 已验证：
   - `quick_validate.py skills/fetch-*/` 通过
   - `quick_validate.py skills/normalize-* skills/query-*` 通过
   - `quick_validate.py skills/*` 通过
   - `compileall` 通过
   - `unittest tests.test_runtime_source_queue_profiles tests.test_agent_entry_gate` 通过
   - `unittest tests.test_gdelt_doc_query_safety tests.test_nasa_firms_fetch_skill` 通过
   - `unittest tests.test_migrated_source_runtime_integration tests.test_realcase_transport_chain_autonomy` 通过
   - `unittest tests.test_signal_plane_workflow tests.test_formal_public_workflow tests.test_diffusion_workflow tests.test_council_query_surface tests.test_reporting_query_surface tests.test_archive_history_workflow tests.test_spatiotemporal_relation_taxonomy` 通过
   - `unittest tests.test_dynamic_investigation_skills tests.test_skill_evidence_only_boundary` 通过
   - `unittest tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_contracts tests.test_milestone_package tests.test_optional_analysis_guardrails tests.test_analysis_workflow` 通过
   - `git diff --check` 通过

## 5.1 本次查证依据

本批提示词升级只引用 provider 能力边界，不引入 source 排序或证据权重。查证
依据包括：

1. AirNow Hourly AQ Obs / Hourly Data fact sheets：文件 host、GMT hour、
   preliminary data、recent file revision、valid rows、negative concentration caveats。
2. Open-Meteo Historical Weather / Air Quality / Flood API docs：reanalysis/model
   source、CAMS Europe/Global domain 差异、GloFAS 5 km river-cell caveat、变量和
   date availability。
3. USGS Instantaneous Values service docs 和 2026 Water Services decommission
   notice：major filter、parameterCd、JSON WaterML shape、provisional data、
   early-2027 migration risk。
4. Bluesky API Hosts/Auth、`searchPosts` API reference、AT Protocol lexicons 和
   rate-limit docs：public cached AppView、auth fallback、query-sensitive search、
   incomplete cursor pagination、429/rate-limit behavior。
5. OpenClaw 本地 contract / query / normalization 代码和测试：`normalized_signals`
   是 DB 可见面，query 只读已归一化或已归档对象，normalize 只写 lineage 和
   signal rows，不做 claim、readiness、coverage sufficiency 或 report basis。

主要一手链接：

- AirNow API docs / Hourly Data Fact Sheet：
  `https://docs.airnowapi.org/docs/HourlyDataFactSheet.pdf`
- Open-Meteo Historical Weather / Air Quality / Flood API docs：
  `https://open-meteo.com/en/docs/historical-weather-api`
  `https://open-meteo.com/en/docs/air-quality-api`
  `https://open-meteo.com/en/docs/flood-api`
- USGS Water Services IV docs：
  `https://waterservices.usgs.gov/rest/IV-Service.html`
- Bluesky docs / AT Protocol lexicons：
  `https://docs.bsky.app/docs/advanced-guides/api-directory`
  `https://docs.bsky.app/docs/api/app-bsky-feed-search-posts`
  `https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons/app/bsky/feed/searchPosts.json`
- NASA FIRMS API：
  `https://firms.modaps.eosdis.nasa.gov/api/area/`

## 6. 全量审计分层

### 6.1 必须逐个深审的 fetch skills

这些 skill 最容易出现 provider 语义、时间窗口、产品类型、搜索/详情链路和
空结果误读问题。每个都需要检查 `SKILL.md`、`agents/openai.yaml`、role surface
command template 和同族 workflow。

已完成：

- `fetch-airnow-hourly-observations`
- `fetch-bluesky-cascade`
- `fetch-gdelt-doc-search`
- `fetch-gdelt-events`
- `fetch-gdelt-mentions`
- `fetch-gdelt-gkg`
- `fetch-nasa-firms-fire`
- `fetch-open-meteo-air-quality`
- `fetch-open-meteo-historical`
- `fetch-open-meteo-flood`
- `fetch-openaq`
- `fetch-usgs-water-iv`
- `fetch-youtube-video-search`
- `fetch-youtube-comments`
- `fetch-regulationsgov-comments`
- `fetch-regulationsgov-comment-detail`

每个 fetch skill 至少应说明：

1. 这个 skill 能观察什么。
2. 它不能证明什么。
3. 是否需要 lint、dry-run、metadata、availability、config probe。
4. zero/failed 输出可能意味着什么。
5. zero/failed 输出不能推出什么。
6. 是否存在同族 follow-up skill。
7. 常见 agent 误用。

### 6.2 统一模板审的 normalize skills

Normalize skill 的共同风险是：agent 把“没有 normalized rows”误读成“原始
数据没有价值”或“证据不存在”。

已完成统一边界审计：

- `normalize-airnow-observation-signals`
- `normalize-bluesky-cascade-public-signals`
- `normalize-gdelt-doc-public-signals`
- `normalize-gdelt-events-public-signals`
- `normalize-gdelt-gkg-public-signals`
- `normalize-gdelt-mentions-public-signals`
- `normalize-nasa-firms-fire-observation-signals`
- `normalize-open-meteo-air-quality-signals`
- `normalize-open-meteo-flood-signals`
- `normalize-open-meteo-historical-signals`
- `normalize-openaq-observation-signals`
- `normalize-regulationsgov-comments-public-signals`
- `normalize-regulationsgov-comment-detail-public-signals`
- `normalize-usgs-water-observation-signals`
- `normalize-youtube-comments-public-signals`
- `normalize-youtube-video-public-signals`

统一要求：

1. Normalize 只把 raw artifacts 转成 DB-backed signal rows。
2. Normalize 不推断事实、因果、readiness 或 policy conclusion。
3. 空 normalized rows 应先检查 artifact shape、source skill mapping、normalizer
   适配和 provenance。
4. receipt-only evidence 可以继续被议会引用，但必须明确其查询/复核限制。

### 6.3 Query skills

Query skill 的共同风险是：agent 把 DB 查询空结果误读成“证据不存在”。

已完成统一边界审计：

- `query-board-delta`
- `query-case-library`
- `query-environment-signals`
- `query-formal-signals`
- `query-normalized-signal`
- `query-public-signals`
- `query-raw-record`
- `query-signal-corpus`
- `query-spatiotemporal-relations`

统一要求：

1. Query 只读取当前 DB 或 archive/corpus 的可见记录。
2. Query 空结果可能意味着数据未 normalize、filter 过窄、round/run 错误或
   archive 未导入。
3. Query 不应直接生成“证据不存在”结论。
4. Query 返回的 refs 应作为 finding、bundle、proposal 或 challenge 的证据基础。

### 6.4 Archive / history skills

Archive/history skill 的共同风险是：agent 把历史案例当成当前事实结论，或忽略
历史成功参数。

已完成统一边界审计：

- `archive-case-library`
- `archive-signal-corpus`
- `materialize-history-context`

统一要求：

1. Archive 保存历史 evidence refs、case summaries 和 signal corpus。
2. History context 提供线索和历史参数样例，不替当前 run 下结论。
3. 历史成功用法可以提示 agent，例如 FIRMS 历史窗口应检查 availability 和 SP
   product，但 agent 仍需自主决定是否复用。

### 6.5 Optional-analysis / review / suggest skills

这些 skill 的共同风险是：advisory 输出被当成裁判、排序、评分或 phase gate。

已完成统一边界审计：

- `aggregate-environment-evidence`
- `apply-approved-formal-public-taxonomy`
- `compare-formal-public-footprints`
- `detect-temporal-cooccurrence-cues`
- `discover-discourse-issues`
- `export-research-issue-map`
- `identify-representation-audit-cues`
- `materialize-research-issue-surface`
- `plan-round-orchestration`
- `project-research-issue-views`
- `propose-next-actions`
- `review-evidence-sufficiency`
- `review-fact-check-evidence-scope`
- `review-spatiotemporal-relation-alternatives`
- `suggest-evidence-lanes`
- `summarize-round-readiness`
- `open-falsification-probe`

统一要求：

1. Optional-analysis 只产生 advisory cues、review notes 或 candidate surfaces。
2. 不输出结论、权重、排序、source selection 或 readiness gate。
3. 必须经过 agent uptake：challenge、finding、evidence request、proposal、
   readiness opinion 或 round synthesis。
4. 空输入应被解释为输入/lineage 限制，不是事实缺席。

### 6.6 Council write / reporting / transition skills

这些 skill 的共同风险是：agent 把 board note 当成 canonical finding，或把
reporting helper 当成调查替代品。

Reporting / publish 链路已完成统一边界审计：

- `draft-council-decision`
- `draft-expert-report`
- `materialize-final-publication`
- `materialize-reporting-handoff`
- `materialize-spatiotemporal-relation-evidence-packet`
- `publish-council-decision`
- `publish-expert-report`

其余 council write / transition skills 待后续专项审计：

- `claim-board-task`
- `close-challenge-ticket`
- `freeze-report-basis`
- `link-source-acquisition-execution`
- `materialize-board-brief`
- `materialize-context-packet`
- `open-challenge-ticket`
- `open-followup-from-review-comment`
- `open-investigation-round`
- `post-board-note`
- `prepare-round`
- `publish-council-decision`
- `publish-expert-report`
- `scaffold-mission-run`
- `submit-agent-position`
- `submit-challenge-disposition`
- `submit-council-proposal`
- `submit-evidence-request`
- `submit-investigation-plan`
- `submit-investigation-scope`
- `submit-readiness-opinion`
- `submit-round-brief`
- `submit-round-synthesis`
- `submit-source-acquisition-proposal`
- `summarize-board-state`
- `update-hypothesis-status`
- `update-source-acquisition-proposal-status`

统一要求：

1. Board note 是人类可读记录，不是 canonical finding。
2. Reporting skill 只消费 frozen/reporting basis，不补造调查结论。
3. Round continuation 不应依赖 challenger 复核轮次；只要存在 live actionable
   route，moderator 可以开 continuation round。
4. Readiness 必须 claim-strength scoped。descriptive ready 不等于 causal/source
   attribution ready。

## 7. 后续批次

### P1：补齐剩余 fetch skills

状态：已完成本批文档和默认提示词升级。后续真实 run 仍需要观察 agents 是否
实际采用这些提示，而不是把空结果直接写成事实缺席。

目标：

1. 深审 AirNow、Bluesky、Open-Meteo、USGS。
2. 更新对应 `SKILL.md` 和 `agents/openai.yaml`。
3. 检查 source family workflow 中是否缺少 metadata/preflight/zero-result 提示。

验收：

1. 每个 fetch skill 都有 `Agent Reasoning Guide`。
2. 每个 fetch skill 的默认 prompt 不会鼓励错误默认参数或过早负面结论。
3. role surface 中的 command template 不把脆弱参数放在唯一显著位置。

### P2：Normalize / query 全量统一

状态：已完成本批文档和默认提示词升级。所有 normalizer 都说明 no-row 是
artifact/schema/mapping/source-pairing 诊断而非原始证据缺席；所有 query skill
都说明 empty result 是 DB/filter/round_scope/lineage/archive 可见性限制而非现实中
证据不存在。

目标：

1. 给所有 normalizer 加统一边界说明。
2. 给所有 query skill 加空结果语义说明。
3. 确保 query/normalize 的默认 prompt 不暗示结论或 sufficiency。

验收：

1. Query 空结果被明确表述为 DB/filter/lineage 限制。
2. Normalize 空结果被明确表述为 artifact/schema/mapping 限制。

### P3：Archive / optional-analysis / reporting 审计

状态：已完成本批文档和默认提示词升级。Archive/history skills 现在明确历史
材料是线索不是当前 run 结论；optional-analysis helper 明确 approval-scoped
advisory/audit 定位和 uptake boundary；reporting skills 明确只消费 frozen /
withheld reporting state，不新增调查结论、不提升 optional helper cues。

目标：

1. 明确 archive/history 是线索，不是结论。
2. 明确 optional-analysis 是 advisory，不是裁判。
3. 明确 reporting/readiness 不替代调查，不固定议程。

验收：

1. 所有 helper skill 都有 uptake boundary。
2. 没有 helper 文档使用 score/rank/weight 作为默认治理语义。

### P4：回归测试与真实 run 验证

状态：静态回归测试已补齐。`tests/test_skill_evidence_only_boundary.py` 现在覆盖
evidence-only skill 的 machine-readable boundary、fetch `skill_use_card`、
fetch/normalize/query/helper/history/reporting `Agent Reasoning Guide`，以及
zero/no-row/empty/advisory 输出不得被解释为事实缺席、source 排序、结论或
phase/readiness gate。真实 run 行为验证仍需在后续执行中观察 agents 是否实际
采用这些提示。

目标：

1. 增加静态测试：高风险 skill 必须有 `Agent Reasoning Guide`。
2. 增加 role surface 测试：关键 fetch skill 必须暴露 `skill_use_card`。
3. 真实案例 run 验证 agents 是否：
   - 主动使用 preflight；
   - 在 doc/search/list 之后触发同族 follow-up；
   - 对 zero result 做参数反思；
   - 不把空结果当成事实缺席；
   - 在 unresolved refs 存在时推动 continuation round 或明确报告边界。

## 8. 完成标准

本计划完成时应满足：

1. 所有 fetch skills 完成深审。
2. 所有 normalize/query/archive/reporting/helper skills 至少完成统一边界审计。
3. 所有 `agents/openai.yaml` 默认提示词都能表达该 skill 的主要能力边界。
4. 新生成 agent workspace 能看到轻量 skill-use discipline。
5. 真实案例中 agent 不再把单次 skill 失败或 zero result 直接报告为“没有数据”。
6. 没有引入厚表单、source 排序、权重计算或 runtime 固定议程。
