# OpenClaw 三层 Skill Uptake 重写任务 Session 提示词

下面这段提示词可直接复制给新的 Codex session。它面向“重写 skill 文档、agent 提示词和 report validation”的实现任务，不要求新 session 主持真实议会 run。

```text
你现在位于 `/home/fpddmw/projects/openclaw-eco-concil_v1`。请负责实现 OpenClaw 的三层 Skill Uptake 能力重写：source-family workflow card、claim-sensitive soft obligations、report validation reverse constraint。

背景：

OpenClaw 是 DB-first、证据可审计的生态环境调查议会系统。runtime kernel 只负责执行、权限、审批、ledger、receipt、DB 持久化、operator 可见状态和可恢复性；runtime 不是 council agent，不选择 source，不固定议程，不判断证据充分性，不给证据排序或打分。moderator 才是议会组织者；investigators 保留自主调查权；challenger 负责 claim boundary、反证和外推风险；report-editor 只能消费 frozen/reporting basis。

当前问题：

三个 case 的 skill 使用诊断表明，问题不是“有大量 skill 没被强制调用”，而是 agent/report 对某些 claim 没有形成对应的证据组织基础：

1. NYC smoke：数据链和 public discourse 分析链基本完整，但大规模环境证据没有通过 `aggregate-environment-evidence` 充分压缩进入报告基础，报告组织仍需更像专业调研报告。
2. Colorado River：已有 1,278,468 条 normalized signals，但百万级 USGS/Open-Meteo/USBR RISE 数据没有被环境聚合摘要充分承接，报告不能依赖 agent 直接阅读原始大数据。
3. PM2.5 NAAQS：当前 run 只有 125 条 Regulations.gov comment listing、1 条 detail 路线和 2 条附件元数据，缺少 candidate corpus audit、批量 detail、附件文本实体化、formal issue classification、public discourse aggregation，因此不应作为正式展示案例。

必须先阅读这些文档：

1. `docs/openclaw-project-overview.md`
2. `docs/openclaw-source-family-workflows.md`
3. `docs/openclaw-claim-strength-obligations.md`
4. `docs/diagnostics/openclaw-skill-usage-matrix.md`
5. `docs/diagnostics/openclaw-case-rerun-and-skill-uptake-audit.md`

你的目标不是硬编排议程，而是让 agent 更容易正确发现、理解和使用本地集成 skills，并让报告层阻止没有 basis 的 claim。

总原则：

1. 不要把 runtime 改成 source 选择器、议程编排器、证据充分性裁判或 source ranking 系统。
2. 不要引入证据权重、source 评分、优先级排序、topic-specific fixed agenda、强制固定 round 数。
3. 不要为了提高 skill 使用率而强制每轮调用大量 skill。
4. 不要把 optional-analysis/helper artifact 自动升级成议会结论；helper 输出必须由 agent position、finding、evidence bundle、readiness opinion、round synthesis 或 report basis 显式承接后才能进入报告。
5. 不要保留无意义兼容分支；以当前架构为准，直接修正 agents/skills/reporting。
6. 不要新增超大文件；如发现职责纠缠，按现有包结构拆成小模块。
7. 优先使用本地集成 skills；web search 不能作为默认调查入口。若系统中存在 role prompt 或 skill 文档暗示可以随意 web search/fetch，需要改成“优先本地集成 skills；外部 web search 只有在任务明确授权且记录边界时才可作为补充”。

三层能力要求：

第一层：Source-family workflow card

目的：让 agent 理解同一 source family 内的多层链路，但不强制 agenda。

请审查并更新相关 skill docs、role surfaces、agent entry prompt 或 skill registry 生成逻辑，使以下语义被清晰暴露：

1. GDELT：
   - `fetch-gdelt-doc-search` 是 DOC recon、文章发现和 DOC tone aggregate 入口。
   - `fetch-gdelt-events`、`fetch-gdelt-mentions`、`fetch-gdelt-gkg` 是行级 follow-up surface。
   - DOC 失败、数量少或查询窄，不能直接说明没有媒体记录；agent 应考虑 query lint/rephrase、window 调整、DOC tone mode 或 table pull。
   - GDELT tone 是 media/document tone，不是公众情绪。
2. YouTube：
   - `fetch-youtube-video-search` 发现视频候选。
   - `fetch-youtube-comments` 才提供公共响应样本。
   - 只有 video search 不能支撑公众情绪、样本内议题或评论结构。
3. Regulations.gov：
   - `fetch-regulationsgov-comments` 的 listing 是候选列表/元数据，不是可读评论正文。
   - 正式评论语义分析需要 candidate audit、comment detail、attachment download/text extraction、normalization、formal issue classification。
   - “See Attached” 或正文缺失项必须进入附件文本路线，不能用 listing 当评论 corpus。
4. Public discourse optional-analysis：
   - `materialize-public-discourse-corpus`、`audit-public-discourse-sample-coverage`、`classify-public-discourse-affect`、`aggregate-public-discourse-annotations`、`summarize-public-discourse-sample` 是样本内语义结构辅助链。
   - social-investigator 不应手写逐条情绪标签；标签应由 bounded annotation worker 或 approved taxonomy 产出。
5. Environment observations：
   - fetch/normalize 后，大规模环境数据应优先通过 `aggregate-environment-evidence` 压缩成 coverage/statistics/sample refs，再由 environmental-investigator 解释。
   - 聚合不能输出风险等级、source ranking、健康暴露结论、水资源短缺严重性、火源证明或输送归因。
6. Governance records：
   - Federal Register、USBR、EPA EIS、Regulations.gov 等是 formal/governance surfaces，但不自动证明政策责任、争议强度或公众立场。

第二层：Claim-sensitive soft obligations

目的：不是规定“每个 case 必须调用哪些 skill”，而是让 agent/report 知道“如果要写某类 claim，需要什么 basis”。

请把以下 soft obligations 写入合适的位置：agent role prompt、round readiness/closing surfaces、reporting handoff、skill docs 或 source-family workflow 文档。它们应是提示/义务，不是 runtime hard gate：

1. 若报告要写“样本内公众情绪/议题/来源叙事比例”，应具备 corpus + coverage audit + annotation + aggregation + denominator。
2. 若报告要写“正式评论主要争点/关切/立场线索”，应具备 candidate corpus audit + readable comment/attachment text corpus + formal issue classification + aggregation。
3. 若报告要写“环境数据呈现阶段性变化、峰值、趋势、运行状态”，应具备 environment aggregation 或显式说明只是 item-level example。
4. 若报告要写“A 与 B 在时间/空间上相互对应”，应具备 temporal co-occurrence cue、relation evidence packet 或 fact-check scope review。
5. 若报告要写“来源归因、因果、影响链、政策责任”，应具备 normalized refs + challenger/review + alternatives/limitations；缺少专业模型时只能写相容性、线索或仍需验证。
6. 若 acquisition attempt 是 failed、blocked、zero-signal、receipt-only、executed-without-normalized-refs，agent 必须反思查询/窗口/参数/同族 follow-up/source switch，moderator 才能把 no-actionable-path 写成非继续理由。

第三层：Report validation reverse constraint

目的：报告层不替议会决定证据够不够，但必须阻止没有 basis 的超范围表述。

请审查并更新 `validate-narrative-report`、`draft-narrative-report`、`materialize-reporting-handoff`、相关 tests 和提示词，使 validator 至少能检查并输出 error/warning：

1. 报告出现“公众意见比例、情绪分布、主要议题、公众总体怎么看”时，必须找到 sample denominator 和 annotation/aggregation artifact；若没有，只能写单条/小样本观察。
2. 报告出现“正式评论显示/主要争点/正式公众意见”时，必须找到 candidate audit、可读 comment text 或 attachment text、formal issue classification 或等价 basis。
3. 报告出现“环境趋势/峰值/运行状态/百万级数据摘要”时，必须找到 `aggregate-environment-evidence` 或等价 aggregation artifact；否则必须降级为 item-level evidence examples。
4. 报告出现“来源归因/因果/影响链/政策责任”时，必须找到 relation/fact-check/challenger review basis；否则要写成“相容、提示、仍需验证”。
5. 报告不能把 GDELT tone 写成公众情绪，不能把 YouTube/Bluesky/Regulations.gov 样本写成总体民意，不能把 public source narrative 当作 physical source attribution。
6. 报告不能写成 runtime 日志；应围绕 mission 问题组织为专业调研/决策参考报告。可以简要展示议会过程，但主体应是事件/议题本身的调查结论、证据链、边界和建议。

建议重点检查或修改的文件范围：

1. `docs/openclaw-source-family-workflows.md`
2. `docs/openclaw-claim-strength-obligations.md`
3. `docs/openclaw-project-overview.md`
4. `skills/*/SKILL.md` 中与 GDELT、YouTube、Regulations.gov、public discourse、environment aggregation、reporting validation 相关的说明。
5. `skills/draft-narrative-report/scripts/draft_narrative_report.py`
6. `skills/validate-narrative-report/scripts/validate_narrative_report.py`
7. `skills/materialize-reporting-handoff/scripts/materialize_reporting_handoff.py`
8. runtime role/agent entry/skill surface 生成代码，重点搜索 `skill_use_card`、`source_family_workflows`、`agent_entry_gate`、`role prompt`、`allowed skill`、`web search`、`fetch`。
9. `tests/` 中 reporting、runtime-governance、signal-plane、operator surfaces、case-study 相关测试。

实现顺序：

1. 先审阅现有代码和 docs，确认三层能力目前落点。
2. 更新 skill docs 和 role surfaces，让 agent 能看到本地集成 skill 的多层链路。
3. 更新 report handoff / narrative report prompt，让 report-editor 按 mission 和 claim basis 写报告，不写运行日志。
4. 更新 validator，把 soft obligations 转成报告层的 basis 检查。
5. 增加或更新测试，覆盖 public proportion、formal comment issue、environment aggregation、causal/source attribution、GDELT tone/public sentiment 混淆、web search 默认禁用/本地 skill 优先。
6. 检查文件体量和职责边界。若出现超大、混杂职责文件，进行适当拆分。
7. 运行质量门：
   - `python3 tools/quality_gate.py syntax`
   - `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting case-study`
   - 如时间允许再运行 `python3 tools/quality_gate.py full`

验收标准：

1. Agent 能从提示词/skill docs 中理解 fetch -> normalize -> query -> optional analysis -> council object -> report basis 的链路。
2. Agent 能理解多层 source family，例如 DOC search 不等于 GDELT 全量，YouTube video search 不等于评论语料，Regulations.gov listing 不等于正式评论正文。
3. Runtime 仍然不选择 source、不排序证据、不固定议程。
4. Report validator 能阻止没有 basis 的公众比例、正式评论争点、环境趋势、因果/归因/责任 claim。
5. Optional-analysis/helper 仍是 advisory，必须被议会对象承接后才能进入报告。
6. 文档清晰，旧工作计划不再制造歧义。
7. 测试通过，并在最终回复中说明修改文件、测试结果、仍有风险。
```

