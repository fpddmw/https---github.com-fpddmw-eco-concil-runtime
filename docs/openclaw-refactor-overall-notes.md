# OpenClaw 总体重构说明、注意点与硬要求

## 1. 目标重新定义

OpenClaw 的最终目标不是“社会信号和物理信号匹配系统”，也不是“环保辟谣工具”。  
目标应被重定义为：

1. 面向决策者的环保政策研究与调查协作系统
2. 支持多角色调查、反证、审议、人工确认
3. 最终产出可审计、可复核、可引用证据 basis 的研究报告

## 2. 非协商要求

以下要求必须视为硬约束：

1. 所有抓取结果、归一化结果、跨轮状态、报告证据 basis 必须落 DB。
2. `runtime kernel` 只负责边界、审计、审批、回放，不负责默认研究结论。
3. `moderator` 是唯一阶段推进者。
4. 规则化 / 启发式 skill 必须经过你本人审计后才能进入默认链。
5. 任何 simulation / scenario 输出都必须明确标记为“假设分析”，不是事实证据。
6. 报告必须面向政策决策，而不是面向“某条 claim 是否被打脸”。

## 3. 数据原则

## 3.1 薄核心契约

强约束的 canonical 层只保留：

1. raw artifacts
2. ingest batches
3. normalized signals
4. source provenance
5. round / board / task / challenge / finding / proposal / readiness / transition
6. evidence bundles
7. report drafts / final reports

## 3.2 厚派生层

以下对象只能是 optional derived layer：

1. claim candidate
2. issue cluster
3. route
4. controversy map
5. representation gap
6. diffusion edge
7. coverage score

它们可以存在，但不能构成系统主干，更不能强制所有议题都沿同一分析链前进。

## 3.3 禁止的设计倾向

1. 为了“结构整齐”而过度抽象中层对象
2. 把 agent 强行塞进统一 claim/route/coverage ontology
3. 把“物理-舆情匹配”当成所有环保问题的默认分析框架
4. 用 fixed pipeline 替代真实调查

## 4. 规则和启发式的审计要求

## 4.1 必须审计的内容

1. 关键词提取规则
2. cluster / route / readiness / next-action 打分规则
3. representation / diffusion / linkage 规则
4. 任意 claim-to-observation matching 规则
5. 任意用于“问题是否 ready / 是否 freeze report basis”的阈值规则
6. social data source 的代表性假设
7. formal data source 的缺失模式
8. environment data source 的时空偏差

## 4.2 审计交付物

每条规则必须有：

1. rule id
2. 所属 skill
3. 规则文本
4. 触发条件
5. 例外条件
6. 已知偏差
7. 代表性风险
8. 样例输入
9. 样例输出
10. 审计状态
11. 生效版本

## 4.3 上线原则

1. 未审计规则不得进入默认链
2. 审计通过也只能先以 optional 模式上线
3. 任何规则变更必须产生新版本号
4. 报告中应能回溯某结论使用了哪版规则

## 5. 建议的系统输出

最终对决策者的输出不应只是一份结论摘要，而应至少包含：

1. 问题定义与决策问题边界
2. 地区 / 议题背景概况
3. 证据来源和时空范围
4. 关键发现
5. 替代方案与比较
6. 风险与不确定性
7. 社会 / 生态 / 法规 / 经济影响
8. 建议措施
9. 剩余争议和待补证据
10. 引用与证据索引

## 6. 两类典型任务在重构后应如何工作

## 6.1 任务 A：某地区是否适合/有必要修建水库

### 正确工作流

1. `moderator` 定义决策问题：
   - 是否有必要
   - 是否适合
   - 比较的是哪些替代方案
2. `environmental-investigator / hydrology-analyst`
   - 收集流域、水文、洪水、降雨、枯水、生态约束
3. `formal-record-investigator / policy-analyst`
   - 收集法规、审批条件、EIA、规划、政策边界
4. `community-impact-analyst / public-discourse-investigator`
   - 收集移民安置、社区感知、舆论、争议点
5. `challenger`
   - 提出替代解释、数据缺口、过度建设风险、偏差来源
6. `moderator`
   - 组织多轮讨论，冻结 evidence bundle
7. `report-editor`
   - 形成报告与建议
8. operator
   - 审批阶段推进和最终发布

### 应有输出

1. `region baseline`
2. `hydrology evidence bundle`
3. `ecology and land-use evidence bundle`
4. `policy and approval bundle`
5. `stakeholder and sentiment bundle`
6. `option comparison`
7. `risk register`
8. `uncertainty register`
9. `decision-maker report`

### 不应退化成

1. “社交媒体说会缺水，物理数据是否支持”
2. “这条 public claim 是否被 observation 打脸”

## 6.2 任务 B：调查某地区环境舆情的原因为何并给出处理建议

### 正确工作流

1. `moderator` 明确问题：
   - 要调查的是成因、演化、误解、利益冲突还是政策执行问题
2. `public-discourse-investigator`
   - 收集媒体、平台、社区表达
3. `formal-record-investigator`
   - 收集正式公告、政策文书、审批记录、投诉处理记录
4. `environmental-investigator`
   - 收集必要的物理背景信息，但这不是默认中心
5. `challenger`
   - 检查偏差样本、平台偏置、误导叙事、错因归纳
6. `moderator`
   - 汇总成因类型、影响群体、可处理措施
7. `report-editor`
   - 形成面向治理者的处置建议

### 应有输出

1. `sentiment drivers`
2. `stakeholder map`
3. `formal-public misalignment summary`
4. `response options`
5. `communication plan`
6. `policy / enforcement / engagement recommendations`

## 7. 对 simulation / dynamics 的要求

## 7.1 可以引入

可以引入：

1. 水文或环境情景比较
2. 政策方案对比
3. 舆情演化情景推演
4. 风险传播路径推演

## 7.2 必须满足

1. 输入假设显式记录
2. 使用的参数来源显式记录
3. 输出类型标记为 `scenario` 或 `simulation`
4. 不得与事实证据混写
5. 必须能被 challenger 质疑

## 8. 测试与验收要求

## 8.1 功能验收

1. investigator 能独立完成 fetch -> normalize -> query -> finding 提交闭环
2. moderator 能组织 round，但不能绕过 operator 直接推进正式状态
3. report-editor 能在冻结 basis 上生成决策者报告
4. 删除导出 artifact 后，DB 仍能恢复研究状态

## 8.2 规则验收

1. 所有启发式规则均有审计记录
2. 所有启发式结果都能回溯到规则版本
3. 默认链不依赖未审计规则

## 8.3 研究质量验收

1. 同一问题可重复执行
2. 多轮调查有清晰状态推进记录
3. 输出报告能列出证据 basis 与不确定性
4. 输出不是 claim-matching 玩具，而是真正可用于政策参考

## 9. 迁移顺序建议

1. 先缩 kernel：把阶段拥有权从 runtime 拿走
2. 再定角色：写死权限矩阵
3. 再瘦数据：缩成薄核心契约
4. 再降 heuristic：全部改 optional
5. 再补 investigator skill
6. 最后重写 reporting

## 10. 一句话结论

当前项目最需要的不是“继续把现有 phase-2 硬化”，而是：

1. 把 runtime 从“流程拥有者”收缩成“边界与审计者”
2. 把 agent 从“报告撰写员”恢复成“调查者”
3. 把输出目标从“争议澄清”抬升成“政策研究报告”

## 11. 2026-04-29 验收审阅回写

已完成：

1. 功能验收的四项主要求已由 targeted regression 覆盖：investigator 可形成 DB finding/evidence bundle，moderator 只能请求推进，operator 审批后才提交状态，report-editor/reporting 链可在冻结 basis 上生成决策者报告。
2. 规则验收的默认链约束已经落地：optional-analysis 需要 `skill_approval_requests -> approval -> run-skill --skill-approval-request-id -> consumption`，legacy helper surface 带 freeze/governance metadata，不再是默认报告依据。
3. 研究质量方向已由 policy research case fixture 覆盖：报告输出面向政策决策，包含 evidence index、citation index、不确定性、剩余争议和建议，而不是 claim matching 结论。

未完成：

1. freeze line 仍是审计冻结记录，不是完整人工审计批准记录。
2. 历史命名和兼容 contract 尚未整体破坏性迁移。
3. 未运行全仓 discover 回归；本次验收运行的是直接相关最小集合加 runtime/board/reporting 大集合。

新发现的问题：

1. 历史文档和部分测试文案仍有 `coverage / report basis / contradiction` 等旧词汇；当前不构成默认链路风险，但会影响后续维护者理解。
2. `freeze-report-basis` 仍作为兼容 skill id 存在；语义已收缩为 `report-basis-freeze`，但命名债需要单独迁移。

是否影响后续计划：

1. 不阻塞当前验收。
2. 后续不得把未完成项解释为允许恢复旧主链；它们应作为审计、命名和历史兼容清理工作推进。

## 12. 2026-04-29 残留风险收尾复核

已完成：

1. `freeze-report-basis` 已替代旧公开 report basis skill 入口；runtime transition kind 改为 `freeze-report-basis`。
2. `runtime-control-freeze` 已从 deliberation `report-basis-freeze` 中拆出，control query surface 不再复用 report basis object kind。
3. council judgement 解析入口改为 `phase2_report_basis_resolution.py`；历史 `report_basis_*` 字段仅作为兼容/replay 字段保留。
4. 全仓 unittest discover 已运行通过：`235` 项，`420.703s`。

未完成：

1. 已完成默认 `report-basis-gate` runtime/CLI/artifact 收口；后续 report-basis-only 收尾已删除旧 promotion 兼容命名。
2. 未删除 frozen legacy analysis kind query 兼容面。

### 2026-04-29 report-basis gate 命名收尾补记

已完成：

1. 默认 gate stage、gate handler、operator command 和 artifact path 已改为 `report-basis-gate / apply-report-basis-gate / runtime/report_basis_gate_<round>.json`。
2. runtime/control/reporting surfaces 已双写 `report_basis_*` 字段；旧 `report_basis_*` 仅作为兼容/replay 字段保留。
3. full discover 已重新运行通过：`235` 项，`216.605s`。

未完成：

1. 底层 DB column 与历史 replay artifact 的破坏性 rename 未执行。
2. optional-analysis legacy query compatibility surface 未物理删除。

新发现的问题：

1. 旧路径测试残留已修正；loader 仍保留 legacy path fallback。

是否影响后续计划：

1. 不阻塞；剩余风险只属于显式 migration 计划，不属于 runtime 默认语义。

新发现的问题：

1. 原先 `report-basis-freeze` 同时代表 report basis 和 runtime control freeze，会造成 registry/query 语义覆盖；本轮已拆分。
2. repo-level dashboard/package 测试不应依赖缺失的 archive master-plan 文档；本轮已改为 synthetic doc fixture。

是否影响后续计划：

1. 不阻塞本轮验收；剩余内容是显式 breaking migration 债务。
2. 后续新增文档和代码必须区分 `report-basis-freeze` 与 `runtime-control-freeze`。

### 2026-04-29 report-basis-only 收尾补记

已完成：

1. 已按“无历史 DB 价值、以最新架构为准”删除旧 promotion 兼容语义：无 `promotion-gate`、`apply-promotion-gate`、`promotion_status`、`promote_allowed`、`promotion_path`、`promotion/` 默认路径残留。
2. runtime gate、control DB、reporting/publication、archive/history context 与测试夹具统一使用 `report_basis_*`。
3. gate withheld 状态改为 `report-basis-freeze-withheld`；冻结完成状态为 `frozen`。

未完成：

1. optional-analysis 仍有 legacy/audit-only 标记，表示冻结的启发式治理边界，不再表示旧 concil 数据兼容。

新发现的问题：

1. 文档早期章节保留了“兼容保留/未做 breaking rename”的历史记录；阅读验收结论时以本节和最新 checklist 回写为准。

是否影响后续计划：

1. 不阻塞。后续计划不再需要为旧 promotion 命名做兼容迁移，只需继续推进 optional-analysis 的人工审计和治理清理。

本次实际运行：

1. `.venv/bin/python -m compileall -q eco-concil-runtime/src skills tests`：通过。
2. 目标回归 `84` 项通过，用时 `81.445s`。
3. `.venv/bin/python -m unittest discover -s tests -v`：`235` 项通过，用时 `222.805s`。

## 13. 2026-04-29 验收补充：默认入口旧 analysis surface 清理

已完成：

1. 本次补充审阅发现默认 agent entry/operator surface 仍暴露 `claim-cluster` analysis 查询模板，与“默认链不依赖旧 claim/coverage ontology”的硬约束冲突。
2. 已修正 `phase2_agent_entry_profile.py` 与 `kernel/agent_entry.py`，默认 operator view 不再输出 `claim-cluster` 查询命令。
3. 已补测试断言，默认 agent entry operator surface 不包含旧 claim analysis command key 或命令文本。

未完成：

1. optional-analysis freeze line 仍为 `audit-pending`，不是完整人工审计批准记录。
2. 旧 analysis kind / query contract 仍保留为 compatibility query / replay surface，未做物理删除。

新发现的问题：

1. 文档结论和代码默认 operator surface 曾有一处偏差：文档说旧 analysis query commands 已移除，代码仍保留 `claim-cluster` 默认模板；本次已同步。
2. 当前 active skill 数量为 `79`，原始 `88` 个 skill 盘点属于迁移基线，后续文档应避免混用这两个口径。

是否影响后续计划：

1. 不阻塞当前重构验收；修复后默认 investigator/moderator entry 与审批链一致。
2. 后续计划仍聚焦完整人工审计、legacy query/contract migration 和必要的全仓回归，不得恢复旧 claim matching 主链。

本次实际运行：

1. `.venv/bin/python -m unittest tests.test_agent_entry_gate tests.test_runtime_source_queue_profiles tests.test_optional_analysis_guardrails tests.test_policy_research_case_fixtures tests.test_skill_approval_workflow tests.test_source_queue_rebuild tests.test_reporting_workflow tests.test_reporting_publish_workflow tests.test_reporting_query_surface tests.test_runtime_kernel tests.test_board_workflow -v`：`111` 项通过。
2. `git diff --check`：通过。

## 14. 2026-04-29 破坏性清理：旧 advisory/compat 接口删除

已完成：

1. 删除 direct council advisory compiler 与测试；runtime controller、planning profile、agent entry 均不再识别该 source。
2. 删除 agent-entry advisory 刷新接口和 artifact path surface：无 `--refresh-advisory-plan`、无 `agent_advisory_plan_path`、无 advisory materializer。
3. 删除 compatibility facade：`kernel/investigation_planning.py`、`phase2_fallback_planning.py`、`kernel/phase2_contract.py`。
4. 删除 active `build-normalization-audit` skill 与 registry/source queue policy。
5. planner 输出不再支持 `agent-advisory / advisory-only`；council proposal 直承路径重命名为 `council_proposal_queue`。

未完成：

1. 旧 analysis kind / query object / canonical contract 命名尚未物理迁移。
2. 若要求连内部 helper 模块名都不得含 `fallback`，还需要后续大规模模块重命名和全仓回归。

新发现的问题：

1. 旧兼容并不只体现在默认链；CLI flag、artifact path、plan normalization 和 state wrapper 也会形成外部接口。
2. `build-normalization-audit` 是实际 active skill，不只是文档残留。

是否影响后续计划：

1. 会破坏外部旧调用；这是本次按用户要求执行的预期结果。
2. 不影响 runtime kernel 新原则；默认 kernel 仍只负责边界、审批、审计、回放和 DB 一致性。

本次实际运行：

1. `.venv/bin/python -m unittest tests.test_investigation_contracts tests.test_agent_entry_gate tests.test_orchestration_planner_workflow tests.test_runtime_kernel -v`：`58` 项通过。
