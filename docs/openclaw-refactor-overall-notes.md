# OpenClaw 工程原则、架构护栏与论文展示建议

## 1. 文档定位

本文收敛项目的工程原则、运行护栏、论文展示重点和后续风险。文件名保留历史路径，但本文不再是总体重构说明。

## 2. 工程原则

1. `DB-first`
   - 议会状态、调查对象、报告依据优先写 DB。
   - artifact 是导出和 handoff，不是唯一状态源。
2. `governed autonomy`
   - agent 可以自主调查和提议，但高影响执行必须受 role、approval、side-effect policy 约束。
3. `evidence-backed judgement`
   - finding、proposal、hypothesis、challenge、readiness、report section 必须引用 evidence refs。
4. `helper demotion`
   - 启发式 helper 默认是 audit/advisory，不是结论。
5. `challenger first-class`
   - 反证、替代解释和不确定性必须是一等议会对象。
6. `report basis freeze`
   - 报告正文必须来自 frozen/reporting basis，而不是临时 helper artifact。
7. `recoverability`
   - 关键流程应能在 artifact 缺失时从 DB 恢复。
8. `explicit basis selection`
   - 证据选择、排除、排序、lead basis 和报告依据权重必须来自显式 agent/council/reporting 对象；skill 和 helper 只能暴露候选项或结构化线索。

## 3. 非目标

当前项目不宣称：

1. 自动完成所有生态环境专业报告。
2. 自动做污染源强归因。
3. 自动替代 EIA、司法鉴定、健康风险评估、合规审计。
4. 自动把公共讨论与环境观测匹配成事实真伪。
5. 自动把 temporal co-occurrence 解释为传播或因果。

## 4. 当前最强展示点

1. council-agent 分工明确，runtime principal 与议会 agent 边界清楚。
2. 多轮调查可持续推进。
3. DB-backed council state 可恢复。
4. optional-analysis 被治理和降权。
5. challenger 能阻止过度结论。
6. report basis freeze 使报告依据可审计。
7. archive/history 支持跨案例复用。

## 5. 论文贡献表述建议

建议表述：

`本文设计并实现了一个受治理、DB-first、runtime-governed、多 council-agent 协作的生态环境争议调查议会框架。系统通过角色权限、证据引用、结构化议会对象、反证挑战、阶段审批和报告依据冻结，降低生成式模型直接给出不可审计环境结论的风险。`

避免表述：

1. “系统能自动得出客观真实结论”。
2. “系统能覆盖所有生态环境报告”。
3. “optional-analysis helper 已经是专业模型”。
4. “temporal cue 能证明污染传播方向”。

## 6. 推荐真实案例评测议题

主题：

`开放型或复合型生态环境议题的动态多轮议会调查`

评测时重点观察：

1. public、formal、environment、fire/weather 证据能否进入 DB-backed signal plane。
2. 议会能否在证据不足时自然提交 proposal/readiness/challenge。
3. moderator 是否能在没有固定时空域的 mission 上先做 scoping/decomposition。
4. 每个 round 是否围绕一个子议题展开，并产出 sub-conclusion 或 evidence gap。
5. challenger 是否能阻断具体 subissue，并触发轻量 supplemental round。
6. context packet 是否能避免补充轮加载全量历史。
7. gate 是否能在证据不足时保持 withheld，或在依据充分时 cautious freeze。
8. report 是否只展示 evidence index、uncertainty register、residual disputes 和被承接过的依据。

当前后续开发计划见 `docs/openclaw-agent-autonomy-archive-workplan.md`。真实案例不应先被写成机械化展示流程；应先真实运行、暴露问题、完成修复，再抽取一条可回放轨迹用于论文展示。

案例材料分层：

1. `raw case inputs`
   - 真实或半真实 public、formal、environment、fire/weather 输入，用于探索运行，不承诺每次完整成功。
2. `evaluation fixtures`
   - 从真实运行中抽取的最小可复核输入，用于重放失败、验证修复和保护回归。
3. `replay artifacts`
   - 只有在真实运行通过后，才从一条成功轨迹中抽取，用于论文展示或 smoke test，不作为默认调查流程。

## 7. 重构收口摘要

本轮重构已经收口。旧的 CI、skills、真实案例和动态调查规划工作计划不再作为独立计划保留；其有效基线内容并入基础文档。当前只保留一份新增开发计划：`docs/openclaw-agent-autonomy-archive-workplan.md`。

完成项：

1. 质量门：`syntax`、targeted suites、full unittest gate 均已稳定运行。
2. DB/recovery/schema：DB-first recovery、schema migration hardening 和旧库升级测试已进入基线。
3. runtime governance：approval、transition request、receipt、ledger、dead letter、operator surface 和 runtime-admin 入口完成第一轮硬化。
4. module decomposition：顶层 `src`、`kernel/operator`、`kernel/planes`、`kernel/execution/controller`、`kernel/governance`、`kernel/archive` 已完成 package 化收敛。
5. skills：当前 skills 保持原子能力边界，不进入批量拆分；后续只在能力混杂时重新评估。
6. spatiotemporal relation：relation cue/query/alternatives/evidence packet 已进入 skills baseline。

当前主要验收线：

1. `python3 tools/quality_gate.py syntax`
2. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting case-study`
3. `python3 tools/quality_gate.py full`

质量门边界：

1. `tools/quality_gate.py` 是当前仓库级入口，保留 `syntax`、`test`、`full`、`ci`、`list` 子命令。
2. `syntax` gate 做 AST parse，并阻断重复字面量 dict key。
3. 默认质量门不依赖真实外部 API 或 secrets；真实 provider 凭据只属于人工 real-case run 环境。
4. case-study replay 只有在真实运行抽取稳定 fixture 后才作为固定回放门；当前不把某个案例结论写成默认剧本。

## 8. 后续工程债

论文前不建议处理的大型迁移：

1. `report_basis_*` 字段/CLI 全面改名。
2. legacy analysis kind DB/query schema 物理迁移。
3. `kernel/cli.py`、`kernel/execution/controller/__init__.py`、`kernel/execution/executor.py` 仍偏大，但目前职责边界清楚，暂不继续硬拆。
4. 全部 artifact trace 字段重命名。
5. 全部 optional helper 人工审计完成。

原因：

1. 这些工作对展示价值有限。
2. 容易引入破坏性回归。
3. 当前默认链已经被治理约束，不需要为论文展示强行清空所有历史命名。

## 9. 留存文档

当前留存文档分三类：

基础文档：

1. `docs/openclaw-project-overview.md`
2. `docs/openclaw-refactor-overall-notes.md`
3. `docs/openclaw-skills-refactor-checklist-v2.md`

唯一新增开发计划：

1. `docs/openclaw-agent-autonomy-archive-workplan.md`

历史运行记录：

1. `docs/openclaw-realcase-nyc-smoke-first-run-timeline.md`
   - 只保留第一次真实 run 的事实时间线，不作为开发计划或当前能力基线。
2. `docs/openclaw-realcase-nyc-smoke-transport-chain-run-timeline.md`
   - 只保留开放式 NYC smoke transport-chain run 的事实时间线和流程复盘，不作为开发计划。

## 10. 验收清单

后续真实案例评测和论文展示建议至少满足；agent 自主取证、多轮 continuation 和 archive/history 复用由 `docs/openclaw-agent-autonomy-archive-workplan.md` 独立跟踪：

1. 能初始化 mission/run/round。
2. 能抓取或导入 public/environment/formal 数据。
3. 能 normalize 并 query 出 item-level evidence refs。
4. 能提交 finding/evidence bundle/proposal/challenge/readiness。
5. 宽泛 mission 不要求预置单一时空域。
6. moderator 能生成 investigation plan、subissues 和 round briefs。
7. 每轮能围绕一个 subissue 产出 sub-conclusion 或 evidence gap。
8. 能通过 challenger 阻断过度结论，并触发 supplemental round。
9. supplemental round 能通过 context packet 压缩上下文。
10. 能 freeze 或 withhold report basis。
11. 能生成 final publication。
12. 能说明 helper 输出为什么不是直接结论。
