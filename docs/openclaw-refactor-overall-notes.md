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

## 3. 非目标

当前项目不宣称：

1. 自动完成所有生态环境专业报告。
2. 自动做污染源强归因。
3. 自动替代 EIA、司法鉴定、健康风险评估、合规审计。
4. 自动把公共讨论与环境观测匹配成事实真伪。
5. 自动把 temporal co-occurrence 解释为传播或因果。

## 4. 当前最强展示点

1. 多 agent 分工明确。
2. 多轮调查可持续推进。
3. DB-backed council state 可恢复。
4. optional-analysis 被治理和降权。
5. challenger 能阻止过度结论。
6. report basis freeze 使报告依据可审计。
7. archive/history 支持跨案例复用。

## 5. 论文贡献表述建议

建议表述：

`本文设计并实现了一个受治理、DB-first、多 agent 协作的生态环境争议调查议会框架。系统通过角色权限、证据引用、结构化议会对象、反证挑战、阶段审批和报告依据冻结，降低生成式模型直接给出不可审计环境结论的风险。`

避免表述：

1. “系统能自动得出客观真实结论”。
2. “系统能覆盖所有生态环境报告”。
3. “optional-analysis helper 已经是专业模型”。
4. “temporal cue 能证明污染传播方向”。

## 6. 推荐真实案例评测议题

主题：

`跨区域烟霾 / PM2.5 时空关系争议的多轮议会调查`

评测时重点观察：

1. public、formal、environment、fire/weather 证据能否进入 DB-backed signal plane。
2. 议会能否在证据不足时自然提交 proposal/readiness/challenge。
3. follow-up round 是否由 evidence gap 和 open challenge 触发，而不是预设剧本触发。
4. challenger 是否能提出本地源、时滞、站点代表性、时间窗错配等质疑。
5. gate 是否能在证据不足时保持 withheld，或在依据充分时 cautious freeze。
6. report 是否只展示 evidence index、uncertainty register、residual disputes 和被承接过的依据。

案例评测计划见 `docs/openclaw-case-study-evaluation-workplan.md`。该议题不应先被写成机械化展示流程；应先真实运行、暴露问题、完成修复，再抽取一条可回放轨迹用于论文展示。

## 7. 重构收口摘要

本轮重构已经收口，除真实案例评测外不再保留阶段性 workplan。完成项：

1. 质量门：`syntax`、targeted suites、full unittest gate 均已稳定运行。
2. DB/recovery/schema：DB-first recovery、schema migration hardening 和旧库升级测试已进入基线。
3. runtime governance：approval、transition request、receipt、ledger、dead letter、operator surface 和 runtime-admin 入口完成第一轮硬化。
4. module decomposition：顶层 `src`、`kernel/operator`、`kernel/planes`、`kernel/execution/controller`、`kernel/governance`、`kernel/archive` 已完成 package 化收敛。
5. skills：当前 82 个 active skills 保持原子能力边界，不进入拆分；后续只在能力混杂时重新评估。
6. spatiotemporal relation：relation cue/query/alternatives/evidence packet 已进入 skills baseline。

当前主要验收线：

1. `python3 tools/quality_gate.py syntax`
2. `python3 tools/quality_gate.py test module-decomposition runtime-governance reporting case-study`
3. `python3 tools/quality_gate.py full`

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

最终保留 5 个文档：

1. `docs/openclaw-project-overview.md`
2. `docs/openclaw-refactor-overall-notes.md`
3. `docs/openclaw-ci-quality-gates-workplan.md`
4. `docs/openclaw-skills-refactor-checklist-v2.md`
5. `docs/openclaw-case-study-evaluation-workplan.md`

## 10. 验收清单

真实案例评测和后续论文展示建议至少满足；具体实施由 `docs/openclaw-case-study-evaluation-workplan.md` 独立跟踪：

1. 能初始化 mission/run/round。
2. 能抓取或导入 public/environment/formal 数据。
3. 能 normalize 并 query 出 item-level evidence refs。
4. 能提交 finding/evidence bundle/proposal/challenge/readiness。
5. 能打开第二轮并 carry over 状态。
6. 能执行至少一个 structured relation helper。
7. 能通过 challenger 阻断过度结论。
8. 能 freeze 或 withhold report basis。
9. 能生成 final publication。
10. 能说明 helper 输出为什么不是直接结论。
