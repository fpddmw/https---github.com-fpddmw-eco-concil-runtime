# OpenClaw 生产化开发计划

## 1. 目标定义

最终目标不是把 legacy runtime 原样搬回来，而是把当前 skill-first 主链推进成一个可在真实任务上稳定运行、可审计、可回滚、可灰度发布的生产系统。

当前系统已经具备：

- raw artifact -> normalize -> evidence -> board -> investigation -> readiness -> promotion -> reporting handoff -> expert report / canonical decision 主链
- 最小 runtime kernel phase-2：manifest、cursor、registry、ledger、executor、promotion gate、round controller、supervisor state
- board 写入的单机多进程安全：filesystem lock + atomic replace + `board_revision`
- 更强的 runtime 审计元数据：命令快照、skill_args、契约声明、解析路径、输入/输出哈希
- contract-aware runtime baseline：`preflight-skill`、`run-skill --contract-mode off|warn|strict`、missing required input / undeclared path override / artifact_ref mismatch 阻断
- final publication artifact：`final_publication_<round_id>.json`
- 本地确定性 workflow 回归

当前系统尚不具备：

- distributed board coordination 或跨主机锁语义
- full permission-aware / sandboxed runtime enforcement
- planner-backed controller 与 board-driven orchestration
- board-driven、agent-decided orchestration
- 真实 orchestration / fetch-plan / execution 闭环
- archive、history context、rich simulation、生产级观测与容错

## 2. 文档分工

- [openclaw-first-refactor-blueprint.md](openclaw-first-refactor-blueprint.md)：唯一架构基线
- [openclaw-skill-phase-plan.md](openclaw-skill-phase-plan.md)：阶段性交付计划
- [openclaw-collaboration-status.md](openclaw-collaboration-status.md)：当前完成度与最近状态
- `openclaw-production-development-plan.md`：从当前状态到生产环境的开发路线、准入门槛与发布顺序

## 3. 生产前定义的环境阶段

### 3.1 开发环境

- 允许本地 deterministic seed 与人工检查
- 允许 artifact schema 快速迭代
- 必须保证完整 unittest 通过

### 3.2 集成环境

- 接入真实 mission scaffold、prepare、fetch execution、artifact import
- 验证跨模块路径、锁、幂等、重复执行与失败恢复
- 必须补齐契约验证与回归集

### 3.3 Shadow Test

- 运行真实任务副本，但不发布正式结果
- 保留人工审核
- 必须记录完整 audit ledger、环境配置、输入快照与输出差异

### 3.4 Production Pilot

- 仅允许有限任务域和人工批准发布
- 必须具备明确的回滚路径
- 必须有运行告警、超时保护、失败重试与禁止覆写 canonical artifact 的机制

## 4. 从当前状态到生产的分批计划

### P1 reporting / decision 完整化

目标：把 promotion basis 真正接成可消费的正式下游对象。

交付项：

1. expert report draft skills
2. canonical expert report publish skill
3. canonical decision publish skill
4. final publication artifact skill
5. reporting / decision 回归测试

完成条件：

- round 在 promoted 与 withheld 两类状态下都能得到稳定的下游正式对象
- 所有 decision artifact 可追溯到 reporting handoff、promotion basis、readiness、supervisor state

当前进度补充：

- `expert report draft`、`canonical expert report publish`、`canonical decision publish`、`final publication artifact` 已完成
- P1 已完成，reporting / decision 主链现在已经能从 promotion basis 稳定收敛到 `final_publication_<round_id>.json`
- 因此后续编码批次不应再扩张新的 reporting 分支，而应转向 planner artifact、真实 orchestration scaffold 与更强的 runtime hardening

### P2 orchestration / contract 闭环

目标：让真实 mission 输入重新接回 skill-first 主链。

交付项：

1. mission scaffold / contract runtime skill 化
2. prepare-round / fetch-plan / import-fetch-execution 的最小闭环
3. 真实 external fetch 与 normalize 主链回归

完成条件：

- 真实任务可以从 mission 输入走到 canonical signal plane，而不是只依赖测试 seed

### P3 archive / history context / simulation 扩展

目标：恢复跨轮次与 richer benchmark 能力。

交付项：

1. case library / signal corpus 接回
2. history context 供 moderator / supervisor 使用
3. richer simulation presets 与 benchmark cases

完成条件：

- 至少两类真实任务具备历史对照与 richer simulation regression

### P4 runtime hardening

目标：把当前可运行系统补成可控系统。

交付项：

1. artifact schema versioning 与兼容策略
2. distributed-safe idempotency / retry / lock / partial rerun 语义
3. observability：run summary、structured logs、failure surface
4. full permission-aware / sandboxed execution enforcement
5. 配置、超时、预算、权限边界

完成条件：

- 失败可定位、可重跑、可防止 canonical artifact 被错误覆写，并且 runtime 能按 declared contract 治理 skill 执行

推进顺序补充：

- P4 的 preflight 与 baseline enforcement 已经落地，后续重点应转向更强的 permission boundary、sandboxed side-effect governance 和 distributed-safe control plane
- planner-backed controller cutover 应放在 planner artifact 稳定之后，而不是和 governance 同时硬切

### P5 shadow test 与 pilot 发布

目标：把系统从开发态推进到受控上线态。

交付项：

1. shadow test playbook
2. production pilot runbook
3. 回滚与人工审批流程
4. 真实案例验收标准

完成条件：

- 至少两轮真实任务 shadow test 无严重审计或控制面问题
- 至少一轮 production pilot 在人工批准下闭环完成

## 5. 推荐执行顺序

1. P1 已完成；下一步先做 planner artifact 与 P2，把真实 mission 输入重新接回 skill-first 主链。
2. 然后做 P3，因为 archive / history / simulation 会显著提高任务质量和回归信心。
3. 再推进 P4，把系统从“能跑”补到“可控”。
4. 最后完成 P5，把系统从 pre-production 推进到受控 pilot 发布。

## 6. 风险清单

- 契约漂移：上下游 artifact 字段如果频繁变化，会直接打断 runtime、reporting 和 decision。
- 控制面语义不足：当前 runtime 还不是生产级调度器，重复执行、失败恢复与部分重跑仍需加固。
- 并发边界风险：当前 board 写入只保证单机文件系统上的多进程安全，还不是跨主机或分布式协调方案。
- 真实外部依赖风险：真实 fetch、第三方 API、网络与认证问题尚未纳入当前主链验证。
- 审计链完整性风险：如果 publish 阶段未强制绑定上游 refs，就会出现“结果存在、来源不清”的问题。
- 治理面风险：runtime 虽然已经读取 contract metadata，但尚未真正按 contract 做 allow/deny、权限边界和 side-effect enforcement。
- 运行成本风险：多 agent、历史上下文、长链 reporting 可能显著提高 token 和运行成本，需要预算边界。

## 7. 环境准入标准

进入 shadow test 之前至少满足：

1. P1 完成
2. P2 至少完成最小 mission -> fetch -> normalize -> promotion 闭环
3. 所有本地与集成回归通过
4. runtime hardening 至少补齐 lock、retry、overwrite guard、structured logs

进入 production pilot 之前至少满足：

1. P1 到 P4 完成
2. 至少两轮 shadow test 成功
3. 有人工批准与回滚机制
4. 有明确的任务域边界，不做无约束全面放量

## 8. 当前最近一步

当前已经完成 reporting / decision 前三批：

- `eco-materialize-reporting-handoff`
- `eco-draft-council-decision`
- `eco-draft-expert-report`
- `eco-publish-expert-report`
- `eco-publish-council-decision`
- `eco-materialize-final-publication`

同时已经完成一轮 control-plane hardening：board 单机锁语义、registry metadata snapshot、ledger 审计增强。

所以下一个最合适的代码批次是：

1. planner artifact schema / minimal planner design
2. mission scaffold / prepare / fetch-plan / import execution 的最小 contract 闭环

这两项应并行规划、顺序落地，而不是继续扩大 runtime 的业务面。