# OpenClaw 生产化开发计划

## 1. 目标定义

最终目标不是把 legacy runtime 原样搬回来，而是把当前 skill-first 主链推进成一个可在真实任务上稳定运行、可审计、可回滚、可灰度发布的生产系统。

当前系统已经具备：

- raw artifact -> normalize -> evidence -> board -> investigation -> readiness -> promotion -> reporting handoff -> expert report / canonical decision 主链
- 最小 runtime kernel phase-2：manifest、cursor、registry、ledger、executor、promotion gate、round controller、supervisor state
- 本地确定性 workflow 回归

当前系统尚不具备：

- final publication artifact
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
2. idempotency / retry / lock / partial rerun 语义
3. observability：run summary、structured logs、failure surface
4. 配置、超时、预算、权限边界

完成条件：

- 失败可定位、可重跑、可防止 canonical artifact 被错误覆写

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

1. 先完成 P1，因为 reporting / decision 是当前主链后面最直接缺失的一段。
2. 再做 P2，因为没有真实 orchestration，就不能验证主链在真实环境是否成立。
3. 然后做 P3，因为 archive / history / simulation 会显著提高任务质量和回归信心。
4. 最后完成 P4 与 P5，把系统从“能跑”推进到“可控上线”。

## 6. 风险清单

- 契约漂移：上下游 artifact 字段如果频繁变化，会直接打断 runtime、reporting 和 decision。
- 控制面语义不足：当前 runtime 还不是生产级调度器，重复执行、失败恢复与部分重跑仍需加固。
- 真实外部依赖风险：真实 fetch、第三方 API、网络与认证问题尚未纳入当前主链验证。
- 审计链完整性风险：如果 publish 阶段未强制绑定上游 refs，就会出现“结果存在、来源不清”的问题。
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

当前已经完成 reporting / decision 前两批：

- `eco-materialize-reporting-handoff`
- `eco-draft-council-decision`
- `eco-draft-expert-report`
- `eco-publish-expert-report`
- `eco-publish-council-decision`

所以下一个最合适的代码批次是：final publication artifact，把 canonical reports 与 canonical decision 收敛成最终发布对象，而不是继续扩大 runtime。