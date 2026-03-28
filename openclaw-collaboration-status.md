# OpenClaw 议会协作状态清单

## 1. 当前定位

- 文档分工：
- `openclaw-first-refactor-blueprint.md` 保留为唯一架构基线。
- `openclaw-skill-phase-plan.md` 保留为交付阶段计划。
- `openclaw-collaboration-status.md` 只保留当前完成度、现状判断与最近建议。
- `openclaw-production-development-plan.md` 负责从当前状态推进到生产环境的开发计划与准入标准。

- Phase A：部分完成，主要是返回契约与测试收口持续进行中。
- Phase B：已完成，candidate -> evidence bridge 已经打通。
- Phase C：已完成到 C2，board 已具备记录、整理、总结、brief 化能力。
- Phase D：已完成 D1 + D2，next actions、probes、round readiness、promotion basis 已经落地。
- Phase E：已完成前两批，reporting handoff、council decision draft、expert report draft、canonical report publish、canonical decision publish 已经落地。
- runtime kernel：已完成到第 2 阶段，manifest / cursor / registry / ledger / executor wrapper、promotion gate、round controller、supervisor entry 已可运行。

## 2. 当前能力面

- 已交付 36 个 skill。
- 其中 10 个属于 signal normalize / query / lookup。
- 其中 3 个属于 candidate / audit。
- 其中 6 个属于 evidence 中间层。
- 其中 8 个属于 board 层，已经覆盖 delta、note、hypothesis、challenge、task、summary、brief。
- 其中 4 个属于 investigation / readiness / promotion 层，已经覆盖 next actions、probe、readiness、promotion basis。
- 其中 5 个属于 reporting / decision 层，已经覆盖 reporting handoff、council decision draft、expert report draft、canonical expert report publish、canonical decision publish。
- 此外已经新增 1 个最小 runtime kernel 包，用于 manifest、cursor、registry、ledger 与 skill execution。

## 3. 距离全流程议会协作还差什么

- [x] 原始 artifact 进入 signal plane。
- [x] signal plane 进入 candidate / evidence 中间层。
- [x] evidence 进入 board working-state。
- [x] board 从 working-state 进入 organized-state 与 brief。
- [x] board brief 进入 next-action queue。
- [x] falsification probe 进入显式生命周期。
- [x] round readiness 进入正式 gate。
- [x] evidence basis 进入 promote / freeze。
- [x] promotion basis 进入 compact reporting handoff。
- [x] reporting handoff 进入 compact council decision draft。
- [x] 最小 runtime kernel 重新建立。
- [x] runtime 第 2 阶段的 promote/freeze gate 与 supervisor 入口回接。
- [x] 全链路 supervisor / simulation 闭环回归。
- [ ] final publication artifact 闭环。
- [ ] real orchestration / archive / history-context 闭环。

判断：如果只看主链能力面，当前仓库已经具备从 raw artifact 到 promote basis、reporting handoff、role report、canonical decision，再到 minimal supervisor state 的 skill-first 主链。

判断：如果看当前精简仓库的工程闭环，而不把 legacy 大 runtime 的全部外延一并算进来，那么目前已经补齐 raw -> promotion -> canonical decision 的主链，但还没有补齐 final publication、真实 orchestration、archive/history-context 这些外层能力：

- 一批是 runtime 第 2 阶段：promote/freeze gate、round controller、supervisor 入口。
- 一批是全链路 supervisor / simulation 回归，把 skill-first 主链重新接成完整运行面。
- 一批是 reporting / decision 第一批：reporting handoff、council decision draft。
- 一批是 reporting / decision 第二批：expert report draft、canonical expert report publish、canonical decision publish。

## 4. D 与旧 runtime 的关系

- D 不是空白区。旧 runtime 在 `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/investigation/actions.py` 中已经有 next-action planning 原型。
- D 不是空白区。旧 runtime 在 `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/reporting/readiness.py` 中已经有 readiness 原型。
- D 不是空白区。旧 runtime 在 `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/reporting/promotion.py` 中已经有 promotion 原型。
- 当前真正缺的是：把这些 runtime-first 模块拆成当前 skill-first 架构可接受的原子能力，并且重新定义 artifact 契约，而不是继续沿用旧 runtime 的耦合形态。

## 5. runtime 如何设置

- runtime 只保留最小内核，不承载 claim / evidence / board / readiness 的业务判断。
- runtime 第 1 层只负责 run manifest、路径解析、artifact registry、receipt/event ledger。
- runtime 第 2 层只负责 skill executor wrapper，把一次 skill 调用落成稳定的 run event。
- runtime 第 3 层是最小 round controller / supervisor CLI，用于把既有 skills 串成稳定的运行面，而不是恢复完整旧框架。

当前已完成：

- `eco-concil-runtime/scripts/eco_runtime_kernel.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/paths.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/registry.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/manifest.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/ledger.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/executor.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/cli.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/gate.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/supervisor.py`
- `skills/eco-materialize-reporting-handoff/`
- `skills/eco-draft-council-decision/`
- `skills/eco-draft-expert-report/`
- `skills/eco-publish-expert-report/`
- `skills/eco-publish-council-decision/`

## 6. runtime 何时动工

- 这一阶段已经完成到第 2 阶段。
- 当前 kernel 已能初始化 run、刷新 skill registry、执行 skill、记录 receipt、追加 audit ledger、推进 round cursor。
- 当前 kernel 也已能对 readiness 落 promotion gate、以单命令跑完 board -> D1 -> D2 -> promotion，并额外写出 supervisor state。
- `eco-summarize-round-readiness` 与 `eco-promote-evidence-basis` 现在已经接入 kernel phase-2 控制流，而没有把业务判断拉回 runtime。

## 7. 推荐的下一步

1. 继续补 reporting / decision 第三批：final publication artifact，把 canonical reports 与 canonical decision 收敛成最终发布对象。
2. 把 orchestration / contract scaffold 接回 skill-first 主链，形成真实 mission -> prepare -> fetch -> normalize 的运行闭环。
3. 把 archive、history context、richer simulation 与 runtime hardening 接回，为 shadow test 和 production pilot 做准备。