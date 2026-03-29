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
- Phase E：已完成前三批，reporting handoff、council decision draft、expert report draft、canonical report publish、canonical decision publish、final publication 已经落地。
- runtime kernel：已完成到第 2 阶段并补入 planner-backed controller preview，manifest / cursor / registry / ledger / executor wrapper、orchestration plan、promotion gate、round controller、supervisor entry 已可运行，但当前仍不是 full board-driven、agent-decided orchestration runtime。

## 2. 当前能力面

- 已交付 38 个 skill。
- 其中 10 个属于 signal normalize / query / lookup。
- 其中 3 个属于 candidate / audit。
- 其中 6 个属于 evidence 中间层。
- 其中 8 个属于 board 层，已经覆盖 delta、note、hypothesis、challenge、task、summary、brief。
- 其中 4 个属于 investigation / readiness / promotion 层，已经覆盖 next actions、probe、readiness、promotion basis。
- 其中 6 个属于 reporting / decision 层，已经覆盖 reporting handoff、council decision draft、expert report draft、canonical expert report publish、canonical decision publish、final publication。
- 其中 1 个属于 orchestration / planning 层，已经覆盖 phase-2 orchestration plan artifact。
- 此外已经新增 1 个最小 runtime kernel 包，用于 manifest、cursor、registry、ledger 与 skill execution。
- board 写入当前已具备单机多进程安全：filesystem lock + atomic replace + `board_revision`。
- runtime registry 当前已能快照 skill contract 与 agent metadata，runtime 也已具备 contract-aware preflight 与 enforcement baseline；但完整 permission boundary 与 sandboxed side-effect enforcement 仍未完成。

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
- [ ] full permission-aware / sandboxed skill execution。
- [x] planner artifact / planner-backed phase-2 controller cutover。
- [ ] board-driven、agent-decided orchestration。
- [ ] real orchestration / archive / history-context 闭环。

判断：如果只看主链能力面，当前仓库已经具备从 raw artifact 到 promote basis、reporting handoff、role report、canonical decision、final publication，再到 planner-backed phase-2 controller 与 minimal supervisor state 的 skill-first 主链。

判断：如果看当前精简仓库的工程闭环，而不把 legacy 大 runtime 的全部外延一并算进来，那么目前更准确的表述是：已经补齐 raw -> promotion -> canonical decision -> final publication 的 skill-first 主链，并把 phase-2 controller 从固定队列切到 planner-backed preview，同时补上了 contract-aware runtime baseline、单机 board 并发写和更强的 runtime 审计元数据；但还没有补齐 full board-driven orchestration、真实 orchestration、archive/history-context、完整 permission boundary 这些外层能力：

- 一批是 runtime 第 2 阶段：promote/freeze gate、round controller、supervisor 入口。
- 一批是全链路 supervisor / simulation 回归，把 skill-first 主链重新接成完整运行面。
- 一批是 reporting / decision 第一批：reporting handoff、council decision draft。
- 一批是 reporting / decision 第二批：expert report draft、canonical expert report publish、canonical decision publish。
- 一批是 reporting / decision 第三批：final publication artifact。

正式上线判断：当前仍不能宣称可以正式上线。更准确的状态是“主链闭环已经可用，但仍处于 pre-production integration 阶段”，因为还缺少真实 mission/fetch/import 执行面、full permission-aware / sandboxed enforcement、distributed-safe control-plane hardening、structured observability 与失败恢复。

legacy 吸收判断：如果只看最有价值的主链业务能力，当前已经基本吸收了旧 runtime 里 raw -> reporting publication 的核心闭环；但 useful legacy 外层能力仍未吸收完全，主要集中在 orchestration / runtime_cli、reporting packet/prompt/recommendation 外壳、archive/history context、richer simulation 与 benchmark surface。

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
- `skills/eco-materialize-final-publication/`
- `skills/eco-plan-round-orchestration/`

## 6. runtime 何时动工

- 这一阶段已经完成到第 2 阶段。
- 当前 kernel 已能初始化 run、刷新 skill registry、执行 skill、记录 receipt、追加 audit ledger、推进 round cursor。
- 当前 kernel 也已能先写 `orchestration_plan_<round_id>.json`，再以 planner-backed queue 跑完 board -> D1 -> D2 -> promotion，并额外写出 supervisor state。
- `eco-summarize-round-readiness` 与 `eco-promote-evidence-basis` 现在已经接入 kernel phase-2 控制流，而没有把业务判断拉回 runtime。
- runtime registry 现在会读取 `SKILL.md` 和 `agents/openai.yaml` 里的基础元数据，kernel 也已支持 `preflight-skill` 和 `run-skill --contract-mode off|warn|strict`。
- runtime enforcement baseline 现在可以阻断缺失 required inputs、未声明 path override、undeclared summary path 与 artifact_ref mismatch，但离完整回放、法证复核与 sandboxed execution 仍有距离。

## 7. 推荐的下一步

1. 先把真实 orchestration / contract scaffold 接回 skill-first 主链，形成 mission -> prepare -> fetch -> normalize 的闭环。
2. 然后补 archive、history context、richer simulation 与 benchmark surface。
3. 再继续做 permission boundary、distributed-safe control plane 和 production hardening。

最近建议补充：

- 下一次编码批次最好先做真实 mission / prepare / fetch-plan / import execution 的最小 contract 闭环。
- 当前 planner-backed cutover 只覆盖 phase-2 内部调度，不应被描述成 full board-driven runtime 已完成。
- 当前最重要的不是再扩张 controller 技巧，而是把真实 orchestration 输入重新接回主链。