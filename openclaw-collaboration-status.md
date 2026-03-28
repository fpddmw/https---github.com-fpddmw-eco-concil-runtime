# OpenClaw 议会协作状态清单

## 1. 当前定位

- Phase A：部分完成，主要是返回契约与测试收口持续进行中。
- Phase B：已完成，candidate -> evidence bridge 已经打通。
- Phase C：已完成到 C2，board 已具备记录、整理、总结、brief 化能力。
- Phase D：已完成 D1 + D2，next actions、probes、round readiness、promotion basis 已经落地。
- runtime kernel：已完成第 1 阶段，manifest / cursor / registry / ledger / executor wrapper 已可运行。

## 2. 当前能力面

- 已交付 31 个 skill。
- 其中 10 个属于 signal normalize / query / lookup。
- 其中 3 个属于 candidate / audit。
- 其中 6 个属于 evidence 中间层。
- 其中 8 个属于 board 层，已经覆盖 delta、note、hypothesis、challenge、task、summary、brief。
- 其中 4 个属于 investigation / readiness / promotion 层，已经覆盖 next actions、probe、readiness、promotion basis。
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
- [x] 最小 runtime kernel 重新建立。
- [ ] runtime 第 2 阶段的 promote/freeze gate 与 supervisor 入口回接。
- [ ] 全链路 supervisor / simulation 闭环回归。

判断：如果只看主链能力面，当前仓库已经基本具备从 raw artifact 到 promote basis 的 skill-first 主链。

判断：如果看工程闭环而不只看 skill 面，则还差两个批次的工作：

- 一批是 runtime 第 2 阶段：promote/freeze gate、round controller、supervisor 入口。
- 一批是全链路 supervisor / simulation 回归，把 skill-first 主链重新接成完整运行面。

## 4. D 与旧 runtime 的关系

- D 不是空白区。旧 runtime 在 `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/investigation/actions.py` 中已经有 next-action planning 原型。
- D 不是空白区。旧 runtime 在 `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/reporting/readiness.py` 中已经有 readiness 原型。
- D 不是空白区。旧 runtime 在 `eco-concil-runtime(abandoned)/src/eco_council_runtime/application/reporting/promotion.py` 中已经有 promotion 原型。
- 当前真正缺的是：把这些 runtime-first 模块拆成当前 skill-first 架构可接受的原子能力，并且重新定义 artifact 契约，而不是继续沿用旧 runtime 的耦合形态。

## 5. runtime 如何设置

- runtime 只保留最小内核，不承载 claim / evidence / board / readiness 的业务判断。
- runtime 第 1 层只负责 run manifest、路径解析、artifact registry、receipt/event ledger。
- runtime 第 2 层只负责 skill executor wrapper，把一次 skill 调用落成稳定的 run event。
- runtime 第 3 层才是可选的 round controller / supervisor CLI，而不是一开始就恢复完整旧框架。

当前已完成：

- `eco-concil-runtime/scripts/eco_runtime_kernel.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/paths.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/registry.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/manifest.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/ledger.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/executor.py`
- `eco-concil-runtime/src/eco_council_runtime/kernel/cli.py`

## 6. runtime 何时动工

- 这一阶段已经开始动工，并且第 1 阶段已完成。
- 当前 kernel 已能初始化 run、刷新 skill registry、执行 skill、记录 receipt、追加 audit ledger、推进 round cursor。
- 下一阶段应在此基础上补 promote/freeze gate、round controller、以及 supervisor 入口。
- `eco-summarize-round-readiness` 与 `eco-promote-evidence-basis` 现在已经是 kernel 第 2 阶段可直接接入的目标对象。

## 7. 推荐的下一步

1. 把 runtime 第 2 阶段补齐：promote/freeze gate、round controller、supervisor 入口。
2. 在 kernel CLI 上接一条最小 end-to-end run 命令，把 board -> D1 -> D2 串成单命令流程。
3. 再做 supervisor / simulation 回归，把 skill-first 主链恢复成完整运行闭环。