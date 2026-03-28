# OpenClaw 议会协作状态清单

## 1. 当前定位

- Phase A：部分完成，主要是返回契约与测试收口持续进行中。
- Phase B：已完成，candidate -> evidence bridge 已经打通。
- Phase C：已完成到 C2，board 已具备记录、整理、总结、brief 化能力。
- Phase D：尚未 skill 化交付，但旧 runtime 中已有 investigation / readiness / promotion 原型。

## 2. 完成 C2 后的当前能力面

- 已交付 27 个 skill。
- 其中 10 个属于 signal normalize / query / lookup。
- 其中 3 个属于 candidate / audit。
- 其中 6 个属于 evidence 中间层。
- 其中 8 个属于 board 层，已经覆盖 delta、note、hypothesis、challenge、task、summary、brief。

## 3. 距离全流程议会协作还差什么

- [x] 原始 artifact 进入 signal plane。
- [x] signal plane 进入 candidate / evidence 中间层。
- [x] evidence 进入 board working-state。
- [x] board 从 working-state 进入 organized-state 与 brief。
- [ ] board brief 进入 next-action queue。
- [ ] falsification probe 进入显式生命周期。
- [ ] round readiness 进入正式 gate。
- [ ] evidence basis 进入 promote / freeze。
- [ ] 最小 runtime kernel 重新建立。
- [ ] 全链路 supervisor / simulation 闭环回归。

判断：如果只看主链能力面，当前仓库在完成 C2 后，距离“可运行的完整议会协作”只差最后一层 Phase D。

判断：如果看工程闭环而不只看 skill 面，则还差两个批次的工作：

- 一批是 Phase D1 / D2 的 4 个核心 skill。
- 一批是最小 runtime kernel 与全链路回归。

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

## 6. runtime 何时动工

- 不建议在 C2 刚完成时立即动工，因为 board artifact 仍然刚从 record 扩展到 organize / brief，D 层输入契约还没稳定。
- 建议在 D1 完成后动工，也就是 `eco-propose-next-actions` 和 `eco-open-falsification-probe` 已经落地并通过一轮脚本级测试之后。
- 到那个时间点再建立最小 runtime kernel，风险最低，因为 board brief -> next action -> probe 这一段链路的输入输出已经基本固定。
- D2 的 `eco-summarize-round-readiness` 与 `eco-promote-evidence-basis` 更适合在 runtime 第 2 阶段接入，用来恢复 gate / freeze / promotion 的编排入口。

## 7. 推荐的下一步

1. 先交付 D1：`eco-propose-next-actions`、`eco-open-falsification-probe`。
2. 再以 D1 的 artifact 契约为基线，启动最小 runtime kernel。
3. 最后交付 D2，并把 readiness / promotion 接回 runtime 的 gate 与 freeze 入口。