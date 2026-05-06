# OpenClaw Runtime-governed Execution 硬化工作计划

## 1. 文档定位

本文是独立工作计划，不作为全局总计划，也不要求其他计划同时执行。

目标是让真实案例评测和常规运行尽量通过 runtime kernel 的受治理入口执行，确保 preflight、role policy、approval、lock、receipt、ledger 和 postflight contract 都被实际覆盖。

## 2. 范围

本计划覆盖：

1. `run-skill` 入口的案例评测化和文档化。
2. approval-gated skill 的审批链路验收。
3. direct script 执行路径的 dev/debug 标注。
4. runtime receipt、ledger、lock、dead letter 的最小检查。
5. 运行失败时的 operator 可见错误。

## 3. 非目标

本计划不做：

1. 删除 skill 脚本。
2. 禁止单元测试直接调用内部函数。
3. 改写所有 CLI 命令。
4. 改变现有 skill id、role policy 或 canonical contract。

## 4. 交付物

1. 真实案例评测使用 `run-skill` 的脚本或命令序列。
2. approval request/approve/run 的端到端测试。
3. 未授权运行被阻断的 guardrail 测试。
4. direct script compatibility 文档。

## 5. 工作阶段

### 当前落地状态

已落地 runtime-governed execution 的第一块代码：

1. `eco-concil-runtime/src/eco_council_runtime/kernel/governance.py`
   - `preflight-skill` 现在对 `optional-analysis` 与 `reporting` 层中声明 `requires_operator_approval=True` 的 skill 强制要求 `--skill-approval-request-id`。
   - 已批准 request 会解除 strict 模式中的 `operator-approval-required` 阻断；未批准、已消费、actor 不匹配或参数作用域不匹配仍会阻断。
2. `eco-concil-runtime/src/eco_council_runtime/kernel/skill_approvals.py`
   - skill approval request 不再限于 optional-analysis，现支持 reporting draft/publish/finalize 链路。
   - state-transition skill 不走 skill approval；`freeze-report-basis`、`open-investigation-round` 等仍应通过 phase transition request/approval。
   - 如果 request 写入了 `requested_skill_args`，执行时必须匹配同一组 skill args，避免复用审批执行另一个参数变体。
3. `tests/test_skill_approval_workflow.py`
   - 覆盖 optional-analysis 未审批阻断与审批消费。
   - 覆盖 reporting publish 未审批阻断。
   - 覆盖 reporting publish 审批后 strict preflight 放行。
   - 覆盖 approval request 参数作用域不匹配时阻断。
   - 覆盖 state-transition skill 误走 skill approval 时被拒绝。
4. `eco-concil-runtime/src/eco_council_runtime/kernel/ledger.py`
   - runtime receipt 现在写入 `runtime-receipt-v2` envelope，保留原始 `skill_payload`。
   - receipt envelope 直接携带 `event_id`、`execution_input_hash`、`payload_hash`、`lock_path`、preflight、postflight、runtime admission 和 attempt 信息。
   - 同一 `receipt_id` 且 payload hash 相同的重复写入会标记为 `unchanged`，不重写既有 receipt。
5. `tests/test_runtime_kernel.py`
   - 覆盖 governed receipt envelope 的关键字段。
   - 覆盖同一 receipt payload 重放时 `receipt_write.write_status=unchanged`。

当前未闭环项：

1. 真实案例评测命令序列仍需改成优先通过 `run-skill`，direct scripts 只作为 dev/debug 兼容。
2. 运行锁的 operator 可见状态仍需固定到 `show-run-state` / runtime health。
3. receipt envelope 已落地；后续还需决定 payload hash 不同但 receipt_id 相同时是否应升级为阻断或 operator review。
4. target 已存在和关键 state transition 幂等性仍需继续补测试。

当前实测状态：

1. `python3 tools/quality_gate.py test runtime-governance` 通过，53 tests。
2. `python3 tools/quality_gate.py test runtime-governance reporting` 通过，76 tests。
3. `python3 tools/quality_gate.py full` 通过，254 tests。

### P0：入口分类

1. 梳理正式入口、debug 入口和测试入口。
2. 标注哪些 skill 必须 approval-gated。
3. 标注哪些命令可继续直接运行但只作为开发兼容。

验收：

1. 文档清楚区分 formal run 与 dev/debug run。
2. 高影响 helper 不被描述为可绕过审批。

### P1：案例评测接入 Runtime

1. 将真实案例探索运行改为通过 runtime run/skill lifecycle 执行。
2. 保留必要 fixture 导入步骤。
3. 记录 receipt、ledger、approval refs。

验收：

1. 案例评测输出中能定位 approval ref、receipt、ledger event。
2. 失败步骤能进入可诊断状态。

### P2：审批与权限测试

1. 覆盖 optional-analysis 未审批阻断。
2. 覆盖 report publish 未审批阻断。
3. 覆盖错误 actor role 被拒绝。
4. 覆盖已批准 request 不能被不同 skill args 复用。
5. 覆盖 state-transition skill 不误走 skill approval。

验收：

1. 未授权执行不会产生业务写入。
2. 拒绝原因可由 operator 查询。
3. reporting publish/finalize 的正式 runtime 执行不能绕过 operator approval。

### P3：运行锁与幂等

1. 检查重复运行、target 已存在、receipt 重放的行为。
2. 固定 runtime receipt envelope 与 receipt replay 状态。
3. 对关键 state transition 增加幂等测试。

验收：

1. 重复执行不会产生重复 council object 或重复 report basis。
2. lock/receipt 行为被测试固定。
3. operator 可从 receipt 直接追踪 event、lock、preflight/postflight 与 admission 决策。

### P4：文档收口

1. 更新案例评测文档和 runtime 文档中的正式运行命令。
2. 将 direct script 路径标为 compatibility/dev/debug。

验收：

1. 用户能按文档走完受治理的案例评测或 replay。
2. 文档不暗示可绕过 approval 进行正式发布。
