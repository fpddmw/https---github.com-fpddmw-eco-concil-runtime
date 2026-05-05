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

验收：

1. 未授权执行不会产生业务写入。
2. 拒绝原因可由 operator 查询。

### P3：运行锁与幂等

1. 检查重复运行、target 已存在、receipt 重放的行为。
2. 对关键 state transition 增加幂等测试。

验收：

1. 重复执行不会产生重复 council object 或重复 report basis。
2. lock/receipt 行为被测试固定。

### P4：文档收口

1. 更新案例评测文档和 runtime 文档中的正式运行命令。
2. 将 direct script 路径标为 compatibility/dev/debug。

验收：

1. 用户能按文档走完受治理的案例评测或 replay。
2. 文档不暗示可绕过 approval 进行正式发布。
