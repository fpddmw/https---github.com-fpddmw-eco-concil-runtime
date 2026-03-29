# OpenClaw Multi-Agent 路线全量开发计划

## 1. 路线定位

这条路线是主路径。

它解决的是：

1. 开放式调查
2. 多角色协作
3. 动态 skill 组合
4. challenge / falsification
5. 跨轮历史利用
6. 从证据到结论的真实推理过程

它不应退化成：

1. 固定表单填写流程
2. 预先写死的 stage machine
3. 由 controller 替 agent 做判断的壳

## 2. 当前代码状态

### 2.1 已具备的前置条件

虽然主路径尚未落地，但下面这些基础已经存在：

1. 统一 skill surface 已存在
2. `registry.py` 能为所有活跃 skills 扫描 agent metadata 与 queue profile
3. `eco-scaffold-mission-run` 已支持 `orchestration_mode=openclaw-agent`
4. `eco-plan-round-orchestration` 已支持 `planner_mode=agent-advisory`
5. board / query / lookup / history / reporting skills 已具备基础可调用面

这意味着 agent 路线不需要从零开始设计技能层。

### 2.2 尚未存在的核心能力

当前还没有真正的 OpenClaw runtime：

1. 没有 `adapters/openclaw/`
2. 没有 managed skill projection
3. 没有 role workspace
4. 没有 turn loop
5. 没有 multi-agent handoff protocol
6. 没有 agent-side audit contract
7. 没有 agent -> source request -> governed fetch queue 的正式桥接

因此当前状态必须明确写成：

1. `agent mode` 只有入口提示和 advisory 规划
2. `agent mode` 还没有成为可运行主链

## 3. 路线完成定义

只有下面六个条件都满足，才能说 OpenClaw route 完成：

1. 至少一个 moderator-led 单轮调查可以从 mission 走到报告与归档
2. 至少两个角色 agent 能共享 board、history、signal plane 并持续协作
3. agent 能自主决定何时 query、lookup、fetch、normalize、challenge、report
4. agent 产生的 source request 能经过 runtime governance bridge 执行
5. 整个过程可审计、可回放、可解释
6. runtime 仍然只是治理与执行后盾，而不是重新成为主控制器

## 4. 开发原则

1. 先做 agent runtime，再谈 agent prompt 调优。
2. 先打通真实工具调用闭环，再扩角色数量。
3. 先把单 agent 路径做实，再做多 agent 协作。
4. board 是协作中枢，不是附属展示层。
5. history / query / lookup 是分析工具，不是替代推理的预制答案。
6. runtime queue 只接手 agent 明确请求的治理执行面，不反客为主。

## 5. 目标运行形态

理想中的 agent route 应该是：

```text
mission
  -> moderator bootstrap
  -> board read / history retrieval
  -> role agents formulate hypotheses and questions
  -> query / lookup / fetch / normalize / analysis skill calls
  -> board mutations, challenge tickets, falsification probes
  -> runtime-governed source queue for controlled acquisition when needed
  -> readiness / promotion / reporting
  -> archive
```

关键判断：

1. agent 自主决定何时调用分析技能
2. agent 自主决定何时发起 challenge
3. 只有当需要受控引入外部数据时，才进入 runtime source-queue bridge

## 6. 全量开发阶段

### 阶段 A0: OpenClaw 接入骨架

状态：`未开始`

目标：

1. 建立独立的 OpenClaw adapter 层
2. 让 shared skills 真正能被 agent runtime 投影使用

必须实现：

1. `adapters/openclaw/` 包
2. managed skill projection
3. skill allowlist / capability projection
4. skill result normalization
5. agent runtime config

建议产物：

1. `openclaw_runtime.py`
2. `skill_projection.py`
3. `agent_session.py`

完成判据：

1. 单个 agent 能加载 skill registry
2. 单个 agent 能在受控环境下调用至少一个 query skill 和一个 board skill

### 阶段 A1: 单 agent moderator baseline

状态：`未开始`

目标：

1. 先跑通一个 moderator-only 调查闭环

必须实现：

1. mission bootstrap
2. board delta read
3. history context read
4. query / lookup 调用
5. board note write
6. next action generation

推荐初始 skill 集：

1. `eco-scaffold-mission-run`
2. `eco-read-board-delta`
3. `eco-materialize-history-context`
4. `eco-query-public-signals`
5. `eco-query-environment-signals`
6. `eco-lookup-normalized-signal`
7. `eco-post-board-note`

完成判据：

1. 单 agent 可从 mission 启动并输出一轮 board-level investigation trace
2. 全程不依赖人工拼接中间 JSON

### 阶段 A2: 多角色 workspace 与协作协议

状态：`未开始`

目标：

1. 让 moderator、sociologist、environmentalist、challenger 真正成为分工角色

必须实现：

1. role workspace layout
2. role identity contract
3. task claim / release 机制
4. handoff envelope
5. shared board cursor

需要定义的协作协议：

1. 谁可以开 challenge
2. 谁可以关闭 challenge
3. 谁负责 hypotheses 更新
4. 谁负责建议 source request
5. moderator 何时仲裁

完成判据：

1. 至少两个角色 agent 可顺序或交替完成一次调查
2. board 可以完整保留协作痕迹

### 阶段 A3: 分析技能主链接入

状态：`未开始`

目标：

1. 让 agent 不只会查询，还会真正生成和修正证据结构

必须实现：

1. query -> normalize signal references
2. claim extraction
3. observation extraction
4. linking
5. scope derivation
6. coverage assessment

建议 agent 常用 skill 组：

1. `eco-query-public-signals`
2. `eco-query-environment-signals`
3. `eco-lookup-raw-record`
4. `eco-lookup-normalized-signal`
5. `eco-extract-claim-candidates`
6. `eco-extract-observation-candidates`
7. `eco-link-claims-to-observations`
8. `eco-derive-claim-scope`
9. `eco-derive-observation-scope`
10. `eco-score-evidence-coverage`

完成判据：

1. agent 能基于 skill 结果更新 hypotheses 与 challenge posture
2. agent 结论不再只是自然语言，而能回写结构化调查状态

### 阶段 A4: source request 到 governed queue bridge

状态：`未开始`

目标：

1. 让 agent 真正拥有“发现缺口 -> 请求新数据 -> 受控执行”的能力

这是整条路线最关键的桥。

必须实现：

1. agent-side source request contract
2. request validation
3. request -> `eco-prepare-round` bridge
4. runtime queue execution callback
5. normalize completion handoff back to agent

必须坚持的边界：

1. agent 负责提出请求和解释为什么需要
2. runtime 负责治理、冻结、执行、审计
3. agent 不直接绕过 runtime 做不受控 fetch

完成判据：

1. agent 能因证据缺口发起至少一种 public source request
2. runtime queue 能接手并把结果送回 signal plane
3. agent 能继续利用新证据推进调查

### 阶段 A5: challenge / falsification 主循环

状态：`未开始`

目标：

1. 发挥 OpenClaw 的核心价值，而不是把它当表单填写器

必须实现：

1. challenge ticket 开闭环
2. falsification probe 生成与跟踪
3. hypothesis competition
4. conflicting evidence adjudication
5. board-centric contradiction management

重点 skill：

1. `eco-open-challenge-ticket`
2. `eco-close-challenge-ticket`
3. `eco-open-falsification-probe`
4. `eco-update-hypothesis-status`
5. `eco-read-board-delta`

完成判据：

1. 至少一个完整案例中，agent 真正经历“提出结论 -> 遭遇挑战 -> 补证或修正”的循环
2. 最终结论能解释为什么保留某一假设而放弃其他假设

### 阶段 A6: 报告、发布与归档闭环

状态：`未开始`

目标：

1. 让 agent route 走到真正可交付的尾部

必须实现：

1. readiness interpretation
2. promotion basis handoff
3. expert report drafting
4. council decision drafting
5. publication aggregation
6. archive write

需要明确：

1. 哪些步骤由 agent 触发
2. 哪些步骤由 runtime gate 冻结
3. 哪些步骤必须人工确认

完成判据：

1. 一条 agent-led run 能产出完整报告与 archive 结果
2. archive 后的 history context 能回用于后续 round

### 阶段 A7: 审计、回放与生产化

状态：`未开始`

目标：

1. 让 agent route 不是黑箱

必须实现：

1. agent turn ledger
2. tool-call trace
3. board mutation trace
4. request / execution / result causality chain
5. replayable turn snapshot
6. approval / human override surface

完成判据：

1. 能回答“某结论是由谁、基于哪些工具调用、在哪些 board 变化后得出的”
2. 能回放关键 turn，而不只是回放最终 artifact

## 7. 近期编码顺序

agent 路线必须按下面顺序推进：

1. A0
2. A1
3. A2
4. A4
5. A3
6. A5
7. A6
8. A7

顺序说明：

1. 先有 adapter 和单 agent loop，才有资格谈 multi-agent
2. source request bridge 必须比大规模分析自治更早落地
3. challenge 主循环必须在报告闭环前落地，否则只是报告生成器

## 8. 当前建议的具体 backlog

### Backlog A: adapter 骨架

1. 新建 `adapters/openclaw/`
2. 定义 agent session / skill projection / tool result contract
3. 从 registry 读取 agent metadata 和 queue profile

### Backlog B: single-agent baseline

1. moderator bootstrap
2. board/history/query tool calls
3. board note write-back
4. turn trace persistence

### Backlog C: source request bridge

1. 定义 agent request schema
2. 定义 bridge skill 或 bridge adapter
3. 接到 `eco-prepare-round`
4. 回写 normalized result summary

### Backlog D: multi-agent protocol

1. task claim
2. role handoff
3. challenge ownership
4. moderation rules

## 9. 路线风险

这条路线最大的风险有四个：

1. 迟迟不写真正的 agent runtime，只继续写文档和 skill 元数据
2. 过早把 route 拉回 runtime controller 逻辑
3. 多 agent 协作协议不清，导致 board 成为噪声源
4. 把 history / query 输出直接当答案，而不是当证据工具

必须持续用代码结构避免这四个风险。

## 10. 路线完成后的定位

这条路线完成后，它应该是：

1. 项目的主调查面
2. OpenClaw 价值真正释放的位置
3. 动态取证、挑战、推翻、修正和协作调查的承载面

它不应该成为：

1. 对 runtime queue 的包装壳
2. 另一套重规则、低自由度的表单流
3. 只会写报告、不会真正调查的“伪 agent”流程
