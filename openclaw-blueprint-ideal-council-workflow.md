# OpenClaw 蓝图理想态工作流说明

## 1. 文档目的

这份文档回答的是：

`未来理想状态下，项目的工作流到底应当怎样跑。`

重点不是当前 runtime 已经怎么实现，而是蓝图里应当如何组织：

1. 调查内循环
2. 议会阶段推进
3. moderator 与其它 agent 的分工
4. state-change / council-state skills 的使用方式

## 2. 核心结论

如果用一句话概括未来理想态：

`保留当前 runtime 的议会阶段划分，但把“由 controller 固定串行推进”改成“由 moderator 和其它 agent 通过显式状态 skill 推进”。`

也就是说，未来应当保留的不是：

1. 固定 skill 名
2. 固定唯一顺序
3. 固定 controller 统领一切

而是保留：

1. 阶段语义
2. 写边界
3. gate 边界
4. 归档与发布边界

## 3. 未来理想态中，哪些东西保留，哪些东西改变

### 3.1 应保留的部分

应保留当前 runtime 的这些阶段语义：

1. planning
2. board summary
3. board brief
4. next actions
5. falsification / contradiction review
6. round readiness
7. promotion gate
8. promotion basis freeze
9. reporting
10. archive

这些阶段语义是有价值的，因为它们对应真实调查中的不同治理边界。

### 3.2 应改变的部分

应改变的是推进方式。

当前更像：

`controller 按固定 queue 逐阶段推进`

未来应改成：

`agent 先调查 -> moderator 决定何时调用哪类 skill -> state-change / council-state skill 显式改变状态 -> runtime 只在高风险边界做 gate`

## 4. 理想态中的参与者

### 4.1 runtime

只负责：

1. sandbox
2. approval
3. ledger / receipt / dead-letter
4. freeze / publish gate
5. archive / replay / benchmark

不负责：

1. 替 agent 安排大部分调查顺序
2. 替 moderator 决定唯一下一步

### 4.2 sociologist

默认负责：

1. public signals 查询
2. narrative grouping
3. claim-like result sets
4. 舆情和公共叙事层的补证

### 4.3 environmentalist

默认负责：

1. environment signals 查询
2. observation / pattern result sets
3. 时空环境证据补证

### 4.4 challenger

默认负责：

1. contradiction scan
2. sample-and-review
3. source overlap 检查
4. 触发 challenge / rebuttal

### 4.5 moderator

默认负责：

1. 消费多个 agent 的结果
2. 写入或调整 board 状态
3. 决定是否保持当前轮、开启下一轮、还是准备 freeze

它是：

`议会状态推进者`

不是：

`唯一调查执行者`

## 5. 理想态工作流总览

### 5.1 Mermaid 时序图

```mermaid
sequenceDiagram
    autonumber
    actor U as Operator / Topic
    participant R as Runtime Governance
    participant M as Moderator
    participant S as Sociologist
    participant E as Environmentalist
    participant C as Challenger
    database DB as Run DB
    participant B as Council State Skills
    participant G as Promotion Gate
    participant P as Publication / Archive

    U->>R: init-run + mission
    R->>DB: 建立 run metadata / evidence plane

    R->>DB: import / fetch / normalize

    par public investigation
        S->>DB: query public signals
        S->>DB: build public result sets
    and environment investigation
        E->>DB: query environment signals
        E->>DB: build environment result sets
    and challenge pressure
        C->>DB: inspect result sets / sample / contradiction
    end

    S-->>M: public findings
    E-->>M: environment findings
    C-->>M: challenge findings

    M->>B: post note / update hypothesis / open challenge / claim task
    B->>DB: 写 deliberation state

    opt evidence insufficient
        M->>B: open next investigation round
        B->>DB: 写 round transition + carryover
    end

    opt need advisory summaries
        M->>DB: board summary / board brief / next actions / readiness
    end

    alt ready to freeze
        M->>G: request promotion gate
        G-->>M: allow-promote
        M->>B: freeze promotion basis
        M->>P: reporting / publication / archive
    else not ready
        G-->>M: freeze-withheld
        M->>B: continue current round or open next round
    end
```

### 5.2 总体逻辑

理想态工作流不再是：

`fetch -> normalize -> candidate -> cluster -> link -> coverage -> readiness -> report`

而是：

1. `fetch / import / normalize`
2. agent 面向 DB 自由查询与分析
3. moderator 通过 council-state skills 把分析结果转成议会状态
4. 当需要治理总结时，再调用 advisory summary skills
5. 只有要进入 freeze / publish 时，runtime 才强制执行 gate

## 6. 理想态中的阶段划分

下面这部分最重要，因为它回答了你那句判断：

`是的，理想状态基本保留当前 runtime 的议会阶段划分，但推进者不再是硬编码 controller，而是 moderator 或其它 agent 通过状态改变 skill 来推进。`

### 阶段 A：Run 初始化

目标：

1. 固定 mission
2. 建 run metadata
3. 建 evidence / deliberation / export 基础容器

推进者：

1. runtime

### 阶段 B：证据入口

目标：

1. 取数
2. 落 raw
3. normalize
4. 写 evidence plane

推进者：

1. runtime 负责执行边界
2. agent 可以提出 fetch request 或 import request

### 阶段 C：自由调查内循环

目标：

1. 查询 normalized signals
2. 生成 analysis result sets
3. 对 public 与 environment 两平面进行支持 / 矛盾比较
4. 根据新发现决定补证方向

推进者：

1. `sociologist`
2. `environmentalist`
3. `challenger`

这里是未来系统最重要的一段，也是最不应被固定 queue 写死的一段。

### 阶段 D：议会状态推进

目标：

1. 把调查发现转成可讨论状态
2. 把冲突和疑点转成显式 challenge
3. 把待办转成 task
4. 把需要跨轮延续的内容转成 round transition

这一阶段主要通过下面这类 skill 推进：

1. `post board note`
2. `update hypothesis`
3. `open / close challenge`
4. `claim task`
5. `open next investigation round`

推进者：

1. 主要是 `moderator`
2. 某些 challenge / rebuttal 也可由 `challenger` 发起

要点：

这里推进的是 `state`，不是直接推进报告。

### 阶段 E：议会摘要与建议阶段

这一阶段保留当前 runtime 的若干阶段语义，但它们应被降级为：

`advisory capabilities`

可保留的能力包括：

1. board summary
2. board brief
3. next actions
4. falsification probes
5. round readiness

在理想态里，它们的地位是：

1. 给 moderator 提供透镜
2. 给 gate 提供输入
3. 给 operator 提供 snapshot

而不是：

1. 唯一主链
2. 强制控制器

### 阶段 F：Freeze / Gate 阶段

这一阶段必须继续保留为硬边界。

目标：

1. 判断是否允许 promotion
2. 明确写出 freeze-withheld 或 allow-promote
3. 冻结 evidence basis

推进者：

1. `moderator` 请求
2. `runtime gate` 执行

这一阶段不应被 agent 绕过。

### 阶段 G：报告与发布阶段

目标：

1. 生成 reporting handoff
2. 生成角色报告
3. 生成 council decision
4. 生成 final publication

推进者：

1. `moderator`
2. reporting / export skills
3. runtime publication boundary

### 阶段 H：归档与历史复用阶段

目标：

1. archive case
2. archive signal corpus
3. 生成 history context
4. 为后续轮次或后续 case 提供复用入口

推进者：

1. runtime

## 7. 理想态里 moderator 如何推进议会进程

这是本文件最关键的一点。

理想态里，moderator 不应通过“操作 controller 内部状态”推进进程，而应通过：

`显式调用 state-change / council-state skills`

具体表现为：

1. 看到证据已经形成可讨论命题，就 `update hypothesis`
2. 看到证据冲突，就 `open challenge`
3. 看到需要补证，就 `claim task`
4. 看到本轮已经乱成一团，需要新一轮边界，就 `open next investigation round`
5. 看到证据已经足够稳定，就请求 `promotion gate`
6. gate 放行后，再 `freeze promotion basis`

所以 moderator 的推进方式是：

`通过一系列显式状态动作推动议会状态迁移`

而不是：

`等待固定 phase-2 queue 自动往后走`

## 8. 理想态里其它 agent 如何影响议会进程

虽然 moderator 是主要推进者，但其它 agent 不应完全被动。

### 8.1 sociologist

可以影响议会进程的方式：

1. 提供新的 public result sets
2. 建议更新某个 hypothesis
3. 提供新的 narrative contradiction

### 8.2 environmentalist

可以影响议会进程的方式：

1. 提供新的 physical support / contradiction result sets
2. 促使 moderator 调整 hypothesis confidence
3. 促使 moderator 开 task 或 challenge

### 8.3 challenger

可以影响议会进程的方式：

1. 触发新的 challenge
2. 要求追加抽样复核
3. 阻止 moderator 过早 freeze

因此理想态不是 moderator 独裁，而是：

1. 多 agent 产生压力
2. moderator 负责正式推进状态

## 9. 理想态 vs 当前实现：简单对比

| 主题 | 当前实现 | 理想状态 |
| --- | --- | --- |
| 议会阶段 | 已有固定 phase-2 阶段 | 保留阶段语义 |
| 推进方式 | controller 固定推进 | moderator / agents 通过状态 skill 推进 |
| board summary / next actions / readiness | 更接近强制主链 | advisory capability |
| open next round | 已有 skill，但仍嵌在 runtime 路线补丁中 | 标准 council-state capability |
| gate | phase-2 内置末端动作 | 继续保留为硬治理边界 |
| reporting / archive | runtime 尾部主链 | 仍保留，但只在 freeze 后进入 |

## 10. 对后续实现的直接指导

如果你后续要按这个理想态继续开发，我建议顺序是：

1. 保留当前 phase group 名称，不急着改名。
2. 先把这些 phase group 从“硬编码顺序”改成“可由 moderator 触发的 capability”。
3. 把 `board summary / brief / next actions / readiness` 明确降级为 advisory outputs。
4. 把 `open next round`、`freeze promotion basis` 明确升级为标准 council-state skills。
5. 最后再逐步削弱 controller 在开放调查中的主导地位。

一句话总结：

`未来理想工作流不是取消议会阶段，而是把议会阶段从“硬编码流水线”改造成“由 moderator 和其它 agent 通过显式状态 skill 推进的治理型调查流程”。`
