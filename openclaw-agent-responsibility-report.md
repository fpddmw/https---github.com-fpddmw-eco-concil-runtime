# OpenClaw Agent 职责划分技术报告

## 1. 文档目的

这份文档只回答一个问题：

`目前项目里，谁负责什么；未来理想形态里，又应当由谁负责什么。`

它不重点讲取数工作流，也不重点讲数据层级，而是把“agent、runtime、board、moderator、challenger”这些职责边界拆开讲清楚。

全文按两个部分组织：

1. 当前阶段的真实实现
2. 蓝图阶段的理想状态

每个主题后面都附一个简单对比，避免把“现在已经做到了什么”和“以后准备做到什么”混在一起。

## 2. 先给结论

当前项目已经有：

1. 一套受治理的 runtime 调查底座
2. 一套按角色组织的任务与 board 状态
3. 一批可供未来 agent 调用的 query / analysis / board / reporting skills

但当前项目还没有：

1. 真正独立运行的多 agent turn loop
2. 拥有独立 session / memory / permission 的角色 agent
3. 由 agent 主导的开放调查主链

所以目前的职责划分，本质上是：

1. `runtime` 负责治理与执行
2. `roles` 负责任务标签与责任提示
3. `board skills` 负责显式状态变更
4. `planner/controller/supervisor` 负责固定 phase-2 控制面

而理想蓝图要求把它重构为：

1. `runtime` 只负责治理边界
2. `agent` 负责调查判断
3. `moderator` 通过 state-change / council-state skills 推进议会状态
4. `board` 不再只是文件快照，而是 agent 协作的共享状态面

## 3. 当前阶段：真实职责划分

### 3.1 当前项目里“谁像 agent”

当前代码里有四类“像 agent 的东西”，但它们并不等价。

| 当前对象 | 当前真实形态 | 主要职责 | 是否是真正独立 agent |
| --- | --- | --- | --- |
| `sociologist` | round task / source role | 关注公共讨论、叙事、文本证据 | 否 |
| `environmentalist` | round task / source role | 关注环境观测、物理指标、时空信号 | 否 |
| `moderator` | board owner + phase-2 owner | 汇总状态、决定 hold / promote、现在也负责开启新轮 | 否 |
| `challenger` | board / probe 责任角色 | 提出 challenge、推进 falsification、增加反证压力 | 否 |
| `planner` | advisory / queue planner | 生成 phase-2 规划建议 | 否 |
| `controller` | runtime phase-2 executor | 执行固定 phase-2 阶段队列 | 否 |
| `supervisor` | runtime supervisor | 对本轮 posture 做分类，给 operator 建议 | 否 |

要点只有一句：

当前这些“角色”大多是责任标签，不是拥有独立智能行为边界的实体 agent。

### 3.2 当前 `sociologist` 的职责

当前阶段，`sociologist` 的职责主要体现在三类对象上：

1. `source_selection_sociologist_<round_id>.json`
2. `round_tasks_<round_id>.json` 中分配给 sociologist 的任务
3. 对 public signals 的 query / extract / clustering / reporting 视角

它当前更像：

1. 公共讨论证据面的默认责任人
2. public source governance 的接收方
3. 角色化报告视角

它当前不像：

1. 能自主选择工具的独立 agent
2. 有自己 turn history 的会话体
3. 能独立提出 fetch request 并与 runtime 谈判的主体

### 3.3 当前 `environmentalist` 的职责

`environmentalist` 的地位与 `sociologist` 基本对称，只是负责另一类证据：

1. OpenAQ、AirNow、Open-Meteo 等环境观测源
2. environment signal 的 normalize / extract / observation merge
3. 环境学家视角的 reporting

当前它本质上是：

1. 物理证据面的默认责任标签
2. source queue 的一个角色槽位
3. 环境报告的一个默认署名视角

它当前仍然不是：

1. 一个真正独立的环境调查 agent
2. 一个可以自己保存长期工作记忆的角色会话

### 3.4 当前 `moderator` 的职责

`moderator` 是当前系统里最接近“代理中心角色”的对象，但仍然不是真正 agent。

它当前承担的职责最多，主要包括：

1. 维护 board 状态
2. 汇总 hypothesis / challenge / task / note
3. 消费 phase-2 artifacts，例如 board summary、board brief、next actions、readiness
4. 决定本轮是否可以进入 promotion / reporting
5. 在证据不足时保持 hold posture
6. 现在还能显式调用 `eco-open-investigation-round` 开启下一轮

所以，当前 `moderator` 更准确的定义是：

`运行时控制面上的议会主持者标签`

而不是：

`拥有独立推理回路的 moderator agent`

### 3.5 当前 `challenger` 的职责

`challenger` 当前主要通过 board 和 falsification 相关能力体现：

1. 开 challenge ticket
2. 推进 contradiction / falsification 任务
3. 对过度自信的 hypothesis 施加反方压力

它的当前价值在于：

1. 避免系统只顺着支持性证据前进
2. 给 board 引入显式反证对象
3. 让“反方视角”不只存在于自然语言说明里

但它目前仍只是：

1. board state 的一个 owner role
2. probe / ticket 的默认责任方

而不是：

1. 独立运行的 adversarial agent

### 3.6 当前 runtime 相关模块的职责

当前真正“在执行”的主体，其实是 runtime 内核，而不是 agent。

#### `source_queue_*`

负责：

1. 解析 mission
2. 组装 source selection
3. 生成 fetch plan
4. 做 drift detection
5. 执行 import / detached fetch

它的本质是：

`受治理的数据入口系统`

#### `controller.py`

负责：

1. 执行固定的 phase-2 阶段
2. 管理 controller 状态
3. 支持 resume / restart

它的本质是：

`固定控制面的执行器`

#### `supervisor.py`

负责：

1. 把 controller 状态分类成 operator 可理解的 posture
2. 给出 hold / promote / reporting-ready 等结论
3. 在 hold 时给出“可以开下一轮”的建议

它的本质是：

`治理监督器`

不是：

`开放调查的主推理者`

### 3.7 当前 board skills 的职责

当前 board skills 是最重要的一组“显式状态变更器”，包括：

1. `eco-post-board-note`
2. `eco-update-hypothesis-status`
3. `eco-open-challenge-ticket`
4. `eco-close-challenge-ticket`
5. `eco-claim-board-task`
6. `eco-open-falsification-probe`
7. `eco-open-investigation-round`

这组 skill 的共同特点是：

1. 都在写显式状态
2. 都会生成可审计 event
3. 都比“直接改 JSON 文件”更接近未来理想态

它们其实已经很接近未来蓝图里的 `State-Change / Council-State Skills` 雏形。

## 4. 当前阶段：职责边界问题

### 4.1 当前最大问题不是“没有角色”

而是：

`角色存在，但角色没有变成真正 agent。`

具体表现为：

1. 没有 `agent_id`
2. 没有 role-specific session state
3. 没有 turn history
4. 没有 write permission boundary
5. 没有跨 agent 协作协议

### 4.2 当前 moderator 负担过重

现在很多原本应由 agent 内部讨论消化的任务，被压回了 `planner + controller + supervisor + moderator` 这条链上。

结果是：

1. moderator 实际上承担了太多推进职责
2. 其它角色更多只是输入面和报告面标签
3. 调查自由度仍然偏低

### 4.3 当前 runtime 仍然承担了过多“流程决定权”

当前 phase-2 仍然默认由固定 queue 推进。

所以现状是：

1. runtime 决定顺序
2. skill 负责执行
3. 角色只负责填充各阶段内容

而不是：

1. agent 决定顺序
2. runtime 只审核边界
3. state-change skill 负责把决定显式落盘

## 5. 蓝图阶段：理想职责划分

### 5.1 蓝图里的核心原则

理想蓝图要求把职责重排成下面这个结构：

| 层 | 理想职责 |
| --- | --- |
| `runtime` | 治理、执行边界、账本、冻结、归档、回放 |
| `agent` | 调查、检索、分析、挑战、提出下一步 |
| `moderator` | 收敛议会状态，调用 council-state skills 推进阶段 |
| `board` | 共享 deliberation state，而不是单一 JSON 文件 |
| `analysis tools` | 供 agent 调用的工具，不再是强制主链 |

### 5.2 理想 `sociologist`

理想状态下，`sociologist` 不再只是 source role，而应是：

1. 一个有 `agent_id` 的 agent session
2. 默认偏好 public signal / narrative tools
3. 可以直接查询 normalized public signals
4. 可以调用 clustering、sampling、history retrieval 等工具
5. 可以把结论写入 board，或提交给 moderator 审议

它的职责边界应是：

1. 默认更关注公共叙事和传播
2. 不是唯一能看 public data 的角色
3. 可以跨域调环境工具，但不是默认偏好

### 5.3 理想 `environmentalist`

理想状态下，`environmentalist` 应是：

1. 一个有独立 turn loop 的 agent
2. 默认偏好 environment signal / physical pattern tools
3. 能把时空观测、指标异常、环境模式组织成结果集
4. 能与 sociologist 的结果做 support / contradiction 对照

它的关键变化是：

1. 不再只是 source queue 的角色槽位
2. 而是物理证据面上的主动调查者

### 5.4 理想 `challenger`

理想状态下，`challenger` 的地位会大幅提升。

它不应只是在 board 上开 challenge ticket，而应是：

1. 一个专门做 contradiction scan 的 agent
2. 一个专门做 source overlap 检查的 agent
3. 一个专门做 sample-and-review 的 agent
4. 一个负责给 moderator 制造“不要过早收敛”压力的 agent

这意味着 `challenger` 在理想态里不是附庸，而是系统可靠性的主动来源。

### 5.5 理想 `moderator`

理想状态下，`moderator` 的角色会更清晰，也更“像议会主持者”：

1. 不再直接包办大多数分析
2. 主要消费其它 agent 的结果集和 board state
3. 通过 state-change / council-state skills 改变议会状态
4. 决定是否：
   - 开 note
   - 更新 hypothesis
   - 开 / 关 challenge
   - claim task
   - open next round
   - freeze promotion basis
   - 进入 publication

所以它的理想形态是：

`议会状态推进者`

而不是：

`所有推理的唯一拥有者`

### 5.6 理想 runtime

理想蓝图下，runtime 的职责反而会更收缩、更纯粹：

1. 审核 side effects
2. 审核 write boundaries
3. 保留 ledger / receipt / dead-letter
4. 负责 promotion / publication gate
5. 负责 archive / replay / benchmark

runtime 不应再负责：

1. 替 agent 决定主调查顺序
2. 替 moderator 生成唯一的下一步
3. 把 analysis skills 串成唯一通路

## 6. 当前实现 vs 理想蓝图：简单对比

### 6.1 角色层面对比

| 主题 | 当前实现 | 理想状态 |
| --- | --- | --- |
| `sociologist` | 任务标签、source role、报告视角 | 独立调查 agent |
| `environmentalist` | 任务标签、source role、报告视角 | 独立物理证据 agent |
| `challenger` | board 反方角色 | 独立反证 / 复核 agent |
| `moderator` | board owner + phase-2 owner | council-state 推进者 |
| `runtime` | 治理 + 固定阶段推进 | 只保留治理和高风险 gate |

### 6.2 能力边界对比

| 主题 | 当前实现 | 理想状态 |
| --- | --- | --- |
| 调查顺序 | 多数由固定 phase-2 queue 决定 | 多数由 agent 决定 |
| 状态推进 | 多数由 controller / supervisor 驱动 | 由 moderator 调用状态 skill 驱动 |
| 协作方式 | 角色标签共享同一条流程 | 多 agent 共享同一状态面协作 |
| 写边界 | skill 级别有显式写操作 | skill 级别写操作 + agent 权限边界都显式 |
| 轮次推进 | 现在可显式开新轮，但仍是 runtime route 内补丁 | 成为标准 council-state capability |

## 7. 建议你目前如何表述给老师或答辩评委

最稳妥的表述是：

1. 当前系统已经完成了“角色化调查底座”，但还没完成“真正多 agent 主调查系统”。
2. 当前 `sociologist / environmentalist / moderator / challenger` 已经不是空名字，它们已经对应到任务、board 状态和报告面。
3. 但它们现在主要还是责任标签和控制面对象，不是拥有独立会话和权限边界的 agent。
4. 下一阶段的核心工作，就是把这些角色从“标签”升级成“实体 agent”。

## 8. 近期最重要的职责演化方向

如果只看 agent 职责，不看别的，我建议后续演化优先级是：

1. 先给角色补 `agent_id / turn history / write permission`。
2. 让 `moderator` 真正只做 state-change / council-state 推进。
3. 让 `challenger` 成为真正独立的反证角色。
4. 把 `controller` 从“阶段推进者”降级成 strict runtime mode 的备份机制。
5. 把调查主导权逐步交还给 agent。

一句话总结：

`当前系统已经有了角色分工，但角色还没有真正成为 agent；蓝图的本质，就是把这些角色从“责任标签”升级为“有边界的调查主体”。`
