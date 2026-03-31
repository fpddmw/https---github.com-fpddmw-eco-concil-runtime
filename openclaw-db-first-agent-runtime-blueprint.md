# OpenClaw DB-First Agent Runtime Blueprint

## 1. 文档定位

这份文档是后续开发的唯一主蓝图。

它固定三件事：

1. 未来主路径是 `db first / openclaw agent mode`。
2. 当前 `runtime route` 不被推翻，而是保留为治理执行面。
3. 所有后续改造都必须服务于“让 OpenClaw 真正成为调查者”，而不是继续把它困在线性工件链里。

## 2. 不变式

下面这些原则不应再改变。

### 2.1 只保留一套 shared skill surface

不再维护两套平行 skill 体系。

未来无论是：

1. runtime route
2. openclaw agent route

都调用同一套 `skills/eco-*`。

### 2.2 runtime 继续保留治理强项

runtime 继续负责：

1. admission
2. sandbox
3. timeout / retry
4. side effect approval
5. ledger / receipt / dead-letter
6. archive / replay / benchmark
7. promotion / publication gate

这些能力不应被“agent 松绑”削弱。

### 2.3 未来主智能体面必须 DB-first

后续不是把更多固定流程搬进 skill，也不是把 controller 改个名字继续写死顺序。

真正的改变必须是：

1. 把数据库变成主工作面。
2. 把 skill 变成分析工具。
3. 把 JSON / Markdown 降级为快照与导出物。

## 3. 为什么当前结构还没有发挥 OpenClaw 的价值

当前限制 OpenClaw 的关键问题，不在“技能少”，而在“上层流程太早替 agent 做决定”。

### 3.1 source ingress 仍然过于冻结

当前 `fetch_plan` 非常适合：

1. governed batch run
2. replay
3. benchmark

但不适合：

1. 调查中途追加 source
2. 临时补抓历史片段
3. agent 自主改变查询策略

### 3.2 phase-2 controller 仍然强绑定 exact stage

当前上层流程仍带有明显的：

1. 固定 stage 名
2. 固定 skill 名
3. 固定顺序
4. planner-backed 单一路径

这会让规划权留在 controller，而不是交给 agent。

### 3.3 分析链压缩过早

当前很多 skill 会把证据过早压成：

1. claim candidates
2. observation candidates
3. links
4. coverage
5. readiness

这些对象本身并非无用，但当前的问题是：

1. 它们常被当成唯一主链。
2. agent 容易先看到压缩结论，而不是原始证据。
3. 一个早期启发式错误会向上传播。

### 3.4 角色目前仍是标签，不是实体 agent

当前 `sociologist / environmentalist / moderator / challenger` 更多是：

1. source role
2. board owner
3. report 视角

而不是：

1. 独立 session
2. 独立工作记忆
3. 独立写权限边界
4. 真正的协作协议参与者

## 4. 目标形态

未来目标可以用一句话概括：

`runtime 管治理，数据库管状态，skill 管分析，moderator 管议会推进，OpenClaw 管调查。`

### 4.1 五个工作平面

#### Governance Plane

由 runtime 负责：

1. 执行边界
2. 权限与副作用审批
3. 账本、回放、归档、发布 gate

#### Evidence Plane

负责证据底座：

1. raw artifacts
2. normalized signals
3. 历史 case / signal corpus 索引

#### Analysis Plane

负责可重复、可组合、可回溯的分析结果：

1. result sets
2. samples
3. clusters
4. contradiction scans
5. support / rebuttal evaluations

#### Deliberation Plane

负责议会状态：

1. hypotheses
2. notes
3. tasks
4. challenge tickets
5. probes
6. round state

#### Export Plane

只负责输出物：

1. board brief
2. reporting handoff
3. expert report
4. council decision
5. final publication

## 5. 数据模型原则

### 5.1 证据层

下层证据面继续保留：

1. 文件系统中的 raw artifact
2. `normalized_signals`
3. `artifact_path + record_locator + raw_json`

也就是说，DB-first 不是删除文件，而是：

`文件保原始证据，数据库保统一查询面。`

### 5.2 分析层

未来不应继续把 analysis 主链建立在一堆强顺序 JSON 文件上。

应改成数据库中的分析对象，例如：

1. `result_sets`
2. `result_items`
3. `result_lineage`
4. `sampling_reviews`
5. `comparison_runs`

核心要求：

1. 每个 result set 都能追溯到 query basis。
2. 每个压缩对象都能追溯到 parent ids 与 artifact refs。
3. 允许同一主题并存多套解释，而不是只保留一个最终压缩结果。

### 5.3 议会层

board 不应再只是文件快照集合，而应成为共享 deliberation state。

目标对象包括：

1. `hypothesis_cards`
2. `challenge_tickets`
3. `board_tasks`
4. `board_notes`
5. `round_transitions`
6. `promotion_freezes`

### 5.4 JSON / Markdown 的新定位

未来仍保留 JSON / Markdown，但只承担：

1. snapshot
2. export
3. handoff
4. human-readable summary

它们不再承担主分析平面。

## 6. Skill 体系重构原则

### 6.1 Skill 分组

未来 skill 体系应稳定分成六组。

1. ingress skills
   负责 fetch / import / normalize
2. query / lookup skills
   负责直接访问 raw、normalized、history
3. analysis skills
   负责聚类、筛选、对比、抽样、评分
4. state-change skills
   负责显式修改 board / round / challenge / freeze 状态
5. export skills
   负责 report / publication / archive
6. governance bridge skills
   负责 agent 向 runtime 提交受治理动作

### 6.2 原子化要求

future skill 应尽量原子化，避免继续把长链逻辑封进单个大 skill。

例如 history retrieval 应拆成：

1. build-history-query
2. query-case-library
3. query-signal-corpus
4. compose-history-context

而不是继续维持一个“帮你决定一切”的大一统 summary skill。

### 6.3 advisory 与 hard gate 必须分离

后续设计必须强制区分两类 skill。

#### advisory skill

例如：

1. board brief
2. next actions
3. readiness summary
4. claim / observation / coverage views

它们必须：

1. 可跳过
2. 可重复
3. 可替换
4. 不得成为调查内循环的唯一路径

#### hard gate skill / runtime gate

例如：

1. side effect approval
2. external fetch execution
3. promotion freeze
4. publication

这些仍应保留硬边界。

### 6.4 原始层和归一化层必须始终可直达

无论未来 summary / ranking / cluster 做得多好，都必须允许 agent：

1. 直接查询 `normalized_signals`
2. 直接 lookup `raw_record`
3. 直接复查 parent evidence

否则分析工具又会重新变成枷锁。

## 7. Agent 角色与写边界

### 7.1 sociologist

默认负责：

1. public evidence 查询
2. narrative grouping
3. claim-like result sets
4. 公共叙事补证

默认可写：

1. public analysis result sets
2. board notes
3. claim-oriented hypothesis proposals

### 7.2 environmentalist

默认负责：

1. environment evidence 查询
2. observation / pattern result sets
3. 时空环境补证

默认可写：

1. environment analysis result sets
2. board notes
3. observation-oriented hypothesis proposals

### 7.3 challenger

默认负责：

1. contradiction scan
2. source overlap 检查
3. sample-and-review
4. rebuttal 与 falsification

默认可写：

1. challenge tickets
2. probes
3. rebuttal result sets

### 7.4 moderator

默认负责：

1. 读取多个 agent 的结果
2. 推进议会状态
3. 决定保持当前轮、开启下一轮或请求 freeze

moderator 的本质是：

`议会状态推进者`

而不是：

`唯一调查执行者`

### 7.5 runtime

runtime 不参与调查判断。

runtime 只负责：

1. 执行动作是否合法
2. 写边界是否越权
3. 外部副作用是否被批准
4. 发布与归档边界是否满足

## 8. 理想工作流

```mermaid
sequenceDiagram
    autonumber
    actor U as Operator
    participant R as Runtime Governance
    participant M as Moderator
    participant S as Sociologist
    participant E as Environmentalist
    participant C as Challenger
    database DB as Run DB
    participant X as State-Change Skills
    participant G as Promotion Gate
    participant P as Publication / Archive

    U->>R: init run + mission
    R->>DB: 建 run metadata / evidence base
    R->>DB: import / fetch / normalize

    par public analysis
        S->>DB: query public signals / raw / history
        S->>DB: build result sets
    and environment analysis
        E->>DB: query environment signals / raw / history
        E->>DB: build result sets
    and challenge pressure
        C->>DB: contradiction scan / sample review
    end

    S-->>M: findings
    E-->>M: findings
    C-->>M: challenges

    M->>X: note / hypothesis / task / challenge / probe
    X->>DB: 写 deliberation state

    opt 证据不足
        M->>X: open next investigation round
        X->>DB: 写 round transition + carryover
        R->>DB: 允许新的 governed ingress
    end

    alt 满足冻结条件
        M->>G: request promotion gate
        G-->>M: allow-promote
        M->>X: freeze promotion basis
        M->>P: reporting / publication / archive
    else 未满足
        G-->>M: freeze-withheld
        M->>X: continue current round or open next round
    end
```

这个工作流和当前 runtime 最大的区别是：

1. 现在是 controller 更像推进者。
2. 未来应当是 moderator 和其它 agent 借助 state-change skill 推进状态。
3. runtime 只在高风险边界重新接管。

## 9. 从当前代码出发，哪些保留，哪些降级，哪些新增

### 9.1 直接保留

下面这些应直接保留并继续复用：

1. `source_queue_*`
2. `signal_plane_normalizer`
3. `query / lookup` 类 skill
4. board state-change 类 skill
5. archive / history persistence
6. publication gate 与 runtime admission

### 9.2 从主链降级为 advisory

下面这些应保留，但从“必经主链”降级为“分析透镜”：

1. `eco-extract-claim-candidates`
2. `eco-cluster-claim-candidates`
3. `eco-extract-observation-candidates`
4. `eco-merge-observation-candidates`
5. `eco-link-claims-to-observations`
6. `eco-score-evidence-coverage`
7. `eco-materialize-board-brief`
8. `eco-propose-next-actions`
9. `eco-summarize-round-readiness`
10. `eco-plan-round-orchestration`

### 9.3 新增重点

后续真正应投入的新增工作包括：

1. run DB 中的 analysis plane 与 deliberation plane schema。
2. appendable / branchable ingress 机制。
3. agent session / workspace / permission boundary。
4. moderator loop。
5. result-set lineage 与审计语义。
6. 将 reporting / publication 改为从 DB 与 board 导出，而不是从强顺序 JSON 链导出。

## 10. 实施顺序

### 阶段 B1：固定 DB-first 合同

完成：

1. evidence / analysis / deliberation 三个平面的最小 schema。
2. result set 与 state-change 的统一 contract。

### 阶段 B2：让 query / lookup 成为主工作面

完成：

1. agent 默认先用 query / lookup。
2. summary / coverage 只作为可选分析工具。

### 阶段 B3：改 ingress 模式

完成：

1. 保留 strict fetch_plan 模式。
2. 新增 appendable / branchable agent ingress。
3. 仍由 runtime 审核外部动作。

### 阶段 B4：建立真正的 deliberation plane

完成：

1. hypothesis / challenge / task / probe / round transition DB 化。
2. moderator 通过 state-change skill 推进议会。

### 阶段 B5：引入多 agent turn loop

完成：

1. 角色 session 与工作记忆边界。
2. 协作协议与写权限边界。
3. moderator、challenger、domain agents 并行工作。

### 阶段 B6：接通 freeze / report / archive

完成：

1. promotion basis freeze 从 deliberation plane 导出。
2. reporting / publication / archive 从 DB 和 board 导出。

## 11. 完成定义

只有同时满足下面条件，才算真正完成 `db first / openclaw agent` 路线：

1. agent 可以直接围绕共享数据库调查，而不是依赖固定 JSON 主链。
2. moderator 能通过 state-change skill 推进轮次和议会状态。
3. advisory summary skill 不再充当唯一控制器。
4. runtime 仍然保留强治理、强审计、强发布边界。
5. reporting / publication / archive 都能从 DB + deliberation state 稳定导出。

## 12. 最终目标的固定表述

以后不再改变的目标表述应当是：

`本项目保留当前 runtime 作为治理执行面，同时把主调查工作面重构为 db-first 的 OpenClaw 多 agent 系统；agent 直接通过原子 skill 操作共享证据面、分析面和议会状态面，runtime 只在高风险边界进行治理、冻结和发布。`
