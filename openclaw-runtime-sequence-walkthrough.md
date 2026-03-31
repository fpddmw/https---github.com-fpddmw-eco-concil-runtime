# OpenClaw Runtime Sequence Walkthrough

## 1. 文档目的

这份文档把当前项目里已经落地的 `runtime route` 整理成一张可讲、可展示、可对照代码的时序图式说明。

它回答的是一个很具体的问题：

从拿到一个环境调查题目开始，系统目前怎样一步步走到：

1. 取数
2. 归一化
3. 分析
4. 议事与准入判断
5. 报告生成
6. 归档与历史复用

## 2. 示例 Case

这里用一个已经在测试和说明中反复使用的案例来讲：

`验证 2023-06-07 纽约烟雾事件是否属实，并生成最终汇报`

你可以把它理解成：

1. 公共讨论面要回答“大家在说什么”
2. 环境观测面要回答“物理证据是否支持这些说法”
3. 系统最后要给出一个可追溯、可解释、可发布的汇报结果

## 3. 总体时序图

如果渲染环境支持 Mermaid，可以直接看下面这张图；如果不支持，后面的分阶段说明就是它的文字展开版。

```mermaid
sequenceDiagram
    autonumber
    actor U as Operator / Topic
    participant K as Runtime Kernel
    participant S as eco-scaffold-mission-run
    participant P as eco-prepare-round
    participant F as eco-import-fetch-execution
    participant N as Normalize Skills
    database DB as analytics/signal_plane.sqlite
    participant A as Analysis Skills
    participant B as Board / Investigation State
    participant C as Planner / Controller / Supervisor
    participant O as eco-open-investigation-round
    participant G as Promotion Gate
    participant R as Reporting Skills
    participant H as Archive / History

    U->>K: 提交题目与 mission
    K->>S: scaffold run
    S-->>K: mission.json + round_tasks + investigation_board

    K->>P: prepare round
    P->>P: family/layer planning + anchor resolution
    P-->>K: source_selection_<role> + fetch_plan

    K->>F: execute fetch plan
    F->>F: import / detached-fetch
    F->>N: 调用对应 normalizer 或 raw-only fallback
    N->>DB: 写入 normalized_signals
    N-->>F: receipt / artifact refs / warnings
    F-->>K: import_execution_<round>.json

    K->>A: extract / cluster / merge / link / scope / coverage
    A->>DB: 读取 normalized signals
    A-->>K: analytics/*.json

    K->>B: post note / hypothesis / challenge / probe
    B-->>K: board state updated

    K->>C: supervise-round
    C->>C: plan phase-2 queue
    C->>B: summarize / brief / next-actions / readiness
    C->>G: apply promotion gate

    alt gate = allow-promote
        G-->>C: promoted
        C->>R: handoff + decision draft + role reports + publish + final publication
        R-->>K: reporting/*.json
    else gate = freeze-withheld
        G-->>C: hold
        C-->>B: 保持调查开放，给出 next-actions / hold posture
        C-->>U: moderator 判断是否继续本轮或开启新轮
        opt open round-002
            U->>O: source_round=round-001, target_round=round-002
            O->>B: 保留 round-001, 写 round-opened event
            O-->>K: round_transition_002 + round_tasks_002
            K->>P: prepare round-002
            A->>DB: 以 round_scope=up-to-current 查询 round-001..002
            A-->>B: 补充新一轮 note / task / hypothesis / challenge
        end
    end

    K->>H: close-round + archive + bootstrap-history-context
    H-->>K: case library + signal corpus + history context
    K-->>U: final publication 或 hold-state + full audit trail
```

## 4. 分阶段说明

### 阶段 1：题目进入系统

系统先把题目写成一个 `mission`，其中至少包含：

1. `topic`
2. `objective`
3. `window`
4. `region`
5. 可选的 `artifact_imports` 或 `source_requests`
6. 如果涉及二级 source，可附带 `source_governance` 来批准非入口层

这一阶段的作用不是分析，而是把“自然语言课题”转成一份可执行、可冻结、可审计的任务合同。

核心产物：

1. `mission.json`
2. `round_tasks_<round_id>.json`
3. `board/investigation_board.json`

### 阶段 2：按角色准备数据入口

系统接着进入 `eco-prepare-round`。

这一层会做三件事：

1. 根据 mission 和任务，把取数需求按角色拆开
2. 生成 `source_selection_<role>_<round_id>.json`
3. 生成真正可执行的 `fetch_plan_<round_id>.json`

这里现在已经不是“简单列出几个 source 名字”，而是会显式生成：

1. `family_plans`
2. `layer_plans`
3. `anchor_mode`
4. `anchor_refs`
5. `depends_on`

也就是说，runtime 已经开始区分：

1. 入口层 `l1`
2. 依赖上游结果的派生层 `l2`

例如当前已经接入并验证的两条典型链路是：

1. `youtube-video-search -> youtube-comments-fetch`
2. `regulationsgov-comments-fetch -> regulationsgov-comment-detail-fetch`

其机理是：

1. `youtube-comments-fetch` 不直接自己猜 `video_id`，而是读取上游 `youtube-video-search` 的 raw artifact
2. `regulationsgov-comment-detail-fetch` 不直接自己猜 `comment_id`，而是读取上游 `regulationsgov-comments-fetch` 的 raw artifact
3. `eco-prepare-round` 会把这种关系写成 `depends_on + anchor_artifact_paths`
4. 如果 mission 允许，还可以从 prior round 读取同 family 的上游 anchor

当前已接入的 source families 主要包括：

1. 公共舆情面：`bluesky`、`gdelt`、`youtube`、`regulationsgov`
2. 物理环境面：`airnow`、`openaq`、`open-meteo`、`usgs-water`、`nasa-firms`

当前角色是显式写死的两类主调查角色：

1. `sociologist`
2. `environmentalist`

它们的含义是：

1. `sociologist` 主要面向公共讨论和叙事信号
2. `environmentalist` 主要面向物理环境和观测信号

这一层的重点是治理，而不是自由推理。

### 阶段 3：执行 fetch / import，并做 normalize

`eco-import-fetch-execution` 会读取 `fetch_plan`，逐步执行：

1. 直接导入已有 artifact
2. 或执行 detached fetch
3. 然后为每个 source 调用对应的 normalize skill

这里现在有一个重要的保底行为：

1. 如果 normalizer 已存在，就把结果写入 `analytics/signal_plane.sqlite`
2. 如果 normalizer 暂时缺失，runtime 不会直接把整轮打崩
3. 它会以 `raw-only` 方式保留 raw artifact，并在 `import_execution_<round_id>.json` 里写明 warning

这意味着“新 source 接入未完全做完”时，系统至少还能先完成：

1. 取数
2. 原始证据归档
3. 审计链保留

而不是因为单一 source 缺 normalize 就整轮失败

这里要强调一个关键点：

原始文件不会消失。

系统会同时保留：

1. 原始 artifact 文件
2. 归一化后的信号数据库记录
3. 对应的 `artifact_path` 和 `record_locator`

所以它不是“只剩一份压缩结果”，而是保留了回溯链。

核心产物：

1. `runtime/import_execution_<round_id>.json`
2. `analytics/signal_plane.sqlite`

补充说明：

1. 不是所有 source 一定都会立刻入库
2. 只有“已有 normalizer”的 source 会写入 `normalized_signals`
3. `raw-only` source 当前仍只保留在 `raw/` 与 `import_execution` 中，等待后续补齐 normalize

## 5. 数据进入统一证据库

normalize 完成后，公共信号和环境信号会被统一写入同一个 run 级证据库：

`analytics/signal_plane.sqlite`

当前这个库至少承担三种作用：

1. 存储规范化后的 `normalized_signals`
2. 保留 `raw_json` 与 artifact 引用
3. 成为后续查询和部分分析 skill 的直接数据源，并且按 `round_id` 支持跨轮回看

这也是当前 runtime 路线里最重要的“统一证据面”。

要点是：

1. 当前数据库不是“一轮一个库”，而是“一次 run 一个统一证据库”。
2. 每条 signal 自带 `round_id`，所以可以区分它来自第几轮。
3. 查询 skill 现在已经支持 `round_scope=current|up-to-current|all`，因此在 `round-003` 可以回看 `round-001`。

### 阶段 4：分析链从数据库读数据，再生成中间分析物

分析链的典型顺序是：

1. `eco-extract-claim-candidates`
2. `eco-extract-observation-candidates`
3. `eco-cluster-claim-candidates`
4. `eco-merge-observation-candidates`
5. `eco-link-claims-to-observations`
6. `eco-derive-claim-scope`
7. `eco-derive-observation-scope`
8. `eco-score-evidence-coverage`
9. `eco-build-normalization-audit`

这一段要注意一个边界：

1. 前面的 query / lookup / extract 类 skill 很多直接读 SQLite
2. 后面的 link、scope、coverage 等则会把结果写成 `analytics/*.json`

所以当前项目不是“全程都靠数据库”，而是：

1. 数据底座在数据库
2. 分析结果层大量以 JSON 工件呈现

### 阶段 5：进入议事状态面

分析物出来以后，系统会把调查从“数据处理”推进到“议事状态管理”。

这里常见的对象包括：

1. board note
2. hypothesis
3. challenge ticket
4. falsification probe

这些对象的作用是把“找到的数据”变成“可讨论、可挑战、可冻结的调查状态”。

这也是为什么当前系统虽然已经出现角色名，但它们更像议事视角和责任标签，而不是完整独立 agent。

### 阶段 6：phase-2 规划、控制与准入

接下来进入 `planner / controller / supervisor` 这一段。

这里的目标不是继续发现新证据，而是判断：

1. 当前 round 是否足够稳定
2. 是否还需要补挑战或补证据
3. 是否允许进入 promotion / reporting

典型 phase-2 阶段包括：

1. `board-summary`
2. `board-brief`
3. `next-actions`
4. `falsification-probes`
5. `round-readiness`
6. `promotion-gate`
7. `promotion-basis`

这部分是当前 runtime 路线里最“控制面”的部分。

### 阶段 7：分支判断

这里会出现两条分支。

#### 分支 A：证据足够，允许推进

如果 `promotion-gate` 判断为 `allow-promote`，系统会：

1. 冻结 promoted evidence basis
2. 生成 reporting handoff
3. 起草 council decision
4. 分别生成 `sociologist` 和 `environmentalist` 的专家报告
5. 发布角色报告
6. 发布 council decision
7. 汇总成 `final_publication_<round_id>.json`

这就是当前项目里最接近“最终报告”的封装物。

#### 分支 B：证据不足，保持调查开放

如果 `promotion-gate` 判断为 `freeze-withheld`，系统不会强行出结论，而是：

1. 冻结 withheld 状态
2. 明确保留 investigation hold posture
3. 让系统回到补证据、补挑战、补行动的状态
4. moderator 可显式调用 `eco-open-investigation-round` 开启 `round-002`

这一点对中期答辩很重要，因为它说明系统不是无条件生成结论，而是具备“不放行”的能力。

### 阶段 7.1：显式开启下一轮

多轮现在不是一个抽象口号，而是一个明确动作：

1. `eco-open-investigation-round` 会保留旧轮 board，不覆盖 `round-001`。
2. 它会在 board 上新建 `round-002`，并写入 `round-opened` 事件。
3. 它会把仍然活跃的 hypothesis、未完成 task、未解决 challenge 转成新轮的 carryover 状态。
4. 它会生成 `investigation/round_tasks_<new_round>.json`，因此下一轮可直接进入 `eco-prepare-round`。
5. 它会生成 `runtime/round_transition_<new_round>.json`，把这次轮次切换记录成可审计工件。

这意味着当前系统已经不只是“hold 住”，而是能把“继续调查”落实成真正的新轮次对象。

### 阶段 7.2：新轮如何继承旧轮数据

目前的跨轮继承分成两层：

1. 状态继承：board 上的 hypothesis / task / challenge carryover 由 `eco-open-investigation-round` 负责。
2. 数据继承：查询 skill 通过 `round_scope=up-to-current` 直接从同一 `signal_plane.sqlite` 读取前轮数据。

因此在一个典型 case 里：

1. `round-001` 完成首轮取证。
2. gate 判断 `freeze-withheld`。
3. moderator 开启 `round-002`。
4. `round-002` 的 agent 可以一边读取本轮新数据，一边直接调出 `round-001` 的 signals 做复核和对照。

### 阶段 8：归档与历史复用

本轮结束后，系统会进入 post-round 阶段：

1. `close-round`
2. `eco-archive-case-library`
3. `eco-archive-signal-corpus`
4. `bootstrap-history-context`

作用是把本轮调查沉淀成后续轮次可以复用的历史材料。

所以项目当前已经不是一次性流水线，而是开始具备“案例库”和“信号语料库”的积累能力。

## 6. 关键状态物一览

| 阶段 | 代表产物 | 作用 |
| --- | --- | --- |
| mission ingress | `mission.json` | 把题目冻结成可执行合同 |
| task scaffold | `round_tasks_<round_id>.json` | 把工作拆成按角色分配的调查任务 |
| source planning | `source_selection_<role>_<round_id>.json` | 记录每个角色允许与选中的信息源，以及 family/layer/anchor 规划 |
| fetch execution | `fetch_plan_<round_id>.json` | 记录本轮取数执行图，包括 depends_on 与 anchor 引用 |
| normalize | `analytics/signal_plane.sqlite` | 存放统一规范化证据 |
| analysis | `analytics/*.json` | 存放 claim、observation、link、scope、coverage 等中间分析物 |
| deliberation | `board/investigation_board.json` | 存放 note、hypothesis、challenge、task |
| phase-2 | `runtime/round_controller_<round_id>.json` | 存放本轮控制面状态 |
| round transition | `runtime/round_transition_<round_id>.json` | 存放 moderator 开启新轮的显式状态切换记录 |
| gate | `runtime/promotion_gate_<round_id>.json` | 存放能否推进的明确判断 |
| reporting | `reporting/*.json` | 存放 handoff、报告、决议、最终发布物 |
| archive | archive DBs | 把本轮结果沉淀为历史上下文 |

## 7. 给老师展示时最好强调的三点

### 第一，当前系统已经不是“只会写报告”

它先做：

1. 数据进入
2. 归一化
3. 证据匹配
4. 状态管理
5. 准入判断

最后才做报告生成。

### 第二，当前系统已经具备审计链

它能回答：

1. 数据从哪里来
2. 经过了哪些 normalize 和分析步骤
3. 哪些证据支持当前结论
4. 为什么这轮被放行或者被拦下

### 第三，当前路线已经形成闭环，但还不是最终形态

当前完成的是：

`受控 runtime 闭环`

下一阶段要重点解决的是：

`更开放的 DB-first agent mode`

也就是说：

1. runtime 路线已经能稳定跑完一条闭环
2. 当前已经支持显式多轮调查与跨轮回看
3. 但真正的多 agent 自主调查，还要在下一阶段继续打开

## 8. 当前边界与真实结论

为了口径准确，最后要补一句真实结论：

当前系统已经可以从题目走到最终发布物或 hold-state，但它还不是“一条命令、完全自由的多 agent 自动调查系统”。

更准确地说，它现在是：

1. 一条已经闭环的、受治理的 runtime 调查流水线
2. 具备数据库证据面、议事状态面、报告导出面
3. 为下一阶段的 OpenClaw agent mode 提供治理底座
