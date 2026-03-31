# OpenClaw 项目总览（当前实现与中期汇报）

## 1. 文档定位

这份文档只负责讲清楚三件事：

1. 目前项目已经实现了什么。
2. 当前 `runtime route` 是怎样实际工作的。
3. 中期汇报时，哪些内容可以被稳妥地当作“已完成成果”。

根目录现在只保留三份文档：

1. 本文档 `openclaw-first-refactor-blueprint.md`
   说明当前实现、当前工作流、当前边界。
2. `openclaw-db-first-agent-runtime-blueprint.md`
   说明未来主目标，也就是 `db first / openclaw agent` 路线。
3. `openclaw-skills-catalog.md`
   作为 skill 索引附录，不再承担路线说明。

阅读顺序建议：

1. 先看本文档，确认“现在做到了什么”。
2. 再看 `db first` 蓝图，确认“以后要变成什么”。
3. 最后按需查 `skills` 附录。

## 2. 一句话结论

当前仓库已经落地的是：

`一条可运行、可审计、可复现的 runtime 调查主链。`

当前仓库还没有真正落地的是：

`以 OpenClaw 多 agent 为主导、以数据库为主工作面的开放式调查主链。`

所以中期汇报时，最稳妥的表达应当是：

1. 当前已经完成一条“受治理的调查执行底座”。
2. 它已经能完成多源取数、归一化入库、分析、议会状态推进、汇报和归档。
3. 未来的主目标不变，仍然是把上层工作面重构为 `db first openclaw agent mode`。

## 3. 当前代码状态

截至 `2026-03-31`，当前项目的事实状态可以固定为：

1. legacy 旧 runtime 已删除，迁移工作已结束。
2. 当前运行时内核收敛在 `eco-concil-runtime/src/eco_council_runtime/kernel/`。
3. 当前仓库共有 `73` 个 skill 目录。
4. 当前已接入 `16` 个 source skill。
5. 全量测试结果为 `python3 -m unittest discover -s tests -q`，共 `78` 项，通过。

当前已验证的关键事实：

1. `mission -> prepare-round -> import/fetch -> normalize -> analysis -> board -> reporting/archive` 主链可跑通。
2. `youtube-video-search -> youtube-comments-fetch` 的依赖链可执行。
3. `regulationsgov-comments-fetch -> regulationsgov-comment-detail-fetch` 的依赖链可执行。
4. `gdelt-events / gdelt-mentions / gdelt-gkg` 已支持 zip 行级 normalize 入库。
5. 当前已注册 source 都有对应 normalizer，不再只停留在 `raw-only` 占位状态。

## 4. 当前两条路线的关系

项目现在其实同时存在两条路线，但成熟度完全不同。

### 4.1 Route A: Runtime Route

这是当前真正落地、可演示、可回归的路线。

它负责：

1. mission 初始化与 round 规划。
2. source governance 与 fetch plan 冻结。
3. import / detached fetch 执行。
4. normalize 入统一证据库。
5. 后续 analysis、board、reporting、archive 主链。
6. 审计、回放、benchmark、operator surface。

它的定位是：

`受治理的执行与审计底座。`

### 4.2 Route B: DB-First OpenClaw Agent Route

这是未来的主目标，但当前还没有主实现。

它未来负责：

1. 多 agent 共享数据库工作面。
2. 动态查询、补证、分支调查。
3. moderator 主导议会状态推进。
4. 让 summary / coverage / readiness 类 skill 变成分析工具，而不是硬控制器。

它的定位是：

`未来的主智能体调查工作面。`

## 5. 当前 Runtime 工作流

当前最适合讲给老师或评委的，不是代码细节，而是下面这条主链。

```mermaid
sequenceDiagram
    autonumber
    actor U as Operator
    participant S as eco-scaffold-mission-run
    participant P as eco-prepare-round
    participant F as eco-import-fetch-execution
    participant N as Normalize Skills
    database DB as signal_plane.sqlite
    participant A as Analysis Skills
    participant B as Board Skills
    participant R as Reporting / Archive

    U->>S: 提交 topic / mission
    S-->>U: mission.json + round_tasks + board 初始化

    U->>P: prepare round
    P-->>U: source_selection + fetch_plan

    U->>F: execute fetch plan
    F->>N: 对每个 source 调用 normalizer
    N->>DB: 写 normalized_signals
    F-->>U: import_execution

    U->>A: query / extract / cluster / link / coverage
    A->>DB: 读统一证据库
    A-->>U: analytics artifacts

    U->>B: note / hypothesis / task / challenge / probe / open round
    B-->>U: board state updated

    U->>R: handoff / report / publish / archive
    R-->>U: final artifacts + history persistence
```

把它展开成文字，就是六个阶段。

### 阶段 1：任务进入系统

`eco-scaffold-mission-run` 负责把题目写成可执行 run。

当前固定产物：

1. `mission.json`
2. `investigation/round_tasks_<round_id>.json`
3. `board/investigation_board.json`

### 阶段 2：准备本轮数据入口

`eco-prepare-round` 负责生成 role 级 source governance 与 `fetch_plan`。

当前已经明确支持：

1. `family / layer / anchor` 编排。
2. `depends_on + anchor_artifact_paths` 自动生成。
3. prior-round anchor 读取。

它的意义是：

`把“本轮允许怎么取数”显式冻结成治理对象。`

### 阶段 3：执行 fetch / import 并归一化

`eco-import-fetch-execution` 负责：

1. 导入已有 artifact。
2. 执行 detached fetch。
3. 调用对应 normalizer。
4. 记录 `import_execution_<round_id>.json`。

归一化后的统一主入口是：

`analytics/signal_plane.sqlite`

其中最关键的是 `normalized_signals`。

### 阶段 4：分析链处理统一证据库

当前大量分析 skill 都已经基于同一个证据面工作，包括：

1. `query / lookup`
2. `claim / observation extract`
3. `cluster / merge`
4. `link / scope / coverage`

要点是：

1. 现在下层证据面已经开始 DB 化。
2. 上层很多分析对象仍以 JSON 工件存在。

### 阶段 5：议会状态推进

当前 board 相关 skill 已经形成一组显式状态变更器：

1. `eco-post-board-note`
2. `eco-update-hypothesis-status`
3. `eco-open-challenge-ticket`
4. `eco-close-challenge-ticket`
5. `eco-claim-board-task`
6. `eco-open-falsification-probe`
7. `eco-open-investigation-round`

这意味着当前系统已经不只是“跑完分析就结束”，而是已经具备：

`显式议会状态 + 多轮调查切换能力。`

### 阶段 6：汇报、发布与归档

当前已经具备：

1. `eco-materialize-reporting-handoff`
2. `eco-draft-council-decision`
3. `eco-draft-expert-report`
4. `eco-publish-council-decision`
5. `eco-publish-expert-report`
6. `eco-materialize-final-publication`
7. `eco-archive-case-library`
8. `eco-archive-signal-corpus`

这保证了当前路线不是“分析 demo”，而是一条能把结果沉淀下来的完整链路。

## 6. 当前数据层级

当前项目的数据主干可以概括为七层。

| 层级 | 当前主要对象 | 当前作用 |
| --- | --- | --- |
| 治理层 | `run_manifest`、ledger、receipts、dead_letters | 记录执行与审计状态 |
| 规划层 | `mission`、`round_tasks`、`source_selection`、`fetch_plan` | 记录本轮要做什么 |
| 原始层 | `raw/` 下各 source artifacts | 保留最原始证据与上游 anchor |
| 归一化层 | `analytics/signal_plane.sqlite` / `normalized_signals` | 统一 public / environment 证据面 |
| 分析层 | `claim / observation / link / coverage` 等 `analytics/*.json` | 形成候选解释与中间分析物 |
| 议会层 | board、hypothesis、challenge、task、probe | 显式调查状态与多轮协作对象 |
| 输出层 | handoff、report、decision、archive | 对外汇报与历史沉淀 |

当前最关键的实现结论有两个：

1. `normalized_signals` 已经是统一证据面。
2. `claim / observation / coverage` 还不是数据库中的通用分析平面，而仍主要是 JSON 工件。

## 7. 当前 Agent / Role 边界

当前项目已经有“角色语义”，但还没有“真正独立运行的多 agent”。

| 当前对象 | 当前真实形态 | 当前职责 | 是否是真正独立 agent |
| --- | --- | --- | --- |
| `runtime` | 内核执行面 | 治理、执行、审计、归档 | 否 |
| `sociologist` | 任务与 source role | public 证据侧责任标签 | 否 |
| `environmentalist` | 任务与 source role | environment 证据侧责任标签 | 否 |
| `moderator` | board / round 控制角色 | 汇总状态、保持或推进轮次 | 否 |
| `challenger` | board / probe 责任角色 | 反证、挑战、falsification 压力 | 否 |
| board skills | 状态变更器 | 显式写 hypothesis / challenge / task / round | 否，但已接近未来雏形 |

所以当前最准确的判断是：

1. 角色已经存在，但主要还是标签与责任边界。
2. 真实执行主体仍然是 runtime。
3. 真正的 agent 协作工作面，要到 `db first` 路线里才会建立。

## 8. 中期汇报时可稳妥主张的成果

以下内容适合被当作“当前阶段已经完成”的成果。

### 8.1 可运行的受治理调查链

可以明确主张：

1. 项目已经能从题目进入系统，一直跑到归档。
2. 这条链不是手工演示，而是有自动化测试支撑的。

### 8.2 多源证据统一入库

可以明确主张：

1. 多类 public source 和 environment source 已进入统一 signal plane。
2. 证据保留 `artifact_path + record_locator + raw_json`，具备回溯链。

### 8.3 多轮调查与议会状态雏形

可以明确主张：

1. 当前已支持显式 board 状态。
2. 已支持 `open next round`。
3. 后续轮次可继续读取前面轮次的 DB 证据。

### 8.4 可审计性已经成形

可以明确主张：

1. runtime 具备 ledger / receipt / dead-letter / operator surface。
2. source ingress 已有 planning snapshot 与 drift detection。
3. archive / history persistence 已接入主链。

## 9. 当前真实短板

这部分也要在汇报时讲清楚，否则容易把“现状”和“蓝图”混在一起。

当前还没有完成的关键点是：

1. 还没有真正独立运行的多 agent turn loop。
2. `sociologist / environmentalist / moderator / challenger` 还不是拥有独立 session 和记忆的实体 agent。
3. 很多 analysis skill 仍然是强线性的 JSON 工件链。
4. `fetch_plan` 仍然更适合 governed batch run，而不适合开放式调查中途随时追加动作。
5. 上层工作面还没有真正把 OpenClaw 当成主调查者。

## 10. 与未来蓝图的关系

当前路线不是未来主目标的替代品，而是未来主目标的底座。

应当固定的关系是：

1. 当前 `runtime route` 保留，并继续承担治理执行面。
2. 未来主目标仍然是 `db first / openclaw agent mode`。
3. 之后不是推翻当前成果，而是把上层调查工作面从“线性工件链”迁移到“共享数据库 + 原子 skill + 多 agent 状态推进”。

因此，后续开发目标没有变化：

`继续保留 runtime 的强治理能力，同时把主智能体工作面转移到 db-first openclaw agent 路线。`
