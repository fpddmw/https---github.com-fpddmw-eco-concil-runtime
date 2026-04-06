# OpenClaw DB-First Master Plan

## 1. 文档定位

这份文档是当前 `db first / openclaw agent mode` 路线的总控计划表。

它负责四件事：

1. 统一定义 `A / B / C / D` 四条路线分别代表什么。
2. 统一定义每条路线下的阶段编号与阶段含义。
3. 给出当前已完成、进行中、未来数次开发的排期顺序。
4. 作为今后调控开发进度的唯一计划源。

约束：

1. `openclaw-db-first-progress-log.md` 只负责记录“已经交付了什么”。
2. 本文档负责定义“为什么这样分路线、下一步按什么顺序推进”。
3. `openclaw-db-first-dashboard.md` 负责给出“当前阶段、下一阶段、阻塞点、路线快照”的生成看板视图。
4. 当历史编号与当前路线语义发生冲突时，以本文档的归一化定义为准。

## 2. Route Legend

| Route | 路线名称 | 主要问题域 | 当前职责边界 |
| --- | --- | --- | --- |
| `A` | Runtime / Governance Stabilization | 运行时稳定化、治理契约、回归硬化 | 保证当前 runtime route 在迁移过程中不失稳 |
| `B` | Deliberation Plane / Moderator Loop | board、round、challenge、task、probe、moderator 工作面 | 把议会状态从 JSON 快照迁到结构化 deliberation state |
| `C` | Analysis Plane / DB-First Analysis | links、scopes、coverage、history/archive 等分析对象 | 把分析链从 JSON-first 改成 analysis-plane-first |
| `D` | Program Control / Documentation | 计划治理、追踪、里程碑、交付规范 | 统一开发计划、进度记录、阶段规范 |

一句话解释：

1. `A` 管“别把现有底座搞坏”。
2. `B` 管“让 moderator 和 board 真正有数据库工作面”。
3. `C` 管“让 analysis 不再依赖线性 JSON 工件链”。
4. `D` 管“让整个项目的开发节奏可追踪、可调度、可汇报”。

## 3. 阶段状态定义

| 状态 | 含义 | 使用规则 |
| --- | --- | --- |
| `planned` | 已进入总计划，但尚未开始实施 | 默认状态 |
| `in_progress` | 当前正在实施 | 同一时刻建议只保留极少数关键阶段处于该状态 |
| `completed` | 已完成独立交付并记录验证 | 必须同步写入 progress log |
| `blocked` | 已明确开始但被依赖或风险阻塞 | 需要在 progress log 写清阻塞原因 |
| `deferred` | 暂不推进，但保留在路线图中 | 需要说明为何延后 |

## 4. 历史编号归一化说明

当前 `openclaw-db-first-progress-log.md` 里的早期编号存在“路线含义漂移”问题，尤其是：

1. 历史 `B2 / B2.1` 实际上已经属于 analysis-plane 迁移，而不是 deliberation-plane 迁移。
2. 因此，今后不再把它们当作 `B` 路线阶段解释。

历史与归一化映射如下：

| Progress Log 历史编号 | 归一化后编号 | 归属路线 | 说明 |
| --- | --- | --- | --- |
| `A1` | `A1` | `A` | review fix pack |
| `B1` | `B1` | `B` | deliberation plane bootstrap |
| `B1.1` | `B1.1` | `B` | board read migration |
| `B1.2` | `B1.2` | `B` | moderator handoff/readiness migration |
| `B1.3` | `B1.3` | `B` | next-action deliberation migration |
| `B1.4` | `B1.4` | `B` | probe source decoupling |
| `B2` | `C1` | `C` | coverage analysis-plane query surface |
| `B2.1` | `C1.1` | `C` | coverage upstream analysis migration |
| `D1` | `D1` | `D` | documentation traceability pack |
| `D2` | `D2` | `D` | master plan and route normalization |

规则：

1. 旧编号保留在 progress log，避免篡改历史。
2. 新开发与新排期一律使用本文档中的归一化编号。

## 5. Master Plan

### 5.1 Route A: Runtime / Governance Stabilization

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `A1` | Review Fix Pack | 修复 review 发现的运行时与契约问题 | `completed` | 关键 contract regressions 消失并通过测试 |
| `A2` | Shared Contract Hardening | 统一 `analysis_sync / deliberation_sync / observed_inputs / *_source` 等共享输出契约 | `completed` | 关键技能输出字段稳定且不再重复漂移 |
| `A2.1` | D1 Contract Metadata Normalization | 收敛 `next-actions / falsification-probe / round-readiness` 的共享 trace contract，并明确区分 artifact presence 与 materialized presence | `completed` | D1 输出通过共享 runtime helper 发出一致的 `*_source / *_sync / observed_inputs` 字段 |
| `A2.2` | Cross-Plane Contract Adoption | 将同一套归一化 contract 扩展到剩余 archive / reporting / promotion 等消费者 | `completed` | 剩余关键 consumer 不再各自手写 trace metadata 语义 |
| `A3` | Governance Regression Hardening | 确保 replay、benchmark、archive、close-round 等治理命令不被 DB-first 迁移破坏 | `completed` | 全量工作流和治理回归稳定 |
| `A4` | Agent Entry Gate | 定义从 runtime route 进入 DB-first agent route 的最小可控入口 | `planned` | 至少一条 operator-visible 入口链路形成闭环 |

### 5.2 Route B: Deliberation Plane / Moderator Loop

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `B1` | Deliberation Plane Bootstrap | 建立 board 的结构化 DB 状态面 | `completed` | board 事件与 round state 可在 DB 中读取 |
| `B1.1` | Board Read Path Migration | board readers 改为 deliberation-plane-first | `completed` | board summary / planner 不再依赖 JSON-only |
| `B1.2` | Moderator Handoff And Readiness Migration | moderator brief 与 readiness 改为 deliberation-plane-first | `completed` | handoff/readiness 不再把 board summary 当作主输入 |
| `B1.3` | Next-Action Deliberation Migration | D1 action planning 改为 deliberation-plane-first | `completed` | `eco-propose-next-actions` 脱离 board summary 主依赖 |
| `B1.4` | Probe Source Decoupling | probe generation 不再硬依赖 `next_actions` artifact | `completed` | probe 可直接从共享 D1 上下文恢复 |
| `B2` | Board Write-Path Migration | board 状态变更从“JSON first, DB sync”转向“DB first, JSON export” | `completed` | 关键 state-change skills 与 round opening 的主写面切到 deliberation plane |
| `B2.1` | JSON Board Export Demotion | `board_summary` / `board_brief` 明确降级为导出物 | `completed` | 运营链路不再把 summary/brief 当硬前置 |
| `B3` | Moderator Control Consolidation | round transition、promotion freeze、probe/challenge/task 编排由 moderator DB 工作面主导 | `completed` | moderator loop 的主要状态推进不再依赖线性工件顺序 |

### 5.3 Route C: Analysis Plane / DB-First Analysis

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `C1` | Coverage Analysis Query Surface | 将 `evidence_coverage` 接入 analysis plane | `completed` | D1/readiness/promotion 可在缺少 coverage JSON 时继续运行 |
| `C1.1` | Coverage Upstream Analysis Migration | 将 coverage 的上游 `links / claim_scope / observation_scope` 接入 analysis plane | `completed` | coverage 在缺少上游 JSON 时仍可运行 |
| `C1.2` | History / Archive Read Migration | 将 history/archive 对 `links / scopes / coverage` 的直接 JSON 读取迁到 analysis plane | `completed` | `eco-materialize-history-context`、`eco-archive-case-library` 改为 analysis-plane-first |
| `C1.3` | Remaining Export Read Migration | 将仍直接读取 analysis JSON 的 reporting/export 消费端迁到 analysis plane | `completed` | 剩余关键 export/read consumers 不再依赖 analysis JSON 作为主输入 |
| `C2` | Generic Result-Set Contract | 强化 `result_sets / result_items` 的通用契约与 lineage 语义 | `completed` | result set 可追溯 query basis、parent ids、artifact refs |
| `C2.1` | Candidate / Cluster Result Migration | 将 claim/observation candidate、cluster、merge 等对象纳入 analysis plane | `completed` | 早期分析链的关键压缩对象可被统一查询 |
| `C2.2` | Non-Python Query Surface | 把当前 runtime-local helper 提升为更正式的查询接口 | `completed` | 非 Python tooling 也能稳定消费 analysis-plane 结果 |

### 5.4 Route D: Program Control / Documentation

| 阶段 | 名称 | 目标 | 当前状态 | 退出标准 |
| --- | --- | --- | --- | --- |
| `D1` | Documentation Traceability Pack | 修复文档引用漂移并建立 progress log | `completed` | 每次交付有仓内记录 |
| `D2` | Master Plan And Route Normalization | 统一 A/B/C/D 路线定义与阶段规划 | `completed` | 本文档成为唯一计划源 |
| `D3` | Progress Dashboard Conventions | 给 progress log 增加更强的阶段索引、里程碑视图、状态汇总约束 | `completed` | 任意时刻都能看出“当前阶段、下一阶段、阻塞点” |
| `D4` | Milestone / Demo Packaging | 面向中期汇报或阶段验收整理固定里程碑包 | `planned` | 能快速导出当前成果清单、风险、下一步 |

## 6. 当前总体判断

截至现在，四条路线的成熟度可以概括为：

1. `A` 路线已完成第一轮修复、`A2` 共享契约硬化、以及 `A3` 治理回归硬化；当前 D1、promotion/reporting draft、canonical publish、final publication、以及治理命令链上的 `show-run-state / close-round / benchmark / replay` 都已经对 DB-backed round task / moderator action / probe 恢复面与 frozen baseline replay 更稳健，下一步主要转向 `A4` agent entry gate。
2. `B` 路线已经完成 deliberation-plane 读路径迁移、关键 board 写路径 DB-first 切换、`board_summary / board_brief` 的运行时降级，以及 `B3` moderator control consolidation 的当前计划范围；promotion freeze / controller / supervisor 控制快照、`next_actions / falsification_probes` 工作快照、source-round carryover、round task scaffold / `prepare-round` 输入恢复、以及 history/archive 上剩余的 moderator action/probe 读路径都已收口到 deliberation plane，使 moderator loop 的主要状态推进不再依赖线性工件顺序。是否继续把 action/probe/challenge/task 拆成更细粒度对象与历史，应视作后续扩展，而不是当前 `B3` 的阻塞项。
3. `C` 路线已经完成 coverage、其上游 links/scopes、history/archive 读取面、剩余关键 export/read consumer 的 analysis-plane-first 迁移、`C2` result-set lineage contract、`C2.1` candidate / cluster / merge family migration，以及 `C2.2` non-Python query surface。现在 shell 和外部脚本已经可以通过 runtime CLI 直接列出 analysis result sets、查询 analysis items、读取 result contract，而不必再依赖 Python helper 导入；当前 master plan 下的 `Route C` 计划范围已全部交付。
4. `D` 路线现在已经有 master plan、progress log、以及生成式 dashboard 三层分工；下一步主要转向 `D4` 的固定里程碑包整理，而不是继续依赖人工通读整份 progress log 才能判断当前控制状态。

## 7. 推荐的未来数次开发顺序

下面是建议的近端开发队列，也就是未来数次迭代最合适的推进顺序。

| 顺序 | 阶段 | 路线 | 为什么先做 | 预期独立交付 |
| --- | --- | --- | --- | --- |
| `1` | `D4` | `D` | analysis / deliberation / governance 三条主线的当前计划范围已大体稳定，适合整理一份固定的阶段验收与 demo 包模板 | 能快速导出当前成果清单、风险、下一步 |
| `2` | `A4` | `A` | 当里程碑包与当前运行时/查询面都更稳定后，再定义 agent entry gate 可以减少入口设计返工 | 至少一条 operator-visible 入口链路形成闭环 |

## 8. 每次开发交付的记录规范

今后每次独立交付都应同时满足：

1. 先在本文档中找到所属路线与阶段。
2. 如果是新阶段，先补本计划表，再开始实施。
3. 交付完成后必须在 `openclaw-db-first-progress-log.md` 记录：
   - `日期 + 阶段编号 + 标题`
   - `Status`
   - `Objective`
   - `Implementation`
   - `Validation`
   - `Tests added or extended`
   - `Known limitations`
   - `Next`
4. 交付完成后应刷新 `openclaw-db-first-dashboard.md`，确保当前控制视图与 plan/log 一致。
5. 若历史编号与当前路线语义冲突，progress log 保留历史编号，但在本文档的 crosswalk 中补映射。

## 9. 当前建议

当前应把本文档视为：

1. 开发顺序的唯一计划表。
2. A/B/C/D 路线含义的唯一解释源。
3. progress log 的上位总控索引。

短期内不要再新增新的路线字母。

如果确需扩展，只允许：

1. 在既有 `A / B / C / D` 中增加阶段。
2. 先更新本文档，再开始开发。
