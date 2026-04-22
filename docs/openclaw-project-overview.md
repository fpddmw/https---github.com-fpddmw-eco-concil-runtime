# OpenClaw 项目总览、当前问题与目标架构

## 1. 文档定位

本文件只承担三件事：

1. 说明 OpenClaw 当前已经做成了什么。
2. 明确当前系统真正存在的问题，而不是只写能力亮点。
3. 作为下一阶段规划与迁移清单的统一基线。

与历史文档不同，本文件不再把“多 agent 平台”当成默认正确方向；它首先回答系统当前是什么，其次回答系统下一步应该改成什么。

## 2. 一句话定位

OpenClaw 当前更准确的定位是：

`一个受治理的、DB-first 的环境调查 workflow engine。`

下一阶段希望把它推进为：

`一个受治理但高自主、DB-native 的环境争议议会系统。`

这里的关键区别不是宣传口径，而是职责分配：

1. `runtime kernel` 只负责治理、执行、持久化、查询与审计。
2. `agent council` 负责实质性的争议判断、提案、挑战与分诊。
3. `database` 是议会状态与流程推进的真实状态源。
4. `artifact` 只承担导出、handoff 与人类可读展示。

## 3. 当前已经完成什么

截至当前仓库状态，OpenClaw 已经完成上一轮 DB-first 主计划的基础收口。真正落下来的不是 skill 数量，而是四个工作面：

1. `runtime / governance`
   - 已能管理 run、round、source governance、admission、execution、receipt、replay 与 archive/publication 边界。
2. `signal plane`
   - 已能把 public 与 environment 两类异构输入写入统一的 `normalized_signals` 工作面。
3. `analysis plane`
   - 已能把 candidate、cluster、scope、link、coverage、controversy-map 与 typed issue decomposition 等对象写入统一 result-set / lineage 结构；其中 `issue-cluster / stance-group / concern-facet / actor-profile / evidence-citation-type / claim-candidate / claim-cluster / claim-scope / verifiability / route / formal-public-link / representation-gap / diffusion-edge / controversy-map` 已具备 canonical contract、item-level query 与 DB-native evidence/lineage 持久化。
4. `deliberation plane`
   - 已能把 hypothesis、challenge、board-task、proposal、next-action、probe、readiness、decision trace、round transition 等议会状态写入 DB-first 状态面。
5. `reporting plane`
   - 已建立独立 reporting canonical plane、独立 query surface，并把 reporting artifact 降级为 DB-backed export；`materialize-reporting-exports` 已可从 SQLite 重建 handoff / decision / expert report / final publication 全套导出物。
6. `runtime / control plane`
   - `promotion-freeze / controller-state / gate-state / supervisor-state` 已进入 runtime canonical contract registry。
   - deliberation DB 已新增 `controller_snapshots / gate_snapshots / supervisor_snapshots` 独立表，`query-control-objects` 也已提供对称的一等 query surface。
   - `show-run-state` 与 phase-2 state wrapper 现在会优先消费这些 control rows，而不是只读 `promotion_freeze.raw_json` 或 runtime artifact。
7. `agent entry / phase-2 orchestration`
   - `openclaw-agent` 轮次进入 phase-2 时，controller 与 agent entry 已能优先采用 `direct-council-advisory` plan，并把 `plan_source / planning_attempts` 写入 controller 状态；当 DB 中已有直接 `proposal / readiness-opinion / probe` 时，advisory queue 已能直接由这些对象编译，不再强制重跑 planner skill。与此同时，`next_actions / probes / readiness` 的 DB/artifact read surface 已从 `investigation_planning.py` 抽到独立模块，phase-2 主链对启发式规划模块的直连已经开始断开。

这说明系统已经脱离“抓数据 + 生成文档”的脚本集合，具备了共享状态、受治理执行和跨轮次恢复的骨架。

## 4. 当前工作流与状态面

### 4.1 当前主干

当前工作流主干可以概括为：

`mission / run -> round / source governance -> fetch / import -> normalize -> analysis -> board / moderation -> reporting / archive`

对应的数据层级大致是：

`raw -> normalized -> analytics -> deliberation -> reporting -> archive/history`

### 4.2 当前状态源的真实情况

当前系统已经明显转向 DB-first；reporting/publication、phase-2 / investigation 中间态，以及 phase-2 control surface 都已经收口到 DB-native only + export rebuild。但整轮议会流程还没有彻底摆脱 orchestration plan / operator export 与旧主链假设。

已经成立的部分：

1. SQLite 已经是 signal、analysis、board 状态查询与恢复的主工作面。
2. result-set / lineage 与 deliberation state 已经形成稳定的 DB 写入面。
3. reporting / publication 现在已经可以从 DB canonical objects 重新物化；artifact-only 文件也会被显式视为 orphaned export，而不是隐式恢复源。
4. `next_actions / probes / readiness / promotion_basis / supervisor_state` 现在也都能从 DB canonical rows/snapshots 重建；phase-2 artifact 已降级为 export-only。
5. `controller / gate / supervisor / promotion_freeze` 现在也拥有独立 runtime canonical rows 与 query surface；控制面不再只是 `promotion_freeze.raw_json` 的嵌套 snapshot。

尚未完全成立的部分：

1. `openclaw-agent` 的 advisory 主路径已基本 DB-native，但 `orchestration_plan` 仍主要是 runtime export，而不是独立 canonical object。
2. 某些 runtime/post-round/benchmark 控制链路仍保留历史导出物约定。
3. analysis plane 的 controversy 主结构链已基本 DB-native；当前残余问题已主要收缩到 `issue / stance / concern / actor / citation` typed decomposition、board/reporting issue-centric 化，以及少数 runtime export 约定与旧 fallback heuristic。

因此，当前最准确的判断不是“议会已经完全基于数据库运作”，而是：

`议会关键状态、controversy 主链、phase-2 / reporting 中间态与 phase-2 control surface 已大体 DB-native，但整轮议会流程还没有完全摆脱 orchestration export 与少数旧主链假设。`

## 5. 当前系统的关键问题

### 5.1 Agent 自主权不足

当前系统里的角色语义是清楚的，但 agent 主要仍是“受治理执行端”而不是“实质性议会参与者”。

当前主要问题：

1. 议会流程的核心推进仍主要依赖 runtime 预定义阶段与 skill 链。
2. agent 的写入能力主要表现为执行预设 command/skill，而不是基于共享状态自主形成可对抗的 deliberation proposal。
3. promotion、publication、archive 仍完全在 agent 外环。
4. 虽然 `openclaw-agent` 轮次已能由 DB 中的 council objects 直接编译 advisory queue，而且 controller 已不再强行插入默认 `promotion-gate` / post-gate 阶段、也不再内嵌 `promotion-gate` 特判，但 phase-2 仍围绕 readiness/promotion 语义和既有 gate handler 展开，agent 还不能真正自定义更宽的 phase-2 语义空间。

这意味着当前系统适合“治理内协作”，不适合被称为“高自主议会”。

### 5.2 Runtime kernel 边界过宽

如果把 `runtime kernel` 理解为最小执行内核，那么当前边界明显过宽。

当前 kernel 同时承担了：

1. admission / execution / ledger / replay
2. phase-2 stage contract 与 controller
3. operator health surface
4. 部分议会流程语义与阶段推进假设

这更像一个完整的 workflow engine，而不是最小 kernel。下一阶段如果不收边界，系统会持续把 domain policy、heuristic scoring 和 moderation logic 堆进 kernel。

### 5.3 启发式与规则占比过高

当前 public-side 主链仍高度依赖规则与固定公式：

1. claim/issue 抽取依赖规则表和文本 pattern。
2. cluster、scope、routing、readiness、probe typing 里有大量固定阈值和公式。
3. 议会流程稳定性主要靠规则化流程，而不是靠 agent 在共享状态上的自主判断。

规则不是问题本身；问题在于它们当前仍是主判断来源，而不是 fallback、bootstrap 或 audit。

### 5.4 数据契约还不够硬

当前契约在运行治理层已经比较完整，但在领域语义层仍不够硬。

已有基础：

1. runtime admission/preflight 边界较清楚。
2. analysis result-set / lineage 已有通用 contract。
3. observed inputs / trace metadata 已开始统一。

主要缺口：

1. `formal-comment-signal` 现在已直接携带 `docket / agency / submitter / issue / stance / concern / citation / route` typed surface，并通过 `normalized_signal_index` 写成 DB index；formal-side 维度不再只存在于 artifact 或临时文本推断里。
2. analysis plane 的 typed controversy issue layer 已建成；`issue-cluster / stance-group / concern-facet / actor-profile / evidence-citation-type / formal-public-link / representation-gap / diffusion-edge / controversy-map` 均已完成强契约、canonical normalization 与 DB item-level query。当前剩余问题不再是“formal-side 完全没 typed surface”，而是 formal typed surface 主要停留在 signal plane，尚未额外拆成独立的 formal-only analysis result-set family。
3. 少数 skill 仍依赖 envelope 兼容字段和松散 dict 约定，尚未全部收口到唯一 object shape。
4. board / reporting 的 shared context 已切到 `issue-cluster-first`；formal signal typed surface 已在 query/linkage 中被直接消费，但更多 deliberation judgement 仍主要读取 issue-layer objects。

### 5.5 议会尚未真正以数据库为唯一工作面

当前系统已经能够把很多状态写入数据库，但“基于数据库运作”需要更强的标准：

1. 没有中间 artifact 时，round 仍能继续推进。
2. 关键 phase-2 对象可以 item-level 查询，而不是只存整包 snapshot。
3. reporting 与 publication 应默认从 DB 重新物化，而不是依赖历史 handoff 文件。

这一标准现在已经在 reporting/publication、phase-2 investigation 中间态、phase-2 control surface、formal/public/environment signal plane typed 化，以及 controversy 主结构链和 issue typed layer 上基本达到；剩余缺口主要转移到 orchestration plan / agent entry / post-round / benchmark 的导出约定，以及旧 empirical 主链默认方向上。

## 6. 下一阶段的目标架构

下一阶段不是继续补更多 surface，而是同时修正研究方向和架构方向。

### 6.1 最小 runtime kernel

目标中的 `runtime kernel` 只保留以下职责：

1. run / round 生命周期管理
2. admission、capability、side-effect governance
3. 执行调度、receipt、ledger、replay
4. DB persistence 与 query surface
5. operator-visible health 与审计入口

以下内容不应继续属于 kernel 本体：

1. 具体领域 stage 语义
2. 争议判断公式
3. board posture 评分逻辑
4. 过多的 phase-specific handoff 约定

这些内容应下沉到 `council policy / domain workflow / typed plane objects`。

### 6.2 更高自主的议会回路

目标不是无治理放权，而是把“实质判断”从 runtime 规则链移回 agent council。

下一阶段的议会回路应满足：

1. agent 在共享 DB 状态上形成 `proposal / challenge / task / probe / readiness opinion`。
2. 每个 proposal 都带有 `rationale / confidence / evidence refs / provenance`。
3. runtime 负责治理和执行，不负责替议会做实质判断。
4. 规则化 heuristic 只在 agent 缺席、输入不足或审计模式下作为 fallback。

### 6.3 一等领域对象与数据契约

下一阶段的 canonical 对象应明确分层：

1. `signal plane`
   - `public-discourse-signal`
   - `formal-comment-signal`
   - `environment-observation-signal`
2. `analysis plane`
   - `issue-cluster`
   - `stance-group`
   - `concern-facet`
   - `actor-profile`
   - `evidence-citation-type`
   - `verifiability-assessment`
   - `verification-route`
   - `formal-public-link`
   - `representation-gap`
   - `diffusion-edge`
3. `deliberation plane`
   - `hypothesis`
   - `challenge`
   - `board-task`
   - `next-action`
   - `probe`
   - `readiness-assessment`
   - `promotion-basis`
4. `runtime / control plane`
   - `promotion-freeze`
   - `controller-state`
   - `gate-state`
   - `supervisor-state`

这里的关键是：议会关键对象不能只存在于导出物里，必须进入可查询的 plane。

### 6.4 数据库是真实状态源

下一阶段的硬方向应是：

1. 所有 council-critical 对象先写 DB，再导出 artifact。
2. artifact 缺失时，系统仍能从 DB-only 状态恢复同等语义。
3. JSON / Markdown 是 export，不再是 phase-2 控制流程的隐性依赖。

### 6.5 Verification lane 降为可选支路

环境观测链应保留，但必须从默认主链降为 optional lane。

只有在以下条件同时满足时才进入 observation matching：

1. 问题本质上是经验性的。
2. 有明确时间范围。
3. 有明确地点范围。
4. 存在可对照的外部观测源。

程序争议、代表性缺口、价值冲突、信任问题和 formal/public 错位，默认不应被硬塞进 observation coverage 流程。

## 7. 下一阶段优先级

下一阶段的优先级必须是：

1. 先写清目标架构、边界与验收标准。
2. 先硬化 canonical contract，再重写主分析链。
3. 先让议会对象 DB-native，再去改 reporting 和 publication。
4. 先提升 agent 在议会中的实质作用，再考虑更复杂的外层 runtime 包装。

不应继续优先的事项：

1. 再扩一批数据源。
2. 继续堆 publication 样式。
3. 仅靠改名或加字段来掩盖旧规则链。
4. 继续把 kernel 当成 domain workflow 的默认承载体。

## 8. 完成定义

当以下条件同时满足时，才能说 OpenClaw 方向真正被纠正：

1. 至少一轮议会可以依靠 agent 形成 `next-action / probe / readiness` 提案，而不是只靠固定公式输出。
2. 删除中间 `next_actions / probes / readiness / board_summary / board_brief` artifact 后，round 仍能从 DB-only 状态继续推进。
3. formal comments 不再只是 generic public signal，而是能生成结构化的争议对象。
4. heuristic 在主链里降为 fallback，并且每次触发都有可审计标记。
5. runtime kernel 的职责边界被明确收紧，domain workflow 不再继续向 kernel 内部膨胀。

## 9. 文档分工

从现在开始，`docs/` 根目录只保留三类 active 文档：

1. `openclaw-project-overview.md`
   - 当前状态、问题与目标架构。
2. `openclaw-next-phase-development-plan.md`
   - 下一阶段的工作流、架构与验收计划。
3. `openclaw-skill-refactor-checklist.md`
   - 迁移、替换、降级与清债清单。

历史路线、进度与里程碑包继续留在 `archive/`，但不再作为当前方向定义的主入口。
