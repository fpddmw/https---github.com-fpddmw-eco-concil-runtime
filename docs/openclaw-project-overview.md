# OpenClaw 项目总览、当前判断与下一阶段方向

## 1. 文档定位

本文件用于替代原先分散的：

1. `01-overall-workflow-report.md`
2. `02-agent-roles-report.md`
3. `03-data-layer-conventions-report.md`
4. `05-skills-summary-report.md`

它现在是本目录唯一的主说明文档，承担三项职责：

1. 说明 OpenClaw 当前到底已经做成了什么。
2. 明确当前系统的真实强项、真实限制和研究边界。
3. 为下一阶段开发规划提供统一基线。

后续如果继续推进，不再为“总体介绍”重复拆散说明，而是在本文件基础上增补少量执行型规划文件。

## 2. 一句话定位

OpenClaw 当前更准确的定位是：

`一个面向生态议题调查的、可治理的 DB-first 多 agent 运行系统。`

但如果进入下一阶段，项目的主问题不应再抽象地停留在“多 agent 调查平台”，而应进一步收束为：

`一个面向环境争议的“争议地图 + 调查分诊”系统。`

这意味着下一阶段的重点不是继续证明“系统可以跑完整流程”，而是回答：

1. 系统能否把正式评论与开放平台舆情组织成结构化的环境争议图谱。
2. 系统能否据此判断哪些内容值得继续核实、回应或升级调查。
3. 系统能否区分“可外部核实的问题”和“更适合做立场/关切/代表性分析的问题”。

## 3. 当前项目已经完成什么

截至当前仓库状态，OpenClaw 已经完成本轮 DB-first 主计划收口。此前文档中的总阶段完成度为 `25 / 25`，`Route A / B / C / D` 四条路线都已完成。

当前已经落下来的核心成果，不是单个 skill 的数量，而是四个工作面：

1. `runtime / governance`
   - 已能管理 run、round、source governance、admission、execution、receipt、replay 与 milestone packaging。
2. `signal plane`
   - 已能把 public 与 environment 两类异构数据写入统一的 `normalized_signals` 工作面。
3. `analysis plane`
   - 已能把 candidate、cluster、scope、link、coverage 等分析对象写入统一 result-set / lineage 结构。
4. `deliberation plane`
   - 已能把 hypothesis、challenge、task、probe、round transition 等议会状态写入 DB-first 状态面。

因此，OpenClaw 当前已经不是“抓数据再写报告”的脚本集合，而是一个能围绕共享状态持续推进调查的系统骨架。

## 4. 当前系统的端到端结构

### 4.1 工作流主干

当前主干可以概括为：

`mission / run -> round / source governance -> fetch -> normalize -> query -> analysis -> board -> reporting / archive`

对应到系统内部，大致分为：

1. run 初始化与 round 脚手架
2. 受治理的数据抓取与导入
3. 统一信号写入与检索
4. 分析结果集生成与 lineage 追踪
5. board 状态推进与 moderator 协调
6. readiness、promotion、reporting、publication
7. archive、history、benchmark、replay

### 4.2 数据层级

当前数据主干仍可概括为：

`raw -> normalized -> analytics -> board -> reporting -> archive/history`

但更重要的变化是：

1. 文件继续保留，主要承担 raw evidence、snapshot、handoff、human-readable export 的作用。
2. SQLite 已成为统一查询、恢复和追踪的主工作面。
3. JSON / Markdown 正在从“状态源”降级为“导出物”。

### 4.3 当前 analysis plane 的默认对象

当前 analysis plane 已稳定支持的对象主要包括：

1. `claim-candidate`
2. `claim-cluster`
3. `observation-candidate`
4. `merged-observation`
5. `claim-scope`
6. `observation-scope`
7. `claim-observation-link`
8. `evidence-coverage`

这套对象说明了一个重要事实：

`当前系统默认把环境调查理解为“公众说法 - 环境观测 - 链接 - 覆盖度”的分析过程。`

这也是下一阶段最需要调整的地方。

## 5. 角色与 skill 结构

### 5.1 角色边界

当前角色设计仍然成立，而且应继续保留：

1. `sociologist`
   - 负责公共叙事、平台讨论、正式评论等公共表达侧分析。
2. `environmentalist`
   - 负责环境观测、指标、时空背景和物理证据侧分析。
3. `challenger`
   - 负责反驳、挑错、施压、证伪和矛盾检查。
4. `moderator`
   - 负责 board 状态推进、调查协调和轮次判断。
5. `runtime`
   - 负责 admission、side effect、ledger、publication、archive 等治理边界。

当前真正已经实现的，不是完整持久的多 session agent 社会，而是：

1. 角色语义明确。
2. 部分写边界和状态对象明确。
3. runtime 已有最小可控 agent entry gate。
4. moderator 与 board 的核心控制面已 DB-first 化。

### 5.2 当前 skill surface

当前仓库共有 `73` 个 skill 目录，可压缩为七类：

| 类别 | 数量 | 当前作用 |
| --- | ---: | --- |
| Runtime 编排 | `5` | 初始化 run、round、fetch/import、plan |
| 数据抓取 | `16` | 拉取 public / environment 原始证据 |
| 数据归一化 | `16` | 将异构数据写入统一信号层 |
| 检索回溯与归档 | `9` | query、lookup、history、archive |
| 分析与证据加工 | `10` | 候选抽取、聚类、合并、scope、link、coverage |
| 议会状态与看板 | `10` | notes、tasks、challenges、probes、readiness |
| 报告与发布 | `7` | handoff、decision、report、publication |

需要强调的是：

`当前 skill 数量已经足够多，下一阶段不应再以“继续补 skill surface”为主，而应转向“重定义核心分析对象和主问题”。`

## 6. 当前系统的真实强项

基于仓库现状，OpenClaw 当前最强的是以下几项：

1. 多源数据接入与统一归一化
   - public source 和 environment source 都已有较完整抓取与 normalizer。
2. DB-first 调查编排
   - round、board、analysis、reporting 已有相对完整的状态面与恢复面。
3. 证据回查与 lineage
   - 从结果回到原始 artifact 的路径比较清楚。
4. 事件式核实
   - 对烟雾、空气质量、洪水、降水、火点等“可观测、可对照”的 case，系统已经具备较强的调查骨架。

如果要给当前系统一个最真实的能力判断，可以表述为：

`OpenClaw 现在已经像一个“可治理的事件调查与核实系统”，还不是一个成熟的环境争议分析系统。`

## 7. 当前系统的主要限制

### 7.1 研究问题仍偏“事件核实”

当前 benchmark 和主分析链，仍然把问题默认理解成：

`公众说法能否被物理观测支持或反驳。`

这对于烟雾、火灾、洪水、污染事件有用，但不足以支撑更广义的环境争议研究。

### 7.2 public-side 分析过度依赖启发式

当前 public claim 抽取、聚类、scope 推导和 claim-observation 匹配都偏强规则、轻语义。这使系统更像一个“规则驱动的核实流水线”，而不是一个能处理复杂环境争议的分析框架。

### 7.3 formal comments 仍只是 generic public signals

`regulations.gov` 评论已经能进系统，但目前主要被当作普通文本信号写入统一表面，还没有被结构化为：

1. 立场
2. 关切面
3. 主体类型
4. 引证类型
5. 程序性与经验性问题的区别

### 7.4 board / readiness / reporting 继承了旧主线

因为 analysis 的主对象仍然是 `claim / observation / link / coverage`，所以 next actions、probe、readiness、promotion、reporting 也自然围绕“补证据覆盖度”来组织，而不是围绕“争议结构”来组织。

## 8. 下一阶段的问题导向

下一阶段最需要解决的不是“系统还能接多少源”，而是：

`OpenClaw 在环境领域究竟要解决什么问题。`

当前最合适的收束方向是：

`环境争议地图 / 调查分诊`

这一定义在环境领域是有明确用途的，至少适用于：

1. 环境政策征求意见与正式评论分析
2. 污染投诉、环境谣言与突发事件舆情分诊
3. 设施选址、治理方案、地方环境冲突等争议场景
4. 正式政策反馈与开放平台讨论之间的错位分析

围绕这个方向，系统要回答的核心问题应改写为：

1. 当前争议围绕哪些 issue cluster 展开。
2. 各平台与正式评论中的主要立场是什么。
3. 各立场的核心关切面是什么。
4. 哪些主体在发声，哪些主体缺位。
5. 哪些说法在跨平台扩散，哪些只是局部表达。
6. 哪些内容适合继续做外部核实，哪些本质上是程序、价值、信任或代表性问题。

## 9. 下一阶段的设计原则

### 9.1 核心对象需要改写

下一阶段默认对象不应再只是：

`claim / observation / link / coverage`

而应逐步转向：

1. `issue cluster`
2. `stance group`
3. `concern facet`
4. `actor profile`
5. `evidence citation type`
6. `diffusion edge`
7. `verifiability flag`
8. `optional verification task`

### 9.2 “物理与舆情匹配”应从核心降为支路

这一能力不是不要，而是不能再当成通用主线。

它应只在以下条件下触发：

1. 说法是经验性的
2. 说法有时间范围
3. 说法有地点范围
4. 外部数据源确实可提供可对照观测

对于下列问题，它不应是默认路径：

1. 立场冲突
2. 程序正义争议
3. 政策解释分歧
4. 信任与代表性问题
5. 社区关切与公共情绪结构

### 9.3 先改 public-side 主链，再改 reporting

下一阶段最优先的不是继续扩张 reporting 模板，而是先把 public analysis 主链重构为：

1. 争议议题抽取
2. 立场抽取
3. 关切抽取
4. 主体识别
5. formal-public linkage
6. diffusion detection
7. verifiability routing

reporting 和 publication 应放在第二阶段之后再调整。

## 10. 汇报时建议使用的主叙事

如果需要用最少的文字讲清当前项目及下一步，可以使用下面四句话：

1. OpenClaw 当前最重要的成果，不是又新增了多少技能，而是已经把调查系统的关键工作面迁到了 DB-first 的 runtime、analysis 和 deliberation 平面。
2. 当前系统最强的是受治理的数据接入、状态推进和事件式核实，而不是一般性的政策评估或成熟的舆情理解。
3. 因此，下一阶段不应再泛泛谈“多 agent 平台”，而应把问题收束为环境争议地图与调查分诊。
4. 在这个方向下，物理证据匹配保留为可选核实支路，而立场、关切、主体、扩散和可核实性判断将成为新的主分析对象。

## 11. 本目录后续文档分工

从现在开始，本目录只保留三类文件：

1. 本文件
   - 作为唯一的主说明文档。
2. `openclaw-next-phase-development-plan.md`
   - 作为下一阶段的分批开发规划。
3. `openclaw-skill-refactor-checklist.md`
   - 作为逐项 skill 改造与新增清单。

这意味着本目录不再继续扩散“并列介绍性文档”，而转向：

`一个主文档 + 少量执行清单`
