# OpenClaw 项目总体说明、开发进展与未来展望

## 1. 项目总体定位

OpenClaw 当前项目的目标，可以概括为：

`构建一个面向生态议题调查的、可治理的 DB-first 多 agent 运行系统。`

这里有三个关键词：

1. `生态议题调查`
   - 关注公共叙事、政策讨论、环境观测、历史案例与证据之间的联动。
2. `多 agent`
   - 不把所有判断集中在一个总控 summary 上，而是引入分工、反驳与协调。
3. `可治理`
   - 强调 runtime 的审批、账本、回放、归档、发布边界，避免“智能性越强，工程可控性越弱”。

## 2. 项目本身有什么作用

从研究和工程两个层面看，项目的作用都比较明确。

### 2.1 研究层面的作用

它为“多角色协同调查系统”提供了一个可以持续演化的实验平台，能够支持以下问题：

1. 不同角色如何围绕同一证据库形成互补判断。
2. 如何把“反驳与证伪”纳入 agent 协作，而不是只做单向摘要。
3. 如何让 moderator 负责状态推进，而不是让 controller 写死一切。
4. 如何在 agent 系统中同时保留灵活性和治理边界。

### 2.2 工程层面的作用

它把若干常见但分离的问题串了起来：

1. 多源数据抓取与规范化。
2. 结构化证据分析。
3. board 式 deliberation。
4. 报告生成与正式发布。
5. 归档、回放与 benchmark。

因此，这个项目并不只是“自动写报告”，而是一个调查型 AI 系统的运行骨架。

## 3. 为什么当前路线有意义

当前蓝图明确指出：过去限制 OpenClaw 价值的关键，不是技能数量不够，而是上层流程太早替 agent 做决定。

具体来说，旧模式的问题包括：

1. source ingress 过于冻结，难以支持调查中途调整策略。
2. controller 过度绑定固定 stage、固定 skill 和固定顺序。
3. 分析链过早压缩为少数中间结论，导致早期偏差向上传播。
4. sociologist / environmentalist / challenger / moderator 还只是标签，不是真正的协作单元。

因此，当前路线的意义在于把项目从“线性工件流水线”转成“围绕共享状态进行调查的系统”。

## 4. 当前开发进展

截至 `2026-04-06`，项目控制面显示：

1. 总阶段完成度为 `20 / 25`。
2. `Route B Deliberation Plane / Moderator Loop` 已 `8 / 8` 完成。
3. `Route C Analysis Plane / DB-First Analysis` 已 `5 / 7` 完成。
4. `Route A Runtime / Governance Stabilization` 已 `4 / 6` 完成。
5. `Route D Program Control / Documentation` 已 `3 / 4` 完成。

### 4.1 已完成的关键里程碑

当前最值得强调的里程碑有六类：

1. `Deliberation Plane Bootstrap`
   - board、hypothesis、challenge、task、round transition 已有结构化数据库状态面。
2. `Board Write-Path Migration`
   - board 核心写路径已从 JSON-first 改为 DB-first。
3. `Board Export Demotion`
   - `board_summary` 与 `board_brief` 已降级为 derived exports。
4. `Coverage / Scope / Link / Candidate` 的 analysis-plane-first 迁移
   - 关键分析对象已可在缺失 JSON 导出时继续运行。
5. `Generic Result-Set Lineage Contract`
   - 分析对象已具备 query basis、parent ids、artifact refs 等 lineage 语义。
6. `Moderator Control Consolidation`
   - gate、controller、supervisor、next_actions、probes、round_tasks 已具备 DB-backed 恢复能力。

### 4.2 一个关键判断

可以明确说：

`项目已经跨过“只有蓝图没有工作面”的阶段，进入“关键工作面已经落地、下一步转向治理硬化和接口正规化”的阶段。`

## 5. 当前仍然存在的限制

虽然进展明显，但目前还不能把系统描述为“完全体”。

当前仍然存在的主要限制包括：

1. 多 agent 会话化和独立工作记忆仍未完全接入主运行链。
2. analysis plane 还缺正式的非 Python query surface。
3. cluster / merge family 仍未全部纳入统一 result-set。
4. moderator snapshot 目前更偏“最新快照恢复”，而不是完整对象历史。
5. `A3` 治理回归硬化尚未完成，需要进一步验证 replay、benchmark、archive、close-round 等治理链。

## 6. 未来展望

根据当前 master plan，未来几次开发的推荐顺序是：

1. `A3 Governance Regression Hardening`
2. `C2.1 Candidate / Cluster Result Migration`
3. `C2.2 Non-Python Query Surface`
4. `D4 Milestone / Demo Packaging`
5. `A4 Agent Entry Gate`

### 6.1 A3 的意义

`A3` 不是锦上添花，而是保证当前 DB-first 改造没有破坏 runtime 作为治理底座的可靠性。

### 6.2 C2.1 / C2.2 的意义

这两步会决定 analysis plane 能否真正从“过渡性 SQLite 帮手”升级为更完整的分析对象工作面。

### 6.3 D4 的意义

`D4` 会把当前已有成果整理成固定的里程碑包，便于中期汇报、阶段验收和演示复用。

### 6.4 A4 的意义

`A4 Agent Entry Gate` 才是把“DB-first agent mode”真正接入 operator 可见运行链的关键入口。

## 7. 适合汇报时的总叙事

建议把本项目的汇报主线压缩成下面四段：

1. 问题定义
   - 传统线性工件链会过早压缩信息、难以恢复状态，也难以支撑多角色调查。
2. 我们做了什么
   - 把 evidence、analysis、deliberation、export 四个平面逐步 DB-first 化，同时保留 runtime 治理能力。
3. 目前做到哪
   - moderator loop 的关键 DB-first 迁移已完成，analysis plane 已成型，项目整体完成度达到 `20 / 25`。
4. 下一步往哪走
   - 先做治理回归硬化，再补齐 analysis 对象迁移和正式 query surface，最后再把 agent entry gate 接通。

## 8. 可以直接用于汇报收尾的一段话

如果需要一句相对完整的总结，可以直接使用：

“OpenClaw 当前的核心进展，不是又新增了多少抓取或分析技能，而是已经把调查系统的关键工作面从线性 JSON 工件链迁移到 DB-first 的 evidence、analysis 和 deliberation 平面。这样一来，系统开始具备可恢复、可追踪、可审计、可协作的能力。下一阶段我们会优先完成治理回归硬化和分析查询面的正式化，为真正的多 agent 调查入口打基础。” 

