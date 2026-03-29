# OpenClaw 当前项目进展说明（教师汇报版）

## 1. 结论先行

当前仓库已经完成了 skill-first 数据主链、board working state 和最小 runtime kernel，但活跃执行流程仍然主要是 runtime/controller 顺序调用 skill。它还不是理想中的 OpenClaw 多 agent 自主协作系统。

因此，下一阶段的第一优先级不应再是继续把调查流程硬编码进 runtime，也不应先做更重的生产控制面，而是先把当前仓库与 /home/fpddmw/projects/skills 仓库中的 skill 一起接入 OpenClaw，让多个 role agent 能在统一 board 和 signal plane 上自主调用 skill、协同推进调查。

## 2. 当前代码流程审查结论

### 2.1 当前活跃流程的真实形态

从活跃代码看，当前系统的主执行路径仍然是：

runtime CLI -> controller/supervisor -> run_skill() -> 本地 Python skill 脚本

几个关键事实如下：

1. run-phase2-round 先运行 eco-plan-round-orchestration，生成 orchestration plan。
2. controller 读取这个 plan 后，按 execution_queue 顺序循环调用 run_skill()。
3. run_skill() 再通过本地 subprocess 执行 skill 脚本，并写 receipt、ledger、manifest、cursor。
4. supervise-round 只是在 controller 结果上再做一层 operator 摘要，并没有把下一步决策权交给 agent。

也就是说，当前系统虽然已经是 skill-first，但工作流仍然主要由 runtime/controller 串行推进。planner 只是把原本硬编码的固定队列外显成 plan artifact，还没有把“下一步做什么、由谁做、调用哪些 skill”的主导权真正交给 OpenClaw agent。

### 2.2 当前系统还不是 OpenClaw agent-native

当前仓库里虽然已经存在：

- moderator、sociologist、environmentalist、challenger、archivist 等 role 语义
- board working state
- query / lookup / board / investigation / reporting skills
- agents/openai.yaml 元数据

但这些还主要停留在：

- skill metadata
- board 字段
- artifact 约定
- planner artifact 的 assigned_role_hint

活跃 runtime 里目前没有真正启用的：

- OpenClaw agent provisioning
- OpenClaw workspace/session adapter
- managed skill projection 或安装逻辑
- role-agent turn loop
- agent 自主调用 skill 的统一执行面

因此，你的判断是对的：现在的代码仍然更接近“runtime 逐项调 skill，按规则推进议会”，而不是“多个 OpenClaw agent 围绕 board 自主调用 skill 进行调查”。

## 3. 为什么下一阶段必须改成 OpenClaw agent-first

### 3.1 这本来就是蓝图要求

当前顶层蓝图已经明确写了三件事：

1. OpenClaw agent 应当是主调查者。
2. runtime 只保留不变式，不应继续承担主要业务推理。
3. 议会流程应该是轻编排加上强 gate，而不是固定 JSON 表单链或固定队列链。

因此，如果接下来继续优先扩 runtime controller 的规则和顺序队列，方向会重新偏回“runtime 替 agent 做决定”，这与蓝图冲突。

### 3.2 OpenClaw 官方文档支持这个方向

OpenClaw 官方文档强调了两点：

1. Gateway 是 sessions、routing 与 channel connections 的单一事实源。
2. OpenClaw 原生支持 multi-agent routing，并且可以按 agent、workspace、sender 做隔离会话。

这意味着对当前项目最合适的接法不是把 OpenClaw 当成一个外层聊天壳，而是把它真正作为：

- 多 agent 协作入口
- skill 调用入口
- role workspace/session 隔离层
- board 协作的人机交互面

换句话说，OpenClaw 的优势只有在“agent 真正拥有 skill 调用权”时才能发挥出来；如果 skill 仍主要由 runtime/controller 串行执行，那么 OpenClaw 只会沦为一个外接 UI，而不是控制平面。

### 3.3 现有两个 skill 仓库都已经具备接入基础

当前两个 skill 面都已经具备最关键的最小接入条件：

1. 当前仓库 skills/ 已经有规范化的 SKILL.md 和 agents/openai.yaml。
2. /home/fpddmw/projects/skills 仓库中的 atomic fetch skills 也已经有同样的 skill 包装结构。
3. 活跃 runtime 的 registry 已经能读取 SKILL.md 与 agents/openai.yaml 元数据。

所以，下一阶段的核心问题已经不是“还要不要再做更多 skill”，而是：

- 如何把两个仓库的 skill 暴露到同一个 OpenClaw skill surface
- 如何给多个 role agent 提供一致的可调用 skill 集
- 如何让 agent 围绕 board、query、fetch、challenge 自主推进调查

## 4. 新的目标工作模式

理想工作模式应明确为：

1. moderator 负责定义 mission、预算、主要假说、当前 round 目标。
2. archivist 调用 archive/history 相关 skill，为当前 round 补历史上下文。
3. sociologist、environmentalist、challenger 读取 board delta，自主决定需要调用哪些 query、lookup、fetch、normalize、extract、board skill。
4. agent 需要新数据时，直接调用已接入 OpenClaw 的 fetch skills，而不是等待 runtime 事先固定 source queue。
5. agent 通过 board skill 写回 note、hypothesis、challenge、task、refs、operator summary。
6. moderator 基于 board 状态和 receipts 判断是继续 probe，还是进入 promotion 和 reporting。
7. runtime 只负责权限、执行封装、artifact contract、ledger、snapshot、promotion gate，而不是主导调查路径。

用一句话概括就是：

OpenClaw role agents -> shared skill surface -> signal plane / board -> runtime governance

而不是：

runtime/controller -> fixed queue -> local scripts -> agents only as labels

## 5. 方向修正后的优先级

后续优先级应调整为：

1. OpenClaw skill integration
2. multi-agent workspace/session provisioning
3. board-driven agent turn loop
4. detached fetch skill integration into agent surface
5. reporting/promotion handoff in agent mode
6. benchmark / simulation
7. 更高阶的可控性、可审计性与生产化准入

这意味着：先把 agent 基本工作框架搭起来，再去补更重的治理控制面；而不是先把过渡态 runtime 加固得越来越复杂。

## 6. 详细全流程工作计划

### 阶段 0：统一口径并冻结旧方向

目标：把“runtime 顺序驱动”明确降级为过渡态，避免继续沿错误方向堆代码。

交付内容：

1. 统一文档口径，明确当前活跃流程仍是 runtime 串行 skill，不是最终形态。
2. 明确下一阶段的第一优先级是 OpenClaw skill integration 与 multi-agent framework。
3. 冻结“继续把更多调查判断塞回 controller/supervisor”的方向。

验收标准：

1. 顶层文档不再把 planner-backed controller 说成最终目标。
2. 顶层计划文档明确写出“先 agent-native，再更强治理”。

### 阶段 1：统一 skill surface

目标：让当前仓库与 /home/fpddmw/projects/skills 仓库里的 skill 一起成为 OpenClaw 可见、可调用的统一 skill 面。

交付内容：

1. 盘点两仓 skill，建立统一索引与分类：fetch、normalize、query、board、investigate、archive、report。
2. 设计 skill projection 或 install 方案，把两仓 skill 投影到 OpenClaw 可识别的 managed skill surface。
3. 支持显式 skills_root 配置，不再假设 runtime 与 detached skills 仓库必须同目录共存。
4. 禁止依赖脆弱的人工复制流程，优先采用可重建、可刷新、可校验的投影或安装机制。

建议实现位点：

- eco-concil-runtime/src/eco_council_runtime/adapters/openclaw/managed_skills.py
- eco-concil-runtime/src/eco_council_runtime/adapters/openclaw/provisioning.py

验收标准：

1. OpenClaw 能看到当前仓库技能。
2. OpenClaw 能看到 detached fetch skills。
3. skills 变更后可以一键刷新到 OpenClaw skill surface。

### 阶段 2：Provision 多 role OpenClaw agent

目标：给 moderator、sociologist、environmentalist、challenger、archivist 提供独立 workspace 和 session。

交付内容：

1. 为每个 role 生成隔离 workspace。
2. 为每个 role 写入固定身份说明、board 使用规则、可调用 skill 范围。
3. 为每个 role 注入同一套 managed skills，但允许有角色级默认提示与偏好。
4. 设计 run 级与 round 级 session 目录结构，确保 board 和 artifacts 可被多个 agent 共享，但身份上下文隔离。

建议实现位点：

- adapters/openclaw/workspaces.py
- adapters/openclaw/turns.py
- adapters/openclaw/prompts.py

验收标准：

1. 能一键 provision 五个基础 role agent。
2. 每个 agent 都能在自己的 OpenClaw 会话里看到统一 skill 面。
3. 每个 agent 都有最小身份与权限说明文件。

### 阶段 3：建立 board-driven agent turn loop

目标：让 agent 围绕 board 自主决定下一步，而不是由 controller 事先规定完整 skill 队列。

交付内容：

1. moderator 开 round，写 mission 与初始 hypothesis。
2. archivist 读取 history context skill，写回历史对照 note。
3. sociologist 与 environmentalist 读取 board delta，自主调用 query、lookup、fetch、normalize、extract skill。
4. challenger 自主创建 falsification 与 contradiction 路径。
5. moderator 根据 board 状态选择继续 probe、冻结 promotion 或进入 reporting。

关键设计要求：

1. runtime 不再提前决定完整 source queue。
2. eco-plan-round-orchestration 可以保留，但应退化为建议器，而不是强制队列控制器。
3. board delta 应成为多 agent 协作的中心对象。

验收标准：

1. 至少一个 round 可以由多个 OpenClaw agent 接力完成。
2. query、board、challenge、archive skill 不再主要通过 runtime 串行 CLI 调度。
3. round 内的下一步 skill 选择主要来自 agent 自主调用记录。

### 阶段 4：把 detached fetch skills 接进 agent 主链

目标：让外部采集不再停留在“prepare + local import”，而是成为 agent 可直接调用的技能层。

交付内容：

1. 将 /home/fpddmw/projects/skills 中的 fetch skills 暴露给 OpenClaw agent。
2. agent 可根据 board 证据缺口，自主选择 fetch family、source、region、time window。
3. fetch 返回的 raw artifact 自动进入当前 run 的 raw store。
4. normalize skills 自动或半自动接续，把数据写入 signal plane。

注意事项：

1. 这一阶段优先做“可用的 agent 数据采集闭环”，不是先做最重的远程执行治理。
2. 复杂 credential、network approval、rate limit guard 可以先做最小可用版，再逐步加严。

验收标准：

1. agent 可以直接调用至少两类 detached fetch skills。
2. 新 fetch 结果可在同一轮中继续被 normalize、query、extract skill 消费。
3. 不再需要事先人工准备 fixture 才能验证外部数据采集主链。

### 阶段 5：改造 reporting 与 promotion 为 agent-native handoff

目标：让 reporting 与 decision 仍保持 skill-first，但触发时机由 moderator 和 board 状态决定。

交付内容：

1. 保留 eco-materialize-reporting-handoff、eco-draft-expert-report、eco-draft-council-decision 等 skill。
2. 这些 skill 的调用时机不再主要由 runtime/controller 固定推进，而由 moderator agent 基于 board 判断发起。
3. supervisor 可以保留为 operator summary，但不再扮演主编排者。

验收标准：

1. promotion 与 reporting skill 仍然可审计。
2. 触发这些 skill 的主体已经从 controller 转向 moderator 或 role agents。

### 阶段 6：补 benchmark 与历史复用

目标：在 agent-native 框架成型后，再验证它是否真正优于当前 runtime 队列式流程。

交付内容：

1. 建立至少两类真实任务 benchmark。
2. 对比“runtime 队列式流程”和“agent-native 流程”的证据覆盖、challenge 质量、round 用时。
3. 继续扩 history context 复用与跨轮次对照。

验收标准：

1. benchmark 不只是单元测试，而是完整多 agent 回放。
2. 可以量化 agent-native workflow 的收益和缺陷。

### 阶段 7：最后再补更强的可控性与可审计性

目标：在 agent 基础框架跑通以后，再系统补强更重的治理控制面。

此阶段再处理：

1. 更强的 permission boundary
2. OS-level sandbox
3. operator approval、rollback、manual recovery
4. richer observability
5. shadow test 与 pilot runbook

原因很简单：如果 agent-native 基本框架都还没搭起来，就过早把大量精力投入到最终控制面，只会把一个过渡态 runtime 加固得更复杂，而不是更接近最终目标。

## 7. 当前阶段的标准表述

当前系统已经完成了 skill-first 数据主链、board working state 和最小 runtime kernel，但活跃执行流程仍然主要是 runtime/controller 串行调用 skill，还没有完全进入 OpenClaw 多 agent 自主协作阶段。下一阶段的核心任务不是继续加重 runtime 规则，而是优先把当前仓库和 detached skills 仓库中的 skill 一起接入 OpenClaw，建立 moderator、sociologist、environmentalist、challenger、archivist 的多 agent 协作基础框架；在这个基础框架稳定后，再继续补更强的任务可控性、审计性和生产化准入。
