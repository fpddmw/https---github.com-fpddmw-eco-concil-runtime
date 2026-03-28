# OpenClaw-First 重构蓝图

## 文档定位

本蓝图用于定义 `eco-council-runtime` 的后续主线重构方向。

目标不是在现有“监督器 + packet/prompt + JSON 表单填写”结构上继续打补丁，而是明确转向一个真正以 OpenClaw 多智能体协作为核心的调查型运行时。

本蓝图回答四个问题：

1. 我们到底要什么。
2. 当前结构为什么不满足这个目标。
3. 最终理想形态应长成什么样。
4. 后续重构应如何分阶段推进。

---

## 1. 核心判断

当前系统的主要问题，不是“没有多角色”，而是“多角色没有真正工作”。

现状中，OpenClaw 更多被当作：

- 表单填写器
- 受限 JSON 产出器
- 大 packet 的被动消费者

而不是：

- 会主动调用 skill 的调查者
- 会并行分工的协作者
- 会提出挑战和反证的竞争性智能体

如果继续沿当前模式扩展，结果只会是：

- 规则越来越多
- packet 越来越大
- prompt 越来越重
- token 花在“搬运和压缩上下文”上，而不是花在“调查和推理”上

因此，后续方向必须调整为：

**让 OpenClaw 成为主调查体，让规则从“替 agent 判断”退回到“限制 agent 行为边界”。**

---

## 2. 我们的需求

## 2.1 硬需求

后续重构必须同时满足以下要求：

1. OpenClaw agent 必须成为主调查执行者，而不是表单填写机器。
2. 议会流程相关控件应尽可能 skill 化，包括任务拆分、证据操作、调查推进、报告汇总和决策整理。
3. 数据应默认进入数据库或可查询工作存储，agent 应通过 skill 查询局部证据，而不是被动接收过度浓缩的 summary。
4. 必须支持多 agent 并行工作，而不是所有判断都串行走单一路径。
5. 必须支持 challenge / falsification / alternative hypothesis 机制，而不仅是补证。
6. 必须保留可审计、可回放、可 promotion 的 canonical artifact 体系。
7. 必须控制 token 消耗，但控制手段应主要来自查询边界、黑板增量和 skill 输出裁剪，而不是过度依赖事前规则压缩。
8. 必须尽可能复用现有数据层、normalize、archive、audit、contract 能力，避免无谓推倒重来。

## 2.2 明确拒绝的状态

以下状态不应再被视为目标架构：

- OpenClaw 只负责填写 `source_selection.json`、`report.json`、`decision.json`
- 主要推理发生在 supervisor 的硬编码规则中
- agent 无法直接操作技能和数据，只能阅读大 packet
- 数据虽然入库，但 agent 主路径仍不使用数据库查询
- 为了控 token，提前把问题压成固定模板，从而牺牲调查自由度

## 2.3 非目标

本次重构不是要做以下事情：

- 取消所有规则
- 允许 agent 无边界地执行任意 shell、任意 SQL、任意文件改写
- 放弃审计和 promotion
- 把 canonical JSON 体系完全删掉

保留边界是必要的，问题在于边界应由内核承担，而不是由大量业务规则替 agent 做决定。

---

## 3. 当前架构的关键缺陷

## 3.1 OpenClaw 被放在了错误的位置

当前主流程中，OpenClaw 的主要职责仍是：

- 读取 packet
- 按 prompt 要求输出 JSON
- 由 supervisor 导入结果

这使它的角色接近“受限表单终端”，而不是“技能驱动的调查智能体”。

## 3.2 规则承担了太多本应由 agent 完成的判断

当前系统中，大量逻辑由本地规则硬编码完成，例如：

- 候选 claim 聚类
- 候选 observation 聚合
- claim-hypothesis 绑定
- 匹配过滤和阈值判断
- 行动排序
- 历史检索排序

这些逻辑可以保留为 baseline，但不应继续作为未来主架构的主要推理载体。

## 3.3 数据虽已入库，但未成为 agent 主工作介质

当前项目已经有：

- per-run analytics SQLite
- case library
- signal corpus

但主工作流依然以：

- JSON 文件
- packet summary
- 预压缩 artifacts

作为 agent 输入。

这意味着数据库更多是“旁路存储”和“归档层”，而不是“调查工作台”。

## 3.4 上下文组织方式浪费 token

当前 token 问题的根源，不是 agent 自由度高，而是上下文组织方式低效：

- 过大的 packet
- 过长的 prompt
- 同一信息跨阶段重复出现
- 原始数据与中间判断反复被搬运

因此，真正的控 token 方案应是：

- 黑板增量
- 数据库局部查询
- skill 结果裁剪
- 引用式上下文

而不是继续把推理前压缩做得越来越重。

---

## 4. 目标架构：最终理想形态

最终理想形态应是一个 **OpenClaw-first investigation runtime**。

它由五层组成。

## 4.1 Runtime Kernel

内核应尽可能小，只负责系统不变式。

内核保留职责：

- agent 身份与权限控制
- token / time / tool budget
- 并发锁
- skill 调用审计
- canonical artifact registry
- promotion gate
- 运行一致性检查

内核不再承担主要业务推理。

## 4.2 Skill Mesh

所有议会流程相关操作应尽量 skill 化。

skill 分为五类：

1. 数据读取 skill
2. 归一化与转换 skill
3. 调查推进 skill
4. 推理辅助 skill
5. promotion 与决策 skill

这些 skill 应被 OpenClaw agent 主动调用，而不是预先写死在 supervisor 阶段机里。

## 4.3 Investigation Board

引入一个持久化调查黑板，作为多 agent 的共同工作台。

黑板不是最终 artifact，而是工作态存储，至少应包含：

- mission card
- hypothesis cards
- evidence refs
- probe tasks
- challenge tickets
- conflicts
- notes
- action queue
- promotion candidates
- round summaries

所有 agent 都围绕黑板协作，而不是各自读一份大 packet 后各自产出孤立 JSON。

## 4.4 Data Fabric

数据层应形成“原始数据 + 可查询工作库 + 归档库 + promotion 产物”四层结构。

1. raw artifacts
2. per-run analytics DB
3. investigation workspace DB
4. archives 和 promoted JSON

agent 默认通过 skill 读取第 2 层和第 3 层，而不是直接把 raw 全量放进上下文。

## 4.5 Promotion Layer

最终报告、证据和决策仍需保留 canonical JSON 和正式文档。

但这些产物不应再由 agent 直接手写成表单，而应由：

- 黑板状态
- skill receipts
- 已批准的 evidence refs
- 已冻结的 summary

promotion 生成。

这样既发挥 agent 主体性，又保留审计性。

---

## 5. 最终项目形态

## 5.1 运行形态

最终系统不应再是“阶段驱动的表单流水线”，而应是“里程碑受控的调查循环”。

推荐运行循环：

1. Orient
2. Probe
3. Challenge
4. Consolidate
5. Deliberate
6. Promote

说明：

- `Orient`：理解 mission、初始化假说、领取任务。
- `Probe`：agent 调用 skill 探查数据、补充证据。
- `Challenge`：challenger 或其他 agent 质疑现有解释，测试替代假说。
- `Consolidate`：把本轮局部发现整理进黑板。
- `Deliberate`：moderator 汇总是否进入 promotion。
- `Promote`：把工作态结果冻结成正式产物。

## 5.2 多 agent 形态

建议保留并扩展固定角色：

- `moderator`
  - 负责议程、优先级、停走判断
- `sociologist`
  - 负责公众叙事、舆论主张、政策文本与评论线索
- `environmentalist`
  - 负责物理观测、环境指标、时空核验
- `challenger`
  - 专门负责反证、替代性假说和挑战现有链路
- `archivist`
  - 负责历史案例检索、对照、迁移经验
- `worker-*`
  - 可选并行执行局部 probe

## 5.3 代码组织形态

最终理想目录建议为：

```text
src/eco_council_runtime/
  domain/
    board/
    evidence/
    investigation/
    mission/
    promotion/
  application/
    kernel/
    skills/
    agents/
    board/
    promotion/
    archive/
  adapters/
    openclaw/
    storage/
    audit/
    fetch/
  cli/
    runtime/
    maintenance/
```

说明：

- `controller/` 应逐步退场，不再作为未来主结构继续扩张。
- `supervisor.py`、`reporting.py`、`orchestrate.py` 等根模块仅保留兼容 facade，或最终退役。

## 5.4 Skill 仓最终形态

所有 agent 可调用的业务能力，应最终从 runtime 主仓中外移为并列的 atomic skills。

参考现有 `/home/fpddmw/projects/skills` 目录的封装方式，后续技能仓应采用：

- 扁平并列目录
- 靠命名规则区分 skill 类型
- 每个 skill 自带最小闭包的脚手架

推荐目录模板：

```text
skills/
  eco-query-public-signals/
    SKILL.md
    scripts/
    assets/
    references/
    agents/
  eco-query-environment-signals/
    SKILL.md
    scripts/
    assets/
    references/
    agents/
  eco-build-evidence-graph/
    SKILL.md
    scripts/
    assets/
    references/
    agents/
```

推荐命名规则：

- `<system>-<verb>-<object>`
- `<system>-<verb>-<qualifier>-<object>`

例如：

- `eco-query-public-signals`
- `eco-query-environment-signals`
- `eco-extract-claim-candidates`
- `eco-derive-claim-scope`
- `eco-build-evidence-graph`
- `eco-compare-hypotheses`
- `eco-promote-round-summary`

原则：

- skill 名称表达能力，不表达角色
- skill 之间尽量并列，不做深层目录嵌套
- skill 是否属于某类，通过名称约定和 metadata 判定，而不是靠父目录分组

需要强调：

**这种扁平 atomic skills 目录应成为“业务能力层”的最终形态，但 runtime 主仓仍需保留一个极小内核。**

---

## 6. Skill 体系设计

## 6.1 Skill 设计原则

每个 skill 必须满足：

- 输入输出 schema 明确
- side effect 可声明
- 权限边界明确
- 可记录 receipt
- 输出可裁剪
- 可被多个 agent 复用

skill 应是“原子且可组合”的，而不是再造一个小 supervisor。

## 6.2 Skill 封装规范

每个 atomic skill 建议都具备以下组成：

- `SKILL.md`
  - 说明 skill 目标、输入、输出、限制、推荐调用模式
- `scripts/`
  - 真实执行逻辑
- `assets/`
  - 环境变量模板、schema、默认配置
- `references/`
  - 外部接口、数据源说明、限制说明、示例
- `agents/`
  - OpenClaw 对该 skill 的调用元数据

可选组成：

- `tests/`
- `examples/`
- `fixtures/`

每个 skill 必须明确四件事：

1. 读什么
2. 写什么
3. 能否产生 side effect
4. 返回结果如何被下游 agent 或 promotion 消费

## 6.3 主体技能组设计

后续不建议按 `moderator / sociologist / environmentalist` 给 skill 分组。

更合理的做法是按“能力面”分组，角色只决定调用权限和默认优先级。

建议的主体技能组如下。

### A. Query Skills

职责：

- 读取 per-run analytics DB
- 读取 workspace DB
- 提供局部裁剪过的证据片段

示例：

- `eco-query-public-signals`
- `eco-query-environment-signals`
- `eco-query-claim-candidates`
- `eco-query-observation-summaries`
- `eco-lookup-raw-record`

### B. Fetch Skills

职责：

- 面向外部信息源执行原子抓取
- 写 raw artifact
- 回传 provenance 和下载摘要

示例：

- `gdelt-doc-search`
- `gdelt-events-fetch`
- `openaq-data-fetch`
- `open-meteo-historical-fetch`

说明：

- 现有 `/home/fpddmw/projects/skills` 中的大多数 source skill 都可直接保留为本组成员。

### C. Normalize Skills

职责：

- 将异构 raw payload 转成 canonical signal
- 将 canonical signal 写入 analytics DB
- 为后续 query 和 evidence 链接提供统一数据面

示例：

- `eco-normalize-public-artifact`
- `eco-normalize-environment-artifact`
- `eco-ingest-public-signals`
- `eco-ingest-environment-signals`

### D. Extraction Skills

职责：

- 从 canonical signals 中生成 claim、observation、scope 等中间调查对象

示例：

- `eco-extract-claim-candidates`
- `eco-extract-observation-candidates`
- `eco-derive-claim-scope`
- `eco-derive-observation-scope`
- `eco-cluster-claims`
- `eco-merge-or-split-candidates`

### E. Evidence Skills

职责：

- 进行 claim/observation 对齐
- 生成 evidence graph、support/contradiction path、isolated/remand

示例：

- `eco-link-claim-to-observations`
- `eco-score-evidence-overlap`
- `eco-build-evidence-graph`
- `eco-materialize-remands`

### F. Investigation Skills

职责：

- 围绕 hypothesis、alternative、challenge、probe 组织调查推进

示例：

- `eco-open-probe`
- `eco-open-challenge`
- `eco-compare-hypotheses`
- `eco-falsification-probe`
- `eco-propose-next-actions`
- `eco-score-action-options`

### G. Board Skills

职责：

- 操作 investigation board
- 管理任务、笔记、黑板卡片、冲突和状态更新

示例：

- `eco-read-board-delta`
- `eco-post-investigation-note`
- `eco-claim-task`
- `eco-update-hypothesis-status`
- `eco-close-probe`

### H. Archive Skills

职责：

- 使用 case library / signal corpus 做横向检索和经验迁移

示例：

- `eco-search-case-library`
- `eco-load-case-bundle`
- `eco-find-similar-patterns`
- `eco-attach-history-note`

### I. Promotion Skills

职责：

- 将工作态结果转成 canonical round artifacts
- 汇总报告和决策基础

示例：

- `eco-promote-claim-candidate`
- `eco-promote-observation-candidate`
- `eco-promote-evidence-chain`
- `eco-draft-report-section`
- `eco-draft-decision-basis`
- `eco-finalize-round-summary`

## 6.4 第一批 skill 分类

### A. 数据读取 skill

- `query_public_signals`
- `query_environment_signals`
- `lookup_signal_by_id`
- `lookup_raw_record`
- `sample_source_rows`
- `query_claim_candidates`
- `query_observation_summaries`

### B. 归一化与转换 skill

- `normalize_raw_artifact`
- `extract_claim_candidates`
- `extract_observation_candidates`
- `derive_claim_scope`
- `derive_observation_scope`
- `cluster_claims`
- `merge_or_split_candidates`

### C. 调查推进 skill

- `claim_task`
- `open_probe`
- `close_probe`
- `open_challenge`
- `request_parallel_probe`
- `post_investigation_note`
- `update_hypothesis_status`

### D. 推理辅助 skill

- `link_claim_to_observations`
- `build_evidence_graph`
- `compare_hypotheses`
- `falsification_probe`
- `propose_next_actions`
- `score_action_options`

### E. 历史与记忆 skill

- `search_case_library`
- `load_case_bundle`
- `find_similar_patterns`
- `attach_history_note`

### F. Promotion skill

- `promote_claim_candidate`
- `promote_observation_candidate`
- `promote_evidence_chain`
- `draft_report_section`
- `draft_decision_basis`
- `finalize_round_summary`

## 6.5 可实现的 agent 工作流

后期理想工作流不应再是“agent 收到大 prompt，填写一个 JSON 表单”，而应是“agent 读取黑板增量，自主调用技能，回写调查状态”。

一个可实现且可落地的 agent 工作流如下。

### Step 1：Moderator 初始化本轮调查板

输入：

- mission
- 上轮 promoted artifacts
- 当前未解决问题

动作：

- 写入 board 的 round card
- 初始化 hypothesis seeds
- 初始化 budget、priority、must-test questions
- 向其他 agent 发出任务配额，而不是发出表单模板

### Step 2：Archivist 拉历史上下文

动作：

- 调用 `eco-search-case-library`
- 调用 `eco-load-case-bundle`
- 回写可借鉴模式、对照案例、历史反证路径

产物：

- history notes
- similar pattern refs
- archived challenge hints

### Step 3：Sociologist 与 Environmentalist 并行探查

Sociologist：

- 调用 query/fetch/normalize/extraction skills
- 形成 public narrative candidates
- 识别需要 challenge 的公众说法

Environmentalist：

- 调用 query/fetch/normalize/evidence skills
- 形成 mission-window observations
- 提供物理支持、矛盾或背景上下文

### Step 4：Challenger 发起反证任务

动作：

- 读取 board 上当前主解释
- 调用 `eco-compare-hypotheses`
- 调用 `eco-falsification-probe`
- 创建 challenge tickets

目标：

- 不让系统只补证
- 强制维持 alternative hypothesis 的竞争

### Step 5：Worker 并行执行局部 probe

若发现局部问题可拆，则由 moderator 或其他 agent 派发 worker probe。

worker 工作模式应非常简单：

- 读取单一任务卡
- 调用少量 skill
- 回传结构化结果和 refs
- 不负责最终综合判断

### Step 6：Consolidate into Board

各 agent 将本轮输出写回黑板：

- notes
- evidence refs
- hypothesis updates
- unresolved questions
- probe results

这一步只更新工作态，不直接写最终 report / decision JSON。

### Step 7：Moderator Deliberation

moderator 负责判断：

- 证据是否足够 promotion
- 哪些 challenge 已解决
- 哪些 alternative 仍活跃
- 是否需要更多 probe

此时 moderator 依赖的是 board + receipts，而不是重新让 agent 填整套表单。

### Step 8：Promotion

如果满足 promotion 条件：

- 调用 promotion skills
- 生成 canonical claims / observations / evidence / reports / decision
- 冻结 snapshot 和 receipts

如果不满足：

- 生成下一轮任务与预算
- 回到 Step 2 或 Step 3

## 6.6 SQL 访问策略

不建议给 agent 开放任意 raw SQL。

建议采用：

- 参数化查询 skill
- 白名单字段
- 结果行数上限
- 时间/空间过滤强约束
- 自动附带 provenance refs
- 查询结果快照化

这样可以兼顾：

- 调查自由度
- token 控制
- 审计能力
- 安全性

---

## 7. 数据与存储设计

## 7.1 数据分层

### 第 1 层：原始数据层

- `raw/` 文件
- sidecar 下载物
- 原始 API/CSV/JSON 载荷

只做保存与引用，不作为主要 prompt 上下文。

### 第 2 层：运行分析层

保留并继续利用现有 per-run analytics DB：

- `public_signals.sqlite`
- `environment_signals.sqlite`

这些数据库继续接收 normalize 后的 canonical signal。

### 第 3 层：调查工作层

新增 `investigation workspace DB`，用于多 agent 工作态。

建议表：

- `board_cards`
- `hypotheses`
- `evidence_nodes`
- `evidence_edges`
- `probe_tasks`
- `challenge_tickets`
- `agent_notes`
- `action_queue`
- `promotion_candidates`
- `skill_receipts`

### 第 4 层：归档与最终产物层

- case library
- signal corpus
- canonical JSON
- final reports
- council decisions

## 7.2 Token 控制策略

token 控制不应主要依赖“提前把问题压扁”，而应依赖以下机制：

- board delta only
- 查询结果裁剪
- 引用式 artifact refs
- 局部 summary cache
- skill 输出长度预算
- agent working memory 压缩

核心原则：

**尽量少搬运文本，多搬运引用和结构。**

---

## 8. 可复用范围

## 8.1 高复用

以下部分应优先复用：

- 各类 source normalize adapter
- per-run analytics SQLite 存储
- `case_library`
- `signal_corpus`
- 审计链与快照机制
- contract/schema 校验
- 路径、IO、缓存、manifest 相关适配层

这些部分已具备工程价值，应作为 v2 基础设施直接保留或平移。

## 8.2 中复用

以下部分可先作为 baseline skill 使用，再逐步弱化硬编码：

- candidate generation
- matching
- evidence materialization
- investigation state
- investigation actions
- history scoring

做法上应是：

- 先 skill 化包装
- 再降低其在主流程中的强制地位
- 最终让 agent 决定何时调用、如何组合

## 8.3 低复用

以下部分不应继续作为主路径：

- packet-heavy source selection
- prompt-heavy report / decision drafting
- 以 `run-agent-step` 为核心的表单导入链
- 过细的 stage-driven agent JSON turn 模式

这些部分可保留为：

- legacy fallback
- benchmark baseline
- 回归验证路径

但不再作为未来主架构中心。

## 8.4 当前代码到 skill 的拆解映射

本节给出一个面向实施的拆解原则：

- 能力强、接口清楚、已可单独运行的模块：优先外移成 atomic skill
- 负责系统不变式的模块：保留在 runtime 内核
- 只服务 packet/prompt 表单主路的模块：降级为 legacy 或放弃主路径

### A. 优先拆成 skill 的部分

这些模块最适合变成 atomic skill 或 skill backend：

- `application/normalize/public_sources.py`
  - 拆为 public normalize / ingest 类 skills
- `application/normalize/environment_sources.py`
  - 拆为 environment normalize / ingest 类 skills
- `application/normalize_candidates.py`
  - 拆为 extraction 类 skills
- `application/normalize_evidence.py`
  - 拆为 evidence 类 skills
- `application/investigation/history_context.py`
  - 拆为 archive / retrieval 类 skills
- `application/investigation/actions.py`
  - 拆为 investigation / action-planning 类 skills
- `application/investigation/state.py`
  - 拆为 board consolidation / state materialization skills

### B. 保留为 runtime 内核的部分

这些模块不应技能化，应保留为系统内核或适配层：

- `controller/audit_chain.py`
  - 保留为 receipt 与 snapshot 内核
- `adapters/normalize_storage.py`
  - 保留为 analytics DB 适配层
- `case_library.py`
  - 保留为 archive store 与 search backend
- `signal_corpus.py`
  - 保留为 archive / corpus backend
- `controller/paths.py`
  - 保留为 canonical artifact 路径规则
- `controller/io.py`
  - 保留为基础 IO 适配
- contract/schema 校验相关能力
  - 保留为 promotion gate

### C. 降级为 legacy 或放弃主路径的部分

这些模块可以暂时保留，但不应继续作为未来主路线扩张：

- `controller/agent_turns.py`
  - 典型表单机调度路径
- `controller/source_selection.py`
  - packet-heavy source selection 主链
- `application/reporting/prompts.py`
  - prompt-heavy 产物驱动
- `application/reporting/packets.py`
  - packet-heavy artifact handoff
- 基于 `run-agent-step` 的 JSON turn 主循环
- 以“当前只允许 agent 产出单个表单文件”为核心假设的逻辑

## 8.5 保留 / skill 化 / 放弃矩阵

| 现有部分 | 后续处理 | 原因 |
| --- | --- | --- |
| source fetch skills 仓 | 直接保留 | 已符合 atomic skill 形态 |
| normalize adapters | 保留实现并 skill 化暴露 | 已有稳定能力边界 |
| analytics SQLite | 直接保留 | 是 agent 查询工作介质的基础 |
| case library / signal corpus | 直接保留 | 已是高价值 archive 基础设施 |
| audit / snapshot / receipt | 直接保留 | 属于内核不变式 |
| candidate / matching / investigation 逻辑 | 先 skill 化，再弱化强制性 | 先复用，再逐步 agent 化 |
| packet/prompt/report form 流 | 降级为 fallback | 不符合 OpenClaw-first |
| stage-driven form orchestration | 放弃主路径地位 | 会持续把 OpenClaw 限定成表单机 |

## 8.6 skill 化拆解顺序

建议按以下顺序拆解现有代码。

### 第一批

- query skills
- archive skills
- board skills

原因：

- 风险最低
- 最快让 OpenClaw 获得真实工作能力

### 第二批

- normalize skills
- extraction skills

原因：

- 可复用现有实现
- 可快速形成 `raw -> canonical -> queryable` 主链

### 第三批

- evidence skills
- hypothesis / challenge / action-planning skills

原因：

- 这是从“数据工作流”走向“调查工作流”的关键

### 第四批

- promotion skills
- legacy fallback 收口

原因：

- 最终目的是替代表单主路径，而不是只增加 skill 数量

---

## 9. 后续工作计划

## Phase 0：冻结旧主路

目标：

- 明确现有结构为 `v1 baseline`
- 停止继续往旧 packet/prompt 路线上加业务复杂度

任务：

- 冻结 `agent_turns`、`source_selection packet/prompt`、`report packet/prompt` 为 legacy 主路径
- 新能力不再落入旧主路径
- 写出 v2 ADR 和架构边界说明

完成标准：

- 旧结构仅接受 bugfix
- 新开发统一面向 v2 蓝图

## Phase 1：建立 skill runtime 与 receipt 体系

目标：

- 让 OpenClaw 真正调用 skill，而不是只接收大 prompt

任务：

- 定义 skill contract
- 定义 skill registry
- 定义 skill receipt schema
- 打通 OpenClaw skill projection 与调用链
- 支持结构化返回与 side-effect 声明

完成标准：

- 至少 5 个只读 skill 可以被 agent 直接调用
- skill 调用可审计、可回放

## Phase 2：建立 investigation board

目标：

- 用黑板替代大 packet

任务：

- 设计 board schema
- 建立 workspace DB
- 实现 board read/write skill
- 实现 delta 读取和 working memory 压缩

完成标准：

- agent 回合默认读取 board delta，而不是全量 packet
- 关键调查对象已能写入黑板

## Phase 3：数据平面 skill 化

目标：

- 把现有 deterministic 数据逻辑变成可调用能力

任务：

- 包装 normalize 相关逻辑为 skill
- 包装 candidate / matching / evidence 逻辑为 skill
- 默认入 analytics DB
- agent 通过 query skill 拉取局部数据

完成标准：

- agent 可以独立调用数据读取、归一化、链接、聚合 skill
- 不再依赖“先跑完全部 pipeline 再把结果塞给 agent”

## Phase 4：建立真正的多 agent 调查循环

目标：

- 从“表单回合”变成“调查回合”

任务：

- 定义 moderator / sociologist / environmentalist / challenger / archivist 分工
- 建立 probe/challenge/task board
- 支持并行 worker 探针
- 建立议会式 deliberation 机制

完成标准：

- 至少一个案例可通过多 agent 协作完成完整调查
- challenger 能够真实提出并推动反证任务

## Phase 5：promotion 层重构

目标：

- 让最终 artifact 来自工作态 promotion，而不是表单填写

任务：

- 设计 promotion candidate schema
- 设计 canonical artifact materializer
- 让 final report / decision 由 board + receipts promotion 生成

完成标准：

- JSON contract 仍保留
- 但 agent 主工作对象不再是最终 JSON 表单

## Phase 6：评测、切换与收束

目标：

- 确认 v2 不是“更复杂但没更强”

任务：

- 建立 v1 vs v2 benchmark
- 比较 token 消耗、并行能力、反证覆盖率、人工接受率
- 通过后切换默认主路径

完成标准：

- v2 在至少 3 类任务上优于 v1
- OpenClaw 成为主调查体

---

## 10. 验收指标

后续重构不应只看“代码写完了没有”，应看下列能力指标。

## 10.1 架构指标

- OpenClaw agent 是否能直接调用 skill 工作
- 主要推理是否从 supervisor 规则迁移到 `agent + skill + board`
- 旧 packet/prompt 路径是否从主路退为 fallback

## 10.2 调查能力指标

- alternative hypothesis 覆盖率
- falsification task 占比
- 并行 probe 数量
- 历史案例实际利用率
- evidence graph 完整度

## 10.3 运行效率指标

- 每 resolved hypothesis 的 token 成本
- 平均 skill 调用数
- 单轮上下文大小
- board delta 平均体积

## 10.4 审计指标

- skill receipt 覆盖率
- promoted artifact 的 provenance 完整度
- 人工复核可追溯性

---

## 11. 风险与约束

## 11.1 主要风险

- 过度自由导致 agent 漫游和工具滥用
- 过早删除旧 baseline，导致无法比较收益
- skill 设计过粗，变成新的“大型流程函数”
- working DB 和 canonical JSON 脱节

## 11.2 约束策略

为避免上述问题，必须保留以下约束：

- 权限白名单
- 参数化查询
- skill 调用预算
- promotion 审批点
- 每轮冻结快照
- fallback baseline

---

## 12. 最终定义

本项目的最终理想形态应当是：

**一个以 OpenClaw 多智能体协作为核心、以 skill mesh 为主要行动接口、以 investigation board 为共享工作台、以数据库查询与证据图为调查介质、以 promotion 机制保证审计与归档的议会型调查运行时。**

换句话说：

- OpenClaw 不是表单机
- supervisor 不是主要推理者
- 规则不是业务判断主角
- 数据库不是旁路存储
- JSON 不是 agent 的主要工作对象

而是：

- agent 调 skill
- skill 操数据
- 黑板组织协作
- 议会推进调查
- promotion 输出正式结果

这就是后续重构应当收敛到的方向。
