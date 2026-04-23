# OpenClaw Runtime Kernel 与 Agent 权限重构清单

## 1. 文档定位

本清单面向新的目标架构：

1. `moderator` 主持议会、推进阶段、请求状态变更。
2. 调查型 agent 负责抓取、归一化、检索、思考、提交结构化发现。
3. `runtime kernel` 负责权限边界、审计、回放、数据库一致性、人工确认。
4. 技能是原子工具，不再替代 agent 做默认调查结论。

本清单默认接受 breaking change，不以兼容当前 phase-2 主链为目标。

## 2. 当前实现的核心偏差

### 2.1 运行时越界

当前 kernel / phase2 模块不仅负责审计和执行，还实际承担了：

1. 议程排序
2. readiness 判断
3. promotion freeze 语义裁决
4. direct council advisory 编排
5. 默认 agent 入口链提示

代表性代码：

1. `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
2. `eco-concil-runtime/src/eco_council_runtime/phase2_direct_advisory.py`
3. `eco-concil-runtime/src/eco_council_runtime/phase2_fallback_*`
4. `eco-concil-runtime/src/eco_council_runtime/phase2_planning_profile.py`
5. `eco-concil-runtime/src/eco_council_runtime/phase2_stage_profile.py`

### 2.2 agent 被降格为“读现成状态并提交意见”

当前默认 agent path 更接近：

1. 读取 board / query / analysis surface
2. 提交 `proposal`
3. 提交 `readiness opinion`
4. 交还 runtime gate

这使得 agent 更像“结构化评论员”，而不是“调查者”。

### 2.3 moderator 不是唯一 phase owner

当前 phase-2 真实 owner 更接近 `planner + controller + gate`。  
`moderator` 是高权限参与者，但不是唯一主持者。

## 3. 重构后的目标架构

## 3.1 kernel 保留职责

runtime kernel 只保留以下职责：

1. 技能执行包装、超时、重试、dead letter、ledger
2. side effect policy、sandbox boundary、approval policy
3. DB canonical store 与 export rebuild
4. agent 角色绑定、技能访问控制、写权限控制
5. 状态推进请求的人工确认与审计
6. 运行时健康面、operator runbook、replay / benchmark

## 3.2 kernel 不再承担的职责

以下职责不再由 kernel 默认拥有：

1. 自动生成调查议程
2. 自动决定问题该走哪条语义路线
3. 自动决定 round 是否足够 ready
4. 自动决定 promotion 是否应被视为支持
5. 自动规定 agent 的固定讨论顺序

这些能力如果保留，只能作为：

1. agent 可选调用的 advisory skill
2. operator 明确批准后使用的分析辅助
3. 明确标记为 heuristic 的派生输出

## 3.3 目标工作流

目标工作流应改为：

1. `moderator` 定义问题、阶段目标、证据缺口、角色分工。
2. 调查 agent 自行使用 fetch / normalize / query / optional analysis skills 完成调查。
3. agent 通过结构化 DB 对象交换发现、质疑、反证、建议。
4. `moderator` 汇总争议点并提交阶段推进请求。
5. `runtime kernel` 校验权限、前置条件、人工确认状态。
6. operator 审批后，状态才真正推进。
7. report agent 在冻结的 evidence basis 上起草报告。

## 4. 需要从 kernel 抽取成 skill 或直接移除的部分

| 当前模块 | 当前问题 | 处理决定 | 目标去向 |
| --- | --- | --- | --- |
| `phase2_direct_advisory.py` | runtime 直接拼接 council queue，越过 moderator 主持 | 从 kernel 主路径移除 | 可选 advisory skill 或废弃 |
| `phase2_fallback_agenda.py` | runtime 内嵌议程启发式 | 抽离 | 可选 `suggest-next-actions` 类 skill |
| `phase2_fallback_policy.py` | runtime 内嵌优先级/pressure 规则 | 抽离 | 可审计规则 skill 或规则配置 |
| `phase2_fallback_context.py` | 为启发式议程服务的共享上下文 | 抽离 | skill 内部库 |
| `phase2_fallback_planning.py` | runtime 计划含隐式议程语义 | 抽离或删除 | skill 内部库 |
| `phase2_action_semantics.py` | runtime 拥有过多 next-action 语义 | 收缩 | 仅保留最小字段校验，其余下沉到 skill |
| `phase2_promotion_resolution.py` | kernel 决定 promotion support/veto 语义 | 拆分 | kernel 只做 prerequisites，语义判断变 moderator / report advisory |
| `phase2_planning_profile.py` | runtime 规定计划来源与行为 | 收缩 | 只保留 registry；策略抽离为 skill |
| `phase2_stage_profile.py` | runtime 固化 phase-2 阶段顺序 | 收缩 | 仅保留 transition contract；阶段建议移入 moderator skill |
| `phase2_agent_entry_profile.py` | runtime 预设 agent 推荐技能序列 | 重写 | 改成 role capability surface |
| `phase2_agent_handoff.py` | 默认 handoff 仍是“提 proposal / readiness” | 重写 | 改成“调查 -> 提 findings -> moderator 请求推进” |
| `kernel/source_queue_profile.py` | `core_queue_default` 实质上替 agent 决策 | 重写 | 改成 capability metadata，不再决定默认调查链 |
| `kernel/controller.py` | runtime 当前是 phase owner | 重写 | 改为 transition executor + audit logger |

## 5. kernel 保留并强化的部分

### 5.1 必须保留

1. `kernel/operations.py`
2. `kernel/executor.py`
3. `kernel/ledger.py`
4. `kernel/manifest.py`
5. `kernel/locking.py`
6. `kernel/registry.py`
7. `kernel/deliberation_plane.py`
8. `kernel/signal_plane_normalizer.py`
9. `kernel/reporting_contracts.py`

### 5.2 保留但重写职责

1. `kernel/controller.py`
   - 从“流程拥有者”改为“transition request executor”
2. `kernel/gate.py`
   - 从“自动 gate runner”改为“前置条件验证 + 审批 hook”
3. `kernel/agent_entry.py`
   - 从“建议 agent 做什么”改为“暴露当前 role、权限、可见数据面”
4. `kernel/source_queue_contract.py`
   - 从“source governance + default source sequencing”改为“source capability registry”

## 6. 需要新增的 kernel 能力

## 6.1 角色与权限

新增建议：

1. `kernel/access_policy.py`
2. `kernel/skill_registry.py`
3. `kernel/role_contracts.py`

每个 skill 必须声明：

1. `allowed_roles`
2. `denied_roles`
3. `required_capabilities`
4. `side_effect_scope`
5. `db_write_planes`
6. `input_object_kinds`
7. `output_object_kinds`
8. `requires_operator_approval`

`run_skill(...)` 必须接收 `actor_role`，并在运行前做强制校验。

## 6.2 状态推进请求

新增建议：

1. `kernel/transition_requests.py`
2. 新表：
   - `transition_requests`
   - `transition_approvals`
   - `transition_rejections`

流程必须变为：

1. `moderator` 提交 `transition request`
2. kernel 检查前置条件与权限
3. request 进入 `pending-operator-confirmation`
4. operator 批准或拒绝
5. 只有批准后才写入正式 round transition / close / promotion freeze

## 6.3 多 agent 讨论面

新增建议：

1. `discussion_messages`
2. `finding_records`
3. `evidence_bundles`
4. `review_comments`

要求：

1. agent 之间交换信息必须 DB-native
2. 必须可 thread
3. 必须可附证据
4. 必须可被 moderator 汇总
5. 不允许只靠 `board_brief.md` 或 prompt 内存交流

## 7. 角色职责与权限边界

## 7.1 建议角色集合

固定四角色已经不够。建议改成“核心角色 + 任务专门角色”模型。

核心角色：

1. `moderator`
2. `environmental-investigator`
3. `public-discourse-investigator`
4. `formal-record-investigator`
5. `challenger`
6. `report-editor`

任务专门角色按 mission 配置启用，例如：

1. `hydrology-analyst`
2. `ecology-analyst`
3. `policy-analyst`
4. `economics-analyst`
5. `community-impact-analyst`

## 7.2 角色职责表

| 角色 | 主要职责 | 允许写入 | 禁止行为 |
| --- | --- | --- | --- |
| `moderator` | 定义议程、分配任务、收口争议、请求阶段推进 | board task, round note, transition request, proposal, readiness opinion | 不直接跑外部 fetch；不替调查员完成调查 |
| `environmental-investigator` | 获取环境/物理/水文/生态证据并形成发现 | finding, evidence bundle, proposal, optional readiness opinion | 不得推进 round；不得 claim/close round |
| `public-discourse-investigator` | 获取舆情/媒体/社区叙事证据 | finding, evidence bundle, proposal | 不得推进 round |
| `formal-record-investigator` | 获取法规、政策、EIA、审批等正式记录 | finding, evidence bundle, proposal | 不得推进 round |
| `challenger` | 发现偏差、缺口、替代解释、证据冲突 | challenge, review comment, proposal | 不得推进 round；默认不跑外部 fetch |
| `report-editor` | 组装证据包、起草报告、格式检查 | report section draft, report draft | 不得修改调查状态；不得推进 round |
| `runtime-operator` | 审批风险 side effect、审批状态推进 | approval / rejection / override | 不直接写研究结论 |

## 7.3 技能访问矩阵

| 技能类别 | moderator | investigator | challenger | report-editor | operator |
| --- | --- | --- | --- | --- | --- |
| fetch | 默认禁用 | 允许 | 默认禁用 | 禁用 | 审批 |
| normalize | 默认只读 | 允许 | 禁用 | 禁用 | 审批 |
| query / lookup | 允许 | 允许 | 允许 | 允许 | 允许 |
| optional heuristic analysis | 允许查看，不默认执行 | 允许按需执行 | 允许按需执行 | 仅查看 | 审批规则版本 |
| board read | 允许 | 允许 | 允许 | 允许 | 允许 |
| board write | 允许 | 限 finding / note 子集 | 限 challenge / review | 禁用 | override only |
| transition request | 仅 moderator | 禁用 | 禁用 | 禁用 | 审批 |
| publish / finalize | moderator + report-editor 协作 | 禁用 | 禁用 | 允许草拟 | 审批 |

## 8. agents 与 skills 的对接模式

## 8.1 技能分层

所有 skill 必须显式标记层级：

1. `fetch`
2. `normalize`
3. `query`
4. `optional-analysis`
5. `deliberation-write`
6. `reporting`
7. `state-transition`

## 8.2 调用模式

1. investigator 先调用 `fetch -> normalize -> query/lookup`
2. 如有必要，再调用 `optional-analysis`
3. 调查结论通过 `finding / proposal / challenge` 写入 DB
4. moderator 读取这些结构化对象后，发起 `state-transition request`
5. runtime kernel 完成验证与 operator approval

## 8.3 必须删除的旧耦合

1. “agent 入口默认等于 proposal/readiness 提交”
2. “controller 默认重跑 next-actions / readiness / promotion”
3. “core_queue_default 决定 agent 调查顺序”
4. “phase2 planner 默认拥有议会阶段语义”

## 9. 代码实施清单

## 9.1 第一批：权限与边界

- [x] 新增 `kernel/access_policy.py`
- [x] 新增 `kernel/skill_registry.py`
- [x] 为全部 88 个 skill 补 `allowed_roles / required_capabilities`
- [x] 修改 `kernel/executor.py`，执行前强制校验角色与 side effect
- [x] 修改 `kernel/cli.py`，所有写操作必须显式携带 `--actor-role`

### 2026-04-23 Session 收口

- 已完成：
  - 新增 `kernel/role_contracts.py`，统一 canonical role、legacy alias、capability contract，消除旧 `environmentalist / sociologist / policy-analyst` 等角色名直接参与 runtime 判权的散点语义。
  - 新增 `kernel/skill_registry.py`，为当前 88 个 skill 集中声明 `skill_layer / allowed_roles / required_capabilities / write_scope / requires_operator_approval / default_actor_role_hint`。
  - 新增 `kernel/access_policy.py`，把 skill 执行与 kernel 写命令的 actor-role 校验集中到 kernel，而不是继续依赖 skill 脚本的隐式约定。
  - 修改 `kernel/governance.py` 与 `kernel/executor.py`，在 preflight / execution / ledger payload 中加入 `actor_role` / `resolved_actor_role` / `skill_access`，执行前先过角色与 side-effect 校验。
  - 修改 `kernel/cli.py` 与 `runtime_command_hints.py`，所有 kernel 写命令都要求显式 `--actor-role`，`run-skill` / `preflight-skill` / round control / export rebuild / benchmark / operator surfaces 全部按新入口生成命令模板。
  - 修改 `kernel/controller.py`、`kernel/supervisor.py`、`kernel/post_round.py`、`kernel/agent_entry.py` 以及 phase2 handoff/profile 相关模块，把请求角色继续向下透传，避免 moderator/runtime-operator 语义在链路中丢失。
  - 补了最小回归测试，覆盖 registry skill access、CLI 显式 `--actor-role`、角色越权拦截、command template 注入、agent entry / runbook surface。

- 未完成：
  - `9.2` 的 `transition_requests / approvals / rejections` 还未开始，阶段推进仍未进入 `request -> approve/reject -> commit` 的完整持久化审批链。
  - operator approval 目前仍主要体现在 skill policy 与 contract mode 上，尚未落成独立的审批记录表和审批命令。

- 新发现的问题：
  - 旧的 operator/runbook/benchmark 命令模板里原本还残留未带 `--actor-role` 的硬编码字符串，已在本次收口中修正；后续新增 runtime command surface 不能再绕过 `runtime_command_hints.py`。
  - 部分“需人工审计”的 heuristic / optional-analysis skill 当前仍依赖 `contract_mode=strict` 才会硬阻断；这说明 batch-2 之前，operator approval 语义还没有完全从“告警”提升到“持久化审批对象”。

- 是否影响后续计划：
  - 不改变 `9.2 / 9.3` 的主顺序，但 `9.2` 现在可以直接建立在 canonical role + centralized skill policy + explicit actor-role command surfaces 之上，不需要再返工第一批边界层。

- 第一批复核结果：
  - 运行时角色边界与 `--actor-role` 强制透传本身无需新的功能性补正；本次仅同步修正了 `supervise-round -> controller` 相关 unittest 断言，确认 batch-1 已落地的 actor-role 透传语义确实在主链生效。
  - 当前没有发现会破坏 `9.2` 的 batch-1 残缺实现，后续可以继续按既定顺序推进 kernel 语义收缩。

## 9.2 第二批：状态推进与人工确认

- [x] 新增 `transition_requests / transition_approvals / transition_rejections`
- [x] 新增 `request-phase-transition`
- [x] 新增 `approve-phase-transition`
- [x] 新增 `reject-phase-transition`
- [x] 修改 `eco-open-investigation-round`，只能消费已批准 transition request
- [x] 修改 promotion / close round 流程，必须经过 operator approval

### 2026-04-23 Session 收口

- 已完成：
  - `kernel/transition_requests.py` 与 deliberation DB schema 已建立 `transition-request / transition-approval / transition-rejection` canonical 持久化链，`query-control-objects` 与 operator/runbook surface 也已对称暴露这三类 runtime object。
  - `request-phase-transition / approve-phase-transition / reject-phase-transition / close-round --transition-request-id` 已在 kernel CLI、access policy、operator hints、handoff surface 中贯通；`moderator` 只能发起 request，`runtime-operator` 才能 approve/reject/commit。
  - `eco-open-investigation-round / eco-promote-evidence-basis / close-round` 现在都只能消费已批准 request；实际 side effect 成功后才把 request 状态写成 `committed`，不再把 `round_transitions` 当审批对象。
  - `kernel/controller.py` 已补上 promotion-stage 治理前置：`promotion-basis` stage 会解析当前 round 该 kind 的最新 request，并只在状态为 `approved / committed` 时注入 `--transition-request-id` 执行；否则 controller 会以清晰的治理阻断失败收口，而不是回退到旧的“无审批直接 promote”语义。
  - `phase2_direct_advisory.py` 与 `eco-plan-round-orchestration` 的 stop-condition 文案已同步改成“moderator request + operator approval first”，避免 advisory/operator surface 继续暗示可直接 promote。
  - 相关测试入口已全部切到新审批链：open-round / promote / close-round / supervise-round / run-phase2-round 相关 unittest 现在都会先创建并批准对应 request，再执行 runtime/skill 主链。
  - 本次实际回归通过：
    - `tests.test_runtime_kernel`
    - `tests.test_board_workflow`
    - `tests.test_archive_history_workflow`
    - `tests.test_control_query_surface`
    - `tests.test_supervisor_simulation_regression`
    - `tests.test_reporting_workflow`
    - `tests.test_reporting_publish_workflow`
    - `tests.test_reporting_query_surface`
    - `tests.test_decision_trace_workflow`
    - `tests.test_deliberation_agenda_workflow`
    - `tests.test_investigation_workflow`
    - `tests.test_benchmark_replay_workflow`
    - `tests.test_orchestration_ingress_workflow`

- 未完成：
  - 就本批明确定义的三条链路（`open-investigation-round / promote-evidence-basis / close-round`）而言，没有新的功能性遗留；更广义的 kernel phase ownership 收缩仍留在 `9.3`。
  - controller 侧的 transition request 自动注入目前只对当前 batch 实际进入 controller 的 `eco-promote-evidence-basis` 做了硬化；如果后续再把新的 state-transition skill 放回 controller 主链，需要继续沿同一治理模式扩展，而不是重回旧式裸 skill 调用。

- 新发现的问题：
  - 旧测试中仍有少量断言默认 `supervise-round` 不会把 `actor_role="runtime-operator"` 继续传给 controller；本次已作为 batch-1 复核的一部分修正，说明测试层面对新边界语义曾经落后于代码实现。
  - 目前 planner/advisory 仍然会产出 `promotion-basis` stage，只是 runtime 不再允许它绕过审批直接执行；这再次说明 `phase2 planner / direct advisory` 的默认 phase ownership 仍需在 `9.3` 继续收缩。

- 是否影响后续计划：
  - 不阻塞 `9.3`。相反，`9.3` 现在可以建立在已经硬化的 `request -> approve/reject -> commit` 审批链之上，继续把 controller 从“默认 phase owner”收缩成“受治理的 transition executor + audit logger”。

## 9.3 第三批：缩小 kernel 语义面

- [x] 把 `phase2_fallback_*` 从 kernel 主路径移除
- [x] 把 `phase2_direct_advisory.py` 从默认计划源移除
- [x] 重写 `kernel/controller.py`，仅负责任务执行与日志
- [x] 重写 `phase2_agent_entry_profile.py`，只输出 capability surface
- [x] 移除 `core_queue_default` 对 phase-2 行为的支配

### 本次收口回写

- 已完成：
  - `phase2_planning_profile.py` 的默认 planning source 已收缩为 `[]`；`kernel/controller.py` 默认路径不再采用 `runtime-planner / agent-advisory / direct-council-advisory`，而是只消费已批准的 `promote-evidence-basis` transition request，或在无批准请求时以 inspection-only/no-op 收口。
  - `kernel/controller.py` 现已把默认 phase-2 语义改成 `transition-executor`：默认只执行 `promotion-gate -> promotion-basis` 这一条已审批 transition 链，不再默认生成 `next-actions / readiness / advisory` 议程；`kernel/supervisor.py` 也同步改成只有在 planning 真正包含 `next-actions` stage 时才消费该 surface。
  - `phase2_agent_entry_profile.py` 与 `kernel/agent_entry.py` 已改为输出 canonical role capability surface、技能层级、transition request/approve/reject 命令模板；默认 recommended skills 与 advisory sources 均为空，agent entry 不再把调查者引导到默认 advisory plan 主链。
  - `kernel/source_queue_profile.py` 已停止向外导出 `core_queue_default`；对外改为 `phase2_behavior` 分类，避免 queue profile 继续暗示 phase-2 默认拥有权。`kernel/operations.py` 的 runbook 文案也已改成 transition/query 导向，而不是 advisory refresh 导向。
  - 已补充并通过本批最小相关回归 `14` 项：
    - `tests.test_agent_entry_gate`
    - `tests.test_runtime_source_queue_profiles`
    - `tests.test_runtime_kernel.RuntimeKernelTests.test_controller_executes_approved_promotion_request_without_planner_stage`
    - `tests.test_runtime_kernel.RuntimeKernelTests.test_controller_completes_without_default_plan_when_no_transition_request_exists`
    - `tests.test_runtime_kernel.RuntimeKernelTests.test_controller_ignores_optional_advisory_artifacts_on_default_path`
    - `tests.test_runtime_kernel.RuntimeKernelTests.test_controller_respects_injected_planning_sources`
    - `tests.test_runtime_kernel.RuntimeKernelTests.test_controller_resume_skips_completed_stages_after_failure`

- 未完成：
  - `phase2_fallback_*` 与 `phase2_direct_advisory.py` 仍作为可注入 / 可选兼容模块保留在仓库内；本批完成的是“移出 kernel 默认主路径”，不是删除这些模块本身。
  - 非默认的 injected planner/advisory 路径仍保留历史 plan metadata（例如 `recommended_skill_sequence`）；如果后续目标是进一步压缩兼容语义面，需要在后续批次继续清理这些 optional surface，而不是把它们重新接回默认链。

- 新发现的问题：
  - stage validation 之前会把显式声明的空 `required_previous_stages=[]` 误当成“未声明”，从而回退到 stage contract 的旧依赖，导致 `transition-executor` 的 `promotion-gate` 被重新绑回 `round-readiness`；本次已同时在 `phase2_controller_state.py` 与 `phase2_stage_profile.py` 修复。
  - 旧 resume 回归仍默认假设 controller 首轮会自动走 runtime planner；本次已把该测试改为显式注入 planning source，避免测试层继续固化旧的 kernel default ownership。

- 是否影响后续计划：
  - 不阻塞 `9.4 / 9.5`。相反，batch3 完成后，后续讨论面 / 报告面可以建立在“kernel 无默认议程所有权、moderator 通过 request/approve/reject/commit 推进、证据 basis 依赖 DB canonical state”这一前提上继续展开。
  - 后续如果再把新的 state-transition skill 放回 controller 执行面，必须沿用本批已经建立的模式：显式 transition request、显式审批状态校验、显式 stage dependency，而不是恢复旧式 planner/fallback 代替治理判断。

## 9.4 第四批：讨论面与报告面

- [ ] 新增 `finding_records`
- [ ] 新增 `discussion_messages`
- [ ] 新增 `evidence_bundles`
- [ ] 新增 `report_section_drafts`
- [ ] 让 `proposal / readiness opinion` 不再是唯一讨论对象

## 10. 验收标准

以下条件全部满足，才算 runtime/kernel 权限重构完成：

1. `moderator` 是唯一可请求阶段推进的 agent。
2. investigator 可以完成完整调查闭环，而不是只能读 query surface。
3. kernel 无默认议程所有权，只负责验证、审计、审批。
4. 任意状态推进都有 `request -> validate -> approve/reject -> commit` 链。
5. 所有写技能都有显式 role allowlist。
6. `environmentalist` 不能调用议会状态推进技能。
7. agent 间讨论可以只依赖 DB-native finding / challenge / comment / evidence bundle。
8. 删除 `next-actions / readiness / promotion` artifact 后，核心研究状态仍可从 DB 恢复。
9. operator 能在 runbook 中看到每一次阶段推进请求、审批人、审批理由和证据 basis。
