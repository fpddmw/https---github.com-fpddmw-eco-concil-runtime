# OpenClaw 彻底修正开发规划

## 1. 规划定位

本文件不是“下一轮小修小补计划”，而是 OpenClaw 的彻底修正施工路线图。

本轮工作的目标不是维持当前系统稳定，也不是最小化迁移成本，而是：

`把 OpenClaw 从受治理但低自主、规则主导、artifact 依赖的 workflow engine，重构为受治理但高自主、DB-native、agent council 驱动的环境争议议会系统。`

这里的“彻底修正”有三层含义：

1. 允许 breaking change。
2. 不以向后兼容和短期稳定性为优先目标。
3. 旧链路只在迁移窗口内暂存，最终必须删除，而不是长期共存。

## 2. 本轮执行姿态

本轮必须明确采用以下执行姿态：

1. `正确性优先于稳定性`
   - 如果旧稳定性来自错误的边界划分、错误的状态面或错误的职责归属，就应该打破它。
2. `重构优先于兼容`
   - 兼容层只能是迁移期措施，不能成为新的长期架构。
3. `删除优先于包裹`
   - 对于旧 `claim -> coverage -> readiness` 主链，优先删除其默认地位，而不是继续加一层包装。
4. `DB canonical 优先于 artifact handoff`
   - 所有 council-critical 对象必须先写 DB，再导出 artifact。
5. `agent judgement 优先于 runtime heuristic`
   - runtime 负责治理、执行、审计；实质判断回到 agent council。

## 2.1 Batch 1 当前已交付

截至本地当前版本，第一批已经不再只是文档设计，已落地以下硬改动：

1. 建立了 deliberation canonical contract registry 与 council object query surface。
2. `next-action / probe / readiness-assessment / promotion-basis` 的主存储入口已强制补齐 `decision_source / evidence_refs / lineage / provenance`，不能再静默写出半结构化对象。
3. `eco-propose-next-actions` 已开始优先消费 `proposal` 对象；存在 council proposal 时，不再默认只跑 heuristic queue。
4. `eco-summarize-round-readiness` 已开始优先消费 `readiness-opinion` 对象；存在 council opinion 时，不再默认只跑 policy formula。
5. phase-2 controller 与 post-round terminal state 已改成 DB-first 读取顺序，artifact 降为 fallback 载体。

## 2.2 Batch 2 当前已交付

第二批已把“议会 judgement 进入 reporting / publication 主链”的追溯链真正打通，已落地以下硬改动：

1. `eco-promote-evidence-basis` 现在直接消费 `proposal / readiness-opinion`，并把 `supporting_proposal_ids / supporting_opinion_ids / rejected_opinion_ids / council_input_counts` 写入 canonical promotion basis。
2. `promotion-basis -> reporting-handoff -> council-decision -> final-publication` 已开始显式携带 `selected_basis_object_ids / supporting_* / rejected_* / decision_trace_ids`，不再只透传结果态。
3. `eco-publish-council-decision` 已在发布 canonical decision 时同步写入 `decision-trace` DB 对象，trace 中记录采纳对象、拒绝对象、evidence refs、lineage 与 provenance。
4. `eco-materialize-final-publication` 已从 DB 查询 `decision-trace`，并把 trace 摘要直接暴露到 final publication；若 trace 缺失会显式报出 `missing-decision-trace`。
5. 已新增 `tests/test_decision_trace_workflow.py`，覆盖 ready/hold 两类 trace 场景；当前相关核心回归共 50 项，已在本地全部通过。

## 2.3 Batch 3 当前已交付

第三批已把 probe 打开链路从“action 派生优先”推进到“council proposal 优先”，已落地以下硬改动：

1. `eco-open-falsification-probe` 现在会直接查询 council proposal，并把 probe-oriented proposal 放在 probe candidate 队列首位；`next-action` 退回为 augment/fallback 输入。
2. 当存在 probe-oriented council proposal 时，probe skill 不再把 `next_actions` 缺失视为阻塞或主 warning，也不再先依赖 heuristic action 才能开 probe。
3. canonical probe 现在显式继承上游 `decision_source / provenance / lineage / source_ids`，proposal 驱动的 probe 不再静默回退成 `heuristic-fallback`。
4. 已新增 council autonomy 回归，覆盖“proposal-only 直接开 probe”和“proposal 优先于 DB-backed heuristic action”两类场景；当前相关核心回归共 61 项，已在本地全部通过。

## 2.4 Batch 4 当前已交付

第四批已把 board judgement 从“操作员参数优先 + artifact 同步”推进到“proposal-first + DB judgement metadata 优先”，已落地以下硬改动：

1. 新增 `board_proposal_support.py`，board judgement 现在直接从 deliberation DB 查询 council proposal，并统一生成 `decision_source / evidence_refs / source_ids / response_to_ids / provenance / lineage`。
2. `hypothesis_cards / challenge_tickets / board_tasks` schema 与迁移链已补齐 `decision_source / evidence_refs_json / source_ids_json / provenance_json / lineage_json`，board judgement 不再只写在导出 JSON 上。
3. `eco-update-hypothesis-status / eco-open-challenge-ticket / eco-close-challenge-ticket / eco-claim-board-task` 已改为 proposal-first；当 proposal 提供足够信息时，不再要求额外输入 `title / status / ticket-id` 才能推进。
4. `hypothesis / challenge / board-task` 已进入 deliberation canonical contract registry，并可通过 `query-council-objects` 直接查询，不再只是 board JSON 内部结构。
5. board 状态更新现在把 canonical judgement metadata 同时写入表列和 `raw_json`，board JSON 只作为 DB-backed 导出视图存在。
6. 已新增 proposal-only board workflow 回归，覆盖 hypothesis update、challenge open、challenge close、board task claim 四条路径，并显式断言 DB 列与 `raw_json` 中的 judgement metadata；当前相关核心回归共 75 项，已在本地全部通过。

## 2.5 Batch 5 当前已交付

第五批已把 promotion judgement 从“skill 内部硬编码 proposal kind 白名单”推进到“共享 council promotion resolution surface”，已落地以下硬改动：

1. 新增 `eco_council_runtime/phase2_promotion_resolution.py`，统一承载 promotion proposal / readiness opinion 的加载、显式 disposition 解析、legacy compatibility fallback 与 council veto 规则。
2. `eco-promote-evidence-basis` 不再依赖固定 `prepare-promotion / ready-for-reporting / publish-council-decision` kind 集合来判定 support；现在优先消费 proposal 内显式 `promotion_disposition / promote_allowed / publication_readiness / handoff_status / moderator_status` 等 structured judgement。
3. `promotion-gate` 现在会显式读取 council proposal，并把 `rejected_proposal_ids / supporting_proposal_ids / promotion_resolution_mode / council_input_counts` 写进 gate snapshot；controller 不会再出现“gate 放行但议会 veto promotion”却无 trace 的盲区。
4. `promotion-basis -> reporting-handoff -> council-decision -> decision-trace` 已开始显式透传 `rejected_proposal_ids / promotion_resolution_mode / promotion_resolution_reasons / council_input_counts`，publication trace 可以直接指出是哪个 veto proposal 阻断了发布。
5. 已新增回归覆盖：
   - 非旧白名单 proposal kind，但带显式 promotion judgement，仍可被识别为 support。
   - ready opinions 存在时，explicit hold proposal 仍可 veto gate / promotion / publication，并在 decision trace 中以 `proposal` 作为 selected object 落库。
6. 当前相关大回归共 `127` 项，已在本地全部通过。

## 2.6 Batch 6 当前已交付

第六批已把 agent-facing proposal / readiness intake 从“零散 store helper + board note 提示”推进到“默认 DB-native 结构化提交链”，已落地以下硬改动：

1. `eco_council_runtime/council_objects.py` 已新增 append/upsert 原语，agent 现在可以逐条提交 `proposal / readiness-opinion`，不再只能通过整轮 replace bundle 写入。
2. 新增 `eco-submit-council-proposal` 与 `eco-submit-readiness-opinion` 两个技能，直接写 canonical `proposal / readiness-opinion` 对象，并保留 `target / evidence_refs / response_to_ids / lineage / provenance / promotion_disposition / publication_readiness` 等结构化字段。
3. `canonical_contracts.py` 已收紧 `proposal / readiness-opinion` 契约；`status / opinion_status / response_to_ids / basis_object_ids` 已进入硬校验面，proposal/opinion 不再只是“最小文本壳”。
4. `phase2_agent_entry_profile.py`、`phase2_agent_handoff.py` 与 `kernel/agent_entry.py` 已把默认 agent 入口切到 submission skill：
   - 默认 write path 优先 `eco-submit-council-proposal / eco-submit-readiness-opinion`
   - operator view 已显式暴露 `query-council-objects` proposal/readiness 命令与 submission command template
   - entry chain 默认要求先提交结构化 proposal / readiness judgement，再回 runtime gate
5. `phase2_direct_advisory.py`、`eco-plan-round-orchestration`、`eco-summarize-round-readiness`、`eco-propose-next-actions`、`eco-open-falsification-probe` 的 follow-up guidance 已开始从 `eco-post-board-note` 转向 submission skills，agent 默认不再被鼓励用 freeform note 代替 council judgement。
6. 已新增 `tests/test_council_submission_workflow.py`，覆盖 proposal/opinion append、query surface、kernel registry execution；同时补强 `test_agent_entry_gate.py` 与 `test_council_autonomy_flow.py`，锁定默认入口与 readiness follow-up 的新方向。
7. 当前扩展后的大回归共 `130` 项，已在本地全部通过。

## 2.7 Batch 7 当前已交付

第七批已把 promotion compatibility 从“保留旧 implicit kind 的迁移窗”推进到“默认明确忽略旧 kind，只有显式 structured judgement 才有 promotion 解释权”，已落地以下硬改动：

1. `eco_council_runtime/phase2_promotion_resolution.py` 已删除旧 `prepare-promotion / ready-for-reporting / publish-council-decision` proposal kind 的 support 语义；proposal 若没有显式 `promotion_disposition / promote_allowed / publication_readiness / handoff_status / moderator_status`，将不再被解释为 promotion support。
2. promotion resolution 现在会把这类残留输入显式标成 `ignored-implicit-promotion-kind`，并写入 `proposal_resolution_records / proposal_resolution_mode_counts`，不再静默兼容。
3. `eco-promote-evidence-basis` 现在会对这类旧输入发出 `ignored-implicit-promotion-kind` warning，使迁移残留在 promotion artifact 中可见。
4. `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-draft-expert-report / eco-publish-expert-report / eco-publish-council-decision` 的 withheld guidance 已从 `eco-post-board-note` 改为 `eco-submit-council-proposal / eco-submit-readiness-opinion`，reporting/publication hold-path 不再把 agent 引回弱结构写入。
5. `eco-promote-evidence-basis` 与 `eco-summarize-round-readiness` 的 skill docs / agent prompts 已更新，明确要求依赖 deliberation DB 中的显式 proposal/readiness judgement，而不是 proposal 名称暗示。
6. 已新增回归：
   - legacy-named promotion proposal 在没有显式 judgement 字段时会被忽略而不是被当作 support。
   - reporting / publication hold-path 的 `suggested_next_skills` 不再包含 `eco-post-board-note`，而是转向 submission skills。
7. 当前扩展后的大回归共 `131` 项，已在本地全部通过。

## 2.8 Batch 8 当前已交付

第八批已把 reporting / fallback 的剩余旧中间态从“字符串特殊处理 + artifact 依赖”推进到“显式 blocker / DB-first supervisor / shared reporting gate”，已落地以下硬改动：

1. 新增 `phase2_action_semantics.py`，`readiness_blocker` 现在成为 fallback / planner / readiness / promotion 共享的一等语义；planner 不再靠 `action_kind == prepare-promotion` 这种旧名字判断是否忽略一个 action。
2. `phase2_fallback_policy.py` 的默认空 agenda cue 已从 `prepare-promotion` 改成 `open-council-readiness-review`，语义从“隐式推动 promotion”改成“显式打开 council readiness review”，并明确 `readiness_blocker = false`。
3. 新增 `reporting_status.py`，`eco-materialize-reporting-handoff / eco-draft-council-decision / eco-draft-expert-report / phase2_posture_profile` 现在共享 `reporting_ready / reporting_blockers / handoff_status` 判定层；`ready-for-reporting / pending-more-investigation` 不再是主判定锚点。
4. `phase2_posture_profile.py` 的 promoted supervisor status 已统一到 `reporting-ready`；reporting handoff 的非 ready 状态统一为 `investigation-open`。
5. `phase2_state_surfaces.py` 已新增 `load_supervisor_state_wrapper`，reporting handoff 现在可以在缺失 `runtime/supervisor_state_*.json` 时，直接从 deliberation DB 的 `promotion_freeze -> supervisor_snapshot` 恢复 supervisor state。
6. `deliberation_plane.py` 已新增迁移列：
   - `moderator_actions.readiness_blocker`
   - `reporting_handoffs.reporting_ready / reporting_blockers_json`
   - `council_decision_records.reporting_ready / decision_gating_json`
   - `expert_report_records.reporting_ready`
   关键 reporting gate 字段不再只存在于 `raw_json`。
7. `eco-promote-evidence-basis` 的 promoted follow-up 已从 `eco-post-board-note` 改为 `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-draft-expert-report`；promoted path 不再把 agent 引回弱结构 note。

## 2.9 Batch 9 当前已交付

第九批已把 runtime/operator/query surface 对 reporting 与 readiness blocker 的可见面，从“内部字段存在但外部仍靠 promotion/status 推断”推进到“显式 DB-native surface + operator/query direct access”，已落地以下硬改动：

1. `kernel/phase2_state_surfaces.py` 已新增 `build_reporting_surface(...)`，并把 `supervisor / reporting-handoff / council-decision / expert-report` wrapper 统一接到 shared reporting surface；runtime 不再让这些入口各自重新推断 `reporting_ready / reporting_blockers / handoff_status`。
2. `kernel/supervisor.py` 现在会把 `reporting_ready / reporting_blockers / reporting_handoff_status` 直接写入 supervisor snapshot 与 promotion freeze；reporting gate 不再只能从后续 handoff artifact 反推。
3. `kernel/cli.py` 现在新增 `show-reporting-state` 命令，`show-run-state` 也新增 top-level `reporting` section；operator 可直接查看 DB-first handoff / decision / publication surface，而不是只看 phase-2 promotion summary。
4. `phase2 operator` view 现在显式暴露：
   - `reporting_ready / reporting_blockers / reporting_handoff_status`
   - `show_reporting_state_command`
   - `query_next_actions_command`
   - `query_readiness_blockers_command`
   操作员不再需要手工翻 raw JSON 才知道是哪个 next-action 在阻塞 readiness。
5. `query-council-objects` 已支持 `--readiness-blocker-only`；`moderator_actions.readiness_blocker` 终于成为真正可查询的 DB filter，而不只是 `raw_json` 内部字段。
6. `kernel/post_round.py` 与 `kernel/benchmark.py` 已改为消费 shared reporting surface；round close / benchmark manifest 现在都会显式携带 `reporting_ready / reporting_blockers / reporting_handoff_status / reporting_surface_source`，不再只透出 `promotion_status`。
7. `post_round` 的未发布 ready posture 已从 `promoted-unpublished` 改成 `reporting-ready-unpublished`，终态语义不再把 reporting readiness 锚死在旧 promotion gate 名词上。
8. 本地扩展回归已更新到 `139` 项，并在当前版本全部通过。

## 2.10 Batch 10 当前已交付

第十批已把 reporting/publication 从“deliberation DB 里有几张记录表”推进到“正式 reporting canonical plane + 独立 query surface + 强校验写库”，已落地以下硬改动：

1. 新增 `eco_council_runtime/reporting_objects.py`，`reporting-handoff / council-decision / expert-report / final-publication` 现在拥有独立 reporting query surface，不再继续混挂在 `query-council-objects` 下。
2. `kernel/cli.py` 现在新增 `query-reporting-objects` 命令；支持 `run_id / round_id / status / stage / agent_role / decision_id` 过滤，同时 `list-canonical-contracts --plane reporting` 已正式开放。
3. `show-reporting-state` 的 operator view 已新增 reporting query command templates；操作员现在不仅能看 handoff/decision/publication surface，还能直接按 stage/role 查询 DB canonical objects。
4. `deliberation_plane.py` 的 `store_reporting_handoff_record / store_council_decision_record / store_expert_report_record / store_final_publication_record` 现在会先 canonicalize 再校验；DB `raw_json` 已统一切到 `reporting-handoff-v1 / council-decision-v1 / expert-report-v1 / final-publication-v1`，不再保留 skill 自己的 `e1.x` envelope 作为真实存储形态。
5. `canonical_contracts.py` 已收紧 reporting plane 契约：`evidence_refs / lineage / provenance` 已进入 reporting objects 的硬校验面；decision 还额外强制 `decision_trace_ids / published_report_refs` 等关键字段具备显式结构。
6. 已新增 `tests/test_reporting_query_surface.py`，覆盖 reporting contract list、独立 query surface、operator query command 以及 DB `raw_json` canonicalization；当前扩展后的大回归已更新到 `141` 项，并在本地全部通过。

## 2.11 Batch 11 当前已交付

第十一批已把 reporting 从“DB-first 但 artifact 仍可暗中回流成状态源”推进到“DB-native only + export-only artifact + 可重建导出面”，已落地以下硬改动：

1. `kernel/phase2_state_surfaces.py` 的 `reporting-handoff / council-decision / expert-report / final-publication` wrapper 已改成 `DB-only`：
   - 只要 DB 中存在 canonical row，就直接返回 DB payload。
   - DB 缺失但 export 文件仍在时，不再把 artifact 当 payload 恢复，而是显式返回 `orphaned-...-artifact` source、`payload_present = false`、`artifact_present = true`。
   - decision / expert-report wrapper 不再偷偷丢掉 `record_id / decision_stage / report_stage`，operator 与上层 skill 可以看到完整 canonical row 字段。
2. `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-draft-expert-report / eco-publish-expert-report / eco-publish-council-decision / eco-materialize-final-publication` 现已统一按 `store_*_record(...)` 返回的 canonical payload 落盘，artifact 与 DB `raw_json` 不再分叉。
3. `eco-publish-expert-report` 与 `eco-publish-council-decision` 已显式钉死 `canonical` stage，并在 publish 时清空 draft 继承来的 `record_id / provenance`；canonical publish 不会再错误复用 draft 主键，导致 draft row 被 `INSERT OR REPLACE` 顶掉。
4. 新增 `eco_council_runtime/reporting_exports.py` 与 CLI `materialize-reporting-exports`：
   - 可从 reporting plane DB 一次性重建 `reporting_handoff / council_decision_draft / council_decision / expert_report_draft_* / expert_report_* / final_publication` 八个导出物。
   - `show-reporting-state` operator view 已新增 `materialize_reporting_exports_command`，operator 可显式把 artifact 重建动作当成导出流程，而不是恢复逻辑。
5. 已补强回归：
   - `tests/test_reporting_query_surface.py` 现在显式断言 reporting artifact 与 DB `raw_json` 全同构。
   - wrapper 会保留 stage/record fields，并把 artifact-only 状态标成 orphaned。
   - `materialize-reporting-exports` 可从 DB 重建全部 reporting 导出物。
   - `tests/test_reporting_publish_workflow.py` 已新增 orphaned draft artifact 阻断 publish 的整合回归。
6. 当前扩展后的主链大回归已更新到 `144` 项，并在本地全部通过。

## 2.12 Batch 12 当前已交付

第十二批已把 phase-2 / investigation 中间态从“DB-first，但 artifact 仍可偷偷回流成状态源”推进到“DB-native only + export-only artifact + phase-2 export rebuild”，已落地以下硬改动：

1. `kernel/phase2_state_surfaces.py` 的 `next-actions / falsification-probes / round-readiness / promotion-basis / supervisor-state` wrapper 已改成 `DB-only`：
   - 只要 DB 中存在 canonical row / snapshot，就直接返回 DB payload。
   - DB 缺失但 export 文件仍在时，不再把 artifact 当 payload 恢复，而是显式返回 `orphaned-...-artifact` source、`payload_present = false`、`artifact_present = true`。
   - `runtime/supervisor_state_*.json` 现在也不再是 phase-2 控制面的恢复源；真正状态来自 `promotion_freezes.supervisor_snapshot`。
2. 新增 `eco_council_runtime/phase2_exports.py` 与 CLI `materialize-phase2-exports`：
   - 可从 deliberation / phase-2 DB 一次性重建 `next_actions / falsification_probes / round_readiness / promoted_evidence_basis / supervisor_state` 五个导出物。
   - `show-run-state` 的 phase-2 operator view 已新增 `materialize_phase2_exports_command`，并补上 `probe / readiness-assessment / promotion-basis` 的 query command template。
3. `eco-materialize-final-publication` 已切到 `load_supervisor_state_wrapper(...)`，publication 不再旁路直读 `supervisor_state` artifact；即使文件缺失，也会优先从 `promotion_freeze -> supervisor_snapshot` 恢复。
4. `kernel/controller.py` 已删除一个残留的 `promotion_basis` artifact fallback；controller completion 现在只信 DB control state，不再用 `promotion/promoted_evidence_basis_*.json` 回填 `promotion_status`。
5. 已补强回归：
   - `tests/test_phase2_state_surfaces.py` 现在显式断言 phase-2 wrapper 的 orphaned-artifact 语义，以及 `materialize-phase2-exports` 的 DB rebuild。
   - `tests/test_reporting_publish_workflow.py` 已新增 final publication 在 supervisor artifact 缺失时的 DB recovery 回归。
   - `tests/test_orchestration_planner_workflow.py / tests/test_board_workflow.py / tests/test_runtime_kernel.py` 已把旧的 artifact-only test seed 收口为 DB canonical seed，防止测试继续帮旧旁路续命。
6. 当前扩展后的主链大回归已更新到 `148` 项，并在本地全部通过。

## 3. 本轮必须解决的核心问题

### 3.1 Agent 自主权不足

当前 agent 更像受治理的执行端，而不是议会判断主体。

彻底修正后的目标：

1. agent 能在共享状态上提交 `proposal / challenge / readiness opinion`。
2. 每条提案都带 `rationale / confidence / evidence refs / provenance`。
3. runtime 不再替 agent 算出唯一“正确动作”。

### 3.2 Runtime kernel 边界过宽

当前 kernel 已吸收了大量 phase-specific orchestration、规则公式和议会语义。

彻底修正后的目标：

1. kernel 只保留治理、执行、持久化、查询、审计。
2. phase policy、争议判断、readiness judgement、promotion judgement 全部移出 kernel。

### 3.3 启发式与规则链压过了议会判断

当前主链的 routing、ranking、readiness、promotion 仍主要靠固定阈值和评分公式。

彻底修正后的目标：

1. heuristic 只保留为 `bootstrap / fallback / audit / guardrail`。
2. 默认主判断路径来自 agent council 的结构化提案与分歧记录。

### 3.4 数据契约不够硬

当前运行治理层契约较完整，但领域对象和 phase-2 对象仍大量依赖 envelope 和 wrapper。

彻底修正后的目标：

1. formal comments 成为一等结构化输入。
2. 争议对象、议会对象、phase-2 对象全部拥有 canonical schema。
3. 关键对象可 item-level 查询，而不是只能整包读取 snapshot。

### 3.5 议会流程还不够 DB-native

当前系统虽然已经 DB-first，但很多控制与恢复链仍显著依赖 artifact。

彻底修正后的目标：

1. 删除中间 artifact 后，round 仍可继续推进。
2. board、phase-2、reporting、publication 默认从 DB 读取 canonical 对象。
3. JSON / Markdown 回到 export-only 地位。

### 3.6 Public-side 主分析链方向仍然偏旧

当前主链仍围绕 claim、coverage、observation link 展开，尚未真正转到 controversy understanding。

彻底修正后的目标：

1. 主链输出从 `claim coverage` 改成 `controversy map`。
2. issue、stance、concern、actor、citation、route、representation、diffusion 成为主分析对象。

### 3.7 Reporting 与 publication 仍未建立在新对象上

当前 reporting 仍较多依赖 handoff 和 wrapper。

彻底修正后的目标：

1. reporting / publication 从 DB canonical 对象重新物化。
2. 议会决策记录可追溯到 proposal、challenge、opinion 与 evidence refs。

## 4. 目标终态架构

彻底修正后的 OpenClaw 应分成五层：

1. `runtime kernel`
   - lifecycle
   - governance
   - execution
   - receipt / ledger / replay
   - persistence / query / audit
2. `council policy / workflow layer`
   - phase assembly
   - fallback policy
   - proposal intake / decision policy
   - optional verification gating
3. `typed planes`
   - signal plane
   - analysis plane
   - deliberation plane
   - reporting plane
4. `agent council loop`
   - proposal
   - challenge
   - readiness opinion
   - decision trace
5. `exports`
   - board summary
   - board brief
   - handoff
   - publication
   - archive package

目标数据主线：

`raw inputs -> typed signals -> controversy analysis objects -> deliberation objects -> decision trace -> reporting exports`

目标职责划分：

1. `runtime`
   - 负责治理、执行、审计与状态持久化。
2. `agents`
   - 负责争议判断、提案、挑战、分诊和 readiness opinion。
3. `database`
   - 负责承载真实状态源。
4. `artifact`
   - 负责导出、handoff 与人类可读展示。

## 5. 本轮硬设计决议

### 5.1 Council judgement belongs to agents

必须成立的边界：

1. runtime 不负责替议会得出唯一动作。
2. agent proposal 默认优先于 heuristic result。
3. challenge、readiness opinion、promotion rationale 必须允许分歧共存。

### 5.2 Runtime kernel must be minimal

kernel 只保留：

1. admission / capability / side-effect policy
2. execution / scheduling / retry / receipt
3. ledger / replay / audit
4. persistence / query surface
5. operator-visible health

以下内容必须迁出 kernel：

1. phase-specific scoring
2. 争议对象语义
3. readiness 主判断逻辑
4. promotion 主判断逻辑
5. 议会阶段固定化编排假设

### 5.3 Database is the canonical state source

必须成立：

1. 所有 council-critical 对象先写 DB。
2. artifact 缺失时，DB-only 仍能恢复等价语义。
3. query surface 能对关键对象做 item-level 读取。

### 5.4 Formal comments are first-class structured inputs

formal comments 不再只是 generic public signal。

至少要具备以下结构：

1. docket
2. agency
3. submitter
4. issue
5. stance
6. concern
7. citation type
8. procedural vs empirical distinction
9. provenance

### 5.5 Heuristics are fallback only

必须成立：

1. heuristic 输出显式标注 `decision_source = heuristic-fallback`。
2. heuristic 只在 agent proposal 缺失、失败或审计模式下触发。
3. heuristic 不再默认驱动 phase-2 judgement。

### 5.6 Verification is an optional lane

observation matching 不再是默认主链。

只有在同时满足以下条件时才进入 verification lane：

1. 问题本质上是 empirical issue。
2. 具有明确时间范围。
3. 具有明确地点范围。
4. 存在可对照的 observation source。

### 5.7 No long-lived dual system

本轮不接受“旧链长期保留、新链逐步加层”的做法。

必须遵守：

1. 旧链只允许作为迁移期 fallback。
2. 每个兼容层都必须带删除条件。
3. 每个 batch 结束后都要减少旧链真实职责。

## 6. 施工总路线

### Batch 0: 冻结旧错误增长

目标：

1. 停止继续把 domain logic 堆进 kernel。
2. 停止继续扩张旧 `claim -> coverage -> readiness` 主链。
3. 明确标记哪些模块属于 legacy。

直接动作：

1. 冻结对旧 claim/coverage 主链的功能扩张。
2. 冻结任何新增 kernel 内 phase-specific 规则。
3. 在文档和代码层给 legacy 模块加迁移说明。

完成标志：

1. 新工作全部围绕新 canonical 对象展开。
2. 新增领域能力不再默认进入 kernel。

### Batch 1: Canonical contract 与 DB schema 重建

目标：

1. 先把 typed objects 和 query surface 定下来。
2. 为后续所有技能改造建立统一落库标准。

必须产出的 canonical 对象：

1. `formal-comment-signal`
2. `public-discourse-signal`
3. `environment-observation-signal`
4. `issue-cluster`
5. `stance-group`
6. `concern-facet`
7. `actor-profile`
8. `evidence-citation-type`
9. `verifiability-assessment`
10. `verification-route`
11. `formal-public-link`
12. `representation-gap`
13. `diffusion-edge`
14. `controversy-map`
15. `proposal`
16. `next-action`
17. `probe`
18. `readiness-opinion`
19. `readiness-assessment`
20. `promotion-basis`
21. `decision-trace`

要求：

1. 每个对象都有 ID、provenance、evidence refs、lineage、decision source。
2. 每个关键对象都能 item-level query。
3. phase-2 对象不再只保存在整包 artifact wrapper 里。

完成标志：

1. 不依赖旧 envelope 也能解释主状态面。
2. DB schema 与 query surface 先于 skill 改造完成。

### Batch 2: Signal plane 彻底 typed 化

目标：

1. 把 signal plane 从 generic normalized signals 扩展到 typed signal contracts。
2. 让 formal comments 真正成为一等输入。

直接动作：

1. 建立 formal comment 的一等 schema 或 typed child objects。
2. 将 public / formal / environment 输入统一纳入 typed signal registry。
3. 保留 raw provenance 与 source-specific metadata。

完成标志：

1. formal comments 不再只靠 `plane='public' + source_skill` 被识别。
2. formal signal 能直接支持 issue / stance / concern / route 分析。

### Batch 3: Public-side 主分析链改写为 controversy chain

目标：

1. 让主分析链从 claim coverage 转向 controversy understanding。
2. 让 issue / stance / concern / actor / citation / route / gap / diffusion 成为默认对象。

直接动作：

1. 重写 claim extractor / cluster / scope 相关语义。
2. 新增 controversy map 物化链。
3. 保留旧 claim 视图仅作为兼容或 fallback，不再作为 canonical 主轴。

完成标志：

1. 系统默认输出不再主要是 claim coverage。
2. formal/public/environment 三类输入可共同落入同一 controversy structure。

### Batch 4: Deliberation plane canonicalization

目标：

1. 让议会关键对象全部进入 deliberation plane。
2. 让 phase-2 从 wrapper 变成可查询的 canonical state。

直接动作：

1. 新增 `proposal / next-action / probe / readiness-opinion / readiness-assessment / promotion-basis / decision-trace` 落库对象。
2. 升级 `hypothesis / challenge / board-task`，使其可锚定 `issue / route / gap / actor / probe / proposal`。
3. 让 board 状态的推进依赖 DB 对象，而不是 summary artifact。

当前状态：

1. `proposal / next-action / probe / readiness-opinion / readiness-assessment / promotion-basis / decision-trace` 已完成 canonical 化与 query surface。
2. `hypothesis / challenge / board-task` 已完成 proposal-first judgement 重写，并补齐 DB judgement metadata。
3. `hypothesis / challenge / board-task` 已完成 canonical contract 与 query surface，对应对象不再只能通过 board artifact 间接读取。
4. board 读取、summary、brief 已能从 deliberation DB 导出；artifact 不再是唯一状态源。

完成标志：

1. 删除中间 artifact 后，round 仍可继续。
2. deliberation plane 能表达提案、分歧、采纳与拒绝。

### Batch 5: 建立 agent council loop

目标：

1. 让 agent 真正成为议会判断主体。
2. 让 runtime 退回治理与执行角色。

直接动作：

1. 定义 `proposal contract`。
2. 定义 `challenge contract`。
3. 定义 `readiness opinion contract`。
4. 定义 `decision trace contract`。
5. 允许多个 agent 对同一 issue 给出不同 readiness opinion。

当前状态：

1. `proposal / challenge / readiness opinion / decision trace` contract 已全部定义。
2. 至少一轮 round 已由 agent proposal 驱动 next actions、probe、board judgement 与 readiness judgement。
3. `openclaw-agent` 轮次进入 phase-2 时，controller 与 agent entry 已默认先尝试 `direct-council-advisory` compiler；只有 direct council inputs 不足或 compiler 失败时才回退 `agent-advisory` planner skill，再失败才回退 runtime planner。
4. `eco-concil-runtime/src/eco_council_runtime/phase2_direct_advisory.py` 已能把 DB 中现成的 `proposal / readiness-opinion / probe` 直接编译成 advisory queue，不再强制经过 planner skill 子进程。
5. `eco-plan-round-orchestration` 在 `agent-advisory` 模式下，若 DB 中已存在直接 `proposal / readiness-opinion`，现在也可以跳过 `next-actions` 重算，直接生成 `readiness-only` 或 `probe -> readiness` 执行队列，作为 direct compiler 不可用时的 fallback。
6. controller 状态与 round-controller ledger 事件已显式记录 `plan_source / planning_attempts`，advisory plan 本身也会暴露 `direct_council_queue / next_actions_stage_skipped / council_input_counts`。
7. heuristic 仍未完全降级为 fallback-only，这一项仍留在后续 batch。

完成标志：

1. 至少一轮 round 由 agent proposal 驱动 next actions 和 readiness judgement。
2. `openclaw-agent` 轮次的 phase-2 controller 默认执行 advisory 路径，而不是先执行 planner-backed queue。
3. advisory plan 在存在直接 council inputs 时，不再强制插入 `next-actions` 重算。
4. heuristic 只在 proposal 缺失时兜底。

### Batch 6: Runtime kernel 收边界

目标：

1. 把 kernel 收回到最小治理/执行内核。
2. 将领域逻辑系统性迁出。

直接动作：

1. 把 phase policy、routing policy、readiness policy、promotion policy 移到 `policy / workflow` 层。
2. 清理 kernel 对固定 stage 语义的依赖。
3. 让 controller 只做 generic execution queue，而不是领域编排器。

当前状态：

1. `controller.py` 已把 `openclaw-agent` 轮次改为 `direct-council-advisory -> agent-advisory -> runtime-planner` 的显式选择链。
2. `agent_entry.py` 也已接入同一条 direct compiler 优先路径，agent 入口不再先调用 planner skill。
3. phase-2 controller artifact 已暴露 `plan_source / planning_attempts / agent_advisory_plan_path`，controller 选择链不再是隐式 planner 语义。
4. `controller.py` 已不再强制注入固定 `promotion-gate` / post-gate 阶段；plan 已能显式声明 `gate_steps / required_previous_stages / stage_kind / gate_handler`，controller 按计划执行。
5. `phase2_contract.py` 已降级为 known-stage default metadata / compatibility fallback，显式 plan 依赖可以覆盖内置依赖。
6. `promotion-gate` 的执行分派、readiness 依赖解析与 controller 状态更新已迁入 `kernel/gate.py`；`controller.py` 现在只消费统一 `gate_result`。
7. `next_actions / probes / readiness` 的 DB/artifact read surface 已抽到 `kernel/phase2_state_surfaces.py`，`gate.py / supervisor.py / benchmark.py` 已不再直接依赖 `investigation_planning.py`。
8. `controller.py / gate.py / investigation_planning.py` 仍明显绑定 readiness/promotion 语义与既有 gate handler，这一块仍是后续收边界的主战场。

完成标志：

1. 新增领域流程不再需要默认修改 kernel。
2. kernel 的职责可在代码与测试中清楚划定。

### Batch 7: Board / reporting / publication 重建

目标：

1. 让 board、reporting、publication 默认消费新 canonical 对象。
2. 让 artifact 真正退回 export-only 地位。

直接动作：

1. `board summary / board brief` 改成纯 DB 导出。
2. `reporting handoff / council decision / expert report / publication` 默认从 DB 物化。
3. 让决策文稿可回溯到 proposal、challenge、readiness opinion、promotion basis。

完成标志：

1. reporting 不再依赖旧 coverage 主线。
2. publication 可从 DB-only 状态重建。

### Batch 8: Legacy chain 清场与 benchmark 证明

目标：

1. 删除旧兼容债。
2. 用 benchmark 证明方向已真正改变。

直接动作：

1. 删除旧 claim/coverage 默认主链地位。
2. 删除 artifact-only phase-2 流程假设。
3. 删除 formal comment generic-signal 假设。
4. 准备争议型政策 case、混合型争议 case、可核实事件 case。

完成标志：

1. OpenClaw 已不再只是事件核实器。
2. OpenClaw 也不再只是 DB-first workflow engine。

## 7. 仓库内的必要拆改方向

本轮不是只改 skill 输出，而是要做仓库级拆改。

### 7.1 `kernel/` 目录的收缩方向

以下模块必须收边界或迁出：

1. `phase2_contract.py`
   - 从固定领域阶段定义，收缩为 generic dependency / execution contract，或迁入 workflow policy。
2. `controller.py`
   - 从 phase-2 领域 controller，收缩为 generic queue executor + receipt writer。
3. `investigation_planning.py`
   - 迁出 kernel，拆成 `policy / workflow / heuristic_fallback`。
4. `agent_entry.py`
   - 从 advisory-first 入口，改成 governed proposal intake 与 council-facing surface。

### 7.2 `analysis_plane.py` 的改造方向

必须完成：

1. typed analysis kinds 的 schema 扩展。
2. 新 controversy objects 的 query surface。
3. 旧 claim coverage 对象降级为 fallback 或兼容视图。

### 7.3 `deliberation_plane.py` 的改造方向

必须新增：

1. `proposal`
2. `next-action`
3. `probe`
4. `readiness-opinion`
5. `readiness-assessment`
6. `promotion-basis`
7. `decision-trace`

必须升级：

1. `hypothesis`
2. `challenge`
3. `board-task`

### 7.4 `skills/` 目录的改造方向

必须重写：

1. 旧 claim 主链 skills
2. phase-2 skills
3. board / reporting / publication skills

必须新增：

1. controversy extraction/materialization skills
2. agent proposal / challenge / readiness opinion skills
3. DB-native reporting materializers

### 7.5 `tests/` 的改造方向

本轮不以保留旧测试为目标。

必须完成：

1. 删除或重写固化旧 coverage-first 语义的测试。
2. 新增 DB-only recovery tests。
3. 新增 agent proposal-driven round tests。
4. 新增 kernel boundary tests。
5. 新增 optional verification lane tests。

## 8. 实施顺序上的硬约束

本轮必须遵守以下顺序：

1. 先定 canonical contract，再改技能输出。
2. 先让 phase-2 对象 DB-native，再重写 reporting。
3. 先建立 agent proposal / opinion loop，再真正降 heuristic。
4. 先把领域逻辑迁出 kernel，再宣布 kernel 已最小化。
5. 先删旧主链默认地位，再讨论是否保留其 fallback 形式。

不允许发生的顺序错误：

1. 在 contract 不清时继续扩 skill。
2. 在 artifact 仍是状态源时先改 publication。
3. 在 agent 仍是 advisory-only 时就宣称实现了 council autonomy。
4. 在 kernel 仍承载 domain policy 时就宣称完成了 runtime 抽象。

## 9. 本轮不追求的东西

以下内容本轮不作为主目标：

1. 更复杂的多 session agent platform。
2. 新一轮大规模数据源扩张。
3. publication 样式继续打磨。
4. 为了兼容而保留旧接口的长期稳定性。

这不是说这些内容不重要，而是说它们不能取代对 autonomy、kernel、contracts、DB-native 的彻底修正。

## 10. 最终验收标准

只有当以下条件同时满足时，才能说本轮彻底修正完成：

1. `agent autonomy`
   - 至少一轮 round 由 agent 提出 `proposal / challenge / readiness opinion` 并驱动 next steps。
2. `minimal kernel`
   - kernel 中不再保留 readiness / promotion / controversy judgement 的主语义。
3. `DB-native council state`
   - 删除 `next_actions / probes / readiness / board_summary / board_brief` artifact 后，round 仍能推进。
4. `first-class formal comments`
   - formal comments 已成为结构化输入，不再只是 generic public signal。
5. `controversy-first analysis`
   - 主链默认输出已从 claim coverage 切换到 controversy map。
6. `queryable phase-2`
   - `proposal / next-action / probe / readiness-opinion / readiness-assessment / promotion-basis / decision-trace` 都可 item-level 查询。
7. `heuristic demotion`
   - heuristic 只在 fallback / audit / guardrail 模式下触发，并有显式 trace。
8. `optional verification`
   - observation matching 只在明确 empirical route 下触发。
9. `DB-native reporting`
   - reporting / publication 默认从 DB canonical 对象重建。
10. `legacy debt removed`
   - 旧 claim/coverage 默认主链、artifact-only phase-2、generic formal comment 识别等兼容债已删除。
