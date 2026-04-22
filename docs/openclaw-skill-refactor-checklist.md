# OpenClaw 彻底重构执行清单

## 1. 使用方式

本清单不是“兼容性维护目录”，而是彻底重构的执行清单。

使用原则：

1. 只要旧实现阻碍目标架构，就改写或删除。
2. 只要兼容层开始变成长期依赖，就判定为失败。
3. 每一项完成都必须减少旧链真实职责，而不是只新增一条平行路径。

## 2. 硬迁移规则

### 2.1 可以 breaking change

本轮允许：

1. 改 schema
2. 改 query surface
3. 改 skill 输出
4. 删旧 artifact wrapper
5. 重写测试

### 2.2 不以稳定性和兼容性为目标

本轮不接受以下说法：

1. “先保留旧链，之后再看要不要删”
2. “先不动 kernel，外面再包一层”
3. “先让 artifact 继续工作，等后面再 DB-native”

### 2.3 一次只允许一个 canonical

每类对象只能有一个真实 canonical shape。

允许存在：

1. 迁移期兼容视图
2. export-only envelope
3. fallback-only heuristic object

不允许存在：

1. 两套长期并行 canonical
2. 旧 wrapper 和新对象都被当成真实状态源

### 2.4 任何兼容层都必须带删除条件

每个兼容层必须明确：

1. 保留理由
2. 退出条件
3. 删除时机

### 2.5 Batch 1 当前状态

- `已完成` deliberation canonical contract registry、council object query surface、reporting DB 恢复链。
- `已完成` `next-action / probe / readiness-assessment / promotion-basis` 主存储入口 canonical 化与 fallback source 显式标注。
- `已完成` `eco-propose-next-actions` 对 `proposal` 的优先消费。
- `已完成` `eco-summarize-round-readiness` 对 `readiness-opinion` 的优先消费。
- `已完成` phase-2 controller / post-round 的 DB-first 控制读取。

### 2.6 Batch 2 当前状态

- `已完成` `eco-promote-evidence-basis` 对 `proposal / readiness-opinion` 的 judgement 吸收，并产出 `supporting_* / rejected_* / council_input_counts`。
- `已完成` `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-publish-council-decision / eco-materialize-final-publication` 对 trace 链字段的显式透传。
- `已完成` canonical `decision-trace` 写库、查询与 final publication 暴露。
- `已完成` `tests/test_decision_trace_workflow.py`，覆盖 ready/hold 两类 decision trace 工作流。

### 2.7 Batch 3 当前状态

- `已完成` `eco-open-falsification-probe` 对 council proposal 的直接消费，probe 打开不再必须依赖 `next_actions` wrapper。
- `已完成` proposal-first probe candidate 合并逻辑；存在 proposal 时优先于 DB-backed heuristic action。
- `已完成` canonical probe 对 `decision_source / provenance / lineage / source_ids` 的显式继承。
- `已完成` `tests/test_council_autonomy_flow.py` 中的 council-driven probe autonomy 回归。

### 2.8 Batch 4 当前状态

- `已完成` `board_proposal_support.py`，board judgement 现在直接消费 DB 中的 council proposal，并统一生成 canonical judgement metadata。
- `已完成` `hypothesis_cards / challenge_tickets / board_tasks` 的 `decision_source / evidence_refs_json / source_ids_json / provenance_json / lineage_json` 落库与迁移。
- `已完成` `[重写]` `eco-open-challenge-ticket / eco-close-challenge-ticket / eco-update-hypothesis-status / eco-claim-board-task` 的 proposal-first 执行路径。
- `已完成` `hypothesis / challenge / board-task` canonical contract 与 `query-council-objects` 查询面。
- `已完成` proposal-only board workflow 回归，覆盖 hypothesis update、challenge open、challenge close、board task claim，并断言 DB 列与 `raw_json` judgement metadata。
- `已完成` 本地大回归 `75` 项通过，board proposal-first 改造未击穿 council / reporting / runtime 主链。

### 2.9 Batch 5 当前状态

- `已完成` 新增 `eco_council_runtime/phase2_promotion_resolution.py`，统一 promotion-stage council proposal / readiness opinion 的 resolution surface。
- `已完成` `eco-promote-evidence-basis` 已移除 skill 内部固定 promotion support kind 白名单；现在优先消费 proposal 内显式 `promotion_disposition / promote_allowed / publication_readiness / handoff_status / moderator_status` judgement，legacy kind 仅保留为 compatibility fallback。
- `已完成` `promotion-gate` 现在会把 `rejected_proposal_ids / supporting_proposal_ids / promotion_resolution_mode / council_input_counts` 写入 gate snapshot，议会 veto 已进入 controller 可见面。
- `已完成` `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-publish-council-decision` 已显式透传 `rejected_proposal_ids / promotion_resolution_mode / promotion_resolution_reasons / council_input_counts`。
- `已完成` decision trace / final publication 现在可以把 veto proposal 作为 selected object 落库并对外导出，不再只能通过 readiness opinion 表达 withheld 路径。
- `已完成` `tests/test_decision_trace_workflow.py` 已新增 explicit support proposal 与 explicit veto proposal 回归；当前相关大回归 `127` 项全部通过。

### 2.10 Batch 6 当前状态

- `已完成` `council_objects.py` 已新增 append/upsert proposal/readiness 原语，agent 可以逐条提交 canonical council object，而不是依赖整轮 replace bundle。
- `已完成` 新增 `eco-submit-council-proposal / eco-submit-readiness-opinion`，默认直接写 DB canonical `proposal / readiness-opinion`，并保留 `target / evidence_refs / response_to_ids / lineage / provenance / promotion_*` judgement 字段。
- `已完成` `canonical_contracts.py` 已收紧 `proposal / readiness-opinion` 契约，`status / opinion_status / response_to_ids / basis_object_ids` 已进入强校验面。
- `已完成` `phase2_agent_entry_profile.py / phase2_agent_handoff.py / kernel/agent_entry.py` 已把默认 agent write path 改为 submission skills，并显式暴露 proposal/readiness 的 query/template command。
- `已完成` `phase2_direct_advisory.py / eco-plan-round-orchestration / eco-summarize-round-readiness / eco-propose-next-actions / eco-open-falsification-probe` 的 follow-up guidance 已开始从 `eco-post-board-note` 转向结构化 submission。
- `已完成` 新增 `tests/test_council_submission_workflow.py`，并补强 `tests/test_agent_entry_gate.py / tests/test_council_autonomy_flow.py`；当前扩展后的大回归 `130` 项全部通过。

### 2.11 Batch 7 当前状态

- `已完成` `phase2_promotion_resolution.py` 已移除 legacy promotion support compatibility；旧 `proposal_kind / action_kind` 名称本身不再授予 promotion support 语义。
- `已完成` legacy 输入现在会被显式标记为 `ignored-implicit-promotion-kind`，并写进 `proposal_resolution_records / proposal_resolution_mode_counts`，而不是静默兼容。
- `已完成` `eco-promote-evidence-basis` 会对这类旧输入发出 `ignored-implicit-promotion-kind` warning，promotion artifact 已能审计残留旧写法。
- `已完成` `eco-materialize-reporting-handoff / eco-draft-council-decision / eco-draft-expert-report / eco-publish-expert-report / eco-publish-council-decision` 的 hold-path `suggested_next_skills` 已从 `eco-post-board-note` 转向 `eco-submit-council-proposal / eco-submit-readiness-opinion`。
- `已完成` `eco-promote-evidence-basis / eco-summarize-round-readiness` 的 skill docs 与 agent prompts 已改写为“explicit DB judgement first”。
- `已完成` 已新增 “legacy named promotion proposal is ignored” 与 reporting/publication hold-path guidance 回归；当前扩展后的大回归 `131` 项全部通过。

### 2.12 Batch 8 当前状态

- `已完成` 新增 `phase2_action_semantics.py`，`readiness_blocker` 已成为 planner / readiness / promotion 共享语义，不再依赖 `prepare-promotion` 旧 action kind 特判。
- `已完成` `phase2_fallback_policy.py` 的默认空 agenda cue 已改为 `open-council-readiness-review`，语义从“隐式 promotion cue”切成“显式 council readiness review cue”。
- `已完成` 新增 `reporting_status.py`，`eco-materialize-reporting-handoff / eco-draft-council-decision / eco-draft-expert-report / phase2_posture_profile` 现在共享 `reporting_ready / reporting_blockers / handoff_status` 判定层。
- `已完成` promoted supervisor status 已统一到 `reporting-ready`，handoff hold 状态已统一到 `investigation-open`。
- `已完成` `phase2_state_surfaces.py` 已新增 `load_supervisor_state_wrapper`；删掉 `runtime/supervisor_state_*.json` 后，reporting handoff 仍可从 deliberation DB 恢复 supervisor state。
- `已完成` `deliberation_plane.py` 已把 `readiness_blocker / reporting_ready / reporting_blockers / decision_gating` 提升成表列并补迁移，不再只藏在 `raw_json`。

### 2.13 Batch 9 当前状态

- `已完成` `phase2_state_surfaces.py` 已新增 `build_reporting_surface(...)`，supervisor / handoff / decision / report wrapper 现在共享同一套显式 reporting gate surface。
- `已完成` `kernel/supervisor.py` 现在会把 `reporting_ready / reporting_blockers / reporting_handoff_status` 直接写入 supervisor snapshot / promotion freeze，不再只能靠后续 handoff 反推。
- `已完成` `kernel/cli.py` 现已新增 `show-reporting-state`，`show-run-state` 也新增 top-level `reporting` section；operator 可直接查看 DB-first reporting surface。
- `已完成` `query-council-objects` 已支持 `--readiness-blocker-only`；`moderator_actions.readiness_blocker` 现在可直接通过 query surface 过滤。
- `已完成` `phase2 operator / post-round operator / benchmark operator` 现已显式暴露 `reporting_ready / reporting_blockers / reporting_handoff_status`，不再只透出 `promotion_status`。
- `已完成` `post_round.py / benchmark.py` 已切到 shared reporting surface；`round_close / benchmark_manifest` 已显式写出 reporting gate 字段与 `reporting_surface_source`。

### 2.14 Batch 10 当前状态

- `已完成` 新增 `eco_council_runtime/reporting_objects.py`，reporting plane 现在拥有独立 `query-reporting-objects` query surface，不再继续复用 deliberation query namespace。
- `已完成` `kernel/cli.py` 已新增 `query-reporting-objects` 与 `list-canonical-contracts --plane reporting`；`show-reporting-state` operator 也已补上 reporting query command templates。
- `已完成` `store_reporting_handoff_record / store_council_decision_record / store_expert_report_record / store_final_publication_record` 现已统一执行 canonical normalization + `validate_canonical_payload(...)`；DB `raw_json` 已从 skill `e1.x` envelope 切到 reporting canonical schema。
- `已完成` `canonical_contracts.py` 已收紧 reporting plane 契约，`evidence_refs / lineage / provenance` 已进入 reporting objects 的硬校验面，decision 还显式要求 `decision_trace_ids / published_report_refs` 等结构字段。
- `已完成` 新增 `tests/test_reporting_query_surface.py`，覆盖 reporting query surface、operator query commands 与 DB canonical raw_json；当前扩展后的大回归 `141` 项全部通过。

### 2.15 Batch 11 当前状态

- `已完成` `kernel/phase2_state_surfaces.py` 的 reporting wrappers 已改成 `DB-only`；artifact-only 文件不再被当成状态源，而是显式标记为 `orphaned-...-artifact`。
- `已完成` decision / expert-report wrapper 不再丢弃 `record_id / decision_stage / report_stage`；wrapper 现在暴露完整 reporting canonical row。
- `已完成` 六个 reporting skill 已统一按 `store_*_record(...)` 返回的 canonical payload 落盘；artifact 与 DB `raw_json` 已不再分叉。
- `已完成` `eco-publish-expert-report / eco-publish-council-decision` 已显式固定 canonical stage，并在 publish 时清空 draft 继承的 `record_id / provenance`，修正 draft row 被 canonical publish 顶掉的问题。
- `已完成` 新增 `eco_council_runtime/reporting_exports.py` 与 CLI `materialize-reporting-exports`，可从 DB 重建全部八个 reporting 导出物；`show-reporting-state` operator 已补上对应 command template。
- `已完成` `tests/test_reporting_query_surface.py / tests/test_reporting_publish_workflow.py` 已补强 artifact=DB 同构、orphaned artifact、export rebuild、publish block on orphaned draft 等回归；当前扩展后的大回归 `144` 项全部通过。

### 2.16 Batch 12 当前状态

- `已完成` `kernel/phase2_state_surfaces.py` 的 `next-actions / falsification-probes / round-readiness / promotion-basis / supervisor-state` wrapper 已全部改成 `DB-only`；artifact-only 文件会被显式标记为 `orphaned-...-artifact`，不再回流成 phase-2 payload。
- `已完成` 新增 `eco_council_runtime/phase2_exports.py` 与 CLI `materialize-phase2-exports`，可从 DB 重建 `next_actions / falsification_probes / round_readiness / promoted_evidence_basis / supervisor_state` 五个 phase-2 导出物；`show-run-state` phase-2 operator 已补上对应 command template 与 query commands。
- `已完成` `eco-materialize-final-publication` 已切到 `load_supervisor_state_wrapper(...)`；publication 不再旁路直读 supervisor artifact，而是优先走 `promotion_freeze -> supervisor_snapshot`。
- `已完成` `kernel/controller.py` 已删除一个残留的 `promotion_basis` artifact fallback；controller completion 现在不会再用旧 export 回填 `promotion_status`。
- `已完成` `tests/test_phase2_state_surfaces.py / tests/test_runtime_kernel.py / tests/test_orchestration_planner_workflow.py / tests/test_board_workflow.py / tests/test_reporting_publish_workflow.py` 已补强 phase-2 orphaned-artifact、export rebuild、publication supervisor DB recovery 与 DB-canonical test seed；当前扩展后的大回归 `148` 项全部通过。

### 2.17 Batch 13 当前状态

- `已完成` `kernel/signal_plane_normalizer.py` 现在会持久化并迁移 `canonical_object_kind`；signal plane 的 typed contract 已不再停留在文档和 runtime 内存层。
- `已完成` `eco-normalize-regulationsgov-comments-public-signals / eco-normalize-regulationsgov-comment-detail-public-signals` 已切到 `plane = formal`、`canonical_object_kind = formal-comment-signal`；formal comments 不再伪装成 generic public rows。
- `已完成` `eco-link-formal-comments-to-public-discourse / eco-detect-cross-platform-diffusion / eco-extract-claim-candidates` 已切到结构化 formal/public 识别，不再依赖 `plane='public' + source_skill` 的历史假设。
- `已完成` 遗留 standalone public/environment normalizer（`youtube-video / bluesky / gdelt-doc / airnow / openaq / open-meteo`）已同步补上 `canonical_object_kind` 持久化与统一 schema migration。
- `已完成` `source_queue_profile` 已把默认主链改成 `claim-scope -> verifiability -> route -> controversy-map`，并把 observation extract/merge/link/scope/coverage 降级为 `route-gated optional lane`。
- `已完成` `tests/test_formal_public_workflow.py / tests/test_migrated_source_runtime_integration.py / tests/test_runtime_source_queue_profiles.py` 已补上 formal-plane、typed signal、optional verification 的结构性回归；本轮 targeted `17` 项与扩展 workflow `87` 项均已本地通过。

### 2.18 Batch 14 当前状态

- `已完成` 新增 `eco-query-formal-signals`，`formal` plane 现在拥有与 `public / environment` 对称的独立 query surface；可直接按 `source_skill / signal_kind / published window / docket_id / agency_id / keyword` 查询。
- `已完成` `phase2 operator / agent entry operator / 默认 role read path` 已全部接入 `query_formal_signals_command`；formal signal 不再只能靠底层表或历史 source whitelist 间接访问。
- `已完成` `phase2_fallback_agenda.py / phase2_fallback_policy.py / phase2_fallback_context.py` 已把 empirical blocker 进一步改成 `route-gated`：只有显式 routed 到 `environmental-observation` 的问题才会被 coverage/support 继续卡住；纯 `formal / discourse / stakeholder` round 不再因为缺少另一侧 material 或 coverage 被硬阻塞。
- `已完成` `eco-summarize-round-readiness` 已切成 `lane-aware readiness`：主判断面现在是 `issue / route / linkage / representation / diffusion / council opinions`，coverage 退为 observation lane supporting posture。
- `已完成` `eco-promote-evidence-basis` 已切成 `lane-aware promotion freeze`：`verification_routes` 现在会冻结 empirical routes，自身成为 basis object；coverage 只在 `route-gated empirical lane` 或 `legacy no-structure fallback` 时进入 `selected_coverages`。
- `已完成` `eco-materialize-reporting-handoff` 已补上 structural-basis key findings fallback；纯 formal/public/discourse promoted round 即使没有 selected coverages，也能从 `issue_clusters / routes / links / gaps / edges` 生成 reporting handoff findings。
- `已完成` 本轮新增/更新回归已覆盖 `formal signal query surface`、`agent entry/operator commands`、`phase2 operator surface`、`non-empirical ready+promote+reporting handoff`、`investigation/reporting/runtime kernel` 主链；本地验证通过：
  - `tests/test_signal_plane_workflow.py`
  - `tests/test_agent_entry_gate.py`
  - `tests/test_phase2_state_surfaces.py`
  - `tests/test_deliberation_agenda_workflow.py`
  - `tests/test_formal_public_workflow.py`
  - `tests/test_diffusion_workflow.py`
  - `tests/test_council_autonomy_flow.py`
  - `tests/test_investigation_workflow.py`
  - `tests/test_reporting_workflow.py`
  - `tests/test_reporting_publish_workflow.py`
  - `tests/test_runtime_kernel.py`
  - `tests/test_analysis_workflow.py`

### 2.19 Batch 15 当前状态

- `已完成` `canonical_contracts.py` 已新增 `claim-candidate / claim-cluster / claim-scope` 强契约，并把 `required_number_fields` 提升成一等校验面；claim-side analysis object 不再只有“有几个字符串字段就算过关”。
- `已完成` 新增 `eco_council_runtime/analysis_objects.py`，把 `claim-candidate / claim-cluster / claim-scope / verifiability-assessment / verification-route` 的 canonical normalization 收口到统一入口；`decision_source / confidence / rationale / provenance / evidence_refs / lineage` 现在不再分散在多个 skill 里各自拼接。
- `已完成` `kernel/analysis_plane.py` 现在会在 sync 阶段对上述 analysis object 做强校验，并把 `decision_source / lineage_json / provenance_json` 落到 `analysis_result_items`；DB 丢失 artifact 后保留的已不再只是弱 `item_json`。
- `已完成` `eco-extract-claim-candidates / eco-cluster-claim-candidates / eco-derive-claim-scope` 已重写为 canonical claim-side object 输出：
  - `claim-candidate` 现在直接写出 `evidence_refs / lineage / rationale / provenance / confidence`。
  - `claim-cluster` 现在显式持久化 `source_signal_ids`，并把 canonical `evidence_refs` 取代旧 `public_refs-only` DB lineage。
  - `claim-scope` 现在显式标注 `claim_input_kind / claim_object_id / basis_claim_ids / source_signal_ids`，不再把上游对象语义全部挤进一个模糊 `claim_id`。
- `已完成` `eco-classify-claim-verifiability / eco-route-verification-lane` 已修正 artifact-ref 证据链错误：`evidence_refs` 不再被 `unique_texts()` 串化成伪字符串，而是继续作为 artifact-ref dict 在 `verifiability / route / controversy-map` 主链中流转。
- `已完成` `eco-link-claims-to-observations / eco-link-formal-comments-to-public-discourse / eco-materialize-controversy-map` 已改成 `evidence_refs-first` 读取；`public_refs` 降级为兼容 alias，而不是 canonical evidence 面。
- `已完成` 本轮本地验证通过：
  - `tests/test_canonical_contracts.py`
  - `tests/test_analysis_workflow.py`
  - `tests/test_controversy_workflow.py`
  - `tests/test_runtime_kernel.py`
  - `tests/test_investigation_workflow.py`
  - `tests/test_formal_public_workflow.py`

### 2.20 Batch 16 当前状态

- `已完成` `canonical_contracts.py` 已把 `formal-public-link / representation-gap / diffusion-edge / controversy-map` 升级为强契约对象：
  - 现在会硬校验 `rationale / provenance / evidence_refs / lineage`。
  - `alignment_score / severity_score / confidence / member_count` 等关键数值字段已进入 `required_number_fields`。
- `已完成` `eco_council_runtime/analysis_objects.py` 已新增四类 canonical normalization helper，并统一 controversy 主链的 `decision_source / rationale / provenance / evidence_refs / lineage / score` 归一化。
- `已完成` `kernel/analysis_plane.py` 已把上述四类结果集接入 `canonical_object_kind` 强校验与更完整的 parent lineage：
  - `formal-public-link` 现在会显式保留 `claim_scope_ids / assessment_ids / route_ids`。
  - `diffusion-edge` 现在会显式保留 `linkage_ids / claim_scope_ids / assessment_ids / route_ids`。
  - `controversy-map` 现在会把 `claim_scope_id / assessment_id / route_id / source_signal_ids` 写入 item row 与 query surface。
- `已完成` `eco-link-formal-comments-to-public-discourse / eco-identify-representation-gaps / eco-detect-cross-platform-diffusion / eco-materialize-controversy-map` 已改成 canonical object 输出；controversy 链不再把弱 wrapper dict 直接 sync 进 DB。
- `已完成` controversy 主链的 DB-native 恢复验证已补齐：
  - 删掉 `formal_public_links / representation_gaps / diffusion_edges / controversy_map` artifact 后，`query_analysis_result_items(...)` 与 runtime kernel query surface 仍能从 `analysis_result_items` 恢复完整对象。
  - `eco-link-formal-comments-to-public-discourse` 已修正 `route.claim_id = cluster_id` 时 route 元数据丢失的问题；formal/public linkage 不再漏掉 `route_ids / assessment_ids / claim_scope_ids` 父链。
- `已完成` 本轮本地验证通过：
  - `tests/test_canonical_contracts.py`
  - `tests/test_analysis_workflow.py`
  - `tests/test_formal_public_workflow.py`
  - `tests/test_diffusion_workflow.py`
  - `tests/test_controversy_workflow.py`
  - `tests/test_runtime_kernel.py`

### 2.21 Batch 17 当前状态

- `已完成` controversy typed decomposition 已真正落到 analysis plane：
  - `issue-cluster`
  - `stance-group`
  - `concern-facet`
  - `actor-profile`
  - `evidence-citation-type`
- `已完成` `canonical_contracts.py` 已把上述五类对象从“只有标签字段的弱对象”升级为强契约：
  - `rationale / provenance / evidence_refs / lineage` 进入硬校验面。
  - `member_count / share_ratio / affected_claim_count / claim_count / source_signal_count / confidence` 等关键数值字段进入 `required_number_fields`。
- `已完成` `eco_council_runtime/analysis_objects.py` 已新增五类 canonical normalization helper，并统一 controversy typed issue layer 的默认值、lineage、decision source 与 score 计算。
- `已完成` `kernel/analysis_plane.py` 已新增五类 result-set config、sync helper、load context：
  - 新对象支持 item-level query。
  - 新对象支持 parent result-set / parent artifact / parent id lineage。
  - 删掉 typed artifact 后，kernel query surface 仍能从 DB 恢复对象。
- `已完成` `eco-materialize-controversy-map` 不再只产出一个 `controversy_map` wrapper：
  - 现在会同时派生并同步 `issue_clusters / stance_groups / concern_facets / actor_profiles / evidence_citation_types` 五组 typed artifact。
  - `controversy-map` 保留为 routing / readiness-facing 高层对象；议会 issue layer 改由 `issue-cluster` 充当 canonical DB surface。
- `已完成` `load_d1_shared_context` 已切到 `issue-cluster-first`：
  - board / agenda / promotion / reporting 共享上下文优先读取 `issue-cluster` DB row，而不是直接把 `controversy-map` wrapper 当作议会 issue object。
  - `stance_groups / concern_facets / actor_profiles / evidence_citation_types` 也已进入 shared context 暴露面。
- `已完成` 修复 analysis plane 一处结构性 lineage bug：
  - `analysis_result_lineage.lineage_id` 之前未把 `result_set_id` 纳入签名，不同 analysis kind 的 query-basis / parent-artifact row 会互相覆盖。
  - 现已改为 result-set scoped lineage id，typed controversy result-set 的 parent result-set contract 不再丢失。
- `已完成` 本轮本地验证通过：
  - `tests/test_canonical_contracts.py`
  - `tests/test_controversy_workflow.py`
  - `tests/test_runtime_kernel.py`
  - `tests/test_deliberation_agenda_workflow.py`
  - `tests/test_reporting_workflow.py`
  - `tests/test_phase2_state_surfaces.py`

### 2.22 Batch 18 当前状态

- `已完成` 新增 `eco_council_runtime/formal_signal_semantics.py`，formal comment 的 `submitter / issue / stance / concern / citation / route` typed 语义已收口到统一 extractor；列表页与详情页 normalizer 不再各写一套互相漂移的规则。
- `已完成` `eco-normalize-regulationsgov-comments-public-signals / eco-normalize-regulationsgov-comment-detail-public-signals` 现在都会把以下 typed 字段直接写入 `formal-comment-signal` metadata：
  - `submitter_name / submitter_type`
  - `issue_labels / issue_terms`
  - `stance_hint`
  - `concern_facets`
  - `evidence_citation_types`
  - `route_hint / route_status_hint`
  - `decision_source = heuristic-fallback / typing_method`
- `已完成` `kernel/signal_plane_normalizer.py` 已新增 `normalized_signal_index`：
  - formal typed 维度不再只躺在 `metadata_json` blob 里。
  - `docket / agency / submitter / issue / concern / citation / stance / route` 已有可查询的 DB index surface。
- `已完成` `eco-query-formal-signals` 已升级为 typed formal query surface：
  - 新增 `submitter_type / issue_label / concern_facet / citation_type / stance_hint / route_hint` 过滤。
  - 返回结果会直接暴露上述 structured fields，而不是只给 `docket / agency`。
- `已完成` `eco-link-formal-comments-to-public-discourse` 已改成优先消费 DB 中的 formal typed metadata：
  - issue assignment 优先读取 signal 自带 `issue_labels / issue_terms`。
  - profile lane/concern/actor votes 会吸收 `route_hint / concern_facets / submitter_type`，不再只靠正文临时猜。
- `已完成` 本轮本地验证通过：
  - `tests/test_signal_plane_workflow.py`
  - `tests/test_formal_public_workflow.py`
  - `tests/test_migrated_source_runtime_integration.py`
  - `tests/test_runtime_kernel.py`
  - `tests/test_phase2_state_surfaces.py`
  - `tests/test_deliberation_agenda_workflow.py`
  - `tests/test_diffusion_workflow.py`

### 2.23 Batch 19 当前状态

- `已完成` 新增 runtime/control canonical object registry：
  - `promotion-freeze`
  - `controller-state`
  - `gate-state`
  - `supervisor-state`
- `已完成` `promotion_freezes` 已补齐 `reporting_ready / reporting_handoff_status / reporting_blockers` 列；控制冻结面不再只靠 `raw_json` 承载 reporting gate 语义。
- `已完成` deliberation DB 已新增 `controller_snapshots / gate_snapshots / supervisor_snapshots` 三张独立表：
  - `controller / gate / supervisor` 不再只是 `promotion_freeze.raw_json` 里的嵌套 blob。
  - `store_promotion_freeze_record(...)` 现在会同时写聚合 freeze row 与独立控制面 row。
- `已完成` 新增 `eco_council_runtime/control_objects.py` 与 CLI `query-control-objects`：
  - runtime control plane 现在拥有与 deliberation / reporting 对称的一等 query surface。
  - 支持 `controller-status / gate-status / promotion-status / supervisor-status / planning-mode / stage-name / gate-handler / reporting-ready-only` 等过滤。
- `已完成` `kernel/phase2_state_surfaces.py` 已新增 `load_controller_state_wrapper / load_promotion_gate_wrapper`，并把 `load_supervisor_state_wrapper` 升级为优先消费独立 control rows：
  - `show-run-state` 不再把 `controller / gate / supervisor` 只当 artifact/freeze summary 读取。
  - phase-2 operator 现已显式暴露 `query_controller_state_command / query_gate_state_command / query_supervisor_state_command / query_promotion_freeze_command`。
- `已完成` control query surface 已补上 DB-authoritative 回归：
  - `tests/test_control_query_surface.py` 会故意篡改 `controller_snapshots / gate_snapshots / supervisor_snapshots / promotion_freezes` 的 `raw_json`，并验证查询结果仍由 DB 列恢复。
  - `tests/test_runtime_kernel.py / tests/test_phase2_state_surfaces.py` 已同步补上 show-run-state operator command 与 control-row/orphaned-artifact 语义。
- `已完成` 本轮本地验证通过：
  - `tests/test_control_query_surface.py`
  - `tests/test_council_query_surface.py`
  - `tests/test_council_autonomy_flow.py`
  - `tests/test_council_submission_workflow.py`
  - `tests/test_investigation_workflow.py`
  - `tests/test_deliberation_agenda_workflow.py`
  - `tests/test_phase2_state_surfaces.py`
  - `tests/test_runtime_kernel.py`
  - 扩展相关回归共 `67` 项，本地全部通过。

### 2.24 Batch 20 当前状态

- `已完成` 新增 `eco_council_runtime/controversy_issue_surfaces.py`，claim-side issue clustering、typed issue decomposition、controversy map aggregation 的共享 builder 已从 skill 脚本内联逻辑中抽离。
- `已完成` 新增独立 typed controversy issue skills：
  - `eco-cluster-issue-candidates`
  - `eco-extract-stance-candidates`
  - `eco-extract-concern-facets`
  - `eco-extract-actor-profiles`
  - `eco-extract-evidence-citation-types`
- `已完成` `eco-materialize-controversy-map` 已从“大一统 extractor”收缩为 `DB-first` 聚合器：
  - 优先读取 `issue-cluster / stance-group / concern-facet / actor-profile / evidence-citation-type`。
  - 只有 typed surface 缺失时才内联补齐，并显式发出 compatibility warning。
- `已完成` `kernel/analysis_plane.py` 的 parent contract 已重排：
  - `issue-cluster` 现在直接指向 `claim-cluster / claim-scope / claim-verifiability / verification-route`，不再把 `controversy-map` 当唯一父面。
  - `stance-group / concern-facet / actor-profile / evidence-citation-type` 现在只以 `issue-cluster` 为父面。
  - `controversy-map` 现在反向依赖 typed issue surfaces，而不是继续直接锚在 claim-side chain。
- `已完成` `source_queue_profile.py` 与 verifiability/route follow-up hints 已改写：
  - `eco-route-verification-lane` 下游现在优先指向 `eco-cluster-issue-candidates`。
  - typed extractor 已成为一等 queue-visible capability，而不是只能由 `eco-materialize-controversy-map` 顺手派生。
- `已完成` 本轮本地验证通过：
  - `python3 -m unittest tests.test_controversy_workflow -v`
  - `python3 -m unittest tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_queries_controversy_map_items_when_artifact_is_missing tests.test_runtime_kernel.RuntimeKernelTests.test_kernel_queries_issue_cluster_items_when_artifact_is_missing tests.test_runtime_source_queue_profiles -v`

### 2.25 Batch 21 当前状态

- `已完成` 新增 `eco_council_runtime/issue_cluster_skill_runner.py`，`issue-cluster` 生成链已从 skill 脚本进一步下沉成共享 runtime helper。
- `已完成` 新增 `eco-extract-issue-candidates`：
  - 现在可以先从 `claim-scope / verifiability / route` 派生 scope-level `issue-cluster` candidate。
  - 默认写出 `analytics/issue_candidates_<round_id>.json`，并同步进 analysis plane 的 canonical `issue-cluster` result set。
- `已完成` `eco-cluster-issue-candidates` 已改为复用共享 helper，只保留 “claim-cluster merged issue surface” 这一层职责。
- `已完成` `source_queue_profile.py` 已把 controversy issue 主链进一步改成 `route -> extract-issue-candidates -> cluster-issue-candidates -> typed issue -> controversy-map`，candidate stage 已进入默认 queue。
- `已完成` deliberation target semantics 已补齐 `actor-profile / proposal` 一等锚点：
  - `moderator_actions / falsification_probes` 已新增 `target_actor_id / target_proposal_id` 列与索引。
  - `query-council-objects` 已新增 `--actor-id / --proposal-id` 过滤。
  - `source_proposal_id` 不再把 target proposal 误判成 source proposal。
- `已完成` 已新增 actor/proposal anchoring 回归与 issue-candidate extractor 回归：
  - `tests/test_council_query_surface.py`
  - `tests/test_council_autonomy_flow.py`
  - `tests/test_controversy_workflow.py`
  - `tests/test_runtime_source_queue_profiles.py`

## 3. Work Package 0: 冻结旧错误增长

- `[ ]` 冻结旧 `claim -> coverage -> readiness` 主链的功能扩张
- `[ ]` 冻结 kernel 内新增 domain policy
- `[ ]` 给 legacy 模块标明迁移状态
- `[ ]` 禁止新增依赖 summary artifact 的流程控制逻辑

## 4. Work Package 1: Canonical contracts 与 DB schema

### 4.1 Signal plane

- `[x]` 建立 `formal-comment-signal`
- `[x]` 建立 `public-discourse-signal`
- `[x]` 建立 `environment-observation-signal`
- `[x]` 每类 signal 都带 provenance、artifact refs、source metadata

### 4.2 Analysis plane

- `[x]` 建立 `issue-cluster`
- `[x]` 建立 `stance-group`
- `[x]` 建立 `concern-facet`
- `[x]` 建立 `actor-profile`
- `[x]` 建立 `evidence-citation-type`
- `[x]` 建立 `verifiability-assessment`
- `[x]` 建立 `verification-route`
- `[x]` 建立 `formal-public-link`
- `[x]` 建立 `representation-gap`
- `[x]` 建立 `diffusion-edge`
- `[x]` 建立 `controversy-map`
- `[x]` 建立 `claim-candidate`
- `[x]` 建立 `claim-cluster`
- `[x]` 建立 `claim-scope`

### 4.3 Deliberation plane

- `[x]` 建立 `hypothesis`
- `[x]` 建立 `challenge`
- `[x]` 建立 `board-task`
- `[x]` 建立 `proposal`
- `[x]` 建立 `next-action`
- `[x]` 建立 `probe`
- `[x]` 建立 `readiness-opinion`
- `[x]` 建立 `readiness-assessment`
- `[x]` 建立 `promotion-basis`
- `[x]` 建立 `decision-trace`

### 4.4 Runtime / control plane

- `[x]` 建立 `promotion-freeze`
- `[x]` 建立 `controller-state`
- `[x]` 建立 `gate-state`
- `[x]` 建立 `supervisor-state`

### 4.5 通用要求

- `[x]` `issue-cluster / stance-group / concern-facet / actor-profile / evidence-citation-type / claim-candidate / claim-cluster / claim-scope / verifiability-assessment / verification-route / formal-public-link / representation-gap / diffusion-edge / controversy-map` 支持 item-level query
- `[x]` 上述 claim-side + typed controversy-chain analysis object 已具备 ID、provenance、evidence refs、lineage、decision source
- `[ ]` phase-2 对象不再只作为整包 snapshot 存在

## 5. Work Package 2: Signal plane 重构

- `[x]` 停止把 formal comments 仅作为 generic public signal 写入系统
- `[x]` 为 formal comments 增加 docket / agency / submitter / stance / concern / citation / route 维度
- `[x]` 保留 formal/public/environment 三类输入的 source-specific provenance
- `[x]` 为 typed signals 提供统一 query surface

## 6. Work Package 3: Analysis plane 改写为 controversy chain

### 6.1 重写旧主链 skills

- `[x]` `[重写]` `eco-extract-claim-candidates`
- `[x]` `[重写]` `eco-cluster-claim-candidates`
- `[x]` `[重写]` `eco-derive-claim-scope`

### 6.2 新增 controversy 主链 skills

- `[x]` `[新增 canonical]` `eco-extract-issue-candidates`
- `[x]` `[新增 canonical]` `eco-cluster-issue-candidates`
- `[x]` `[新增 canonical]` `eco-extract-stance-candidates`
- `[x]` `[新增 canonical]` `eco-extract-concern-facets`
- `[x]` `[新增 canonical]` `eco-extract-actor-profiles`
- `[x]` `[新增 canonical]` `eco-extract-evidence-citation-types`
- `[x]` `[新增 canonical]` `eco-link-formal-comments-to-public-discourse`
- `[x]` `[新增 canonical]` `eco-identify-representation-gaps`
- `[x]` `[新增 canonical]` `eco-detect-cross-platform-diffusion`
- `[x]` `[新增 canonical]` `eco-classify-claim-verifiability`
- `[x]` `[新增 canonical]` `eco-route-verification-lane`
- `[x]` `[新增 canonical]` `eco-materialize-controversy-map`

### 6.3 强约束

- `[x]` `claim-candidate / claim-cluster / claim-scope` 输出 `confidence`
- `[x]` `claim-candidate / claim-cluster / claim-scope` 输出 `rationale`
- `[ ]` 每个 extractor 输出 `provenance`
- `[ ]` heuristic 输出显式标记 `decision_source = heuristic-fallback`
- `[ ]` 旧 claim 输出只保留为兼容视图或 fallback，不再是 canonical 主轴

## 7. Work Package 4: Deliberation plane 与 council objects

### 7.1 重写 phase-2 skills

- `[x]` `[重写]` `eco-propose-next-actions`
- `[x]` `[重写]` `eco-open-falsification-probe`
- `[x]` `[重写]` `eco-summarize-round-readiness`
- `[x]` `[重写]` `eco-promote-evidence-basis`

### 7.2 重写 board skills

- `[x]` `[重写]` `eco-claim-board-task`
- `[x]` `[重写]` `eco-open-challenge-ticket`
- `[x]` `[重写]` `eco-close-challenge-ticket`
- `[x]` `[重写]` `eco-update-hypothesis-status`

### 7.3 结构性要求

- `[x]` `next-action` 可锚定 `issue / route / gap / actor / proposal`
- `[x]` `probe` 可由 agent proposal 或 policy fallback 生成
- `[x]` `readiness-assessment` 能表达多 agent 分歧
- `[x]` `promotion-basis` 冻结的是 controversy judgement，而不是只冻结 coverages
- `[x]` `decision-trace` 记录采纳了哪个 proposal、拒绝了哪些 proposal、理由是什么
- `[x]` `hypothesis / challenge / board-task` DB 行与 `raw_json` 已显式承载 `decision_source / evidence_refs / source_ids / provenance / lineage`

## 8. Work Package 5: 建立 agent council loop

### 8.1 当前状态

- `[x]` `openclaw-agent` 轮次进入 phase-2 时，controller 与 agent entry 现在都会先尝试 `direct-council-advisory` compiler，只有 direct council inputs 不足或 compiler 失败时才回退 `agent-advisory` planner skill。
- `[x]` advisory plan 已存在时会直接采用；advisory 物化失败时才会回退 `planner-backed` phase-2。
- `[x]` controller 状态现在显式记录 `plan_source / planning_attempts / agent_advisory_plan_path`，agent 路径与 fallback 路径不再混在一条隐式 planner 语义里。
- `[x]` `eco-plan-round-orchestration` 在 `agent-advisory` 与 `runtime-phase2` 模式下，若 DB 中已存在直接 `proposal / readiness-opinion`，现在都可以跳过 `next-actions` 重算，直接产出 `probe -> readiness` 或 `readiness-only` 队列。
- `[x]` advisory plan 现在会显式暴露 `direct_council_queue / next_actions_stage_skipped / council_input_counts`，能区分“由 council inputs 直接驱动的 advisory”与“仍依赖 wrapper/action snapshot 的 advisory”。
- `[x]` `eco-concil-runtime/src/eco_council_runtime/phase2_direct_advisory.py` 已接入主链，能把 DB 中的 `proposal / readiness-opinion / probe` 直接编译为 advisory queue，并把 `plan_source = direct-council-advisory` 写入 advisory artifact、controller 状态与 planning attempts。
- `[x]` orchestration plan 已升格为 canonical deliberation-plane object：`orchestration-plan / orchestration-plan-step` contract、表、query surface 与 export rebuild 已补齐；`phase2_planning_profile.py / controller.py / cli.py / benchmark.py / agent_entry.py / phase2_exports.py` 现默认先读 DB，runtime/advisory artifact 退回 export/fallback 载体。

- `[x]` 定义 `proposal contract`
- `[x]` 定义 `challenge contract`
- `[x]` 定义 `readiness opinion contract`
- `[x]` 定义 `decision trace contract`
- `[x]` 允许多个 agent 对同一问题提交相互冲突的 judgement
- `[x]` runtime 默认执行 agent proposal，而不是替 agent 先算出结论
- `[x]` heuristic 只在 proposal 缺失、失败或审计模式下触发

## 9. Work Package 6: Runtime kernel 收边界

### 9.0 当前状态

- `[x]` `controller.py` 已把 `openclaw-agent` 轮次改成 `direct-council-advisory -> agent-advisory -> runtime-planner` 的三级回退链；`runtime planner` 不再是默认入口。
- `[x]` phase-2 controller artifact 与 round-controller ledger 事件现在都会暴露 `plan_source`，controller 已能区分 `direct-council-advisory / agent-advisory / runtime-planner`。
- `[x]` controller 在采纳 advisory/runtime plan 时现在会强绑定当前 `run_id / round_id / controller_authority` 再写入 deliberation plane；弱 advisory artifact 即使缺字段，也不能再绕开 DB plan contract。
- `[x]` `controller.py` 不再强行注入固定 `promotion-gate` / post-gate 序列；plan 现在可以显式声明 `gate_steps / required_previous_stages / stage_kind / gate_handler`，controller 会按计划执行。
- `[x]` `phase2_contract.py` 已从“controller 唯一真理表”退化成 known-stage default metadata / compatibility fallback；显式 plan 依赖可以覆盖内置依赖。
- `[x]` `promotion-gate` 的执行分派、readiness 依赖解析与 controller 状态更新已迁入 `kernel/gate.py`；`controller.py` 现在只消费统一 `gate_result`，不再内嵌 `promotion-gate` 特判。
- `[x]` `next_actions / probes / readiness` 的 DB/artifact read surface 已抽到 `kernel/phase2_state_surfaces.py`，`gate.py / supervisor.py / benchmark.py` 不再直接依赖 `investigation_planning.py`。
- `[x]` `promotion_basis / reporting_handoff / council_decision / expert_report / final_publication` 的 DB/artifact read surface 也已并入 `kernel/phase2_state_surfaces.py`；reporting / publication 相关 skills 已切到新 surface，`investigation_planning.py` 只剩 compatibility re-export，不再持有这些实现。
- `[x]` `gate.py` 已支持 handler registry / dispatch；controller 不再导入或硬编码 `promotion-gate` 实现，`promotion-gate` 也不再是唯一合法 gate handler。
- `[x]` `promotion-gate` 默认实现已迁到 `eco_council_runtime/phase2_gate_handlers.py`；`kernel/gate.py` 现在只剩 gate dispatch/runtime，不再持有 promotion/readiness 领域逻辑。
- `[x]` `controller.run_phase2_round_with_contract_mode(...)` 已改成显式接收 `gate_handlers`；默认 gate/profile 现在只能从组合根显式注入，controller 不再默认拥有该 profile。
- `[x]` `phase2_fallback_planning.py` 已拆成 `phase2_fallback_common.py / phase2_fallback_contracts.py / phase2_fallback_agenda.py / phase2_fallback_context.py` 四个明确职责模块；原文件退成 compatibility facade，skills / reporting contracts / kernel compatibility layer 也开始直接依赖这些新边界。
- `[x]` `phase2_fallback_agenda.py` 内的 score / pressure / probe / readiness-blocker 规则已继续抽成 `eco_council_runtime/phase2_fallback_policy.py`，fallback 动作现在会显式写出 `policy_profile / policy_source / policy_owner`。
- `[x]` agent proposal 到执行 action 的投影已统一抽到 `eco_council_runtime/phase2_proposal_actions.py`；`phase2_direct_advisory.py`、`eco-propose-next-actions`、`eco-open-falsification-probe`、`eco-plan-round-orchestration` 不再各自维护一套 proposal->action 规则副本。
- `[x]` phase-2 默认 gate profile 已从 handler 实现文件拆到 `eco_council_runtime/phase2_gate_profile.py`；`phase2_gate_handlers.py` 现在只保留 handler 实现，不再同时承担“默认 profile 注册表”。
- `[x]` `runtime_command_hints.py / phase2_agent_handoff.py` 已把默认 runtime command hints 与 agent handoff chain 提升到 kernel 外；`kernel/agent_entry.py` 现在只消费注入的 `hard_gate_command_builder / entry_chain_builder`，不再内建默认 runtime handoff 流程。
- `[x]` `scripts/eco_runtime_kernel.py` 现在是默认 phase-2 gate/profile 与 agent handoff profile 的组合根；`cli.py / supervisor.py / agent_entry.py` 只消费注入参数，不再私自装配默认 profile。
- `[x]` `phase2_fallback_agenda_profile.py` 已接管 `open-challenge / task / hypothesis / issue / route / assessment / link / gap / edge / coverage -> action` 的映射与启停顺序；`phase2_fallback_agenda.py` 现在只剩通用 context 装配、去重、排序和统计。
- `[x]` 新增 profile-overridable 回归：可注入自定义 agenda profile 覆盖默认 fallback 议会流程，可注入自定义 agent handoff profile 覆盖默认 runtime handoff 命令链。
- `[x]` `phase2_council_execution.py` 已统一 `proposal-authoritative / proposal-augmented / fallback-only` 三种议会执行模式；`eco-propose-next-actions / eco-open-falsification-probe / eco-summarize-round-readiness / eco-plan-round-orchestration` 现在共享同一套 proposal-vs-heuristic 决策面，并显式写出 observed / selected / suppressed fallback counts。
- `[x]` `phase2_stage_profile.py` 现在持有默认 stage definitions、gate/post-gate 默认蓝图与 stage validation；`kernel/phase2_contract.py` 已退成 compatibility facade，不再是 controller 的默认真理表。
- `[x]` `phase2_controller_state.py` 已接管 phase-2 stage blueprint、controller planning snapshot、step merge、planner attempt summary、failure/event shape；`kernel/controller.py` 现在主要只剩 injected planning source 执行、gate dispatch、skill execution 与持久化。
- `[x]` `phase2_agent_entry_profile.py` 已接管 agent-entry 默认 role definitions、recommended skills、operator commands/notes、next-round suggestion builder 与 advisory refresh source 顺序；`kernel/agent_entry.py / cli.py` 现在只消费 injected entry profile，不再内建默认议会入口教程或 advisory materialization 链。
- `[x]` `phase2_posture_profile.py` 已接管 controller completion follow-up、supervisor classification / top-actions / round-transition / operator notes / failure notes；`kernel/controller.py / kernel/supervisor.py / cli.py` 现在只消费 injected posture profile。
- `[x]` `phase2_round_profile.py` 已接管默认 `next_round_id` sequencing；`kernel/supervisor.py / kernel/agent_entry.py` 不再共享或持有内建轮次递增策略，round handoff policy 可以通过 posture / entry profile 双向覆写。
- `[x]` `kernel/cli.py / post_round.py / benchmark.py` 已把 reporting/publication operator surface 从 `promotion_status` 主导切到 shared reporting surface；operator/query 现在可直接读取 `reporting_ready / blockers / readiness_blocker`。
- `[x]` 新增 plan DB 回归，覆盖 phase-2 export rebuild、DB 列覆盖 stale `raw_json`、agent advisory plan identity binding 与 runtime kernel advisory 主链。
- `[x]` 本地扩展大回归 `139` 项通过，覆盖 phase-2 / agent-entry / council / board / reporting / publication / post-round / benchmark 主链。

### 9.1 必须收缩或迁出的模块

- `[x]` 收缩或迁出 `eco-concil-runtime/src/eco_council_runtime/kernel/phase2_contract.py`
- `[x]` 收缩或迁出 `eco-concil-runtime/src/eco_council_runtime/kernel/controller.py`
- `[x]` 迁出 `eco-concil-runtime/src/eco_council_runtime/kernel/investigation_planning.py`
- `[x]` 重写 `eco-concil-runtime/src/eco_council_runtime/kernel/agent_entry.py`

### 9.2 必须保留在 kernel 的职责

- `[ ]` admission / capability / side-effect governance
- `[ ]` execution / retry / receipt
- `[ ]` ledger / replay / audit
- `[ ]` persistence / query surface
- `[ ]` operator-visible health

### 9.3 不再允许留在 kernel 的职责

- `[ ]` readiness 主判断逻辑
- `[ ]` promotion 主判断逻辑
- `[ ]` controversy scoring formula
- `[ ]` fixed phase policy
- `[ ]` 默认议会编排假设

## 10. Work Package 7: Reporting / publication 重建

- `[x]` `[重写]` `eco-summarize-board-state`
- `[x]` `[重写]` `eco-materialize-board-brief`
- `[x]` `[重写]` `eco-materialize-reporting-handoff`
- `[x]` `[重写]` `eco-draft-council-decision`
- `[x]` `[重写]` `eco-draft-expert-report`
- `[x]` `[重写]` `eco-publish-council-decision`
- `[x]` `[重写]` `eco-publish-expert-report`
- `[x]` `[重写]` `eco-materialize-final-publication`
- `[x]` board summary / brief 只作为 DB 导出物存在
- `[x]` reporting / publication 默认从 canonical DB 对象物化
- `[x]` reporting / publication canonical objects 已支持 item-level query

## 11. Work Package 8: Verification lane 降级为 optional lane

- `[x]` `[降级为 optional lane]` `eco-extract-observation-candidates`
- `[x]` `[降级为 optional lane]` `eco-merge-observation-candidates`
- `[x]` `[降级为 optional lane]` `eco-derive-observation-scope`
- `[x]` `[降级为 optional lane]` `eco-link-claims-to-observations`
- `[x]` `[降级为 optional lane]` `eco-score-evidence-coverage`
- `[x]` observation chain 只在 verifiability + route 明确允许时触发
- `[x]` readiness 默认不再围绕 coverage 公式展开

## 12. Work Package 9: 删除兼容债

- `[x]` 删除“formal comments 只是 generic public signal”的长期假设
- `[x]` 删除“next_actions / probes / readiness 只以 artifact wrapper 存在”的长期假设
- `[x]` 删除“coverage 是默认主链”的长期假设
- `[ ]` 删除“board / reporting 依赖 summary artifact 才能推进”的长期假设
- `[ ]` 删除“kernel 默认承载新增 domain policy”的长期假设
- `[ ]` 删除“旧 envelope 可以无限期作为 canonical 输出”的长期假设

## 13. Work Package 10: 测试与 benchmark 改写

- `[ ]` 删除或重写固化旧 coverage-first 语义的测试
- `[x]` 新增 DB-only recovery tests
- `[x]` 新增 agent proposal-driven round tests
- `[x]` 新增 board canonical query-surface tests
- `[x]` 新增 reporting canonical query-surface tests
- `[x]` 新增 kernel boundary tests
- `[x]` 新增 optional verification lane tests
- `[ ]` 准备争议型政策 case
- `[ ]` 准备混合型争议 case
- `[ ]` 准备可核实事件 case

## 14. 硬完成检查表

- `[x]` canonical signal / analysis / deliberation / runtime control 对象已经定义并落库
- `[x]` formal comments 已成为一等结构化输入
- `[x]` `hypothesis / challenge / board-task / proposal / next-action / probe / readiness-opinion / readiness-assessment / promotion-basis / decision-trace` 已可 item-level 查询
- `[x]` `promotion-freeze / controller-state / gate-state / supervisor-state` 已可 item-level 查询
- `[x]` `reporting-handoff / council-decision / expert-report / final-publication` 已可 item-level 查询
- `[x]` 删除 `board_summary / board_brief / next_actions / probes / readiness` artifact 后，round 仍可继续
- `[x]` 主链默认输出已不再是 `claim-observation-link-coverage`
- `[x]` observation matching 只在明确可核实时触发
- `[ ]` agent proposal 已带 `rationale / confidence / evidence refs / provenance`
- `[ ]` heuristic 已降为 fallback，并带显式 trace
- `[x]` reporting / publication 默认从 DB canonical 对象物化
- `[ ]` kernel 已不再承载 readiness / promotion / controversy judgement 的主语义
- `[ ]` 至少一个争议型政策 case、一个混合型争议 case、一个可核实事件 case 稳定通过新验收
