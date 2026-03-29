# OpenClaw 交付入口

阶段性交付状态与后续顺序已经统一收敛到 [openclaw-full-development-report.md](openclaw-full-development-report.md)。

如果你当前只关心：

1. 哪些能力块已经完成
2. 现在最该做的下一批是什么
3. 后续工作流应当按什么顺序推进

请直接阅读：

- [openclaw-full-development-report.md](openclaw-full-development-report.md)

当前交付层面的简版结论是：

1. normalize -> evidence -> board -> promotion -> reporting 主链已经完成。
2. phase-2 controller 已经切到 planner-backed preview，但这只是过渡态，不是最终 OpenClaw 工作模式。
3. 本批次已经补入 mission scaffold、prepare-round、fetch-plan、import execution 的最小 ingress 闭环。
4. sibling detached skills 仓库已经具备 atomic data-source fetch skills；当前仓库剩余的第一优先级不是继续补 fetcher，而是把两仓 skill 一起接入 OpenClaw。
5. archive/history context 与 single-host runtime hardening baseline 已经交付；下一批优先级应先转向 OpenClaw multi-agent framework，同时把 runtime source-queue mode 收敛为第二编排流程，再做 detached fetch integration、simulation/benchmark 和 production admission。

### 5.2 下一批验收条件

下一批如果要算完成，至少要满足：

1. 当前仓库 `skills/` 与 detached `/home/fpddmw/projects/skills` 能被统一暴露到 OpenClaw skill surface
2. moderator、sociologist、environmentalist、challenger、archivist 至少能以最小 role workspace 形式被 provision 出来
3. runtime source-queue mode 被明确保留为第二编排流程，但不再与 skill surface 分叉
4. 至少一个 round 能由多个 OpenClaw agent 通过 board 协作推进，而不是只靠 runtime 固定队列
5. unittest 需要新增 OpenClaw adapter / managed skill projection / agent turn integration regression，而不是只更新文档口径

## 6. runtime 当前边界

runtime 在当前阶段仍不应承担新的业务推理，推荐继续维持下面边界：

1. `next_actions_<round_id>.json`、`falsification_probes_<round_id>.json`、`round_readiness_<round_id>.json`、`promoted_evidence_basis_<round_id>.json`、`reporting_handoff_<round_id>.json`、`council_decision_draft_<round_id>.json`、`expert_report_draft_<role>_<round_id>.json`、`expert_report_<role>_<round_id>.json`、`council_decision_<round_id>.json` 的契约应继续保持稳定。
2. 最小 runtime kernel 负责 run manifest、artifact path resolver、receipt/event ledger、skill executor wrapper、round cursor、promotion gate、OpenClaw adapter、managed skill projection、role-agent provisioning，以及当前已经落地的 contract-aware / permission-aware 治理。
3. reporting / decision 仍然优先以 atomic skill 方式推进，而不是把新业务逻辑塞回 runtime。

换句话说，runtime 现在仍只是一个 planner-backed 的 phase-2 preview；下一步应该补的是 OpenClaw adapter 和 agent-native 框架，而不是继续扩大 controller 的顺序编排权。

## 7. 当前补充状态

- `run-phase2-round` 现在可以把 `board -> D1 -> D2 -> promotion` 串成单命令流程。
- `supervise-round` 现在会在 controller 结果上额外落出 operator 视角的 `supervisor_state_<round_id>.json`。
- `show-run-state` 现在会同时回显最新 round 的 gate / controller / supervisor 快照。
- `show-run-state` 现在也会回显最新 round 的 `orchestration_plan_<round_id>.json`。
- `eco-materialize-reporting-handoff` 与 `eco-draft-council-decision` 已经把 promotion basis 接到 reporting / decision 第一批下游对象。
- `eco-draft-expert-report`、`eco-publish-expert-report`、`eco-publish-council-decision` 已经把 role draft、canonical report 与 canonical decision 接回 skill-first 主链。
- `eco-materialize-final-publication` 已经把 reporting 主链从 canonical report / decision 收敛到最终发布对象。
- `eco-plan-round-orchestration` 已经把 phase-2 controller 先前的固定 queue 显式化为一个 planner artifact，并驱动 planner-backed cutover。
- board 写入路径现在已切到 filesystem lock + atomic replace + `board_revision`，当前目标是先保证同机多进程安全，而不是宣称分布式协作已经完成。
- runtime registry 现在会快照 skill contract 与 agent metadata，ledger 也会记录命令快照、skill_args、解析路径和输入/输出哈希。
- runtime 现在已经具备 contract-aware preflight 与 enforcement baseline：支持 `preflight-skill`、`run-skill --contract-mode off|warn|strict`，并能阻断缺失 required inputs、未声明 path override、undeclared summary path 与 artifact_ref mismatch。
- runtime 现在已经补上 timeout budget、retry/backoff、high-risk side-effect approval、exclusive execution lock 和 structured failure payload；controller、supervisor 与 CLI 也会透传 execution policy。
- 活跃实现里目前仍没有真正启用的 OpenClaw adapter、managed skill projection 或 role-agent turn loop；这部分应成为下一批编码主目标。
- 活跃 runtime 的 prepare/import 仍只是最小 import baseline，不足以作为即将投产的 source-queue 流程。
- 当前完整 unittest 集已经覆盖 runtime timeout、retry、side-effect approval、planner cutover 与后续 orchestration integration 回归，并会继续扩展到 detached fetch integration 与 benchmark。

## 8. 面向生产的开发指引

- 生产路线、环境准入条件、shadow test 与 pilot 条件统一收敛在 [openclaw-production-development-plan.md](openclaw-production-development-plan.md)。
- 本计划只继续追踪“做哪一批能力”，不重复维护生产准入细则。

这份文档是当前蓝图下的执行型阶段计划，后续批次应在此基础上继续推进。