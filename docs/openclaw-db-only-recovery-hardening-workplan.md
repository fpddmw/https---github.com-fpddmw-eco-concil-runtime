# OpenClaw DB-only Recovery 硬化工作计划

## 1. 文档定位

本文是独立工作计划，不作为全局总计划，也不要求其他计划同时执行。

目标是验证并强化 OpenClaw 在 artifact 缺失、artifact orphan、运行中断等情况下从 DB 恢复关键调查状态的能力。

## 2. 范围

本计划覆盖：

1. board state、readiness、proposal、challenge、hypothesis 的 DB 恢复。
2. relation cue、relation evidence packet、reporting handoff 的 DB 优先读取。
3. artifact 删除后的 query、gate、reporting 链路。
4. artifact-only orphan 的识别和降权。
5. recovery warning、operator 可见状态和测试 fixture。

## 3. 非目标

本计划不做：

1. 大规模物理 schema 迁移。
2. 删除所有 artifact 导出。
3. 重命名 legacy `report_basis_*` 字段。
4. 改变 council object 的语义契约。

## 4. 交付物

1. DB-only recovery 测试集合。
2. artifact 缺失和 orphan artifact 的 fixture。
3. query/reporting/gate 的恢复行为说明。
4. operator warning 或 recovery note 输出规范。

## 5. 工作阶段

### 当前落地状态

已落地第一块 recovery 代码：

1. `tests/test_db_only_recovery.py`
   - 覆盖 board JSON export 删除后，`query-board-delta` 与 `materialize-board-brief` 从 deliberation DB 恢复。
   - 覆盖 relation cue artifact 删除后，`query-spatiotemporal-relations` 仍从 analysis DB 返回 relation row，并暴露 `artifact_present=false`。
   - 覆盖 relation cue artifact 与 relation packet artifact 删除后，`materialize-spatiotemporal-relation-evidence-packet` 可从 DB 重新 materialize，并输出 `relation-cue-artifact-missing-db-recovered` warning。
   - 覆盖 reporting handoff 与 decision draft artifact 删除后，`show-reporting-state` 与 `draft-expert-report` 从 reporting DB 恢复。
   - 覆盖 reporting handoff、council decision、expert report artifact 删除后，`materialize-final-publication` 基于 DB records 重新生成 final publication。
   - 覆盖 expert report draft artifact orphan 后，`publish-expert-report` 阻断发布并输出 `missing-report-draft` warning，不复用 artifact-only 内容。
2. `tools/quality_gate.py`
   - 新增 `db-recovery` targeted suite。
3. `.github/workflows/quality-gates.yml`
   - targeted gates 接入 `db-recovery`。
4. `eco-concil-runtime/src/eco_council_runtime/spatiotemporal_relation_evidence_packet.py`
   - relation cue artifact 缺失但 DB row 存在时，packet materialization 明确输出 DB-backed recovery warning。

当前未闭环项：

1. reporting artifact 删除 / orphan artifact 已有集中 recovery 覆盖，但还可继续补齐 `materialize-reporting-exports` 的批量重建断言。
2. open-investigation-round 的 artifact 缺失恢复仍需作为独立 recovery 场景固定。
3. 多轮 carryover 在旧 round artifact 缺失时的恢复仍需固定为独立场景。

### P0：恢复面梳理

1. 列出必须能从 DB 恢复的对象。
2. 标记仍依赖 artifact 的兼容路径。
3. 定义 artifact 缺失时的 warning 语义。

验收：

1. 文档列清 DB-authoritative 与 artifact-export-only 边界。
2. 无需改动业务语义即可编写 fixture。

### P1：Board 与多轮恢复

1. 删除 board artifact 后读取 deliberation plane。
2. 验证 open challenges、board tasks、readiness、transition record 可恢复。
3. 验证 follow-up round carryover 不依赖旧 artifact。

验收：

1. board artifact 缺失不阻断 second round。
2. orphan board artifact 不覆盖 DB 当前状态。

### P2：Analysis 与 Relation 恢复

1. 删除 relation cue/evidence packet artifact。
2. 从 analysis/result set 与 reporting/council basis 恢复 relation evidence。
3. 验证 relation query 不依赖 artifact。

验收：

1. `query-spatiotemporal-relations` 在 artifact 缺失时仍返回 DB-backed cue。
2. relation packet 缺失时能给出可操作 warning，而不是静默失败。

### P3：Reporting 恢复

1. 删除 reporting handoff、decision、expert report 导出。
2. 从 DB reporting records 重建或重新 materialize。
3. 验证 orphan draft artifact 不会被发布链路复用。
4. 验证 frozen basis 不被 orphan artifact 修改。

验收：

1. final publication 可基于 DB records 重新生成。
2. artifact-only 内容不会进入 frozen report basis。
3. 发布链路在 artifact-only draft 上阻断，而不是静默恢复。

### P4：回归门

1. 增加完整 recovery test。
2. 将恢复路径纳入 CI 或本地验收命令。

验收：

1. 缺 artifact、orphan artifact、重复 materialize 三类场景均有测试。
2. 恢复行为对 operator 可见且可追踪。
