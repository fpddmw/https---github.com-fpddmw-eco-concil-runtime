# OpenClaw Runtime Kernel 与 Agent Council 架构

## 1. 文档定位

本文描述当前 runtime kernel、agent council、权限、审批、状态推进和多轮协作架构。文件名保留历史路径，但本文不再是重构 checklist。

## 2. Runtime Kernel 职责

runtime kernel 是运行治理层，不是实质调查判断者。

职责范围：

1. run / round 生命周期。
2. skill registry 与 skill contract 解析。
3. actor role、capability、access policy。
4. preflight / postflight contract 检查。
5. skill approval、transition approval、side-effect governance。
6. skill execution、timeout、retry、lock、receipt。
7. ledger event、dead letter、manifest、cursor。
8. controller/gate/supervisor snapshot。
9. archive、history bootstrap、replay、benchmark、health。

非职责：

1. 不替 agent 判断事实结论。
2. 不把 optional helper 输出直接提升为报告结论。
3. 不绕过 moderator/operator 审批推进阶段。
4. 不把 artifact 当作唯一状态源。

## 3. Agent Council 职责

agent council 负责实质调查和议会判断。

主要写入对象：

1. `finding`
   - 调查发现，必须引用 evidence refs。
2. `evidence-bundle`
   - 把多个证据组织为可复核证据包。
3. `proposal`
   - 建议行动、冻结、阻断、挑战、更新假设或推进下一轮。
4. `readiness-opinion`
   - 表达 ready、blocked、needs-more-data。
5. `hypothesis`
   - 记录待检验解释或调查命题。
6. `challenge`
   - 记录反证、疑点、替代解释。
7. `board-task`
   - 记录任务分配、carryover、follow-up。
8. `probe`
   - 记录反证或 falsification 子任务。
9. `report-section-draft`
   - 报告章节草案与引用 basis。

## 4. 角色权限

| 角色 | 主要职责 | 关键权限 |
| --- | --- | --- |
| `moderator` | 主持议程、协调议会、请求状态转换 | query、proposal、readiness、board task、round bootstrap、state transition、report draft/publish |
| `environmental-investigator` | 环境与物理证据调查 | fetch、normalize、query、analysis、finding、evidence bundle、proposal、readiness |
| `public-discourse-investigator` | 公共讨论与社区证据调查 | fetch、normalize、query、analysis、finding、evidence bundle、proposal、readiness |
| `formal-record-investigator` | 正式记录与政策材料调查 | fetch、normalize、query、analysis、finding、evidence bundle、proposal、readiness |
| `challenger` | 反证、质疑、替代解释 | query、analysis、review comment、challenge、probe、proposal、readiness |
| `report-editor` | 报告草拟与发布准备 | query、report draft、report publish |
| `runtime-operator` | 运行治理、审批、归档、恢复 | runtime admin、archive、derived export、approval |

## 5. 审批模型

OpenClaw 使用两类审批：

1. `skill approval`
   - 用于 optional-analysis、report publish、planner 等高影响或启发式 skill。
   - 典型链路：`request-skill-approval -> approve-skill-approval -> run-skill --skill-approval-request-id`。
2. `transition approval`
   - 用于打开新调查轮、冻结报告依据、关闭轮次等状态变更。
   - 典型链路：`request-transition -> approve-transition -> run state transition skill`。

审批目标：

1. 保留人工/操作员可控边界。
2. 让高影响输出有明确 approval ref。
3. 阻止 helper 或 agent 自行推进不可逆阶段。

## 6. Controller / Gate / Supervisor

### Controller

controller 负责执行已允许的阶段计划或 transition-executor 路径。

主要行为：

1. 读取 planning source 或 transition request。
2. 运行 skill stage 或 gate stage。
3. 每步写 controller state、orchestration step、ledger。
4. 不替代 council proposal 做实质判断。

### Gate

gate 负责把 policy profile、readiness、proposal、opinion 汇总为可审计 gate result。

当前关键 gate：

1. `report-basis-gate`
   - 只在 readiness 与 council inputs 允许时放行 freeze。
   - council veto / withhold 可以阻断报告依据冻结。

### Supervisor

supervisor 负责把 controller/gate/reporting 状态转换为 operator 可见姿态。

典型状态：

1. `reporting-ready`
2. `hold-investigation-open`
3. `controller-failed`
4. `report-basis-withheld`

当证据不足时，supervisor 会推荐继续调查和 `open-investigation-round`。

## 7. 多轮运行机制

多轮调查由 `open-investigation-round` 实现。

流程：

1. council 判断当前 round 证据不足。
2. moderator 请求 `open-investigation-round` transition。
3. runtime-operator 批准。
4. skill 读取 source round 的 DB-backed board state。
5. skill 创建 target round，写入 carried hypotheses、follow-up tasks、transition note。
6. skill 生成新的 `round_tasks_<round_id>.json` 并存 DB snapshot。
7. 新 round 继续 `prepare-round -> fetch/import -> normalize -> query -> council write`。

可靠性设计：

1. source round 不被覆盖。
2. target round 已存在时执行 noop，避免重复 mutation。
3. board artifact 缺失时可从 deliberation plane 恢复。
4. transition record 写入 DB，artifact 只是导出。
5. query 支持 `round_scope=up-to-current`。

## 8. 状态持久化

主要持久化面：

1. `analytics/signal_plane.sqlite`
   - signal、analysis、deliberation、runtime snapshots、reporting records。
2. `runtime/*.json`
   - controller、supervisor、gate、transition、receipt、approval、fetch plan 等导出。
3. `board/*.json|md`
   - board summary/brief 兼容导出。
4. `reporting/*.json`
   - reporting handoff、decision、expert report、final publication 导出。
5. `archive/*.json` 与 shared archive SQLite
   - close-round 后的 signal/case archive。

DB 优先级：

1. 查询、恢复、报告链路优先读 DB canonical rows。
2. artifact 缺失时，能从 DB wrapper 恢复的对象不应视为丢失。
3. artifact-only 文件如果没有 DB row，应视为 orphaned export 或兼容输入。

## 9. 运行护栏

1. 写入型 skill 必须声明 actor role。
2. optional-analysis helper 必须 approval-gated。
3. 事实性 judgement 必须引用 evidence refs。
4. report basis freeze 必须经过 transition approval。
5. reporting publish 必须经过 operator approval。
6. runtime-operator 不做实质调查判断。
7. helper 输出不得自动进入 report basis。

## 10. 后续工作入口

本文不维护统一后续计划。runtime 相关后续工作按独立文档跟踪：

1. `docs/openclaw-runtime-governed-execution-workplan.md`
   - 正式运行入口、approval、receipt、ledger、lock、权限阻断和 direct script 兼容边界。
2. `docs/openclaw-db-only-recovery-hardening-workplan.md`
   - 多轮调查、board、analysis、reporting 在 artifact 缺失时的 DB-first 恢复。
3. `docs/openclaw-case-study-evaluation-workplan.md`
   - supervisor/operator action 与 evidence gap、follow-up round、cautious/withheld report 的真实案例评测闭环。
4. `docs/openclaw-schema-migration-hardening-workplan.md`
   - runtime/control/reporting 相关 schema 的版本和幂等迁移。
5. `docs/openclaw-module-decomposition-workplan.md`
   - 继续减少 runtime kernel 中的领域语义，但以行为不变的小步拆分推进。
