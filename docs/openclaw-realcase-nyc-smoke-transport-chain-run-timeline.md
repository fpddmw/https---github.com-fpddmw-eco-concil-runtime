# OpenClaw 纽约烟霾开放式调查测试时间线

测试对象：`runs/openclaw-realcase-nyc-smoke-transport-chain-20230607`

案例主题：`2023 New York City smoke haze event`

用户 mission：`请调查 2023 年纽约烟霾事件：说明发生了什么、可能原因是什么、哪些证据支持或限制这些判断，并给出证据支持的结论。`

运行窗口：本轮 mission 未提供明确 `window` / `region`，runtime 将 round 保持为 `scoping-required`。

执行时间：`2026-05-09T17:04:44Z` 至 `2026-05-10T04:17:46Z`

说明：本文按本次 run 的本地产物整理。主要依据包括 `runtime/audit_ledger.jsonl`、`runtime/mission_scaffold_round-001.json`、`runtime/source_selection_*_round-001.json`、`analytics/signal_plane.sqlite`、`deliberation/`、`board/`、`runtime/receipts/` 和 `runtime/runtime_health.json`。

文档性质：本文是本次开放式 mission run 的事实记录和技术复盘，不是开发计划，也不是最终调查报告。它记录 council 在未被显式提示加拿大、火点、输送链的情况下自行走到哪里，以及流程在哪里被治理或编排边界卡住。

## 1. 总体结论

本次测试没有进入 report-basis freeze，也没有生成 final publication。最终状态是：

- `runtime_health.alert_status=green`
- `open_dead_letter_count=0`
- `pending_transition_request_count=0`
- `pending_skill_approval_request_count=0`
- board rollup 为 `in-flight`
- `challenge-38c1d2bb31df` 仍 open
- `boardtask-163850dbe59f` 和 `boardtask-cd2f07e40d4e` 仍 claimed/open
- 没有 evidence bundle、hypothesis、proposal、report-basis、reporting handoff 或 final publication

本轮实质完成的是一个开放式 scoping + 初步环境证据试探，不是完整的纽约烟霾事件归因调查。它支持以下受限判断：

- mission 没有提供时间窗、空间范围、数据源或假设；runtime 正确保持 `scoping-required`，没有自动锁定调查范围或来源。
- moderator 自行建立了 scoping boundary、investigation plan 和 round brief，将问题拆成事件窗口、受体区域、上游来源、输送路径、观测影响和证据限制。
- environmental-investigator 在未被 mission 显式提示加拿大或 FIRMS 的情况下，自主抓取了 AirNow、Open-Meteo historical、NASA FIRMS historical 三类证据，并提交了一个 interim finding。
- 该 finding 支持“纽约受体区在 2023 年 6 月 6-8 日出现明显 PM2.5 / AQI 异常，6 月 7 日达到高峰；同期本地受体风场并非静稳局地累积；宽泛上游扫描中存在大量火点，尤其在纽约以北方向”。
- 该 finding 没有证明唯一因果链，也没有关闭 mixed/local alternatives。
- social-investigator 没有编造公共讨论或正式记录证据，而是明确记录 public/formal lane 当前没有 selected sources、没有 ingested rows，因此不能贡献 substantive finding。
- challenger 成功阻止 premature attribution，把“时间误界定、空间误界定、替代或混合源解释”写成 live challenge 和 acceptance test。
- moderator 最终把 round 记录为 coordination freeze point，而不是 readiness-ready 或 report-basis-ready。

最重要的能力信号：在 mission 只说“调查 2023 年纽约烟霾事件”且没有提到加拿大的情况下，environmental-investigator 自主发现并引入了上游火点方向的证据。这说明 agent 具备一定开放式调查能力。

最重要的系统缺口：source-selection 和 approval request 两条治理链没有顺畅支撑 agent 自主调查。social/formal lane 被 source-selection pending 卡住；challenger 想运行 `open-falsification-probe` 时被 preflight 拦截，但没有形成可由 runtime-operator 审批的正式 skill approval request。

## 2. Council Agent 与 Runtime Principal 分工

本节继续显式区分概念模型和代码角色模型：`runtime` 是维持议会可运行、可编排、可审计的框架，不是 agent；`runtime-operator` 是代码中的审批、审计和死信处理主体，不参与事实推理；`moderator` 才是议会组织者。

| 概念层级 | 代码角色/主体 | 本轮实际动作 | 输出或影响 |
| --- | --- | --- | --- |
| council agent | `moderator` | 建立 scoping plan、round brief、challenge disposition、readiness block、coordination tasks、freeze-point board notes | 组织议会，保持问题开放；没有请求 freeze-report-basis |
| council agent | `environmental-investigator` | 提出时间/地理 scope，提交环境证据请求，自主抓取 AirNow、Open-Meteo、FIRMS，提交 interim finding、agent position 和 readiness opinion | 提供本轮唯一 substantive evidence-backed finding，但明确 not ready |
| council agent | `social-investigator` | 提出 public/formal evidence request，查询 social/formal lane，发现 0 rows 和 source-selection pending，提交 gap/blocker board notes | 没有编造社会/正式记录证据；把 lane 正确标成 blocker |
| council agent | `challenger` | 提出反证 evidence request，打开 live challenge，记录 acceptance test，尝试 falsification helper 但被 governance 拦下 | 保持 attribution 不可锁定；未绕过 approval gate |
| council agent | `report-editor` | 未进入 reporting 阶段 | 没有 expert report 或 final publication |
| runtime principal | `runtime-operator` | 启动 run、注册 OpenClaw agents、处理 non-blocking dead letters、检查 runtime health | 维持流程健康；未审批调查方向，未运行未授权 probe |
| runtime framework | runtime kernel / gate / ledger | 记录 receipts、admission、dead letters、health、agent entry surfaces | 保持审计链，但也暴露 source-selection / approval request 链接不顺 |

## 3. Mission 与初始状态

本轮 mission 是面向用户的自然语言请求 envelope，不是 moderator 的调查计划，也不是事实归因：

```json
{
  "topic": "2023 New York City smoke haze event",
  "objective": "请调查 2023 年纽约烟霾事件：说明发生了什么、可能原因是什么、哪些证据支持或限制这些判断，并给出证据支持的结论。",
  "source_requests": [],
  "artifact_imports": []
}
```

`mission_scaffold_round-001.json` 的关键结果：

- `scoping_required=true`
- `missing_fields=["window.start_utc", "window.end_utc", "region.label", "region.geometry"]`
- `request_source_count=0`
- `import_source_count=0`
- `task_count=0`
- `seeded_hypothesis_ids=[]`
- `intent_sources_by_role.environmental-investigator=[]`
- `intent_sources_by_role.social-investigator=[]`

含义：runtime 没有从 mission 自动生成具体 source selection、任务或假设。调查自由度主要交给 council agents；但 source-selection artifacts 因缺少源请求而保持 pending / empty。

## 4. Source Selection 状态

本轮两个主要调查角色都有可用 source families，但最终 selected sources 都为空。

| 角色 | allowed sources | selected sources | 状态 | 影响 |
| --- | --- | --- | --- | --- |
| `environmental-investigator` | AirNow、OpenAQ、Open-Meteo historical / air quality / flood、USGS Water、NASA FIRMS | `[]` | `pending` | 自动队列没有给环境 lane 选源；agent 后续通过自身 role surface 自主触发 governed fetch |
| `social-investigator` | Bluesky、GDELT doc/events/mentions/GKG、YouTube、Regulations.gov | `[]` | `pending` | public/formal lane 没有可执行 fetch+normalize 路径，后续只能记录 gap/blocker |

这与第一次真实案例 run 不同：第一次 run 中 source_requests 直接激活了 GDELT 和 Open-Meteo，本轮为测试开放式调查能力故意没有提供 source_requests。

## 5. 按时间顺序的完整过程

### 5.1 Run 初始化与 Agent 注册

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-09T17:04:44Z | `scaffold-mission-run` | `moderator` | 从 `input/mission.json` materialize mission scaffold、board、round tasks | mission 缺少 window/region/source_requests，进入 `scoping-required` | runtime 没有生成 seeded hypotheses 或 source tasks |
| 2026-05-10T03:43 前后 | `start-council-run` / agent registration | `runtime-operator` | materialize agent entry gate 和 OpenClaw agent registration | 注册 moderator、environmental-investigator、social-investigator、challenger、report-editor | operator 只做运行框架准备 |

本轮 agent id 在 OpenClaw 中被规范化为：

- `openclaw-realcase-nyc-smoke-transport-chain-20230607-moderator`
- `openclaw-realcase-nyc-smoke-transport-chain-20230607-environment`
- `openclaw-realcase-nyc-smoke-transport-chain-20230607-social-inve`
- `openclaw-realcase-nyc-smoke-transport-chain-20230607-challenger`
- `openclaw-realcase-nyc-smoke-transport-chain-20230607-report-edit`

### 5.2 Moderator 建立 scoping frame

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T03:43:15Z | `submit-investigation-plan` | `moderator` | 提交 `investigation-plan-38dfe0c18164` | 先组织边界设定、事件描述、候选原因评估和证据限制 | 不把因果叙事提前固定 |
| 2026-05-10T03:43:15Z | `submit-investigation-scope` | `moderator` | 提交 `investigation-scope-63f6ee9758eb` | 受体区域为 NYC；上游 source region 待证据确定 | 明确 source region 不由 mission 预设 |
| 2026-05-10T03:43:16Z | `submit-round-brief` | `moderator` | 提交 `round-brief-aba4d2a9c32a` | round mode 为 `scoping`，邀请 environmental、social、challenger | 要求 scope refinements、evidence requests、early positions |

moderator 的关键行为是把 mission 转成议会可讨论对象，而不是替 agent 写结论。它的问题设置包括：

- 什么 verified event window 和 receptor/upstream boundary 应该锚定调查？
- NYC 发生了什么，哪些 observable conditions 被直接支持？
- 哪些 candidate source / transport explanations 可支持，限制是什么？

### 5.3 Environmental investigator 建立环境 lane

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T03:45:49Z | `submit-investigation-scope` | `environmental-investigator` | 提交 `investigation-scope-018969a52a5c` | 需要把证据限定到 June 2023 NYC smoke episode 和邻近输送期 | 补足 temporal-window |
| 2026-05-10T03:45:49Z | `submit-investigation-scope` | `environmental-investigator` | 提交 `investigation-scope-bec295992634` | 锚定 NYC 暴露，同时保留上游输送上下文 | 补足 geographic-region |
| 2026-05-10T03:45:50Z | `submit-evidence-request` | `environmental-investigator` | 提交 `evidence-request-3c6a21c3c813` | 要求先建立 NYC 烟霾 episode timeline 和 spatial footprint，再谈 source attribution | 将环境 lane 正式化 |

这一阶段还没有实证 fetch，只是把环境调查的范围和证据需求写入 council state。

### 5.4 Social investigator 建立 public/formal lane

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T03:47:47Z | `submit-evidence-request` | `social-investigator` | 提交 `evidence-request-dfe196ae4634` | 需要 public discourse、formal record、policy-relevant evidence 支撑事件如何被描述、警告和因果化 | 与环境 lane 并列，但没有数据源被选中 |

该 agent 后续没有直接 fetch 公共材料，因为 source selection 为空。

### 5.5 Challenger 打开反证约束

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T03:49:55Z | `submit-evidence-request` | `challenger` | 提交 `evidence-request-088e32e4e71d` | 要求测试 upwind wildfire transport 与 local confounders、mixed sources、timing/region mis-bounding | 反证 lane 正式进入 council |
| 2026-05-10T03:49:56Z | `open-challenge-ticket` | `challenger` | 打开 `challenge-38c1d2bb31df` | 不得在 contradiction testing 前锁定 wildfire-transport attribution | 约束后续 readiness 和报告基础 |
| 2026-05-10T03:52:22Z | `submit-challenge-disposition` | `moderator` | 提交 `challenge-disposition-ec9c0a41ab1d` | `upheld-as-live-constraint` | moderator 接受 challenger 的 live constraint |

challenger 的 challenge statement 要求测试三类矛盾路径：

- temporal mis-bounding of the NYC haze episode
- geographic mis-bounding between receptor region and candidate upstream source region
- alternative or mixed-source explanations

### 5.6 Moderator 第一次 readiness 判断

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T03:52:22Z | `submit-agent-position` | `moderator` | 提交 `agent-position-452799b9d490` | `needs-more-evidence` | 有 scoping frame 和 evidence lanes，但没有可承载结论的 evidence uptake |
| 2026-05-10T03:52:23Z | `submit-readiness-opinion` | `moderator` | 提交 `readiness-opinion-59049bb441fe` | `blocked`，`sufficient_for_report_basis=false` | 没有 evidence bundles / positions 解决 live challenge |

这一步是治理上正确的：moderator 没有因为已有 plan/scope/evidence requests 就升级 readiness。

### 5.7 Environmental investigator 自主抓取环境证据

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T03:57 - 04:02 | governed fetch attempts | `environmental-investigator` | 自主选择 AirNow、Open-Meteo historical、NASA FIRMS historical 作为第一轮环境证据 | 初始几次 CLI 调用格式错误或 NRT source 不覆盖 2023，随后自行修正 | 产生 dead letters，但成功 receipts 后被 operator 关闭 |
| 2026-05-10T04:05:17Z | `submit-finding-record` | `environmental-investigator` | 提交 `finding-38401351d52e` | NYC receptor spike aligns with June 7 peak and active upwind fire window | 本轮唯一 substantive finding |

该 finding 使用的 evidence refs：

- `runtime-receipt-d81cde995240d881f116`：AirNow hourly observations
- `runtime-receipt-4ae6306b8da0101cf228`：Open-Meteo historical receptor meteorology
- `runtime-receipt-108b9e79a869d3d11c3f`：NASA FIRMS historical fire detections

finding 的关键内容：

- AirNow receipt 覆盖 NYC-area bounding box，时间为 `2023-06-05` 至 `2023-06-09`。
- PM2.5 daily maxima 从 6 月 5 日的 `60` 上升到 6 月 6 日的 `196`，并在 6 月 7 日达到 `413`。
- fetch set 中最高观测 AQI 为 `413`，站点为 `Bklyn - PS 314`，时间为 `2023-06-07T19:00Z`。
- Open-Meteo 显示 6 月 6-7 日 10m wind average 约 `15-16 km/h`，主导方向为 northerly/westerly，不像纯静稳本地累积。
- FIRMS broad upstream scan 返回 `20,398` detections，其中 rough eastern-Canada candidate band 返回 `18,432` detections。
- 结论只支持继续做 transport-focused source testing，同时保留 mixed/local alternatives。

这一步是本轮最重要的 agent 能力信号：mission 没有提加拿大，agent 自主找到上游火点证据。但证据仍是 receipt-level descriptive summary，没有形成 normalized environmental signal 查询结果或 evidence bundle。

### 5.8 Social investigator 发现 source-selection blocker

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T04:07:41Z | `post-board-note` | `social-investigator` | 提交 `boardnote-49e64edd15c1` | `query-public-signals=0 rows`，`query-formal-signals=0 rows` | 记录为空是 evidence gap，不是负面证据 |
| 2026-05-10T04:10:37Z | `claim-board-task` | `moderator` | 提交 `boardtask-163850dbe59f` | public/formal lane 需要 operator-approved fetch+normalize 或 artifact ingestion | readiness/challenge closure 前必须解决 |
| 2026-05-10T04:11:57Z | `post-board-note` | `social-investigator` | 提交 `boardnote-a3b61a656d01` | `source_selection_social-investigator_round-001.json` 为 `pending`，`selected_sources=[]` | blocker 是 source-selection，而不是社会证据不存在 |
| 2026-05-10T04:12:58Z | `claim-board-task` | `moderator` | 提交 `boardtask-cd2f07e40d4e` | 将 social/formal source-selection pending 升级为 board-level escalation | 保持与 evidence-request 和 live challenge 绑定 |

这一段暴露的不是 agent 能力问题，而是 runtime/source-selection 链路问题：social agent 看到了可以调查的 lane，但没有被授权或自动选择任何 source。

### 5.9 Challenger 的 falsification probe 被治理拦截

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T04:08:54Z | `post-board-note` | `challenger` | 提交 `boardnote-6b02d09e1aee` | 把 live challenge 转成 acceptance test | 时间、地理、alternative/mixed-source 必须 evidence-backed |
| 2026-05-10T04:08:54Z | `preflight-skill open-falsification-probe` | runtime gate | 拦截 challenger 运行 falsification helper | 缺少显式 `skill_approval_request_id` | challenger 没有绕过 |
| 2026-05-10T04:09:32Z | `resolve-dead-letter` | `runtime-operator` | 关闭 admission-block dead letter | 记录该 block 已被遵守，且没有正式 approval request 可审批 | 没有运行未授权 probe |

关键问题：challenger 知道该 probe 需要 operator approval，但系统没有自动生成 `skill-approval-request` 对象。因此 operator 无法执行“提权审批”，只能确认 admission block 被遵守。

### 5.10 Environmental readiness posture

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T04:15:29Z | `submit-agent-position` | `environmental-investigator` | 提交 `agent-position-760f9a920d79` | 环境证据支持真实 June 7 receptor spike，并保持 cross-border transport 为 live explanatory route | 不足以主张 singular source attribution |
| 2026-05-10T04:15:42Z | `submit-readiness-opinion` | `environmental-investigator` | 提交 `readiness-opinion-64df2f129a57` | `not-ready`，`sufficient_for_report_basis=false`，confidence `0.76` | open contradiction requirement、source geometry 过宽、无 council-wide readiness |

environmental agent 后续尝试把已有 artifacts 推入 normalized signal query，但 `query-environment-signals` 参数和表结构不匹配，未产生新的 evidence claim。该失败最终被 operator 记录为 nonblocking query contract mismatch。

### 5.11 Moderator 最终 freeze point

| 时间 | 事件 | Agent / 角色 | 行动 | 结论和理由 | 交互 |
| --- | --- | --- | --- | --- | --- |
| 2026-05-10T04:17:30Z | `summarize-board-state` | `moderator` | 生成 `board-summary-83a777952e32` | board rollup 为 `in-flight` | 1 open challenge，2 open tasks，0 hypotheses |
| 2026-05-10T04:17:31Z | `post-board-note` | `moderator` | 提交 `boardnote-41f624d99f22` | 当前状态是 coordination freeze point | 不支持 readiness、challenge closure 或 report-basis lock |
| 2026-05-10T04:17:46Z | `materialize-runtime-health` | `runtime-operator` | 刷新 runtime health | `green`，0 dead letters，0 failed/blocked events | 流程健康，但调查未完成 |

最终 board summary：

- `challenge_open=1`
- `tasks_open=2`
- `tasks_claimed=2`
- `hypotheses_total=0`
- `notes_total=6`
- `status_rollup=in-flight`

open challenge：

- `challenge-38c1d2bb31df`: `Do not lock wildfire-transport attribution before contradiction testing`

open tasks：

- `boardtask-163850dbe59f`: `Resolve empty public/formal evidence lane before readiness review`
- `boardtask-cd2f07e40d4e`: `Escalate pending source-selection blocker for social/formal lane`

## 6. Agent-by-agent 复盘

### 6.1 Moderator

moderator 是本轮实际组织者。它没有替其他 agent 做调查结论，而是持续维护议会边界：

- 将 mission 转成 investigation plan、scope、round brief。
- 邀请 environmental、social、challenger 三个角色进入 scoping。
- 接受 challenger 的 live constraint。
- 在 evidence lanes 未满足时提交 `needs-more-evidence` position 和 `blocked` readiness opinion。
- 对 social/formal lane 空结果建立 coordination tasks。
- 最终把 round 固定为 coordination freeze point，而不是强行进入 report basis。

评价：moderator 表现稳健，没有过早发布结论。但它也没有自动发起 phase transition 或 skill approval request，导致一些应由治理链推动的动作没有形成正式请求。

### 6.2 Environmental Investigator

environmental-investigator 是本轮唯一产出 substantive finding 的角色。

它做对的事：

- 自主把调查从泛化 NYC smoke haze 推向具体环境证据。
- 自行发现 AirNow、Open-Meteo、FIRMS 组合能测试受体异常、气象背景和上游火点。
- 在 mission 未提示加拿大的情况下引入 broad upstream / eastern-Canada candidate band。
- 没有把火点存在直接等同于输送因果。
- 最终主动声明 not-ready。

它暴露的问题：

- 多次先用错 skill CLI 形态，造成 dead letters。
- FIRMS 首次选择 NRT source，发现不覆盖 2023 后才切换 historical source。
- 后续 normalized signal query 未成功，finding 仍是 receipt-level descriptive summary。
- 没有形成 evidence bundle 或 hypothesis card。

评价：开放式调查能力强于当前 source-selection 自动链路；但工具契约提示、source family historical/NRT 区分、环境信号归一化查询接口仍需改进。

### 6.3 Social Investigator

social-investigator 没有做 substantive social finding。它做的是 gap governance：

- 提交 public-discourse/formal-record evidence request。
- 查询 public/formal signal surfaces，均为 0 rows。
- 读取 `source_selection_social-investigator_round-001.json`，发现 selected sources 为空、status pending。
- 明确声明这不是负面证据，而是 source-selection / ingestion blocker。

评价：该 agent 没有编造证据，这是正确行为。但当前系统没有给它开放式选源或请求选源的顺滑路径，导致它只能记录 blocker。

### 6.4 Challenger

challenger 本轮发挥了治理价值：

- 主动开出 falsification lane。
- 把 wildfire-transport attribution 锁定前必须满足的 contradiction tests 写成 live challenge。
- 在 environmental finding 出现后仍保持约束，没有让 council 过早进入报告。
- 尝试 `open-falsification-probe` 时被 approval gate 拦下，且没有绕过。

评价：challenger 约束有效。但 approval-gated helper 没有被自然转成 formal approval request，导致 runtime-operator 无法审批执行。

### 6.5 Report Editor

report-editor 本轮没有进入实际工作，因为：

- no report-basis freeze
- no reporting handoff
- no council decision draft
- no expert-report draft

评价：这是正确结果。当前 round 不应进入 reporting。

### 6.6 Runtime Operator

runtime-operator 本轮只做程序性工作：

- 启动 run。
- materialize agent registration。
- 注册 OpenClaw agents。
- 关闭已被 agent 自行修正或非阻断的 dead letters。
- 检查 runtime health。
- 未审批任何未形成正式 request 的 probe 或 transition。

评价：operator 没有干预调查方向，符合本轮测试要求。

## 7. 本轮证据支持的调查判断

### 7.1 已支持

在本轮已提交的 council objects 中，证据支持以下判断：

- 2023 年 6 月 6-8 日，NYC receptor area 出现明显 PM2.5 / AQI 异常。
- 本轮 fetch set 中最高观测值出现在 `2023-06-07T19:00Z`，站点为 `Bklyn - PS 314`，AQI 为 `413`。
- 6 月 6-7 日 receptor meteorology 显示 10m wind 平均约 `15-16 km/h`，主导 northerly/westerly quadrant，不像纯静稳本地累积。
- 同一窗口 broad upstream FIRMS scan 显示大量火点，其中 rough eastern-Canada candidate band 有大量 detections。
- 因此，“上游野火输送”是一个应继续测试的 live explanatory route。

### 7.2 未支持

本轮尚不能支持：

- 唯一源归因。
- 完整传输链确认。
- 具体烟羽路径或轨迹。
- 哪些火点实际贡献了 NYC smoke exposure。
- official advisory / formal record / public discourse framing。
- health impact conclusion。
- policy recommendation。
- report-basis freeze 或 final publication。

### 7.3 议会最终 posture

最终 posture 不是“调查失败”，而是：

```text
coordination freeze point / in-flight / not report-basis-ready
```

原因是：

- environmental lane 有初步证据，但 source geometry 仍宽。
- social/formal lane 没有 selected sources 或 ingested rows。
- challenger live challenge 未关闭。
- 没有 evidence bundle、hypothesis、proposal 或 readiness consensus。

## 8. 暴露问题

### 8.1 Source selection 对开放式 mission 支持不足

mission 没有 source_requests 时，source-selection artifacts 对 environmental 和 social 都给出 `selected_sources=[]`。这让 social lane 无法推进，也让 environmental lane 依赖 agent 自行绕过自动队列去调用 fetch skills。

这不是要加入厚重启发式或权值排序，而是需要一个更薄的机制：agent 应能从 evidence-request 或 board task 触发 source-selection request / operator-approved ingestion request，而不是 runtime 自动替它排序选源。

### 8.2 Approval-gated helper 没有形成正式 approval request

challenger 尝试 `open-falsification-probe` 时，runtime preflight 正确拦截。但系统没有自动生成 `skill-approval-request`，导致 operator 无法审批。

治理上正确的是：

- agent 表达需要运行 approval-gated helper；
- moderator 或 runtime surface 形成正式 approval request；
- runtime-operator 审批；
- agent 消费 `skill_approval_request_id` 执行。

本轮只完成了第一步和 preflight block。

### 8.3 Tool contract 可读性仍不足

多个 agent 自行修正了 CLI 参数错误，但 dead letters 表明工具契约仍不够顺手：

- fetch skills 需要内部 `fetch` 子命令，agent 初次容易漏掉。
- FIRMS NRT / historical source 区分没有在 agent surface 中足够显眼。
- `query-environment-signals` 不支持 `--signal-kind`，agent 多次尝试错误参数。
- readiness opinion 参数使用 hyphenated option，agent 初次使用 underscore option。

这些问题不需要引入更多议程规则，而需要更清晰的 role-local command templates / examples。

### 8.4 环境证据未进入完整归一化查询闭环

environmental finding 的 evidence refs 是 runtime receipts，属于可审计证据，但后续 normalized signal plane query 没有成功。结果是 finding 能说明 receipt-level summary，却没有形成可被后续 agent 稳定复查的 normalized query result set。

### 8.5 当前没有 evidence bundle / hypothesis 中间层

本轮 environmental finding 已经能支持一个 provisional hypothesis，但没有通过 `update-hypothesis-status` 或 `submit-evidence-bundle` 固化为更强的跨 agent 讨论对象。moderator 因此也没有进入 proposal 或 freeze gate。

## 9. 与第一次真实案例 run 的差异

| 维度 | 第一次真实案例 run | 本次开放式调查 run |
| --- | --- | --- |
| mission | 预置 source requests、窗口和目标更具体 | 用户自然语言请求，无 source requests、无 Canada hint |
| source selection | 直接选中 GDELT 和 Open-Meteo | environmental/social selected sources 都为空 |
| 数据链路 | fetch + normalize + finding + bundle + report basis + publication | receipt-level environmental finding + blocker notes |
| agent 自主性 | source_requests 主导较强 | environmental agent 自主发现火点方向 |
| challenger | 作为 report caveat | 作为 live contradiction constraint |
| final output | final publication ready-for-release，但边界偏薄 | 无 final report，停在 coordination freeze |
| 主要风险 | 过早进入报告，证据边界混乱 | 治理链不足以把开放式调查自然推进到下一阶段 |

本轮的质量更保守：没有在证据不足时发布结论。但也说明开放式 run 需要更好的 agent-triggered source selection 和 approval request 封装。

## 10. 收口判断

本轮 council 已经证明：

- agent 能从泛化用户 mission 中自主提出调查边界；
- environmental lane 能在未提示加拿大的情况下发现上游火点证据；
- challenger 能有效防止 premature attribution；
- moderator 能维持 not-ready / freeze point，而不是强行报告；
- runtime 能保持审计闭环并恢复 green health。

本轮同时证明：

- social/formal lane 目前无法靠现有 source-selection 自动推进；
- approval-gated optional analysis 缺少正式 request 生成链；
- query/fetch command templates 对 agent 不够清晰；
- receipt-level evidence 还没有自然升级成 evidence bundle / hypothesis / report-basis 的完整议会链。

因此，本轮合理结论是：

```text
OpenClaw council has demonstrated meaningful open-investigation behavior for the NYC smoke case, especially in the environmental lane, but the run correctly remains not report-ready. The next system work should improve agent-triggered source selection, approval-request generation, and evidence-bundle/hypothesis handoff without adding thick contracts, heuristic scoring, or runtime-authored investigation direction.
```
