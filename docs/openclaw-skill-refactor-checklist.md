# OpenClaw Skills 改造逐项清单

## 1. 使用方式

本清单只服务下一阶段开发，不再承担“总体介绍”功能。

状态约定：

1. `[保留]`
2. `[降级]`
3. `[第一批重构]`
4. `[第二批重构]`
5. `[新增]`
6. `[暂缓]`

## 2. 保留为底座

### 2.1 Runtime 与 round 编排

- `[保留]` `eco-scaffold-mission-run`
  - 保留运行脚手架能力，但后续要调整 mission 描述与 seed task 口径。
- `[保留]` `eco-open-investigation-round`
  - 保留 round 推进，不作为方向问题。
- `[保留]` `eco-plan-round-orchestration`
  - 保留编排骨架，后续再适配新对象。
- `[保留]` `eco-prepare-round`
  - 保留 source governance 与 fetch plan 编译。
- `[保留]` `eco-import-fetch-execution`
  - 保留受治理导入执行面。

### 2.2 数据抓取与归一化

- `[保留]` public fetchers
  - `regulationsgov-comments-fetch`
  - `regulationsgov-comment-detail-fetch`
  - `youtube-video-search`
  - `youtube-comments-fetch`
  - `bluesky-cascade-fetch`
  - `gdelt-doc-search`
  - `gdelt-events-fetch`
  - `gdelt-gkg-fetch`
  - `gdelt-mentions-fetch`
- `[保留]` public normalizers
  - 所有 `eco-normalize-*-public-signals`
- `[保留]` environment fetchers
  - `airnow-hourly-obs-fetch`
  - `openaq-data-fetch`
  - `open-meteo-air-quality-fetch`
  - `open-meteo-flood-fetch`
  - `open-meteo-historical-fetch`
  - `nasa-firms-fire-fetch`
  - `usgs-water-iv-fetch`
- `[保留]` environment normalizers
  - 所有 `eco-normalize-*-observation-signals`

### 2.3 查询、回查与归档

- `[保留]` `eco-query-public-signals`
- `[保留]` `eco-query-environment-signals`
- `[保留]` `eco-query-signal-corpus`
- `[保留]` `eco-query-case-library`
- `[保留]` `eco-lookup-normalized-signal`
- `[保留]` `eco-lookup-raw-record`
- `[保留]` `eco-materialize-history-context`
- `[保留]` `eco-archive-case-library`
- `[保留]` `eco-archive-signal-corpus`

说明：

这组能力构成下一阶段底座，不宜因为方向调整而误删。

## 3. 降级为可选核实支路

- `[降级]` `eco-extract-observation-candidates`
  - 保留，改为 verification lane 输入，不再作为默认主链。
- `[降级]` `eco-merge-observation-candidates`
  - 保留，服务环境观测聚合。
- `[降级]` `eco-derive-observation-scope`
  - 保留，但只在核实支路使用。
- `[降级]` `eco-link-claims-to-observations`
  - 不再作为核心语义分析能力，只在经验性 claim 上按需调用。
- `[降级]` `eco-score-evidence-coverage`
  - 从“全局 readiness 核心”降为“核实支路的局部支持指标”。

说明：

这组 skill 不是删除，而是从“默认主线”降为“条件触发支路”。

## 4. 第一批必须重构

- `[第一批重构]` `eco-extract-claim-candidates`
  - 改造方向：从 claim 抽取转向 `issue / stance / concern` 的初步抽取入口。
- `[第一批重构]` `eco-cluster-claim-candidates`
  - 改造方向：从 lexical fingerprint 聚类转向 issue cluster。
- `[第一批重构]` `eco-derive-claim-scope`
  - 改造方向：从轻量 location/tag 猜测转向 `verifiability / dispute type` 评估。
- `[第一批重构]` `eco-propose-next-actions`
  - 改造方向：从补 coverage 转向补争议结构缺口。
- `[第一批重构]` `eco-open-falsification-probe`
  - 改造方向：从 generic falsification 改为面向争议不确定点、主体缺口和扩散疑点的 probe。
- `[第一批重构]` `eco-summarize-round-readiness`
  - 改造方向：从 coverage gating 改为 controversy map readiness。

### 第一批重构的预期替换逻辑

- 旧主链：
  - `claim -> cluster -> scope -> link -> coverage`
- 新主链：
  - `issue -> stance -> concern -> actor -> diffusion -> verifiability`

## 5. 第二批再重构

- `[第二批重构]` `eco-build-normalization-audit`
  - 后续可扩展到 formal/public alignment audit。
- `[第二批重构]` `eco-claim-board-task`
  - 需改成能基于争议对象开 task。
- `[第二批重构]` `eco-open-challenge-ticket`
  - 需支持对 issue cluster、actor、diffusion edge 提 challenge。
- `[第二批重构]` `eco-close-challenge-ticket`
  - 跟随 challenge 对象变化调整。
- `[第二批重构]` `eco-summarize-board-state`
  - 要从 board 里读出争议结构，而不只是旧对象计数。
- `[第二批重构]` `eco-materialize-board-brief`
  - 改为输出 controversy-oriented board brief。
- `[第二批重构]` `eco-promote-evidence-basis`
  - 不再只冻结 coverages，应冻结争议地图中的关键对象。
- `[第二批重构]` `eco-materialize-reporting-handoff`
  - 输出结构应从 evidence basis 扩展为 controversy map handoff。
- `[第二批重构]` `eco-draft-council-decision`
  - 报告内容要消费新对象。
- `[第二批重构]` `eco-draft-expert-report`
  - sociologist / environmentalist 的报告结构都需调整。
- `[第二批重构]` `eco-publish-council-decision`
  - 跟随 reporting contract 变化。
- `[第二批重构]` `eco-publish-expert-report`
  - 跟随 reporting contract 变化。
- `[第二批重构]` `eco-materialize-final-publication`
  - 最终 publication 要能表达争议结构与分诊判断。

## 6. 应新增的 skills

- `[新增]` `eco-extract-issue-candidates`
  - 从 formal/public signals 中抽取争议议题。
- `[新增]` `eco-cluster-issue-candidates`
  - 将相近议题组织成 issue cluster。
- `[新增]` `eco-extract-stance-candidates`
  - 识别支持、反对、条件支持、程序性批评、事实性质疑等立场。
- `[新增]` `eco-extract-concern-facets`
  - 抽取健康、生态、成本、程序、公平、信任、社区影响等关切面。
- `[新增]` `eco-extract-actor-profiles`
  - 识别居民、机构、企业、NGO、专家、媒体等主体。
- `[新增]` `eco-extract-evidence-citation-types`
  - 区分法规文本、科研、媒体、个人经历、二手传言等引证类型。
- `[新增]` `eco-link-formal-comments-to-public-discourse`
  - 连接正式评论与开放平台争议表达。
- `[新增]` `eco-detect-cross-platform-diffusion`
  - 分析议题或说法的跨平台传播关系。
- `[新增]` `eco-identify-representation-gaps`
  - 识别哪些关切在正式评论里被放大或被忽略。
- `[新增]` `eco-classify-claim-verifiability`
  - 判断哪些说法可进入外部数据核实。
- `[新增]` `eco-route-verification-lane`
  - 作为主链到核实支路的路由器。
- `[新增]` `eco-materialize-controversy-map`
  - 汇总 issue、stance、concern、actor、diffusion、verifiability，形成最终争议地图。

## 7. 暂缓事项

- `[暂缓]` 更复杂的持久 multi-session agent runtime
  - 不是下一阶段主矛盾。
- `[暂缓]` 更细的 auth / hard isolation
  - 重要，但不优先于研究问题收束。
- `[暂缓]` 再扩一批新数据源
  - 先把现有数据源的分析价值做出来。
- `[暂缓]` 继续强化 publication 样式
  - 先让核心分析对象成立。

## 8. 开发顺序检查表

- `[ ]` 先定义新的 analysis 对象与 contract
- `[ ]` 再改 public-side 主分析链
- `[ ]` 再做 formal-public linkage 与 diffusion
- `[ ]` 再把 environment verification 改成 optional lane
- `[ ]` 最后重构 board / readiness / reporting

## 9. 完成判断

当以下条件同时满足时，可以认为 skill 改造方向成立：

- `[ ]` 系统默认输出已不再是 `claim-observation-link-coverage`
- `[ ]` formal comments 已能生成结构化争议对象
- `[ ]` next actions 已不再主要围绕补 coverage
- `[ ]` 物理与舆情匹配只在明确可核实时触发
- `[ ]` 至少一个争议型 case 能稳定跑通新主链
