# OpenClaw 下一阶段开发规划

## 1. 规划目标

下一阶段不再延续“补完上一轮蓝图”的思路，而是明确开启新一轮问题导向开发。

新的主目标是：

`把 OpenClaw 从“事件式核实系统”推进为“环境争议地图 / 调查分诊系统”。`

这一阶段的产出应同时满足三类要求：

1. 研究上能回答更清楚的问题。
2. 工程上能落到现有 DB-first 骨架里。
3. 展示上能用少量 case 明确体现环境领域价值。

## 2. 本阶段不追求的内容

为避免方向继续发散，下一阶段暂不把以下内容设为主目标：

1. 泛化的“环境政策评估”
2. 完整持久的 multi-session agent runtime
3. 更复杂的发布与 publication 包装
4. 再扩一批数据源以追求覆盖面
5. 继续强化默认的 claim-observation-link-coverage 主链

这些方向不是永久放弃，而是优先级后移。

## 3. 本阶段的核心问题

建议用下面的问题作为下一阶段总问题：

`对于一个环境争议案例，OpenClaw 能否整合正式评论与开放平台讨论，识别主要议题、立场、关切、主体与扩散关系，并据此判断哪些内容需要进一步核实、回应或补充调查？`

围绕这个总问题，系统应形成四类输出：

1. `争议结构`
   - 当前争议包含哪些 issue cluster。
2. `争议参与者`
   - 谁在表达，谁缺位，哪些主体相互呼应或冲突。
3. `调查分诊`
   - 哪些说法需要进入核实支路，哪些不应走物理证据匹配。
4. `行动建议`
   - 下一轮调查是补 formal-public linkage、补代表性缺口，还是补外部验证。

## 4. 分批路线

### Batch 0: 研究问题与演示口径收束

目标：

1. 统一项目对外叙事。
2. 把 demo case 从“单纯核实事件”扩展到“环境争议案例”。
3. 明确评价标准，不再只看 coverage 与 readiness。

本批产物：

1. 主文档重写
2. 下一阶段开发规划
3. skill 改造清单
4. 一套新的 case 选题标准

完成标志：

1. 能用一句话讲清系统新方向。
2. 能明确区分“当前已会做什么”和“下一阶段要解决什么”。

### Batch 1: 重定义 analysis 对象

目标：

1. 把 analysis plane 的主对象从事件核实对象转向争议分析对象。
2. 为后续新 skill 提供统一的结果集和 lineage 语义。

本批建议新增或调整的对象：

1. `issue-cluster`
2. `stance-candidate`
3. `concern-facet`
4. `actor-profile`
5. `evidence-citation-type`
6. `diffusion-edge`
7. `verifiability-assessment`

完成标志：

1. 新对象能进入 analysis plane。
2. 不依赖 observation/link/coverage 也能形成一轮基础分析。

### Batch 2: 重构 public-side 主分析链

目标：

1. 替换当前过度启发式的 claim 抽取与聚类主线。
2. 让系统能先生成争议地图，而不是直接进入核实。

本批核心能力：

1. issue 抽取与聚类
2. stance 抽取
3. concern 抽取
4. actor 抽取
5. evidence citation type 抽取

完成标志：

1. 对 formal comments 和开放平台文本都能生成统一争议结构。
2. 输出不再只是 `claim cluster`，而是可供 moderator 使用的争议图。

### Batch 3: formal-public linkage 与 diffusion

目标：

1. 让正式评论与开放平台讨论之间形成可追踪联系。
2. 判断争议是否跨平台扩散，以及扩散路径如何。

本批核心能力：

1. `formal comment <-> public discourse` 对齐
2. cross-platform diffusion detection
3. representation gap detection

完成标志：

1. 能回答“正式程序里说了什么”和“开放平台上真正热议什么”是否一致。
2. 能指出高传播议题与高正式关注议题的重叠与错位。

### Batch 4: verification routing 与可选环境核实支路

目标：

1. 保留环境观测链，但把它降为“按需调用的支路”。
2. 在争议地图之后，再决定哪些内容值得走 observation matching。

本批核心能力：

1. verifiability classification
2. verification routing
3. optional claim-observation verification

完成标志：

1. 系统能明确区分：
   - 该去查环境数据的内容
   - 不该去查环境数据的内容
2. 物理与舆情匹配不再作为默认主链出现。

### Batch 5: board / reporting / benchmark 适配

目标：

1. 让 moderator、next actions、readiness、handoff 能消费新的争议对象。
2. 用新的 benchmark case 验证系统已经不只是做“事件核实”。

本批核心调整：

1. `next actions` 从“补 coverage”改为“补争议结构缺口”
2. `probe` 从“削弱当前说法”改为“定位争议不确定点或代表性缺口”
3. `readiness` 从“证据覆盖度”改为“争议图是否足够支持下一步动作”
4. reporting 从“证据 basis”扩展为“争议结构 + 分诊判断”

完成标志：

1. 新 case 能稳定产出争议地图。
2. board 和 reporting 能围绕争议结构继续推进。

## 5. 实施顺序上的硬约束

下一阶段建议遵守三个顺序约束：

1. 先改对象，再改 skill。
   - 否则会出现新 skill 继续往旧对象里硬塞的问题。
2. 先改 public-side 主链，再改 reporting。
   - 否则 reporting 只能继续消费旧分析结构。
3. 先建立 routing，再决定 observation 链保留方式。
   - 否则系统仍会下意识把每个问题都变成“去找环境观测证据”。

## 6. 推荐 case 类型

下一阶段至少应准备两类 case，而不是只留一种 benchmark：

1. `争议型政策 case`
   - 适合体现正式评论、公众讨论、立场与关切分析。
2. `可核实事件 case`
   - 适合保留环境数据核实支路，证明 optional verification lane 仍有价值。

这样才能清楚展示：

1. 哪些问题是“争议理解”
2. 哪些问题是“经验核实”
3. OpenClaw 如何在两者之间分诊

## 7. 阶段完成标准

下一阶段至少应满足以下判断标准，才算真正完成方向切换：

1. 能用争议地图而不是 coverage 图来解释系统输出。
2. formal comments 不再只是 generic public signal，而是能参与结构化争议分析。
3. 物理与舆情匹配只在明确可核实时触发。
4. next actions 不再默认围绕 `expand-coverage / resolve-contradiction` 组织。
5. 至少有一个 case 能明显证明系统不只是“事件核实器”。
