# OpenClaw 大模块拆分工作计划

## 1. 文档定位

本文是独立工作计划，不作为全局总计划，也不要求其他计划同时执行。

目标是在不改变行为和公共接口的前提下，逐步拆分当前过大的 runtime/kernel 模块，降低维护风险和回归半径。

## 2. 范围

本计划覆盖：

1. `deliberation_plane.py` 拆分。
2. `optional_analysis_helpers.py` 拆分。
3. `analysis_plane.py` 拆分。
4. `cli.py` 子命令组织拆分。
5. import compatibility、CLI compatibility 和测试保护。

## 3. 非目标

本计划不做：

1. 改变 skill id。
2. 改变 CLI 命令名称和参数语义。
3. 改变 DB schema。
4. 同时重写业务逻辑。
5. 为拆分而新增抽象层。

## 4. 交付物

1. 每个大模块的拆分边界说明。
2. 小步迁移 PR 或提交序列。
3. compatibility import shims。
4. 每步拆分后的 targeted tests 与 full test 验收记录。

## 5. 工作阶段

### P0：边界盘点

1. 统计大文件中的函数族、调用关系、测试覆盖。
2. 识别公共 import 和 CLI 入口。
3. 定义每个模块的禁止改动面。

验收：

1. 每个候选拆分有明确 owner 文件和兼容入口。
2. 没有先做语义重写。

### P1：Optional-analysis Helpers 拆分

1. 按 relation、scope review、evidence aggregation、formal/public 对照、workflow helper 划分。
2. 保留原模块 re-export。
3. 针对 helper governance 运行回归测试。

验收：

1. optional helper 行为和输出 JSON 不变。
2. approval/governance 字段不回退。

### P2：Analysis Plane 拆分

1. 把 result set persistence、relation query、typed projection、schema ensure 分离。
2. 保留现有 public API。

验收：

1. relation taxonomy tests 通过。
2. query public/formal/environment 相关测试不变。

### P3：Deliberation Plane 拆分

1. 按 council object 写入、board task、readiness、challenge/probe、report basis freeze 划分。
2. 保留现有 wrapper 和 import path。

验收：

1. 多轮、challenge、readiness、freeze tests 通过。
2. DB object shape 不变。

### P4：CLI 拆分

1. 按 runtime admin、signal query、analysis、deliberation、reporting、archive 分组。
2. 保留原 CLI 命令。
3. 增加轻量 command smoke tests。

验收：

1. `--help` 和关键子命令保持兼容。
2. 案例评测或 replay 命令无需修改。

### P5：收尾

1. 删除仅内部使用且已迁移的死代码。
2. 更新模块文档。
3. 记录仍保留的 legacy naming debt。

验收：

1. 全量测试通过。
2. 拆分没有引入新的业务能力或 schema 迁移。
