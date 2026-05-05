# OpenClaw CI 与质量门工作计划

## 1. 文档定位

本文是独立工作计划，不作为全局总计划，也不要求其他计划同时执行。

目标是建立最小但有效的自动化质量门，防止核心 runtime、skills、DB schema 和真实案例评测链路在后续开发中回归。

## 2. 范围

本计划覆盖：

1. Python unittest CI。
2. targeted test jobs。
3. `compileall` 或等价语法检查。
4. 可选 ruff 基线。
5. 文档中的验收命令一致性检查。
6. 不依赖外部网络和 secrets 的 fixture-only 运行。

## 3. 非目标

本计划不做：

1. 一开始强制高覆盖率阈值。
2. 大规模类型标注迁移。
3. 接入真实外部 API 凭据。
4. 把所有 lint 问题一次性清零。

## 4. 交付物

1. CI workflow 配置。
2. 本地等价测试命令。
3. targeted tests 分组说明。
4. CI 失败排查文档。

## 5. 工作阶段

### 当前落地状态

已落地第一块代码：

1. `tools/quality_gate.py`
   - 仓库级质量门入口。
   - 提供 `syntax`、`test`、`full`、`ci`、`list` 子命令。
   - 自动设置 `PYTHONPATH`，覆盖 root 与 `eco-concil-runtime/src`。
   - `syntax` gate 使用 AST parse，并阻断重复字面量 dict key，避免 Python 静默以后写覆盖前写。
2. `.github/workflows/quality-gates.yml`
   - GitHub Actions 入口。
   - 当前执行 syntax gate、全部已定义 targeted gates 和 full unittest gate。
3. 当前代码清理
   - 已清理 runtime/reporting 链路中暴露出的重复 `report_basis_*`、`report_basis_gate*` 与 reporting audit 字段。

当前可用命令：

1. `python3 tools/quality_gate.py syntax`
2. `python3 tools/quality_gate.py test relation-taxonomy optional-guardrails db-recovery runtime-governance reporting case-study`
3. `python3 tools/quality_gate.py full`
4. `python3 tools/quality_gate.py ci`

当前实测状态：

1. `syntax` 通过，覆盖 202 个 Python 文件，并确认无重复字面量 dict key。
2. `db-recovery` targeted gate 通过，6 tests。
3. `db-recovery runtime-governance reporting` targeted gate 通过，76 tests。
4. `full` gate 通过，248 tests；当前耗时约 3 分 27 秒，后续应继续拆更快的 targeted jobs。

当前未闭环项：

1. case-study replay job 只能在真实案例运行并抽取 replay fixture 后启用为稳定门。
2. 轻量 lint 仍未接入；当前 `syntax` 是 AST parse + duplicate-key gate，不检查未定义名称、复杂度或风格。
3. packaging/install gate 仍未接入；当前测试通过 `PYTHONPATH` 运行，没有验证 editable install。
4. 文档验收命令一致性检查仍未自动化。

### P0：基线命令

1. 固定 Python 版本。
2. 固定 full unittest 命令。
3. 固定 relation taxonomy、runtime governance、recovery 的 targeted 命令。

验收：

1. 本地命令和 CI 命令一致。
2. CI 不需要真实外部 API。

### P1：基础 CI

1. 增加 full unittest job。
2. 增加 AST parse 或等价语法检查。
3. 阻断重复字面量 dict key。
4. 缓存依赖但不依赖本地 artifact。

验收：

1. 干净 checkout 可运行测试。
2. 失败日志能定位测试名。

### P2：Targeted Jobs

1. relation taxonomy job。
2. DB-only recovery job。
3. runtime-governed execution job。
4. case-study evaluation job。
5. case-study replay job，仅在真实案例运行和修复完成后启用。

验收：

1. 常见回归可在较短 job 中暴露。
2. full job 仍作为最终兜底。

### P3：轻量 Lint

1. 引入 ruff 或等价工具的最小规则。
2. 先覆盖语法、未定义名称、明显错误。
3. 保留当前 duplicate-key gate，避免等到完整 lint 才拦截静默覆盖。
4. 暂缓风格类大改。

验收：

1. lint 不强迫无关格式化 churn。
2. 新增明显错误会被 CI 阻断。

### P4：文档验收命令

1. 汇总文档中出现的测试命令。
2. 确保核心命令能在 CI 或本地 fixture 环境执行。
3. 标注需要手工环境的命令。

验收：

1. 文档不保留已失效验收命令。
2. case-study、relation、recovery、runtime 四类命令有明确运行位置。
