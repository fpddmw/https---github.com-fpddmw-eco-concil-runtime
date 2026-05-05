# OpenClaw Schema Migration 硬化工作计划

## 1. 文档定位

本文是独立工作计划，不作为全局总计划，也不要求其他计划同时执行。

目标是在保持轻量 DB-first 架构的前提下，为 SQLite schema 增加明确版本、幂等迁移和旧库升级测试。

## 2. 范围

本计划覆盖：

1. schema version 记录。
2. `schema_migrations` 或等价迁移 ledger。
3. `ensure_*_schema` 的幂等升级行为。
4. 缺列、旧表、旧 index fixture 的升级测试。
5. schema 兼容说明。

## 3. 非目标

本计划不做：

1. 引入 Alembic 或重型 ORM。
2. 大规模重命名表、列、CLI 参数。
3. 改变 canonical object 语义。
4. 要求迁移到非 SQLite 存储。

## 4. 交付物

1. schema version 查询接口或 CLI 输出。
2. 幂等 schema migration ledger。
3. 旧 DB fixture 和升级测试。
4. schema 变更约定文档。

## 5. 工作阶段

### P0：版本策略

1. 选择当前 baseline schema version。
2. 定义 migration id、applied_at、checksum 或 description 字段。
3. 定义 `ensure_schema` 与 migration ledger 的关系。

验收：

1. 新库初始化后能查询 schema version。
2. 重复执行 schema ensure 不产生重复记录或错误。

### P1：迁移入口

1. 增加统一 migration runner。
2. 将现有分散的 `ALTER TABLE IF MISSING` 行为纳入 ledger。
3. 保留旧入口兼容。

验收：

1. 所有现有测试仍通过。
2. 新旧入口的最终 schema 一致。

### P2：旧库 Fixture

1. 制作缺少 metadata index 的旧库。
2. 制作缺少 relation cue 字段或 result kind 支持的旧库。
3. 制作缺少 reporting/runtime snapshot 扩展字段的旧库。

验收：

1. 旧库打开后可自动升级。
2. 旧数据仍可查询。

### P3：失败与回滚边界

1. 明确迁移失败时的错误记录。
2. 确保失败不会静默进入半升级状态。
3. 对 operator 暴露 migration status。

验收：

1. 失败迁移有可读错误。
2. 重试行为可预测且幂等。

### P4：文档与 CI

1. 记录新增 schema 的流程。
2. 将旧库升级测试纳入 CI。

验收：

1. 新增列或 index 必须附带 migration test。
2. CI 能发现旧库不兼容。
