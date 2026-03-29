# OpenClaw 下一批计划

## 1. 当前批次只追三个结果

1. runtime source-queue 重建方案收敛成可编码清单。
2. 所有活跃 skills 都有统一的 source-queue profile。
3. OpenClaw adapters 成为下一批唯一主实现面。

## 2. 下一批验收线

1. prepare-round 不再只是本地 artifact import planner，而是 source-selection aware planner。
2. import-fetch-execution 能区分 import step 与 future detached fetch step。
3. runtime registry 能完整导出所有 skills 的 queue 角色、阶段和调用方式。
4. OpenClaw managed skill projection 开始接管统一 skill surface。

## 3. 参考文档

1. [openclaw-runtime-source-queue-rebuild.md](openclaw-runtime-source-queue-rebuild.md)
2. [openclaw-dual-flow-skill-assessment.md](openclaw-dual-flow-skill-assessment.md)
