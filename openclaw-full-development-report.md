# OpenClaw 当前判断

## 1. 现在的真实状态

- skill-first 主链已经存在。
- OpenClaw 多 agent 主框架还没有落地。
- 活跃 runtime source-queue 还只是最小 import baseline，不接近 production-ready。
- 下一步重点不是继续扩 controller，而是补 source-queue 重建骨架和 OpenClaw adapters。

## 2. 现在只保留四个判断

1. 一层 shared skills。
2. OpenClaw 多 agent 是主路径。
3. runtime source-queue 是第二编排面。
4. 生产化工作后置到两条编排都跑通之后。

## 3. 当前编码顺序

1. 重建 runtime source-selection 与 fetch-plan governance。
2. 让 runtime registry 为所有 skills 输出机器可读的 source-queue profile。
3. 落地 adapters/openclaw 和 managed skill projection。
4. provision role workspaces，并接上最小 turn loop。
5. 补 detached fetch live execution。
6. 最后再做 sandbox、approval、rollback、runbook。

## 4. 文档入口

1. 架构基线：[openclaw-first-refactor-blueprint.md](openclaw-first-refactor-blueprint.md)
2. runtime queue 重建：[openclaw-runtime-source-queue-rebuild.md](openclaw-runtime-source-queue-rebuild.md)
3. skill 双流程适配：[openclaw-dual-flow-skill-assessment.md](openclaw-dual-flow-skill-assessment.md)
