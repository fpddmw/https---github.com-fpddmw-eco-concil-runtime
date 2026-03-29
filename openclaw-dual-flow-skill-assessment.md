# OpenClaw 双流程 Skill 适配

## 1. 结论

保留两条编排流程，但只保留一层 shared skills：

1. OpenClaw 多 agent
2. runtime source-queue

## 2. 当前 runtime queue 判断

当前活跃 queue 还不是 production-ready。它只有最小 import baseline，没有完整的 source selection、fetch governance 和 detached fetch execution。

## 3. 适配分类

现在不再维护 46 行人工表格。统一结论改为 runtime registry 的机器可读 profile：

- bridge：ingress 和 queue bridge，例如 scaffold、prepare、import execution
- direct：normalize、analysis、audit、readiness、promotion、reporting、archive-write
- advisory：query、lookup、history context、board 写入、advisory planner

## 4. 当前已落地的第一步

runtime registry 现在会为所有活跃 skills 导出 source_queue_profile。后续 queue planner、OpenClaw adapter 和 operator surface 都应复用这份元数据，而不是再写一套人工判断。

## 5. 继续阅读

1. [openclaw-runtime-source-queue-rebuild.md](openclaw-runtime-source-queue-rebuild.md)
2. [openclaw-full-development-report.md](openclaw-full-development-report.md)
