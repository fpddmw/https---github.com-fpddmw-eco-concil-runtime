# OpenClaw-First 蓝图

## 1. 目标

只保留一个目标结构：一层共享 skill surface，两种编排方式。

1. OpenClaw 多 agent 协作
2. runtime source-queue 受控批处理

两条流程都调用同一套 skills。runtime 不再承载新的业务推理，只负责治理、不变式、审计、存储和执行封装。

## 2. 结构

```text
skills/
  eco-.../

eco-concil-runtime/
  src/eco_council_runtime/
    kernel/
    adapters/openclaw/
    board/
    storage/
    audit/
    promotion/

tests/
```

## 3. 两种编排的职责

### OpenClaw 多 agent

- 主路径
- 适合开放式调查、challenge、falsification、动态取证
- agent 自主决定何时调用 query、lookup、fetch、normalize、board、reporting skills

### runtime source-queue

- 第二编排面
- 适合 replay、benchmark、nightly run、固定场景模板
- 只做受控 source selection、fetch plan、execution、gate，不复制 skill 逻辑

## 4. 绝对边界

- 不再维护两套 skill 体系。
- 不把业务判断重新塞回 runtime controller。
- 当前活跃 queue 仍只是 import baseline，不是接近投产的 production queue。

## 5. 当前应读的文档

1. [openclaw-full-development-report.md](openclaw-full-development-report.md)
2. [openclaw-runtime-source-queue-rebuild.md](openclaw-runtime-source-queue-rebuild.md)
3. [openclaw-dual-flow-skill-assessment.md](openclaw-dual-flow-skill-assessment.md)
