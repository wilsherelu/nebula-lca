# 文档整理提示词

Last updated: 2026-05-24

下面提示词用于后续文档整理 agent 执行项目文档梳理。目标是把当前散落在 `docs/`、`docs/dev/`、`docs/development-plans/`、`docs/ecoinvent/`、`agent-memory/` 的内容整理成清晰的当前架构、后续计划和历史归档。

## 可直接使用的提示词

```text
你是 nebula-lca 项目的文档整理执行者。请只做文档整理，不修改产品代码。

硬性限制：
- 只能使用 read 和 write 工具。
- 禁止使用 PowerShell、shell、脚本、测试命令、格式化命令。
- 不要从控制台乱码输出复制中文；以文件原始 UTF-8 内容为准。
- 不要删除历史内容；如果需要归档，优先创建归档索引和迁移说明。
- 不要把自己称为 Codex 子代理；只按文档整理执行者身份工作。

工作目录：
- D:\VibeCoding\nebula-lca

主要输入：
- docs/
- docs/dev/
- docs/development-plans/
- docs/ecoinvent/
- agent-memory/

整理目标：
1. 当前项目介绍和架构指引
   - 输出到 agent-memory/main-handoff.md 或新的 agent-memory 当前入口文档。
   - 内容只保留“现在应该先读什么、系统当前怎么跑、关键模块在哪里、当前有效基线是什么”。
   - 避免把历史开发流水账放进当前入口。

2. 后续开发计划
   - ecoinvent LCI 导入相关事项输出或合并到 docs/dev/ecoinvent-lci-import-future-plan.md。
   - 如果发现全项目级后续计划，先建立明确专题文件名，不要使用 future-development-plan.md 这类通用名称。
   - 只放未完成、待验证、后续可能拆分的事项。
   - 已完成事项不要继续留在后续计划里。

3. 历史开发归档
   - 建议新增 docs/dev/historical-archive-index.md。
   - 为旧 handoff、旧计划、旧设计记录建立索引，标注：
     - 文件路径
     - 时间或阶段
     - 主题
     - 当前是否仍有效
     - 如果已过期，指向哪个当前文档

4. 文档目录边界
   - docs/dev/：当前开发契约、算法说明、后续计划、归档索引。
   - docs/ecoinvent/：ecoinvent 专题资料、导入与 runtime 说明。
   - docs/development-plans/：仍有参考价值的阶段性计划；过期计划应在归档索引中标注。
   - agent-memory/：给后续 agent 的入口记忆、当前状态和接手顺序，不放长篇历史叙述。

重点判断规则：
- 当前仍指导开发的内容：放入当前架构/开发契约。
- 尚未完成的内容：放入 future-development-plan.md。
- 已完成或过期的内容：放入 historical-archive-index.md。
- 有冲突时，以 2026-05-24 之后的 handoff 和 git 当前代码为准。
- ecoinvent LCI 导入事项不要放进通用 future-development-plan.md，应放入 docs/dev/ecoinvent-lci-import-future-plan.md。

交付要求：
- 先阅读相关文档，列出将要整理的文件清单。
- 再写入新的索引或合并后的文档。
- 最后给出简短摘要：新增/修改了哪些文档，哪些旧文档被标为历史归档，哪些事项仍在后续计划中。
```

## 整理后建议形态

- `agent-memory/main-handoff.md`：当前入口索引，短而准。
- `docs/dev/ecoinvent-lci-import-future-plan.md`：ecoinvent LCI 导入相关未完成事项和后续开发计划。
- `docs/dev/historical-archive-index.md`：历史文档索引，不承载当前决策。
- `docs/dev/*.md`：稳定的开发契约、算法说明、测试指引。
- `docs/ecoinvent/*.md`：ecoinvent 导入、LCI、LCIA、runtime 专题。
