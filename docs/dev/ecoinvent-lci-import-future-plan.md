# ecoinvent LCI 导入后续开发计划

Last updated: 2026-05-24

本文只记录 ecoinvent LCI 导入、增量导入、基础流 MasterData、LCIA runtime 关联防护中未完成、待验证或后续可能拆分的开发事项。已经完成的历史开发内容不要继续堆在这里，应进入归档索引或对应专题文档。

## 当前基线

- ecoinvent LCI 导入已支持压缩向量写入、断点续导、并发解析 + 有界队列 + 单 writer 批量提交。
- ecoinvent 增量导入已支持 MasterData 复用，以及已导入 dataset 的 metadata-only fast skip。
- writer 已从逐 dataset flush 演进到 batch writer，并加入 worker 侧 write plan、flow-key batch resolve、SQLite Core upsert、streaming aggregation 等优化。
- 最新 1000 dataset overwrite 实测曾达到约 `145s`，结果口径：`930` 非空向量、`70` 空向量、`2464768` nnz。后续性能判断必须看 `stats_json` 的互斥分段，不再只看总耗时。
- ecoinvent LCIA active runtime 应保持官方 broad runtime：9795 flows、633 indicators、481813 factors、46 method sets。
- LCIA 方法选择应展示“方法集”级别，例如 EF v3.1、ReCiPe midpoint、ReCiPe endpoint；默认可选 EF v3.1，但不应默认一次性计算全部 600+ 指标。
- TIDAS/ILCD 基本流体系当前只应使用 EF v3.1；ecoinvent 基本流体系可以选择 ecoinvent runtime 中的其他方法集。

## 已完成归档（不要再作为后续计划重复展开）

- 导入任务 stale recovery：后端查询前恢复过期 `running/pending/paused` job，前端只自动挂载 10 分钟内活跃任务。
- MasterData 复用：增量导入和断点续导不应重复解析/导入 Units、UnitConversions、ElementaryExchanges、IntermediateExchanges。
- global imported fast skip：no-overwrite 命中全局已导入 dataset 时只解析 metadata，不解析 exchanges，不重写 vector。
- parser/writer 流水线：并发 parser + 有界队列 + 单 writer 持续批量提交已经落地。
- batch writer：已避免逐 dataset `_flush_single()` 主路径，改为批量预取、批量 upsert、批量 checkpoint。
- worker-side write plan：parser worker 侧生成 `LciWritePlan`，提前完成 unit canonicalize 和 logical flow-key aggregation。
- SQLite Core upsert / streaming aggregation：SQLite 路径改用 `ON CONFLICT DO UPDATE`，streaming aggregation 接入 overwrite/new dataset 路径，并有回归测试证明与旧 single-pass 聚合一致。
- **互斥分段诊断计时**（19 个 writer/worker 字段 + queue occupancy p50/p95/max + SQLAlchemy driver-level cursor timing）。
- **A/B debug 模式**：`parser_blackhole`（drain queue 不写 DB）和 `writer_replay`（缓存 write_plans 后 replay writer）。
- LCIA runtime 防护：小 preview/runtime artifact 不应覆盖 official broad runtime。
- ecoinvent 基本流优先：导入 ecoinvent MasterData 时，商业版 LCI 的 ecoinvent 基本流应优先覆盖历史冲突 flow。

## P0 / P1 后续事项

### 1. 拆分基础流信息刷新与 LCI 覆盖导入

当前前端导入选项里，基础流 MasterData 和 LCI dataset/vector 的导入基本随同一次导入流程处理。短期可以保持现状，但后续如果发现基础流字段选取不够，需要补充导入字段时，应拆成独立能力：

- 刷新 MasterData / 基础流目录：只更新 units、elementary exchanges、intermediate exchanges 等基础目录字段。
- 覆盖 LCI 数据：重新解析 `.spold` exchanges、更新 process、exchange、compressed vector 和 global import 记录。
- 增量 LCI 导入：已导入 dataset 继续走 fast skip，不重复解析 exchanges。

验收重点：

- 单独刷新基础流字段时，不应重复导入 LCI dataset/vector。
- LCI 覆盖导入仍由明确的 overwrite/reimport 选项控制。
- UI 文案要能让用户区分“补基础目录字段”和“重算 LCI 向量”。

### 2. 导入性能继续强化：先修诊断口径，再决定架构改造

**已完成：互斥分段诊断计时 + A/B debug 模式**

当前判断：不要继续把 SQLite upsert 当作首要瓶颈。已有证据显示 `batch_db_upsert_wall_seconds` 很低，历史 `batch_prefetch_wall_seconds` 高值很可能混入了 queue wait、writer idle、ORM materialization 或统计边界问题。

已实现互斥分段计时（writer 12 个 + worker 7 个 + SQLAlchemy driver-level），新增 A/B 模式：

- `parser_blackhole`：通过 `executor.debug_parser_blackhole = True` 开启，parser 产出后 writer 不写 DB、不 pack，只 drain queue。
- `writer_replay`：通过 `executor.debug_writer_replay = True` 开启，缓存 `LciWritePlan`，之后调用 `executor.debug_replay_writer()` replay writer。

下一步优先看 stats 解释力——能否解释 `duration_seconds` 的主要组成。

如果 `queue_get_block_seconds` 很高，优先继续优化 parser；如果 writer replay 很高，优先优化 run-level flow-key resolve 和 vector pack。

### 3. run-level flow-key resolve

如果诊断确认 flow-key resolve 仍明显占用 writer 热路径，下一步改成 run-level set-based：

- worker/write plan 继续输出 canonical logical flow-key 字段和稳定 key hash。
- writer 不在每个 batch 内逐批 resolve/create `LciBiosphereFlowKey`。
- 一个 import run 收集 distinct logical keys 后，一次性 `INSERT ... ON CONFLICT DO NOTHING` 创建缺失字典项。
- 再一次性 join 回 `flow_key_id`，用于 vector pack 或 staging matrix build。
- hash 只能用于索引/加速，唯一约束仍落在原始 canonical 字段，避免 hash collision 破坏正确性。

### 4. staging + processed matrix cache 试点

如果目标继续压到约 `60s/1000 datasets` 或全库约 30 分钟，应参考 Brightway 的分层思路：SQLite 保存语义数据和可审计 staging，计算使用 processed matrix/cache。

最小试点路线：

- 阶段 A：parser worker 完成 dataset-local aggregation，writer 只 append compact staging rows，不 resolve flow id，不 pack blob。
- 阶段 B：解析结束后，用 set-based SQL 构建 flow dictionary，再按 process 分组 pack per-process vector，或生成全局 CSC/CSR matrix cache。
- staging row 需要紧凑，避免反复存长 UUID/JSON；优先考虑短 integer dictionary 或 UUID BLOB。
- 用 generation/run id 做切换：新 generation 验证通过后再切 active，失败不影响旧 runtime。

matrix/cache 方向：

- per-process compressed vector blob 继续保留，用于 UI 展示、单 process 查询、增量替换。
- 批量 LCIA/LCI 计算应逐步引入全局 processed matrix cache，避免每次从大量 blob 解压。
- 如果当前导入的是 cumulative LCI vector 型 `.spold`，per-process blob 可继续作为 canonical 结果；但导入结束后应生成全局 CSC matrix cache 用于批量计算。

### 5. 导入弹窗与任务恢复体验

已做 stale job 恢复和前端 10 分钟活跃任务挂载保护。后续还需要关注：

- completed/failed/canceled job 不应让导入弹窗回到 importing 0%。
- job directory 被清理后，前端应显示可理解的最终状态，而不是误导用户以为导入失败。
- 任务列表查询和单 job 轮询都应在返回前恢复 stale job，避免旧 running/pending/paused 卡住 UI。

### 6. LCIA runtime 防护

继续保证测试、preview、fixture 产物不能覆盖正式 active runtime：

- 小 preview/runtime artifact 不应替换已有 official broad runtime。
- runtime 切换和生成脚本要明确 source system：ecoinvent、tidas/ilcd 不得混用。
- 计算结果中如果出现 `missing flow_uuids from B matrix`，需要先检查目标产品 source system 与 LCIA runtime source system 是否一致。

### 7. LCI 只读展示与基本流查看

后续仍需要补齐更好的只读检查能力：

- process 的 technosphere / biosphere exchanges 可读展示。
- 压缩向量展开后的 elementary flow 明细查看。
- 显示 flow UUID、名称、单位、方向、compartment/subcompartment 等字段。
- 对缺字段的基础流补充导入要走“基础流刷新”，不要被迫重导 LCI。

## 暂不做

- 暂不引入多个 DB writer。
- 暂不把测试 runtime、preview runtime 或导入缓存纳入 git。
- 暂不把历史 handoff 长文继续堆进当前开发计划。
- 暂不把 staging/matrix cache 一次性铺完整；先用 1000 dataset pilot 证明结果一致和耗时收益。
