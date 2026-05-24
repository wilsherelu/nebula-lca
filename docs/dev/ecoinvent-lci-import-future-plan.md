# ecoinvent LCI 导入后续开发计划

Last updated: 2026-05-24

本文只记录 ecoinvent LCI 导入、增量导入、基础流 MasterData、LCIA runtime 关联防护中未完成、待验证或后续可能拆分的开发事项。已经完成的历史开发内容不要继续堆在这里，应进入归档索引或对应专题文档。

## 当前基线

- ecoinvent LCI 导入已支持压缩向量写入、断点续导、并发解析 + 有界队列 + 单 writer 批量提交。
- ecoinvent 增量导入已支持 MasterData 复用，以及已导入 dataset 的 metadata-only fast skip。
- ecoinvent LCIA active runtime 应保持官方 broad runtime：9795 flows、633 indicators、481813 factors、46 method sets。
- LCIA 方法选择应展示“方法集”级别，例如 EF v3.1、ReCiPe midpoint、ReCiPe endpoint；默认可选 EF v3.1，但不应默认一次性计算全部 600+ 指标。
- TIDAS/ILCD 基本流体系当前只应使用 EF v3.1；ecoinvent 基本流体系可以选择 ecoinvent runtime 中的其他方法集。

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

### 2. 导入性能继续强化

已经完成第一阶段流水线调度和 global skip 前移。后续如果 100/1000 数据集导入仍慢，再按证据继续优化：

- 对比 workers=1/2/4/8 的真实样本表现，保留默认 workers=2，除非新证据说明更高并发稳定更快。
- 单独计时 XML metadata parse、exchange parse、`_flush_single()`、commit、vector pack。
- 只有当写入侧被证明是瓶颈时，再考虑 bulk upsert 或 vector pack 重写。
- SQLite 本地桌面场景不优先引入多 writer，避免锁竞争。

建议继续保留并展示这些诊断字段：

- `parse_wall_seconds`
- `write_wall_seconds`
- `commit_count`
- `write_batch_size`
- `queue_max_observed`
- `avg_parse_ms`
- `avg_flush_ms`
- `global_skip_fast_count`
- `metadata_parse_count`
- `exchange_parse_count`
- `avg_metadata_parse_ms`
- `avg_exchange_parse_ms`
- `masterdata_reused`

### 3. 导入弹窗与任务恢复体验

已做 stale job 恢复和前端 10 分钟活跃任务挂载保护。后续还需要关注：

- completed/failed/canceled job 不应让导入弹窗回到 importing 0%。
- job directory 被清理后，前端应显示可理解的最终状态，而不是误导用户以为导入失败。
- 任务列表查询和单 job 轮询都应在返回前恢复 stale job，避免旧 running/pending/paused 卡住 UI。

### 4. LCIA runtime 防护

继续保证测试、preview、fixture 产物不能覆盖正式 active runtime：

- 小 preview/runtime artifact 不应替换已有 official broad runtime。
- runtime 切换和生成脚本要明确 source system：ecoinvent、tidas/ilcd 不得混用。
- 计算结果中如果出现 `missing flow_uuids from B matrix`，需要先检查目标产品 source system 与 LCIA runtime source system 是否一致。

### 5. LCI 只读展示与基本流查看

后续仍需要补齐更好的只读检查能力：

- process 的 technosphere / biosphere exchanges 可读展示。
- 压缩向量展开后的 elementary flow 明细查看。
- 显示 flow UUID、名称、单位、方向、compartment/subcompartment 等字段。
- 对缺字段的基础流补充导入要走“基础流刷新”，不要被迫重导 LCI。

## 暂不做

- 暂不引入多个 DB writer。
- 暂不把测试 runtime、preview runtime 或导入缓存纳入 git。
- 暂不重写 `_flush_single()` 的业务副作用，除非性能证据证明它是主瓶颈。
- 暂不把历史 handoff 长文继续堆进当前开发计划。
