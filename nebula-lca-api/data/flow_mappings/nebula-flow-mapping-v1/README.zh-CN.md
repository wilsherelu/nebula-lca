# Nebula Flow Mapping

Nebula Flow Mapping 是一个纯数据的 Flow 标识符互操作数据集，用于关联 TianGong Flow UUID 与 ecoinvent Flow UUID。1.0.0 版本包含已认证的中间流和基本流映射，不包含过程数据、交换清单、Flow 名称或描述、排放因子、LCIA 特征化因子、供应商信息或数据库导出内容。

1.0.0 共包含 11,143 条已认证关系：中间流 1,379 条（L1 147 条、L2 1,232 条），基本流 9,764 条（L1 6,354 条、L2 3,410 条）。

## 数据文件

- `data/intermediate-flow-mappings.v1.jsonl`：已认证中间流映射。
- `data/elementary-flow-mappings.v1.jsonl`：已认证基本流映射。
- `data/unit-conversions.v1.json`：独立维护的通用物理单位换算，不属于排放因子或 LCIA 因子。

需要通用单位换算的映射通过 `conversion_rule_id` 引用独立规则；映射行本身不嵌入数值换算因子或影响因子。

L1 表示公开 L1 子图内的严格身份映射：每个 TianGong UUID 和 ecoinvent UUID 最多出现一次。L2 表示经审核的兼容映射：每个 TianGong UUID 只选择一个 ecoinvent UUID，但同一个 ecoinvent UUID 可以被多个 TianGong Flow 复用，消费端应建议用户确认。

本数据集只提供标识符互操作关系。用户仍需分别合法取得底层数据库的访问权。
