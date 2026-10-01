# Nebula One 与 Nebula LCA 专业建模往返合同 v1

Nebula One 负责应用项目、建模会话、确认接入和冻结结果；Nebula LCA 负责专业模型编辑、版本保存和权威模型快照。浏览器回传只用于定位候选版本，不能作为模型内容或哈希的服务端证据。

## 打开编辑器

编辑既有模型时使用：

```text
/projects/{project_id}?version={version}&handoff_id={handoff_id}&state={opaque_state}&return_target=nebula_one
```

`version` 必须是正整数。指定版本不存在时编辑器显示错误，不回退到 `latest`。

新建专业模型时从根路由进入：

```text
/?handoff_id={handoff_id}&state={opaque_state}&return_target=nebula_one
```

此模式要求用户新建 Nebula LCA 项目。协作会话中关闭自动保存，只有手动保存成功且服务端返回 `created_new_version=true` 和 `graph_hash` 后，才允许返回候选版本。

保存后编辑器将当前 URL 的 `version` 更新为新版本，并用内部查询参数 `base_version` 保留本次协作最初打开的版本。保存候选仅写入当前浏览器标签页的 `sessionStorage`；刷新后必须由精确版本接口返回相同的 project、version 和 graph hash，才恢复“返回 Nebula One”按钮。

## 返回 Nebula One

编辑器不接受请求传入的任意 `return_url`。部署方通过以下环境变量配置唯一允许的 Nebula One origin：

```text
VITE_NEBULA_ONE_RETURN_ORIGIN=http://127.0.0.1:6131
```

编辑器固定导航到该 origin 的 `/modeling/return`。保存成功后的查询参数为：

```text
handoff_id
state
status=saved
project_id
base_version       # 新建模型省略
saved_version
upstream_graph_hash
```

取消只返回 `handoff_id`、`state` 和 `status=cancelled`，不返回模型身份。

## 服务端复核

One 必须校验会话归属、一次性 state、有效期和预期的基础模型身份，然后读取：

```text
GET /api/provider/v1/models/{project_id}/versions/{saved_version}/snapshot
```

仅当快照中的 `project_id`、`version` 和 `graph_hash` 与保存候选一致时，才可以进入用户确认。One 随后对 `snapshot.graph` 计算并保存自己的 hydrated/canonical graph SHA-256。Nebula LCA 的 `graph_hash` 基于规范化、瘦身后的模型图，不能与 One 的 hydrated graph SHA-256 合并为同一字段。

用户明确确认后，One 建立新的专业模型分析项目或模型来源；取消、过期、版本缺失、state 不符和哈希不符均不得创建项目或触发计算。既有分析项目、已冻结结果和锁定模板保持不变。

Provider 快照优先读取 `graph.metadata.functional_unit`。若旧模型没有该结构化对象，但编辑器已在 `graph.metadata.project_preferences.target_product` 明确保存 process UUID、Flow UUID 和正数量，则 provider 只在该身份唯一命中产品输出时，从端口读取单位及版本身份并生成结构化功能单位；不解析名称或显示字符串，无法唯一解析时继续返回缺失 issue。

## 已复用接口

- `GET /api/projects/{project_id}/versions/{version}`：编辑器读取明确版本。
- `POST /api/projects/{project_id}/versions`：追加保存模型版本。
- `GET /api/provider/v1/models/{project_id}/versions/{version}/snapshot`：One 服务端权威复核。

版本内容没有原地更新接口，但当前默认版本保留策略可能清理较旧版本。One 应在确认阶段立即摄取并冻结快照；若未来要求长期重新打开任意上游历史版本，需要另行增加外部版本保留合同。
