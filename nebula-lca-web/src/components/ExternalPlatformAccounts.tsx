import { useEffect, useMemo, useState } from "react";

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;

type UiLanguage = "zh" | "en";
type PlatformAccount = {
  id: string;
  platform: string;
  alias: string;
  base_url?: string | null;
  auth_type: string;
  status: string;
  has_credential: boolean;
  last_validated_at?: string | null;
  last_validation_status?: string | null;
  last_validation_message?: string | null;
  metadata?: Record<string, unknown>;
};

type RemoteKind = "flows" | "processes" | "models";
type RemoteItem = {
  remote_id: string;
  remote_version?: string | null;
  source?: string | null;
  flow_name?: string;
  flow_uuid?: string;
  process_name?: string;
  process_uuid?: string;
  model_name?: string;
  model_uuid?: string;
};

type RemoteSearchResponse = {
  items: RemoteItem[];
  total: number;
  page: number;
  page_size: number;
  has_more: boolean;
};

type Props = {
  uiLanguage: UiLanguage;
  onStatus?: (text: string) => void;
};

const emptyForm = {
  alias: "TianGong LCA",
  baseUrl: "",
  publishableKey: "",
  environmentLabel: "",
  authType: "api_key" as "api_key" | "bearer",
  secret: "",
  status: "active" as "active" | "disabled",
};

const remoteKinds: RemoteKind[] = ["flows", "processes", "models"];

const formatTime = (value?: string | null): string => {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { hour12: false });
};

const requestJson = async <T,>(url: string, init?: RequestInit): Promise<T> => {
  const resp = await fetch(url, init);
  if (!resp.ok) {
    const payload = (await resp.json().catch(() => ({}))) as { detail?: { message?: string } | string; message?: string };
    const detail = typeof payload.detail === "string" ? payload.detail : payload.detail?.message;
    throw new Error(detail ?? payload.message ?? `HTTP ${resp.status}`);
  }
  return (await resp.json()) as T;
};

export function ExternalPlatformAccounts(props: Props) {
  const { uiLanguage, onStatus } = props;
  const zh = uiLanguage === "zh";
  const [accounts, setAccounts] = useState<PlatformAccount[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testingId, setTestingId] = useState("");
  const [errorText, setErrorText] = useState("");
  const [editingId, setEditingId] = useState("");
  const [form, setForm] = useState(emptyForm);
  const [selectedAccountId, setSelectedAccountId] = useState("");
  const [remoteKind, setRemoteKind] = useState<RemoteKind>("flows");
  const [remoteQuery, setRemoteQuery] = useState("");
  const [remotePage, setRemotePage] = useState(1);
  const [remoteResult, setRemoteResult] = useState<RemoteSearchResponse | null>(null);
  const [remoteLoading, setRemoteLoading] = useState(false);
  const [syncingKey, setSyncingKey] = useState("");

  const tiangongAccounts = useMemo(
    () => accounts.filter((account) => account.platform === "tiangong"),
    [accounts],
  );
  const selectedAccount = useMemo(
    () => tiangongAccounts.find((account) => account.id === selectedAccountId) ?? tiangongAccounts[0],
    [selectedAccountId, tiangongAccounts],
  );

  useEffect(() => {
    if (!selectedAccountId && tiangongAccounts[0]) {
      setSelectedAccountId(tiangongAccounts[0].id);
    }
  }, [selectedAccountId, tiangongAccounts]);

  const loadAccounts = async () => {
    setLoading(true);
    setErrorText("");
    try {
      const payload = await requestJson<PlatformAccount[]>(`${API_BASE}/data-platforms/accounts`);
      setAccounts(payload);
    } catch (error) {
      const message = error instanceof Error ? error.message : "load failed";
      setErrorText(zh ? `账号加载失败：${message}` : `Failed to load accounts: ${message}`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadAccounts();
  }, []);

  const resetForm = () => {
    setEditingId("");
    setForm(emptyForm);
    setErrorText("");
  };

  const editAccount = (account: PlatformAccount) => {
    setEditingId(account.id);
    setForm({
      alias: account.alias,
      baseUrl: account.base_url ?? "",
      publishableKey: typeof account.metadata?.publishable_key === "string" ? account.metadata.publishable_key : "",
      environmentLabel: typeof account.metadata?.environment_label === "string" ? account.metadata.environment_label : "",
      authType: account.auth_type === "bearer" ? "bearer" : "api_key",
      secret: "",
      status: account.status === "disabled" ? "disabled" : "active",
    });
    setErrorText("");
  };

  const saveAccount = async () => {
    if (!form.alias.trim()) {
      setErrorText(zh ? "账号名称不能为空。" : "Account alias is required.");
      return;
    }
    if (!editingId && !form.secret.trim()) {
      setErrorText(zh ? "首次绑定需要填写 API Key 或 Bearer Token。" : "API Key or Bearer token is required for first binding.");
      return;
    }
    setSaving(true);
    setErrorText("");
    try {
      const credential =
        form.secret.trim().length > 0
          ? form.authType === "bearer"
            ? { token: form.secret.trim() }
            : { api_key: form.secret.trim() }
          : undefined;
      const body = {
        platform: "tiangong",
        alias: form.alias.trim(),
        base_url: form.baseUrl.trim() || null,
        auth_type: form.authType,
        credential,
        status: form.status,
        metadata: {
          publishable_key: form.publishableKey.trim(),
          environment_label: form.environmentLabel.trim(),
        },
      };
      if (editingId) {
        await requestJson<PlatformAccount>(`${API_BASE}/data-platforms/accounts/${encodeURIComponent(editingId)}`, {
          method: "PATCH",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
      } else {
        await requestJson<PlatformAccount>(`${API_BASE}/data-platforms/accounts`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(body),
        });
      }
      onStatus?.(zh ? "天工账号已绑定到星云。" : "TianGong account bound to Nebula.");
      resetForm();
      await loadAccounts();
    } catch (error) {
      const message = error instanceof Error ? error.message : "save failed";
      setErrorText(zh ? `保存失败：${message}` : `Save failed: ${message}`);
    } finally {
      setSaving(false);
    }
  };

  const testAccount = async (accountId: string) => {
    setTestingId(accountId);
    setErrorText("");
    try {
      const result = await requestJson<{ ok: boolean; message: string }>(
        `${API_BASE}/data-platforms/accounts/${encodeURIComponent(accountId)}/test`,
        { method: "POST" },
      );
      onStatus?.(zh ? `天工账号校验：${result.message}` : `TianGong account check: ${result.message}`);
      await loadAccounts();
    } catch (error) {
      const message = error instanceof Error ? error.message : "test failed";
      setErrorText(zh ? `校验失败：${message}` : `Check failed: ${message}`);
    } finally {
      setTestingId("");
    }
  };

  const deleteAccount = async (accountId: string) => {
    setSaving(true);
    setErrorText("");
    try {
      await requestJson(`${API_BASE}/data-platforms/accounts/${encodeURIComponent(accountId)}`, { method: "DELETE" });
      if (editingId === accountId) resetForm();
      onStatus?.(zh ? "天工账号已删除。" : "TianGong account deleted.");
      await loadAccounts();
    } catch (error) {
      const message = error instanceof Error ? error.message : "delete failed";
      setErrorText(zh ? `删除失败：${message}` : `Delete failed: ${message}`);
    } finally {
      setSaving(false);
    }
  };

  const searchRemote = async (nextPage = remotePage) => {
    if (!selectedAccount) {
      setErrorText(zh ? "请先绑定天工账号。" : "Bind a TianGong account first.");
      return;
    }
    setRemoteLoading(true);
    setErrorText("");
    try {
      const params = new URLSearchParams({
        q: remoteQuery,
        page: String(nextPage),
        page_size: "10",
        data_source: "tg",
        state_code: "100",
      });
      const payload = await requestJson<RemoteSearchResponse>(
        `${API_BASE}/data-platforms/accounts/${encodeURIComponent(selectedAccount.id)}/${remoteKind}/search?${params.toString()}`,
      );
      setRemoteResult(payload);
      setRemotePage(payload.page);
    } catch (error) {
      const message = error instanceof Error ? error.message : "search failed";
      setErrorText(zh ? `远程查询失败：${message}` : `Remote search failed: ${message}`);
    } finally {
      setRemoteLoading(false);
    }
  };

  const syncRemoteItem = async (item: RemoteItem) => {
    if (!selectedAccount) return;
    const remoteId = item.remote_id;
    const key = `${remoteKind}:${remoteId}`;
    setSyncingKey(key);
    setErrorText("");
    try {
      const body =
        remoteKind === "flows"
          ? { remote_flow_id: remoteId, remote_version: item.remote_version ?? null }
          : remoteKind === "processes"
            ? { remote_process_id: remoteId, remote_version: item.remote_version ?? null }
            : { remote_model_id: remoteId, remote_version: item.remote_version ?? null, project_name: item.model_name ?? undefined };
      await requestJson(`${API_BASE}/data-platforms/accounts/${encodeURIComponent(selectedAccount.id)}/${remoteKind}/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      onStatus?.(zh ? "已按需导入选中远程数据。" : "Selected remote data imported on demand.");
    } catch (error) {
      const message = error instanceof Error ? error.message : "sync failed";
      setErrorText(zh ? `导入失败：${message}` : `Import failed: ${message}`);
    } finally {
      setSyncingKey("");
    }
  };

  const itemTitle = (item: RemoteItem): string => item.flow_name ?? item.process_name ?? item.model_name ?? item.remote_id;
  const itemUuid = (item: RemoteItem): string => item.flow_uuid ?? item.process_uuid ?? item.model_uuid ?? item.remote_id;

  return (
    <section className="pm-page">
      <div className="pm-page-head">
        <div>
          <h2>{zh ? "外部平台账号" : "External Platform Accounts"}</h2>
          <p>{zh ? "绑定天工 LCA 的 API Key 或 Bearer Token，供后续远程数据同步使用。" : "Bind a TianGong LCA API Key or Bearer token for future remote data sync."}</p>
        </div>
        <div className="pm-head-actions">
          <button type="button" className="pm-ghost-btn" onClick={() => void loadAccounts()} disabled={loading}>
            {zh ? "刷新" : "Refresh"}
          </button>
        </div>
      </div>

      <div className="pm-platform-grid">
        <div className="pm-platform-form">
          <h3>{editingId ? (zh ? "更新天工账号" : "Update TianGong Account") : (zh ? "绑定天工账号" : "Bind TianGong Account")}</h3>
          <label>
            <span>{zh ? "账号名称" : "Alias"}</span>
            <input value={form.alias} onChange={(event) => setForm((prev) => ({ ...prev, alias: event.target.value }))} />
          </label>
          <label>
            <span>{zh ? "Supabase URL" : "Supabase URL"}</span>
            <input
              placeholder="https://..."
              value={form.baseUrl}
              onChange={(event) => setForm((prev) => ({ ...prev, baseUrl: event.target.value }))}
            />
          </label>
          <label>
            <span>{zh ? "Publishable Key" : "Publishable Key"}</span>
            <input
              type="password"
              autoComplete="off"
              value={form.publishableKey}
              onChange={(event) => setForm((prev) => ({ ...prev, publishableKey: event.target.value }))}
            />
          </label>
          <label>
            <span>{zh ? "环境名" : "Environment"}</span>
            <input
              placeholder={zh ? "测试 / 生产 / 客户环境" : "test / prod / customer"}
              value={form.environmentLabel}
              onChange={(event) => setForm((prev) => ({ ...prev, environmentLabel: event.target.value }))}
            />
          </label>
          <label>
            <span>{zh ? "认证方式" : "Auth Type"}</span>
            <select value={form.authType} onChange={(event) => setForm((prev) => ({ ...prev, authType: event.target.value as "api_key" | "bearer" }))}>
              <option value="api_key">API Key</option>
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          <label>
            <span>{form.authType === "bearer" ? "Bearer Token" : "API Key"}</span>
            <input
              type="password"
              autoComplete="off"
              placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""}
              value={form.secret}
              onChange={(event) => setForm((prev) => ({ ...prev, secret: event.target.value }))}
            />
          </label>
          <label>
            <span>{zh ? "状态" : "Status"}</span>
            <select value={form.status} onChange={(event) => setForm((prev) => ({ ...prev, status: event.target.value as "active" | "disabled" }))}>
              <option value="active">{zh ? "启用" : "Active"}</option>
              <option value="disabled">{zh ? "停用" : "Disabled"}</option>
            </select>
          </label>
          {errorText && <div className="pm-field-error">{errorText}</div>}
          <div className="pm-platform-actions">
            {editingId && (
              <button type="button" className="pm-ghost-btn" onClick={resetForm} disabled={saving}>
                {zh ? "取消编辑" : "Cancel"}
              </button>
            )}
            <button type="button" className="pm-primary-btn" onClick={() => void saveAccount()} disabled={saving}>
              {saving ? (zh ? "保存中..." : "Saving...") : (zh ? "保存绑定" : "Save Binding")}
            </button>
          </div>
        </div>

        <div className="pm-platform-list">
          <div className="pm-table-wrap pm-platform-table-wrap">
            <table className="pm-table">
              <thead>
                <tr>
                  <th>{zh ? "账号" : "Account"}</th>
                  <th>{zh ? "认证" : "Auth"}</th>
                  <th>{zh ? "状态" : "Status"}</th>
                  <th>{zh ? "最近校验" : "Last Check"}</th>
                  <th>{zh ? "操作" : "Actions"}</th>
                </tr>
              </thead>
              <tbody>
                {tiangongAccounts.map((account) => (
                  <tr key={account.id}>
                    <td>
                      <strong>{account.alias}</strong>
                      <div className="pm-platform-muted">{account.base_url || (zh ? "未配置 API 地址" : "No API base URL")}</div>
                      <div className="pm-platform-muted">
                        {typeof account.metadata?.environment_label === "string" && account.metadata.environment_label
                          ? account.metadata.environment_label
                          : (zh ? "未配置环境名" : "No environment label")}
                      </div>
                    </td>
                    <td>{account.has_credential ? account.auth_type : "-"}</td>
                    <td>
                      <span className={`pm-status-badge ${account.status === "active" ? "pm-status-badge--balanced" : "pm-status-badge--unchecked"}`}>
                        {account.status === "active" ? (zh ? "启用" : "Active") : (zh ? "停用" : "Disabled")}
                      </span>
                    </td>
                    <td>
                      <div>{formatTime(account.last_validated_at)}</div>
                      <div className="pm-platform-muted">{account.last_validation_message || "-"}</div>
                    </td>
                    <td>
                      <div className="pm-row-actions">
                        <button type="button" className="pm-link-btn primary" onClick={() => void testAccount(account.id)} disabled={testingId === account.id}>
                          {testingId === account.id ? (zh ? "校验中" : "Checking") : (zh ? "校验" : "Check")}
                        </button>
                        <button type="button" className="pm-link-btn" onClick={() => editAccount(account)}>
                          {zh ? "编辑" : "Edit"}
                        </button>
                        <button type="button" className="pm-link-btn danger" onClick={() => void deleteAccount(account.id)} disabled={saving}>
                          {zh ? "删除" : "Delete"}
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
                {tiangongAccounts.length === 0 && (
                  <tr>
                    <td colSpan={5}>{loading ? (zh ? "加载中..." : "Loading...") : (zh ? "尚未绑定天工账号。" : "No TianGong account bound.")}</td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          <div className="pm-platform-note">
            {zh
              ? "API Key 按 Base64 JSON 邮箱/密码格式提交到后端，后端仅在内存中换取 Supabase session；搜索只写远程缓存，导入选中项才写星云目录或项目。"
              : "The backend uses the Base64 JSON API Key only in memory to get a Supabase session. Search only writes remote cache; selected import writes Nebula catalog or projects."}
          </div>
        </div>
      </div>

      <div className="pm-remote-browser">
        <div className="pm-remote-browser-head">
          <div>
            <h3>{zh ? "远程数据浏览" : "Remote Browser"}</h3>
            <p>{zh ? "按需检索天工 Flow / Process / Model，选中后导入星云。" : "Search TianGong Flow / Process / Model on demand and import selected records."}</p>
          </div>
          <select value={selectedAccount?.id ?? ""} onChange={(event) => setSelectedAccountId(event.target.value)} disabled={tiangongAccounts.length === 0}>
            {tiangongAccounts.map((account) => (
              <option key={account.id} value={account.id}>{account.alias}</option>
            ))}
          </select>
        </div>
        <div className="pm-remote-tabs">
          {remoteKinds.map((kind) => (
            <button
              key={kind}
              type="button"
              className={remoteKind === kind ? "active" : ""}
              onClick={() => {
                setRemoteKind(kind);
                setRemotePage(1);
                setRemoteResult(null);
              }}
            >
              {kind === "flows" ? "Flow" : kind === "processes" ? "Process" : "Model"}
            </button>
          ))}
        </div>
        <div className="pm-remote-search">
          <input
            value={remoteQuery}
            onChange={(event) => setRemoteQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") void searchRemote(1);
            }}
            placeholder={zh ? "输入关键词搜索远程数据" : "Search remote data"}
          />
          <button type="button" className="pm-primary-btn" onClick={() => void searchRemote(1)} disabled={remoteLoading || !selectedAccount}>
            {remoteLoading ? (zh ? "查询中..." : "Searching...") : (zh ? "查询" : "Search")}
          </button>
        </div>
        <div className="pm-table-wrap pm-remote-table-wrap">
          <table className="pm-table">
            <thead>
              <tr>
                <th>{zh ? "名称" : "Name"}</th>
                <th>ID</th>
                <th>{zh ? "版本" : "Version"}</th>
                <th>{zh ? "来源" : "Source"}</th>
                <th>{zh ? "操作" : "Actions"}</th>
              </tr>
            </thead>
            <tbody>
              {(remoteResult?.items ?? []).map((item) => {
                const key = `${remoteKind}:${item.remote_id}`;
                return (
                  <tr key={`${item.remote_id}:${item.remote_version ?? ""}`}>
                    <td><strong>{itemTitle(item)}</strong></td>
                    <td><span className="pm-platform-muted">{itemUuid(item)}</span></td>
                    <td>{item.remote_version ?? "-"}</td>
                    <td>{item.source ?? "-"}</td>
                    <td>
                      <button type="button" className="pm-link-btn primary" onClick={() => void syncRemoteItem(item)} disabled={syncingKey === key}>
                        {syncingKey === key ? (zh ? "导入中" : "Importing") : (zh ? "导入" : "Import")}
                      </button>
                    </td>
                  </tr>
                );
              })}
              {(!remoteResult || remoteResult.items.length === 0) && (
                <tr>
                  <td colSpan={5}>{remoteLoading ? (zh ? "加载中..." : "Loading...") : (zh ? "暂无远程结果。" : "No remote results.")}</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
        <div className="pm-remote-footer">
          <span>{zh ? `共 ${remoteResult?.total ?? 0} 条` : `${remoteResult?.total ?? 0} total`}</span>
          <div className="pm-row-actions">
            <button type="button" className="pm-ghost-btn" onClick={() => void searchRemote(Math.max(1, remotePage - 1))} disabled={remoteLoading || remotePage <= 1}>
              {zh ? "上一页" : "Previous"}
            </button>
            <span>{remotePage}</span>
            <button type="button" className="pm-ghost-btn" onClick={() => void searchRemote(remotePage + 1)} disabled={remoteLoading || !remoteResult?.has_more}>
              {zh ? "下一页" : "Next"}
            </button>
          </div>
        </div>
      </div>

      <div className="pm-platform-note pm-send-placeholder">
        <strong>{zh ? "Send to TianGong" : "Send to TianGong"}</strong>
        <span>{zh ? "第二阶段接入：从星云项目直接创建/保存天工 draft；当前不走 TIDAS ZIP 主路径。" : "Phase 2: create/save TianGong drafts directly from Nebula projects. TIDAS ZIP remains fallback/debug only."}</span>
        <button type="button" className="pm-ghost-btn" disabled>{zh ? "接口占位" : "API placeholder"}</button>
      </div>
    </section>
  );
}
