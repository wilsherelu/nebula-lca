import { useEffect, useMemo, useState } from "react";

const RAW_API_BASE = ((import.meta.env.VITE_API_BASE_URL as string | undefined) ?? "/api").replace(/\/$/, "");
const API_BASE = RAW_API_BASE.endsWith("/api") ? RAW_API_BASE : `${RAW_API_BASE}/api`;
const TIANGONG_SUPABASE_URL = "https://qgzvkongdjqiiamzbbts.supabase.co";
const TIANGONG_PUBLISHABLE_KEY = "sb_publishable_EFWH4E61tpAtf82WQ37xTA_Fxa5OPyg";

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

type RemotePreviewResponse = {
  remote_kind: string;
  remote_id: string;
  remote_version?: string | null;
  title: string;
  description?: string | null;
  summary?: Record<string, unknown>;
  related?: Array<Record<string, unknown>>;
  warnings?: string[];
};

type RemoteSyncResponse = {
  job_id: string;
  status: string;
  tidas_import_job_id?: string | null;
  tidas_import_report?: {
    import_type?: string;
    inserted?: number;
    updated?: number;
    skipped?: number;
    failed?: number;
    warning_count?: number;
    unresolved_count?: number;
    created_projects?: Array<{ project_id?: string; name?: string; version?: number }>;
    errors?: string[];
    warnings?: string[];
  } | null;
};

type Props = {
  uiLanguage: UiLanguage;
  onStatus?: (text: string) => void;
};

const emptyForm = {
  alias: "TianGong LCA",
  baseUrl: TIANGONG_SUPABASE_URL,
  publishableKey: TIANGONG_PUBLISHABLE_KEY,
  environmentLabel: "",
  authType: "basic" as "basic" | "bearer" | "api_key",
  email: "",
  password: "",
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
  const [previewLoadingKey, setPreviewLoadingKey] = useState("");
  const [remotePreview, setRemotePreview] = useState<RemotePreviewResponse | null>(null);
  const [syncingKey, setSyncingKey] = useState("");
  const [lastSync, setLastSync] = useState<RemoteSyncResponse | null>(null);

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
      authType: account.auth_type === "bearer" ? "bearer" : account.auth_type === "api_key" ? "api_key" : "basic",
      email: "",
      password: "",
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
    if (!editingId && form.authType === "basic" && (!form.email.trim() || !form.password)) {
      setErrorText(zh ? "首次绑定需要填写天工邮箱和密码。" : "TianGong email and password are required for first binding.");
      return;
    }
    if (!editingId && form.authType === "bearer" && !form.secret.trim()) {
      setErrorText(zh ? "首次绑定 Bearer Token 需要填写 token。" : "Bearer token is required for first binding.");
      return;
    }
    if (!editingId && form.authType === "api_key" && !form.secret.trim()) {
      setErrorText(zh ? "首次绑定兼容 API Key 需要填写凭据。" : "Legacy API Key credential is required for first binding.");
      return;
    }
    setSaving(true);
    setErrorText("");
    try {
      const credential =
        form.authType === "basic"
          ? form.email.trim() && form.password
            ? { username: form.email.trim(), password: form.password }
            : undefined
          : form.secret.trim().length > 0
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
      });
      const payload = await requestJson<RemoteSearchResponse>(
        `${API_BASE}/data-platforms/accounts/${encodeURIComponent(selectedAccount.id)}/${remoteKind}/search?${params.toString()}`,
      );
      setRemoteResult(payload);
      setRemotePreview(null);
      setRemotePage(payload.page);
      setLastSync(null);
    } catch (error) {
      const message = error instanceof Error ? error.message : "search failed";
      setErrorText(zh ? `远程查询失败：${message}` : `Remote search failed: ${message}`);
    } finally {
      setRemoteLoading(false);
    }
  };

  const previewRemoteItem = async (item: RemoteItem) => {
    if (!selectedAccount) return;
    const key = `${remoteKind}:${item.remote_id}`;
    const params = new URLSearchParams();
    if (item.remote_version) params.set("remote_version", item.remote_version);
    setPreviewLoadingKey(key);
    setErrorText("");
    try {
      const suffix = params.toString() ? `?${params.toString()}` : "";
      const payload = await requestJson<RemotePreviewResponse>(
        `${API_BASE}/data-platforms/accounts/${encodeURIComponent(selectedAccount.id)}/${remoteKind}/${encodeURIComponent(item.remote_id)}/preview${suffix}`,
      );
      setRemotePreview(payload);
    } catch (error) {
      const message = error instanceof Error ? error.message : "preview failed";
      setErrorText(zh ? `预览失败：${message}` : `Preview failed: ${message}`);
    } finally {
      setPreviewLoadingKey("");
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
      const result = await requestJson<RemoteSyncResponse>(`${API_BASE}/data-platforms/accounts/${encodeURIComponent(selectedAccount.id)}/${remoteKind}/sync`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      setLastSync(result);
      const report = result.tidas_import_report;
      const failed = Number(report?.failed ?? 0);
      const warnings = Number(report?.warning_count ?? 0) + Number(report?.unresolved_count ?? 0);
      onStatus?.(
        zh
          ? `已按需导入：新增 ${report?.inserted ?? 0}，更新 ${report?.updated ?? 0}，失败 ${failed}，警告 ${warnings}。`
          : `Imported on demand: ${report?.inserted ?? 0} inserted, ${report?.updated ?? 0} updated, ${failed} failed, ${warnings} warnings.`,
      );
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
          <p>{zh ? "用天工账号登录换取 Supabase session，供后续远程数据同步使用。" : "Sign in with a TianGong account to obtain a Supabase session for remote data sync."}</p>
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
            <select value={form.authType} onChange={(event) => setForm((prev) => ({ ...prev, authType: event.target.value as "basic" | "bearer" | "api_key" }))}>
              <option value="basic">{zh ? "天工账号登录" : "TianGong Login"}</option>
              <option value="bearer">Bearer Token</option>
              <option value="api_key">{zh ? "兼容 API Key" : "Legacy API Key"}</option>
            </select>
          </label>
          {form.authType === "basic" ? (
            <>
              <label>
                <span>{zh ? "天工邮箱" : "TianGong Email"}</span>
                <input
                  type="email"
                  autoComplete="username"
                  placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""}
                  value={form.email}
                  onChange={(event) => setForm((prev) => ({ ...prev, email: event.target.value }))}
                />
              </label>
              <label>
                <span>{zh ? "天工密码" : "TianGong Password"}</span>
                <input
                  type="password"
                  autoComplete="current-password"
                  placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""}
                  value={form.password}
                  onChange={(event) => setForm((prev) => ({ ...prev, password: event.target.value }))}
                />
              </label>
            </>
          ) : (
            <label>
              <span>{form.authType === "bearer" ? "Bearer Token" : (zh ? "兼容 API Key" : "Legacy API Key")}</span>
              <input
                type="password"
                autoComplete="off"
                placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""}
                value={form.secret}
                onChange={(event) => setForm((prev) => ({ ...prev, secret: event.target.value }))}
              />
            </label>
          )}
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
              ? "推荐使用天工账号登录：密码只提交到星云后端换取 Supabase session，后端保存加密 session/refresh token，不向前端返回 token；搜索只写远程缓存，导入选中项才写星云目录或项目。"
              : "TianGong Login is recommended: the password is sent only to the Nebula backend to obtain a Supabase session. The backend stores encrypted session/refresh tokens and never returns tokens to the frontend. Search only writes remote cache; selected import writes Nebula catalog or projects."}
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
                setRemotePreview(null);
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
                      <button type="button" className="pm-link-btn" onClick={() => void previewRemoteItem(item)} disabled={previewLoadingKey === key}>
                        {previewLoadingKey === key ? (zh ? "预览中" : "Previewing") : (zh ? "预览" : "Preview")}
                      </button>
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
        {remotePreview && (
          <div className="pm-remote-sync-report">
            <div>
              <strong>{remotePreview.title}</strong>
              <span>{remotePreview.remote_kind}</span>
              <span>{remotePreview.remote_version ?? "-"}</span>
            </div>
            {remotePreview.description && <p>{remotePreview.description}</p>}
            <div>
              {Object.entries(remotePreview.summary ?? {}).slice(0, 8).map(([key, value]) => (
                value ? <span key={key}>{key}: {String(value)}</span> : null
              ))}
            </div>
            {(remotePreview.related ?? []).length > 0 && (
              <p>
                {(remotePreview.related ?? []).slice(0, 3).map((item) => String(item.name ?? item.flow_id ?? item.process_id ?? "")).filter(Boolean).join(" | ")}
              </p>
            )}
          </div>
        )}
        {lastSync?.tidas_import_report && (
          <div className="pm-remote-sync-report">
            <div>
              <strong>{zh ? "最近导入" : "Last Import"}</strong>
              <span>{lastSync.tidas_import_report.import_type ?? remoteKind}</span>
              <span>Job {lastSync.tidas_import_job_id ?? lastSync.job_id}</span>
            </div>
            <div>
              <span>{zh ? `新增 ${lastSync.tidas_import_report.inserted ?? 0}` : `${lastSync.tidas_import_report.inserted ?? 0} inserted`}</span>
              <span>{zh ? `更新 ${lastSync.tidas_import_report.updated ?? 0}` : `${lastSync.tidas_import_report.updated ?? 0} updated`}</span>
              <span>{zh ? `跳过 ${lastSync.tidas_import_report.skipped ?? 0}` : `${lastSync.tidas_import_report.skipped ?? 0} skipped`}</span>
              <span>{zh ? `失败 ${lastSync.tidas_import_report.failed ?? 0}` : `${lastSync.tidas_import_report.failed ?? 0} failed`}</span>
              <span>{zh ? `警告 ${Number(lastSync.tidas_import_report.warning_count ?? 0) + Number(lastSync.tidas_import_report.unresolved_count ?? 0)}` : `${Number(lastSync.tidas_import_report.warning_count ?? 0) + Number(lastSync.tidas_import_report.unresolved_count ?? 0)} warnings`}</span>
            </div>
            {(lastSync.tidas_import_report.errors?.[0] || lastSync.tidas_import_report.warnings?.[0]) && (
              <p>{lastSync.tidas_import_report.errors?.[0] ?? lastSync.tidas_import_report.warnings?.[0]}</p>
            )}
          </div>
        )}
      </div>

      <div className="pm-platform-note pm-send-placeholder">
        <strong>{zh ? "Send to TianGong" : "Send to TianGong"}</strong>
        <span>{zh ? "第二阶段接入：从星云项目直接创建/保存天工 draft；当前不走 TIDAS ZIP 主路径。" : "Phase 2: create/save TianGong drafts directly from Nebula projects. TIDAS ZIP remains fallback/debug only."}</span>
        <button type="button" className="pm-ghost-btn" disabled>{zh ? "接口占位" : "API placeholder"}</button>
      </div>
    </section>
  );
}
