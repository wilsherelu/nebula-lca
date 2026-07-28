import { getApiBase } from "../apiBase";
import { useEffect, useMemo, useState } from "react";

const API_BASE = getApiBase();
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
  metadata?: Record<string, unknown>;
  flow_name?: string;
  flow_name_en?: string | null;
  flow_uuid?: string;
  flow_type?: string;
  default_unit?: string;
  unit_group?: string;
  process_name?: string;
  process_name_en?: string | null;
  process_uuid?: string;
  process_type?: string;
  model_name?: string;
  model_name_en?: string | null;
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
  flow_count?: number;
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

type RemoteSyncError = Error & {
  failedFlowUuid?: string | null;
  rolledBack?: boolean;
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
  authType: "basic" as "basic" | "api_key",
  email: "",
  password: "",
  secret: "",
  status: "active" as "active" | "disabled",
};

const remoteKinds: RemoteKind[] = ["flows", "processes", "models"];
const flowTypeOptions = ["all", "Product flow", "Elementary flow", "Waste flow", "Other flow"] as const;
const processTypeOptions = ["all", "Unit process, single operation", "Unit process, black box", "LCI result", "Partly terminated system", "Avoided product system"] as const;

const formatTime = (value?: string | null): string => {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("zh-CN", { hour12: false });
};

const needsCredentialRebind = (account: PlatformAccount): boolean => {
  const message = (account.last_validation_message ?? "").toLowerCase();
  return message.includes("ciphertext") || message.includes("integrity check");
};

const accountValidationLabel = (account: PlatformAccount, zh: boolean): string => {
  if (account.last_validation_status === "ok") return zh ? "已连接" : "Connected";
  if (needsCredentialRebind(account)) return zh ? "需重新绑定" : "Rebind required";
  if (account.last_validation_status === "failed") return zh ? "校验失败" : "Check failed";
  return zh ? "待校验" : "Check required";
};

const requestJson = async <T,>(url: string, init?: RequestInit): Promise<T> => {
  const resp = await fetch(url, init);
  if (!resp.ok) {
    const payload = (await resp.json().catch(() => ({}))) as {
      detail?: { message?: string; failed_flow_uuid?: string | null; rolled_back?: boolean } | string;
      message?: string;
    };
    const detail = typeof payload.detail === "string" ? payload.detail : payload.detail?.message;
    const error = new Error(detail ?? payload.message ?? `HTTP ${resp.status}`) as RemoteSyncError;
    if (typeof payload.detail === "object" && payload.detail !== null) {
      error.failedFlowUuid = payload.detail.failed_flow_uuid;
      error.rolledBack = payload.detail.rolled_back;
    }
    throw error;
  }
  return (await resp.json()) as T;
};

export function ExternalPlatformAccounts(props: Props) {
  const { uiLanguage, onStatus } = props;
  const zh = uiLanguage === "zh";
  const [accounts, setAccounts] = useState<PlatformAccount[]>([]);
  const [saving, setSaving] = useState(false);
  const [testingId, setTestingId] = useState("");
  const [errorText, setErrorText] = useState("");
  const [editingId, setEditingId] = useState("");
  const [accountDialogOpen, setAccountDialogOpen] = useState(false);
  const [validationDialog, setValidationDialog] = useState<{ ok: boolean; message: string; checkedAt?: string } | null>(null);
  const [form, setForm] = useState(emptyForm);
  const [selectedAccountId, setSelectedAccountId] = useState("");
  const [remoteKind, setRemoteKind] = useState<RemoteKind>("flows");
  const [remoteQuery, setRemoteQuery] = useState("");
  const [remoteStateMode, setRemoteStateMode] = useState<"open" | "all">("open");
  const [remoteFlowType, setRemoteFlowType] = useState("Product flow");
  const [remoteProcessType, setRemoteProcessType] = useState("all");
  const [remotePage, setRemotePage] = useState(1);
  const [remoteResult, setRemoteResult] = useState<RemoteSearchResponse | null>(null);
  const [remoteLoading, setRemoteLoading] = useState(false);
  const [previewLoadingKey, setPreviewLoadingKey] = useState("");
  const [remotePreview, setRemotePreview] = useState<RemotePreviewResponse | null>(null);
  const [syncingKey, setSyncingKey] = useState("");
  const [lastSync, setLastSync] = useState<RemoteSyncResponse | null>(null);
  const [modelSyncConfirm, setModelSyncConfirm] = useState<{ open: boolean; item: RemoteItem | null }>({ open: false, item: null });
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
    setErrorText("");
    try {
      const payload = await requestJson<PlatformAccount[]>(`${API_BASE}/data-platforms/accounts`);
      setAccounts(payload);
    } catch (error) {
      const message = error instanceof Error ? error.message : "load failed";
      setErrorText(zh ? `账号加载失败：${message}` : `Failed to load accounts: ${message}`);
    }
  };

  useEffect(() => {
    void loadAccounts();
  }, []);

  const resetForm = () => {
    setEditingId("");
    setForm(emptyForm);
    setErrorText("");
    setAccountDialogOpen(false);
  };

  const openNewAccountDialog = () => {
    setEditingId("");
    setForm(emptyForm);
    setErrorText("");
    setAccountDialogOpen(true);
  };

  const editAccount = (account: PlatformAccount) => {
    setEditingId(account.id);
    setForm({
      alias: account.alias,
      baseUrl: account.base_url ?? "",
      publishableKey: typeof account.metadata?.publishable_key === "string" ? account.metadata.publishable_key : "",
      environmentLabel: typeof account.metadata?.environment_label === "string" ? account.metadata.environment_label : "",
      authType: account.auth_type === "api_key" ? "api_key" : "basic",
      email: "",
      password: "",
      secret: "",
      status: account.status === "disabled" ? "disabled" : "active",
    });
    setErrorText("");
    setAccountDialogOpen(true);
  };

  const saveAccount = async () => {
    if (!form.alias.trim()) {
      setErrorText(zh ? "账号名称不能为空。" : "Account alias is required.");
      return;
    }
    if (!editingId && form.authType === "basic" && (!form.email.trim() || !form.password)) {
      setErrorText(zh ? "首次绑定需要填写天工平台用户名（邮箱）和密码。" : "TianGong platform username (email) and password are required for first binding.");
      return;
    }
    if (!editingId && form.authType === "api_key" && !form.secret.trim()) {
      setErrorText(zh ? "首次绑定需要填写天工账户页面生成的 API Key。" : "A TianGong API Key is required for first binding.");
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
            ? { api_key: form.secret.trim() }
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
      const result = await requestJson<{ ok: boolean; message: string; checked_at?: string }>(
        `${API_BASE}/data-platforms/accounts/${encodeURIComponent(accountId)}/test`,
        { method: "POST" },
      );
      setValidationDialog({ ok: result.ok, message: result.message, checkedAt: result.checked_at });
      onStatus?.(zh ? `天工账号校验：${result.message}` : `TianGong account check: ${result.message}`);
      await loadAccounts();
    } catch (error) {
      const message = error instanceof Error ? error.message : "test failed";
      setValidationDialog({ ok: false, message });
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
      if (remoteStateMode === "open") {
        params.set("state_code", "100");
      } else {
        params.set("state_scope", "all");
      }
      if (remoteKind === "flows" && remoteFlowType !== "all") {
        params.set("flow_type", remoteFlowType);
      }
      if (remoteKind === "processes" && remoteProcessType !== "all") {
        params.set("process_type", remoteProcessType);
      }
      const payload = await requestJson<RemoteSearchResponse>(
        `${API_BASE}/data-platforms/accounts/${encodeURIComponent(selectedAccount.id)}/${remoteKind}/search?${params.toString()}`,
      );
      setRemoteResult(payload);
      setRemotePreview(null);
      setRemotePage(payload.page);
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

  const performRemoteSync = async (item: RemoteItem) => {
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
      const syncError = error as RemoteSyncError;
      const message = error instanceof Error ? error.message : "sync failed";
      const failedFlow = syncError.failedFlowUuid
        ? (zh ? `；失败 Flow：${syncError.failedFlowUuid}` : `; failed flow: ${syncError.failedFlowUuid}`)
        : "";
      const rollback = syncError.rolledBack
        ? (zh ? "；本次范围已回滚" : "; scoped changes rolled back")
        : "";
      setErrorText(zh ? `导入失败：${message}${failedFlow}${rollback}` : `Import failed: ${message}${failedFlow}${rollback}`);
    } finally {
      setSyncingKey("");
    }
  };

  const syncRemoteItem = async (item: RemoteItem) => {
    if (remoteKind !== "models") {
      await performRemoteSync(item);
      return;
    }
    const previewMatches = remotePreview
      && remotePreview.remote_id === item.remote_id
      && (remotePreview.remote_version ?? null) === (item.remote_version ?? null);
    if (!previewMatches) {
      setErrorText(zh ? "模型导入前需要先预览当前版本。" : "Preview the current model version before importing.");
      onStatus?.(zh ? "请先预览该模型的当前版本。" : "Preview this model version first.");
      return;
    }
    setModelSyncConfirm({ open: true, item });
  };

  const confirmModelSync = async () => {
    const item = modelSyncConfirm.item;
    setModelSyncConfirm({ open: false, item: null });
    if (!item || !selectedAccount) return;
    await performRemoteSync(item);
  };

  const itemTitle = (item: RemoteItem): string => {
    if (!zh) return item.flow_name_en ?? item.process_name_en ?? item.model_name_en ?? item.flow_name ?? item.process_name ?? item.model_name ?? item.remote_id;
    return item.flow_name ?? item.process_name ?? item.model_name ?? item.remote_id;
  };
  const itemType = (item: RemoteItem): string => item.flow_type ?? item.process_type ?? (remoteKind === "models" ? "Model" : "-");
  const itemSummary = (item: RemoteItem): string => {
    const classification = typeof item.metadata?.classification === "string" ? item.metadata.classification : "";
    if (classification) return classification;
    if (item.default_unit || item.unit_group) return [item.default_unit, item.unit_group].filter(Boolean).join(" / ");
    return "-";
  };
  const itemModifiedAt = (item: RemoteItem): string => {
    const raw = typeof item.metadata?.modified_at === "string" ? item.metadata.modified_at : "";
    return raw ? formatTime(raw) : "-";
  };
  const previewFieldLabel = (key: string): string => key.replace(/_/g, " ");
  const previewValue = (value: unknown): string => {
    if (value === null || value === undefined || value === "") return "-";
    if (typeof value === "boolean") return value ? (zh ? "是" : "Yes") : (zh ? "否" : "No");
    if (typeof value === "object") return JSON.stringify(value);
    return String(value);
  };
  const previewRelatedText = (item: Record<string, unknown>): string => {
    const name = previewValue(item.name ?? item.flow_id ?? item.process_id);
    const direction = previewValue(item.direction);
    const amount = previewValue(item.amount);
    const unit = previewValue(item.unit);
    const ref = item.is_reference_flow ? (zh ? "参考" : "reference") : "";
    return [direction, name, amount !== "-" ? amount : "", unit !== "-" ? unit : "", ref].filter(Boolean).join(" · ");
  };
  return (
    <section className="pm-page pm-platform-workbench">
      <div className="pm-page-head">
        <div>
          <h2>{zh ? "天工数据接入" : "TianGong Data Access"}</h2>
        </div>
        <div className="pm-head-actions">
          <span className={`pm-connection-pill ${selectedAccount?.last_validation_status === "ok" ? "connected" : ""}`}>
            {selectedAccount
              ? `${selectedAccount.alias} · ${accountValidationLabel(selectedAccount, zh)}`
              : (zh ? "未绑定" : "Not bound")}
          </span>
          {selectedAccount && (
            <button type="button" className="pm-ghost-btn" onClick={() => void testAccount(selectedAccount.id)} disabled={testingId === selectedAccount.id}>
              {testingId === selectedAccount.id ? (zh ? "校验中" : "Checking") : (zh ? "校验" : "Check")}
            </button>
          )}
          <button type="button" className="pm-ghost-btn" onClick={() => selectedAccount ? editAccount(selectedAccount) : openNewAccountDialog()}>
            {selectedAccount ? (zh ? "账号设置" : "Account") : (zh ? "绑定账号" : "Bind Account")}
          </button>
        </div>
      </div>

      {errorText && <div className="pm-field-error pm-workbench-error">{errorText}</div>}

      <div className="pm-remote-workspace">
        <div className="pm-remote-toolbar">
          {remoteKinds.map((kind) => (
            <button
              key={kind}
              type="button"
              className={`pm-remote-tab ${remoteKind === kind ? "active" : ""}`}
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
          <input
            className="pm-remote-query-input"
            value={remoteQuery}
            onChange={(event) => setRemoteQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") void searchRemote(1);
            }}
            placeholder={zh ? "搜索名称、中文名、英文名或同义词" : "Search name, localized title, or synonym"}
          />
          <select value={remoteStateMode} onChange={(event) => setRemoteStateMode(event.target.value as "open" | "all")}>
            <option value="open">{zh ? "开放数据" : "Open data"}</option>
            <option value="all">{zh ? "全部状态" : "All states"}</option>
          </select>
          {remoteKind === "flows" && (
            <select value={remoteFlowType} onChange={(event) => setRemoteFlowType(event.target.value)}>
              {flowTypeOptions.map((value) => (
                <option key={value} value={value}>{value === "all" ? (zh ? "全部流类型" : "All flow types") : value}</option>
              ))}
            </select>
          )}
          {remoteKind === "processes" && (
            <select value={remoteProcessType} onChange={(event) => setRemoteProcessType(event.target.value)}>
              {processTypeOptions.map((value) => (
                <option key={value} value={value}>{value === "all" ? (zh ? "全部数据集类型" : "All dataset types") : value}</option>
              ))}
            </select>
          )}
          <button type="button" className="pm-primary-btn" onClick={() => void searchRemote(1)} disabled={remoteLoading || !selectedAccount}>
            {remoteLoading ? (zh ? "查询中..." : "Searching...") : (zh ? "查询" : "Search")}
          </button>
          {!selectedAccount && (
            <button type="button" className="pm-primary-btn" onClick={openNewAccountDialog}>
              {zh ? "绑定天工账号" : "Bind TianGong Account"}
            </button>
          )}
        </div>

        <div className="pm-remote-table-panel">
          <table className="pm-table pm-remote-result-table">
            <thead>
              <tr>
                <th>{zh ? "名称" : "Name"}</th>
                <th>{zh ? "类型" : "Type"}</th>
                <th>{zh ? "分类 / 摘要" : "Classification / Summary"}</th>
                <th>{zh ? "版本" : "Version"}</th>
                <th>{zh ? "更新" : "Updated"}</th>
                <th>{zh ? "操作" : "Actions"}</th>
              </tr>
            </thead>
            <tbody>
              {(remoteResult?.items ?? []).map((item) => {
                const key = `${remoteKind}:${item.remote_id}`;
                return (
                  <tr key={`${item.remote_id}:${item.remote_version ?? ""}`}>
                    <td>
                      <strong>{itemTitle(item)}</strong>
                    </td>
                    <td>
                      <span>{itemType(item)}</span>
                      {itemType(item) === "LCI result" && (
                        <span className="pm-lci-result-badge" title={zh ? "清单结果（背景 LCI 数据集）" : "LCI result (background LCI dataset)"}>LCI</span>
                      )}
                    </td>
                    <td><span className="pm-result-summary">{itemSummary(item)}</span></td>
                    <td>{item.remote_version ?? "-"}</td>
                    <td>{itemModifiedAt(item)}</td>
                    <td>
                      <div className="pm-row-actions">
                        <button type="button" className="pm-link-btn primary" onClick={() => void syncRemoteItem(item)} disabled={syncingKey === key}>
                          {syncingKey === key ? (zh ? "导入中" : "Importing") : (zh ? "导入" : "Import")}
                        </button>
                        <button type="button" className="pm-link-btn" onClick={() => void previewRemoteItem(item)} disabled={previewLoadingKey === key}>
                          {previewLoadingKey === key ? (zh ? "预览中" : "Previewing") : (zh ? "预览" : "Preview")}
                        </button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {(!remoteResult || remoteResult.items.length === 0) && (
                <tr>
                  <td colSpan={6}>{remoteLoading ? (zh ? "加载中..." : "Loading...") : (zh ? "暂无远程结果。" : "No remote results.")}</td>
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

      {lastSync?.tidas_import_report && (
        <div className="pm-refresh-result-strip">
          <div className="pm-refresh-result-content">
            <>
              <strong>{zh ? "最近导入" : "Last Import"}</strong>
              <span>{zh ? `新增 ${lastSync?.tidas_import_report?.inserted ?? 0}` : `${lastSync?.tidas_import_report?.inserted ?? 0} inserted`}</span>
              <span>{zh ? `更新 ${lastSync?.tidas_import_report?.updated ?? 0}` : `${lastSync?.tidas_import_report?.updated ?? 0} updated`}</span>
              <span>{zh ? `失败 ${lastSync?.tidas_import_report?.failed ?? 0}` : `${lastSync?.tidas_import_report?.failed ?? 0} failed`}</span>
              {lastSync.flow_count !== undefined && (
                <span>{zh ? `引用 Flow ${lastSync.flow_count}` : `${lastSync.flow_count} referenced flows`}</span>
              )}
              <span>
                {zh
                  ? `警告 ${Number(lastSync?.tidas_import_report?.warning_count ?? 0) + Number(lastSync?.tidas_import_report?.unresolved_count ?? 0)}`
                  : `${Number(lastSync?.tidas_import_report?.warning_count ?? 0) + Number(lastSync?.tidas_import_report?.unresolved_count ?? 0)} warnings`}
              </span>
            </>
          </div>
          <button
            type="button"
            className="pm-refresh-result-close"
            onClick={() => {
              setLastSync(null);
            }}
            title={zh ? "关闭" : "Close"}
          >
            ×
          </button>
        </div>
      )}


      {accountDialogOpen && (
        <div className="pm-modal-backdrop" role="presentation" onMouseDown={(event) => {
          if (event.target === event.currentTarget) resetForm();
        }}>
          <div className="pm-account-dialog" role="dialog" aria-modal="true">
            <div className="pm-dialog-head">
              <h3>{editingId ? (zh ? "编辑天工账号" : "Edit TianGong Account") : (zh ? "绑定天工账号" : "Bind TianGong Account")}</h3>
              <button type="button" className="pm-icon-btn" onClick={resetForm}>×</button>
            </div>
            <div className="pm-platform-form pm-platform-form--dialog">
              <label><span>{zh ? "账号名称" : "Alias"}</span><input value={form.alias} onChange={(event) => setForm((prev) => ({ ...prev, alias: event.target.value }))} /></label>
              <label><span>Supabase URL</span><input value={form.baseUrl} onChange={(event) => setForm((prev) => ({ ...prev, baseUrl: event.target.value }))} /></label>
              <label><span>Publishable Key</span><input type="password" autoComplete="off" value={form.publishableKey} onChange={(event) => setForm((prev) => ({ ...prev, publishableKey: event.target.value }))} /></label>
              <label><span>{zh ? "环境名" : "Environment"}</span><input value={form.environmentLabel} onChange={(event) => setForm((prev) => ({ ...prev, environmentLabel: event.target.value }))} /></label>
              <label>
                <span>{zh ? "认证方式" : "Auth Type"}</span>
                <select value={form.authType} onChange={(event) => setForm((prev) => ({ ...prev, authType: event.target.value as "basic" | "api_key" }))}>
                  <option value="basic">{zh ? "天工账号登录" : "TianGong Login"}</option>
                  <option value="api_key">{zh ? "天工 API Key" : "TianGong API Key"}</option>
                </select>
              </label>
              {form.authType === "basic" ? (
                <>
                  <label><span>{zh ? "天工平台用户名（邮箱）" : "TianGong platform username (email)"}</span><input type="text" autoComplete="username" placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""} value={form.email} onChange={(event) => setForm((prev) => ({ ...prev, email: event.target.value }))} /></label>
                  <label><span>{zh ? "天工密码" : "TianGong Password"}</span><input type="password" autoComplete="current-password" placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""} value={form.password} onChange={(event) => setForm((prev) => ({ ...prev, password: event.target.value }))} /></label>
                </>
              ) : (
                <label><span>{zh ? "天工 API Key" : "TianGong API Key"}</span><input type="password" autoComplete="off" placeholder={editingId ? (zh ? "留空则保留原凭据" : "Leave blank to keep existing credential") : ""} value={form.secret} onChange={(event) => setForm((prev) => ({ ...prev, secret: event.target.value }))} /></label>
              )}
              <label>
                <span>{zh ? "状态" : "Status"}</span>
                <select value={form.status} onChange={(event) => setForm((prev) => ({ ...prev, status: event.target.value as "active" | "disabled" }))}>
                  <option value="active">{zh ? "启用" : "Active"}</option>
                  <option value="disabled">{zh ? "停用" : "Disabled"}</option>
                </select>
              </label>
            </div>
            <div className="pm-dialog-actions">
              {editingId && (
                <button type="button" className="pm-ghost-btn danger" onClick={() => void deleteAccount(editingId)} disabled={saving}>
                  {zh ? "删除绑定" : "Delete Binding"}
                </button>
              )}
              <button type="button" className="pm-ghost-btn" onClick={resetForm}>{zh ? "取消" : "Cancel"}</button>
              <button type="button" className="pm-primary-btn" onClick={() => void saveAccount()} disabled={saving}>{saving ? (zh ? "保存中..." : "Saving...") : (zh ? "保存" : "Save")}</button>
            </div>
          </div>
        </div>
      )}

      {validationDialog && (
        <div className="pm-modal-backdrop" role="presentation" onMouseDown={(event) => {
          if (event.target === event.currentTarget) setValidationDialog(null);
        }}>
          <div className="pm-validation-dialog" role="dialog" aria-modal="true">
            <div className="pm-dialog-head">
              <h3>{zh ? "天工账号校验" : "TianGong Account Check"}</h3>
              <button type="button" className="pm-icon-btn" onClick={() => setValidationDialog(null)}>×</button>
            </div>
            <div className="pm-validation-body">
              <span className={`pm-status-badge ${validationDialog.ok ? "pm-status-badge--balanced" : "pm-status-badge--unchecked"}`}>
                {validationDialog.ok ? (zh ? "通过" : "OK") : (zh ? "未通过" : "Failed")}
              </span>
              <p>{validationDialog.message}</p>
              <small>{formatTime(validationDialog.checkedAt)}</small>
            </div>
            <div className="pm-dialog-actions">
              <button type="button" className="pm-primary-btn" onClick={() => setValidationDialog(null)}>{zh ? "知道了" : "OK"}</button>
            </div>
          </div>
        </div>
      )}

      {remotePreview && (
        <aside className="pm-preview-drawer">
          <div className="pm-dialog-head">
            <h3>{remotePreview.title}</h3>
            <button type="button" className="pm-icon-btn" onClick={() => setRemotePreview(null)}>×</button>
          </div>
          <div className="pm-preview-meta">
            <span>{remotePreview.remote_kind}</span>
            <span>{remotePreview.remote_version ?? "-"}</span>
          </div>
          {remotePreview.description && <p>{remotePreview.description}</p>}
          <dl>
            {Object.entries(remotePreview.summary ?? {}).slice(0, 10).map(([key, value]) => (
              value ? <div key={key}><dt>{previewFieldLabel(key)}</dt><dd>{previewValue(value)}</dd></div> : null
            ))}
          </dl>
          {(remotePreview.related ?? []).length > 0 && (
            <div className="pm-preview-related">
              <strong>{remotePreview.remote_kind === "process" ? (zh ? "交换流" : "Exchanges") : (zh ? "关联项" : "Related")}</strong>
              {(remotePreview.related ?? []).slice(0, 5).map((item, index) => (
                <span key={index} title={previewRelatedText(item)}>{previewRelatedText(item)}</span>
              ))}
            </div>
          )}
        </aside>
      )}

      {modelSyncConfirm.open && modelSyncConfirm.item && (
        <div className="pm-modal-mask" onClick={() => setModelSyncConfirm({ open: false, item: null })}>
          <div className="pm-modal" onClick={(event) => event.stopPropagation()}>
            <div className="pm-modal-head">
              <strong>{zh ? "确认导入模型" : "Confirm Model Import"}</strong>
              <button type="button" className="pm-link-btn" onClick={() => setModelSyncConfirm({ open: false, item: null })}>
                {zh ? "关闭" : "Close"}
              </button>
            </div>
            <p>
              {zh
                ? `即将导入模型：${modelSyncConfirm.item.model_name ?? modelSyncConfirm.item.remote_id}`
                : `About to import model: ${modelSyncConfirm.item.model_name ?? modelSyncConfirm.item.remote_id}`}
            </p>
            {(() => {
              const relatedCount = Array.isArray(remotePreview?.related) ? remotePreview.related.length : 0;
              if (relatedCount > 0) {
                return (
                  <p>
                    {zh
                      ? `此模型依赖 ${relatedCount} 个关联项（交换流 / 上下游数据）。导入时将一并处理这些依赖。`
                      : `This model depends on ${relatedCount} related item(s) (exchanges / upstream-downstream data). Dependencies will be processed during import.`}
                  </p>
                );
              }
              return (
                <p>
                  {zh ? "此模型当前没有已知的依赖项。" : "This model has no known dependencies from the preview."}
                </p>
              );
            })()}
            {remotePreview?.warnings && remotePreview.warnings.length > 0 && (
              <div className="pm-warning" style={{ marginTop: "8px" }}>
                <strong>{zh ? "警告" : "Warnings"}:</strong>
                <ul style={{ margin: "4px 0 0 0", paddingLeft: "20px" }}>
                  {remotePreview.warnings.map((w, idx) => (
                    <li key={idx}>{w}</li>
                  ))}
                </ul>
              </div>
            )}
            <div className="pm-modal-actions">
              <button type="button" className="pm-ghost-btn" onClick={() => setModelSyncConfirm({ open: false, item: null })}>
                {zh ? "取消" : "Cancel"}
              </button>
              <button type="button" className="pm-primary-btn" onClick={() => void confirmModelSync()} disabled={Boolean(syncingKey)}>
                {syncingKey ? (zh ? "导入中" : "Importing") : (zh ? "确认导入" : "Confirm Import")}
              </button>
            </div>
          </div>
        </div>
      )}
    </section>
  );
}
