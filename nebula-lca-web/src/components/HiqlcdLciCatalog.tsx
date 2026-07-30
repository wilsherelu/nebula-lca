import { useEffect, useMemo, useState } from "react";
import { getApiBase } from "../apiBase";

const API_BASE = getApiBase();

type UiLanguage = "zh" | "en";
type Account = {
  id: string;
  alias: string;
  status: string;
  has_credential: boolean;
  last_validation_status?: string | null;
  last_validation_message?: string | null;
};
type Dataset = {
  remote_id: string;
  remote_version?: string | null;
  process_name?: string;
  metadata?: Record<string, unknown>;
};
type SearchResult = { items: Dataset[]; total: number; page: number; has_more: boolean };
type Preview = { title: string; description?: string | null; summary?: Record<string, unknown>; related?: Array<Record<string, unknown>> };
type ImportResult = { reference_product_name: string; vector_nnz: number; remote_dataset_version?: string | null };

type Props = { uiLanguage: UiLanguage; onStatus?: (text: string) => void };

async function requestJson<T>(url: string, init?: RequestInit): Promise<T> {
  const response = await fetch(url, init);
  if (!response.ok) {
    const payload = await response.json().catch(() => ({})) as { detail?: { message?: string } | string; message?: string };
    const detail = typeof payload.detail === "string" ? payload.detail : payload.detail?.message;
    throw new Error(detail ?? payload.message ?? `HTTP ${response.status}`);
  }
  return response.json() as Promise<T>;
}

export function HiqlcdLciCatalog({ uiLanguage, onStatus }: Props) {
  const zh = uiLanguage === "zh";
  const t = (cn: string, en: string) => zh ? cn : en;
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [selectedId, setSelectedId] = useState("");
  const [accountOpen, setAccountOpen] = useState(false);
  const [alias, setAlias] = useState("HiQLCD LCI");
  const [apiKey, setApiKey] = useState("");
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<SearchResult | null>(null);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [previewingId, setPreviewingId] = useState("");
  const [importingId, setImportingId] = useState("");
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");

  const selected = useMemo(
    () => accounts.find((account) => account.id === selectedId) ?? accounts[0],
    [accounts, selectedId],
  );

  const loadAccounts = async () => {
    const rows = await requestJson<Account[]>(`${API_BASE}/data-platforms/hiqlcd/accounts`);
    setAccounts(rows);
    if (!selectedId && rows[0]) setSelectedId(rows[0].id);
  };

  useEffect(() => { void loadAccounts().catch((reason) => setError(reason instanceof Error ? reason.message : "load failed")); }, []);

  const saveAccount = async () => {
    if (!selected && !apiKey.trim()) {
      setError(t("请填写 HiQLCD API Key。", "Enter the HiQLCD API Key."));
      return;
    }
    setSaving(true);
    setError("");
    try {
      const body = { alias: alias.trim() || "HiQLCD LCI", api_key: apiKey.trim(), status: "active" };
      const account = selected
        ? await requestJson<Account>(`${API_BASE}/data-platforms/hiqlcd/accounts/${encodeURIComponent(selected.id)}`, { method: "PATCH", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) })
        : await requestJson<Account>(`${API_BASE}/data-platforms/hiqlcd/accounts`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
      setSelectedId(account.id);
      setApiKey("");
      setAccountOpen(false);
      setNotice(t("HiQLCD API Key 已保存。", "HiQLCD API Key saved."));
      await loadAccounts();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t("保存失败", "Save failed"));
    } finally {
      setSaving(false);
    }
  };

  const testAccount = async () => {
    if (!selected) return;
    setTesting(true);
    setError("");
    try {
      const checked = await requestJson<{ ok: boolean; message: string }>(`${API_BASE}/data-platforms/hiqlcd/accounts/${encodeURIComponent(selected.id)}/test`, { method: "POST" });
      setNotice(checked.message);
      onStatus?.(checked.message);
      await loadAccounts();
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t("校验失败", "Validation failed"));
    } finally {
      setTesting(false);
    }
  };

  const search = async (nextPage = page) => {
    if (!selected) {
      setError(t("请先绑定 HiQLCD API Key。", "Bind a HiQLCD API Key first."));
      return;
    }
    if (!query.trim()) {
      setError(t("请输入数据集关键词。", "Enter a dataset keyword."));
      return;
    }
    setLoading(true);
    setError("");
    setPreview(null);
    try {
      const params = new URLSearchParams({ q: query.trim(), page: String(nextPage), page_size: "10", locale: zh ? "zh" : "en" });
      const payload = await requestJson<SearchResult>(`${API_BASE}/data-platforms/hiqlcd/accounts/${encodeURIComponent(selected.id)}/datasets/search?${params.toString()}`);
      setResult(payload);
      setPage(payload.page);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t("查询失败", "Search failed"));
    } finally {
      setLoading(false);
    }
  };

  const previewDataset = async (dataset: Dataset) => {
    if (!selected) return;
    setPreviewingId(dataset.remote_id);
    setError("");
    try {
      const params = new URLSearchParams({ locale: zh ? "zh" : "en" });
      if (dataset.remote_version) params.set("dataset_version", dataset.remote_version);
      setPreview(await requestJson<Preview>(`${API_BASE}/data-platforms/hiqlcd/accounts/${encodeURIComponent(selected.id)}/datasets/${encodeURIComponent(dataset.remote_id)}/preview?${params.toString()}`));
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t("预览失败", "Preview failed"));
    } finally {
      setPreviewingId("");
    }
  };

  const importDataset = async (dataset: Dataset) => {
    if (!selected) return;
    setImportingId(dataset.remote_id);
    setError("");
    try {
      const imported = await requestJson<ImportResult>(`${API_BASE}/data-platforms/hiqlcd/accounts/${encodeURIComponent(selected.id)}/datasets/import`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ dataset_id: dataset.remote_id, dataset_version: dataset.remote_version, locale: zh ? "zh" : "en" }),
      });
      const message = t(`已导入背景 LCI：${imported.reference_product_name}（${imported.vector_nnz} 条基本流）。`, `Background LCI imported: ${imported.reference_product_name} (${imported.vector_nnz} elementary flows).`);
      setNotice(message);
      onStatus?.(message);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : t("导入失败", "Import failed"));
    } finally {
      setImportingId("");
    }
  };

  const sourceLabel = (dataset: Dataset) => String(dataset.metadata?.source_name ?? "HiQLCD");
  const location = (dataset: Dataset) => String(dataset.metadata?.location ?? "-");

  return (
    <section className="pm-page pm-external-platforms">
      <div className="pm-page-head">
        <div>
          <h2>{t("HiQLCD 背景 LCI", "HiQLCD Background LCI")}</h2>
          <p>{t("检索并导入背景清单，在过程清单中直接关联。", "Search and import background inventories for direct association in a process inventory.")}</p>
        </div>
        <div className="pm-head-actions">
          {selected && <span className={`pm-status-badge ${selected.last_validation_status === "ok" ? "pm-status-badge--balanced" : "pm-status-badge--unchecked"}`}>{selected.last_validation_status === "ok" ? t("已连接", "Connected") : t("待校验", "Check required")}</span>}
          <button type="button" className="pm-ghost-btn" disabled={!selected || testing} onClick={() => void testAccount()}>{testing ? t("校验中", "Checking") : t("校验", "Validate")}</button>
          <button type="button" className="pm-primary-btn" onClick={() => { setAlias(selected?.alias ?? "HiQLCD LCI"); setApiKey(""); setAccountOpen(true); }}>{selected ? t("账号设置", "Account settings") : t("绑定 API Key", "Bind API Key")}</button>
        </div>
      </div>

      {error && <div className="pm-warning">{error}</div>}
      {notice && <div className="pm-refresh-result-strip"><div className="pm-refresh-result-content"><span>{notice}</span></div><button type="button" className="pm-refresh-result-close" onClick={() => setNotice("")}>×</button></div>}

      <div className="pm-remote-toolbar">
        <input value={query} placeholder={t("检索产品或过程名称", "Search product or process") } onChange={(event) => setQuery(event.target.value)} onKeyDown={(event) => { if (event.key === "Enter") void search(1); }} />
        <button type="button" className="pm-primary-btn" disabled={loading || !selected} onClick={() => void search(1)}>{loading ? t("查询中", "Searching") : t("查询", "Search")}</button>
      </div>

      <div className="pm-remote-table-panel">
        <table className="pm-table pm-remote-result-table">
          <thead><tr><th>{t("数据集", "Dataset")}</th><th>{t("来源", "Source")}</th><th>{t("地区", "Location")}</th><th>{t("版本", "Version")}</th><th>{t("操作", "Actions")}</th></tr></thead>
          <tbody>
            {(result?.items ?? []).map((dataset) => <tr key={`${dataset.remote_id}:${dataset.remote_version ?? ""}`}>
              <td><strong>{dataset.process_name ?? dataset.remote_id}</strong></td><td>{sourceLabel(dataset)}</td><td>{location(dataset)}</td><td>{dataset.remote_version ?? "-"}</td>
              <td><div className="pm-row-actions"><button type="button" className="pm-link-btn primary" disabled={importingId === dataset.remote_id} onClick={() => void importDataset(dataset)}>{importingId === dataset.remote_id ? t("导入中", "Importing") : t("导入背景 LCI", "Import background LCI")}</button><button type="button" className="pm-link-btn" disabled={previewingId === dataset.remote_id} onClick={() => void previewDataset(dataset)}>{previewingId === dataset.remote_id ? t("预览中", "Previewing") : t("预览", "Preview")}</button></div></td>
            </tr>)}
            {(!result || result.items.length === 0) && <tr><td colSpan={5}>{loading ? t("正在查询…", "Searching…") : t("输入关键词后查询 HiQLCD 数据集。", "Enter a keyword to search HiQLCD datasets.")}</td></tr>}
          </tbody>
        </table>
      </div>
      {result && <div className="pm-remote-footer"><span>{t(`共 ${result.total} 条`, `${result.total} total`)}</span><div className="pm-row-actions"><button type="button" className="pm-ghost-btn" disabled={loading || page <= 1} onClick={() => void search(page - 1)}>{t("上一页", "Previous")}</button><span className="pm-remote-page-indicator">{page}</span><button type="button" className="pm-ghost-btn" disabled={loading || !result.has_more} onClick={() => void search(page + 1)}>{t("下一页", "Next")}</button></div></div>}

      {preview && <aside className="pm-preview-drawer"><div className="pm-dialog-head"><h3>{preview.title}</h3><button type="button" className="pm-icon-btn" onClick={() => setPreview(null)}>×</button></div>{preview.description && <p>{preview.description}</p>}<dl>{Object.entries(preview.summary ?? {}).slice(0, 8).map(([key, value]) => <div key={key}><dt>{key}</dt><dd>{String(value ?? "-")}</dd></div>)}</dl></aside>}

      {accountOpen && <div className="pm-modal-backdrop" role="presentation" onMouseDown={(event) => { if (event.target === event.currentTarget && !saving) setAccountOpen(false); }}><div className="pm-account-dialog" role="dialog" aria-modal="true"><div className="pm-dialog-head"><h3>{t("HiQLCD API Key", "HiQLCD API Key")}</h3><button type="button" className="pm-icon-btn" disabled={saving} onClick={() => setAccountOpen(false)}>×</button></div><div className="pm-platform-form pm-platform-form--dialog"><label><span>{t("账号名称", "Account name")}</span><input value={alias} onChange={(event) => setAlias(event.target.value)} /></label><label><span>API Key</span><input type="password" autoComplete="off" value={apiKey} placeholder={selected ? t("留空则保留当前密钥", "Leave blank to keep the current key") : ""} onChange={(event) => setApiKey(event.target.value)} /></label></div><div className="pm-dialog-actions"><button type="button" className="pm-ghost-btn" disabled={saving} onClick={() => setAccountOpen(false)}>{t("取消", "Cancel")}</button><button type="button" className="pm-primary-btn" disabled={saving} onClick={() => void saveAccount()}>{saving ? t("保存中", "Saving") : t("保存", "Save")}</button></div></div></div>}
    </section>
  );
}
