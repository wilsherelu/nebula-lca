"""Smoke script for TianGong Flow/Process acceptance testing.

Uses Python standard library HTTP calls against the Nebula LCA API
to exercise the Flow and Process import path end-to-end.

Usage:
    python tiangong_flow_process_smoke.py
    python tiangong_flow_process_smoke.py --skip-sync
    python tiangong_flow_process_smoke.py --flow-query bio --process-query biofuel --page-size 5
    NEBULA_API_BASE=http://localhost:8001/api python tiangong_flow_process_smoke.py

Environment variables:
    NEBULA_API_BASE   - API base URL (default: http://127.0.0.1:8001/api)
    TIANGONG_ACCOUNT_ID - Optional fixed account ID (auto-discovered if omitted)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


DEFAULT_API_BASE = "http://127.0.0.1:8001/api"
DEFAULT_FLOW_QUERY = "electricity"
DEFAULT_PROCESS_QUERY = "electricity"
DEFAULT_PAGE_SIZE = 3


def _no_proxy_for(url: str) -> bool:
    """Return True when the URL targets local 127.0.0.1 or localhost."""
    parsed = urllib.parse.urlparse(url)
    host = parsed.hostname or ""
    return host in ("127.0.0.1", "localhost")


def _build_request(url: str, method: str = "GET", body: object | None = None, token: str | None = None) -> urllib.request.Request:
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    return req


def _api_call(url: str, *, method: str = "GET", body: object | None = None, token: str | None = None) -> tuple[int, object]:
    """Perform an HTTP call, disabling proxy for local URLs."""
    req = _build_request(url, method=method, body=body, token=token)

    # Disable proxy use for local 127.0.0.1/localhost
    if _no_proxy_for(url):
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    else:
        opener = urllib.request.build_opener()

    try:
        with opener.open(req, timeout=30) as resp:
            raw = resp.read().decode("utf-8")
            if not raw:
                return resp.status, None
            try:
                return resp.status, json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                return resp.status, {"error_text": raw[:1024]}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        if raw:
            try:
                return exc.code, json.loads(raw)
            except (json.JSONDecodeError, ValueError):
                return exc.code, {"error_text": raw[:1024]}
        return exc.code, {"error_text": str(exc)}
    except Exception as exc:
        return 0, {"error": str(exc)}


def _discover_tiangong_account(api_base: str) -> str | None:
    """Find a validated TianGong account from the account list."""
    status, result = _api_call(f"{api_base}/data-platforms/accounts")
    if status != 200 or not isinstance(result, list):
        return None
    for acc in result:
        if acc.get("platform") == "tiangong" and acc.get("last_validation_status") == "ok":
            return acc.get("id")
    return None


def _search_flows(api_base: str, account_id: str, query: str, page_size: int) -> tuple[int, object]:
    params = urllib.parse.urlencode({"q": query, "page": 1, "page_size": page_size})
    url = f"{api_base}/data-platforms/accounts/{account_id}/flows/search?{params}"
    return _api_call(url)


def _search_processes(api_base: str, account_id: str, query: str, page_size: int) -> tuple[int, object]:
    params = urllib.parse.urlencode({"q": query, "page": 1, "page_size": page_size})
    url = f"{api_base}/data-platforms/accounts/{account_id}/processes/search?{params}"
    return _api_call(url)


def _preview_flow(api_base: str, account_id: str, remote_id: str, remote_version: str | None = None) -> tuple[int, object]:
    params = urllib.parse.urlencode({"remote_version": remote_version}) if remote_version else ""
    url = f"{api_base}/data-platforms/accounts/{account_id}/flows/{remote_id}/preview"
    if params:
        url = f"{url}?{params}"
    return _api_call(url)


def _preview_process(api_base: str, account_id: str, remote_id: str, remote_version: str | None = None) -> tuple[int, object]:
    params = urllib.parse.urlencode({"remote_version": remote_version}) if remote_version else ""
    url = f"{api_base}/data-platforms/accounts/{account_id}/processes/{remote_id}/preview"
    if params:
        url = f"{url}?{params}"
    return _api_call(url)


def _sync_flow(api_base: str, account_id: str, remote_id: str, remote_version: str | None = None) -> tuple[int, object]:
    url = f"{api_base}/data-platforms/accounts/{account_id}/flows/sync"
    body: dict[str, object] = {"remote_flow_id": remote_id, "overwrite": True}
    if remote_version is not None:
        body["remote_version"] = remote_version
    return _api_call(url, method="POST", body=body)


def _sync_process(api_base: str, account_id: str, remote_id: str, remote_version: str | None = None) -> tuple[int, object]:
    url = f"{api_base}/data-platforms/accounts/{account_id}/processes/sync"
    body: dict[str, object] = {"remote_process_id": remote_id, "overwrite": True}
    if remote_version is not None:
        body["remote_version"] = remote_version
    return _api_call(url, method="POST", body=body)


def _list_sync_jobs(api_base: str, account_id: str) -> tuple[int, object]:
    url = f"{api_base}/data-platforms/accounts/{account_id}/sync-jobs"
    return _api_call(url)


def _list_sync_records(api_base: str, account_id: str) -> tuple[int, object]:
    url = f"{api_base}/data-platforms/accounts/{account_id}/sync-records"
    return _api_call(url)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Smoke test for TianGong Flow/Process import path via Nebula LCA API."
    )
    parser.add_argument(
        "--api-base",
        default=os.environ.get("NEBULA_API_BASE", DEFAULT_API_BASE),
        help="Nebula API base URL (overrides NEBULA_API_BASE env, default: http://127.0.0.1:8001/api)",
    )
    parser.add_argument(
        "--account-id",
        default=os.environ.get("TIANGONG_ACCOUNT_ID"),
        help="Fixed TianGong account ID (auto-discovered if omitted)",
    )
    parser.add_argument(
        "--flow-query",
        default=DEFAULT_FLOW_QUERY,
        help=f"Flow search query (default: {DEFAULT_FLOW_QUERY})",
    )
    parser.add_argument(
        "--process-query",
        default=DEFAULT_PROCESS_QUERY,
        help=f"Process search query (default: {DEFAULT_PROCESS_QUERY})",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help=f"Page size for search (default: {DEFAULT_PAGE_SIZE})",
    )
    parser.add_argument(
        "--skip-sync",
        action="store_true",
        help="Skip Flow/Process sync steps (read-only validation)",
    )
    args = parser.parse_args()

    api_base = args.api_base.rstrip("/")
    passed_steps = 0
    failures: list[dict] = []
    warnings: list[dict] = []

    # Step 1: discover account
    account_id = args.account_id
    if not account_id:
        account_id = _discover_tiangong_account(api_base)
    if not account_id:
        failures.append({
            "step": "account_discovery",
            "error": "No validated TianGong account found. Run account creation and test first.",
        })
        _print_final(account_id, passed_steps, False, failures, warnings, [], [], [], 0)
        sys.exit(1)

    # Step 2: search flows
    flow_status, flow_result = _search_flows(api_base, account_id, args.flow_query, args.page_size)
    flow_remote_id = None
    flow_remote_version: str | None = None
    flow_preview_job_id = None
    flow_sync_job_id = None
    flow_tidas_summary: dict | None = None

    if flow_status != 200:
        failures.append({"step": "flow_search", "status": flow_status, "detail": str(flow_result)})
    elif not isinstance(flow_result, dict) or not flow_result.get("items"):
        warnings.append({"step": "flow_search", "message": "Flow search returned no items"})
    else:
        flow_items = flow_result.get("items", [])
        first_flow = flow_items[0]
        flow_remote_id = first_flow.get("remote_id")
        flow_remote_version = first_flow.get("remote_version")
        passed_steps += 1

        # Preview first flow
        preview_status, preview_result = _preview_flow(api_base, account_id, flow_remote_id, flow_remote_version)
        if preview_status != 200:
            warnings.append({"step": "flow_preview", "status": preview_status, "message": "Flow preview failed"})
        else:
            flow_preview_job_id = preview_result.get("remote_id")
            passed_steps += 1

            # Sync unless skip
            if not args.skip_sync:
                sync_status, sync_result = _sync_flow(api_base, account_id, flow_remote_id, flow_remote_version)
                if sync_status != 200:
                    failures.append({"step": "flow_sync", "status": sync_status, "detail": str(sync_result)})
                else:
                    passed_steps += 1
                    flow_sync_job_id = sync_result.get("job_id")
                    flow_tidas_report = sync_result.get("tidas_import_report")
                    if flow_tidas_report:
                        flow_tidas_summary = {
                            "inserted": flow_tidas_report.get("inserted", 0),
                            "updated": flow_tidas_report.get("updated", 0),
                            "failed": flow_tidas_report.get("failed", 0),
                        }

    # Step 3: search processes
    process_status, process_result = _search_processes(api_base, account_id, args.process_query, args.page_size)
    process_remote_id = None
    process_remote_version: str | None = None
    process_preview_job_id = None
    process_sync_job_id = None
    process_tidas_summary: dict | None = None

    if process_status != 200:
        failures.append({"step": "process_search", "status": process_status, "detail": str(process_result)})
    elif not isinstance(process_result, dict) or not process_result.get("items"):
        warnings.append({"step": "process_search", "message": "Process search returned no items"})
    else:
        process_items = process_result.get("items", [])
        first_process = process_items[0]
        process_remote_id = first_process.get("remote_id")
        process_remote_version = first_process.get("remote_version")
        passed_steps += 1

        # Preview first process
        preview_status, preview_result = _preview_process(api_base, account_id, process_remote_id, process_remote_version)
        if preview_status != 200:
            warnings.append({"step": "process_preview", "status": preview_status, "message": "Process preview failed"})
        else:
            process_preview_job_id = preview_result.get("remote_id")
            passed_steps += 1

            # Sync unless skip
            if not args.skip_sync:
                sync_status, sync_result = _sync_process(api_base, account_id, process_remote_id, process_remote_version)
                if sync_status != 200:
                    failures.append({"step": "process_sync", "status": sync_status, "detail": str(sync_result)})
                else:
                    passed_steps += 1
                    process_sync_job_id = sync_result.get("job_id")
                    process_tidas_report = sync_result.get("tidas_import_report")
                    if process_tidas_report:
                        process_tidas_summary = {
                            "inserted": process_tidas_report.get("inserted", 0),
                            "updated": process_tidas_report.get("updated", 0),
                            "failed": process_tidas_report.get("failed", 0),
                        }

    # Step 4: query sync history
    job_status, job_result = _list_sync_jobs(api_base, account_id)
    remote_ids = []
    versions = []
    for current_remote_id in (flow_remote_id, process_remote_id):
        if current_remote_id and current_remote_id not in remote_ids:
            remote_ids.append(current_remote_id)
    for current_version in (flow_remote_version, process_remote_version):
        if current_version and current_version not in versions:
            versions.append(current_version)
    all_sync_job_ids: list[str] = []
    tidas_import_summaries: list[dict] = []
    sync_record_count = 0

    if job_status != 200:
        failures.append({"step": "sync_jobs_history", "status": job_status, "detail": str(job_result)})
    elif isinstance(job_result, list):
        all_sync_job_ids = [j.get("id", "") for j in job_result if j.get("id")]
        for job in job_result:
            stats = job.get("stats")
            if isinstance(stats, dict):
                tidas = stats.get("tidas_import")
                if tidas:
                    tidas_import_summaries.append({
                        "job_id": tidas.get("job_id"),
                        "import_type": tidas.get("import_type"),
                        "inserted": tidas.get("inserted", 0),
                        "updated": tidas.get("updated", 0),
                        "failed": tidas.get("failed", 0),
                    })
                remote_id = job.get("remote_process_id")
                if remote_id:
                    remote_ids.append(remote_id)

    record_status, record_result = _list_sync_records(api_base, account_id)
    if record_status != 200:
        failures.append({"step": "sync_records_history", "status": record_status, "detail": str(record_result)})
    elif isinstance(record_result, list):
        sync_record_count = len(record_result)
        for rec in record_result:
            rid = rec.get("remote_id")
            if rid and rid not in remote_ids:
                remote_ids.append(rid)
            rv = rec.get("remote_version")
            if rv and rv not in versions:
                versions.append(rv)

    _print_final(
        account_id,
        passed_steps,
        len(failures) == 0,
        failures,
        warnings,
        remote_ids,
        versions,
        all_sync_job_ids,
        sync_record_count,
        flow_tidas_summary=flow_tidas_summary,
        process_tidas_summary=process_tidas_summary,
    )


def _print_final(
    account_id: str | None,
    passed_steps: int,
    pass_ok: bool,
    failures: list[dict],
    warnings: list[dict],
    remote_ids: list,
    versions: list,
    sync_job_ids: list,
    sync_record_count: int,
    *,
    flow_tidas_summary: dict | None = None,
    process_tidas_summary: dict | None = None,
) -> None:
    """Print the final JSON output only. Credential-safe: no secrets, stable keys."""
    # Sanitize failures and warnings to strip any potential credential leakage
    def _sanitize(item: dict) -> dict:
        return {k: v for k, v in item.items() if k not in ("credential", "password", "token", "api_key")}

    output = {
        "summary": {
            "pass": pass_ok,
            "failures": len(failures),
            "warnings": len(warnings),
            "passed_steps": passed_steps,
        },
        "account_id": account_id,
        "remote_ids": remote_ids,
        "versions": versions,
        "sync_job_ids": sync_job_ids,
        "sync_record_count": sync_record_count,
        "failures": [_sanitize(f) for f in failures],
        "warnings": [_sanitize(w) for w in warnings],
    }
    if flow_tidas_summary:
        output["flow_tidas_import"] = flow_tidas_summary
    if process_tidas_summary:
        output["process_tidas_import"] = process_tidas_summary
    print(json.dumps(output, ensure_ascii=False, default=str))


if __name__ == "__main__":
    main()
