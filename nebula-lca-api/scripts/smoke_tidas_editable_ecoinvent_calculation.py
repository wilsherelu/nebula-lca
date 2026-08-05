"""Run the editable TIDAS -> ecoinvent -> LCIA workflow through HTTP APIs."""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from typing import Any


def _request(
    api_base: str,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
    timeout: int = 180,
    retries: int = 2,
) -> Any:
    url = f"{api_base.rstrip('/')}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    for attempt in range(retries + 1):
        try:
            with opener.open(request, timeout=timeout) as response:
                raw = response.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            if exc.code in {502, 503, 504} and attempt < retries:
                time.sleep(attempt + 1)
                continue
            try:
                detail = json.loads(raw)
            except json.JSONDecodeError:
                detail = raw[:2000]
            raise RuntimeError(f"{method} {path} failed with HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            if attempt < retries:
                time.sleep(attempt + 1)
                continue
            raise RuntimeError(f"{method} {path} failed: {exc.reason}") from exc
    raise RuntimeError(f"{method} {path} failed after retries")


def _port(item: dict[str, Any], port_id: str, *, link: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = {
        "id": port_id,
        "flowUuid": item["flow_uuid"],
        "name": item.get("flow_name") or item["flow_uuid"],
        "flow_name_en": item.get("flow_name_en"),
        "unit": item.get("unit") or "unit",
        "unitGroup": item.get("unit_group"),
        "amount": float(item.get("amount") or 0),
        "type": item.get("type") or "technosphere",
        "direction": item["direction"],
        "showOnNode": True,
        "isProduct": bool(item.get("is_product")),
    }
    if link is not None:
        payload["intermediateFlowLink"] = link
    return payload


def _camel_link(link: dict[str, Any]) -> dict[str, Any]:
    return {
        "sourceFlowUuid": link["source_flow_uuid"],
        "targetFlowUuid": link["target_flow_uuid"],
        "amountFactor": link["amount_factor"],
        "sourceUnit": link["source_unit"],
        "targetUnit": link["target_unit"],
        "mappingLevel": link["mapping_level"],
        "mappingReason": link["mapping_reason"],
        "ruleId": link["rule_id"],
        "ruleOrigin": link["rule_origin"],
        "status": link["status"],
        "packageId": link.get("package_id"),
        "packageVersion": link.get("package_version"),
        "packageHash": link.get("package_hash"),
        "applicationMode": link.get("application_mode"),
        "sourceFlowType": link.get("source_flow_type"),
        "targetFlowType": link.get("target_flow_type"),
        "flowSubtypeOverride": bool(link.get("flow_subtype_override")),
        "warnings": link.get("warnings") or [],
    }


def _flatten_numbers(value: Any) -> list[float]:
    if isinstance(value, list):
        numbers: list[float] = []
        for item in value:
            numbers.extend(_flatten_numbers(item))
        return numbers
    if isinstance(value, (int, float)):
        return [float(value)]
    return []


def _select_link(api_base: str, process: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    for source in process.get("inputs") or []:
        if source.get("type") != "technosphere" or float(source.get("amount") or 0) <= 0:
            continue
        resolved = _request(
            api_base,
            "/intermediate-flow-links/resolve-batch",
            method="POST",
            body={
                "items": [{
                    "node_id": "api-smoke-consumer",
                    "port_id": "api-smoke-converted-input",
                    "flow_uuid": source["flow_uuid"],
                    "direction": "input",
                    "exchange_type": "technosphere",
                    "unit": source.get("unit"),
                }],
                "l2_limit": 5,
                "include_unreviewed_candidates": True,
            },
        )
        item = (resolved.get("items") or [{}])[0]
        resolution = item.get("resolution")
        if item.get("status") not in {"L1", "L2", "L3"} or not isinstance(resolution, dict):
            continue
        if item["status"] == "L2":
            resolution = _request(
                api_base,
                "/intermediate-flow-links/confirm-l2",
                method="POST",
                body={"source_flow_uuid": source["flow_uuid"], "rule_id": resolution["rule_id"]},
            )
        elif item["status"] == "L1":
            resolution = {**resolution, "status": "auto"}
        else:
            resolution = {**resolution, "status": "user_confirmed"}
        return source, resolution
    raise RuntimeError("The imported Process has no executable intermediate-flow conversion with a positive amount.")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-base", default="http://127.0.0.1:8001/api")
    parser.add_argument("--remote-process-id", required=True)
    parser.add_argument("--remote-version", required=True)
    parser.add_argument("--project-name")
    args = parser.parse_args()
    api_base = args.api_base.rstrip("/")

    accounts = _request(api_base, "/data-platforms/accounts")
    accounts = [
        row for row in accounts
        if row.get("platform") == "tiangong"
        and row.get("status") == "active"
        and row.get("last_validation_status") == "ok"
    ]
    if len(accounts) != 1:
        raise RuntimeError(f"Expected one validated TianGong account, found {len(accounts)}.")
    account_id = accounts[0]["id"]

    synced = _request(
        api_base,
        f"/data-platforms/accounts/{urllib.parse.quote(account_id)}/processes/sync",
        method="POST",
        body={
            "remote_process_id": args.remote_process_id,
            "remote_version": args.remote_version,
            "overwrite": True,
        },
    )
    if synced.get("status") != "completed":
        raise RuntimeError(f"TIDAS Process sync did not complete: {synced.get('status')}")

    imported = _request(
        api_base,
        "/reference/processes/import",
        method="POST",
        body={
            "process_uuids": [synced["process_uuid"]],
            "import_mode": "editable_clone",
            "target_kind": "unit_process",
            "replace_existing": True,
        },
    )
    process = (imported.get("imported_processes") or [None])[0]
    if not isinstance(process, dict) or process.get("import_mode") != "editable_clone":
        raise RuntimeError("Editable Process import did not return an editable clone.")

    converted_source, confirmed_link = _select_link(api_base, process)
    target_flow_uuid = confirmed_link["target_flow_uuid"]
    providers = _request(
        api_base,
        "/intermediate-flow-links/providers?"
        + urllib.parse.urlencode({"target_flow_uuid": target_flow_uuid}),
    )
    candidates = [row for row in providers.get("providers") or [] if row.get("has_lci_vector")]
    if not candidates:
        raise RuntimeError("No exact ecoinvent provider with an imported LCI vector was found.")
    provider = next((row for row in candidates if row.get("location") == "CN"), candidates[0])

    provider_import = _request(
        api_base,
        "/reference/processes/import",
        method="POST",
        body={
            "process_uuids": [provider["process_uuid"]],
            "import_mode": "locked",
            "target_kind": "lci_dataset",
            "replace_existing": True,
        },
    )
    provider_process = (provider_import.get("imported_processes") or [None])[0]
    if not isinstance(provider_process, dict):
        raise RuntimeError("The selected ecoinvent provider could not be imported as an LCI dataset.")
    provider_output = next(
        (row for row in provider_process.get("outputs") or [] if row.get("flow_uuid") == target_flow_uuid),
        None,
    )
    if provider_output is None:
        raise RuntimeError("The imported provider does not expose the exact converted target Flow.")

    link_payload = _camel_link(confirmed_link)
    input_ports = []
    converted_port_id = "input-converted"
    for index, row in enumerate(process.get("inputs") or []):
        is_converted = row.get("flow_uuid") == converted_source.get("flow_uuid")
        input_ports.append(_port(row, converted_port_id if is_converted else f"input-{index}", link=link_payload if is_converted else None))
    output_ports = [_port(row, f"output-{index}") for index, row in enumerate(process.get("outputs") or [])]
    reference_output = next((row for row in output_ports if row.get("isProduct")), output_ports[0])
    provider_port = _port(provider_output, "provider-output")
    provider_port["amount"] = float(provider_output.get("amount") or 1)

    source_amount = float(converted_source.get("amount") or 0)
    target_amount = source_amount * float(confirmed_link["amount_factor"])
    graph = {
        "functionalUnit": f"1 {reference_output['unit']}",
        "nodes": [
            {
                "id": "api-smoke-provider",
                "hidden": True,
                "node_kind": "lci_dataset",
                "mode": "normalized",
                "lci_role": "provider",
                "process_uuid": provider_process["process_uuid"],
                "name": provider_process["process_name"],
                "location": provider_process.get("location") or "GLO",
                "reference_product": provider_output.get("flow_name") or target_flow_uuid,
                "reference_product_flow_uuid": target_flow_uuid,
                "reference_product_direction": "output",
                "inputs": [],
                "outputs": [provider_port],
            },
            {
                "id": "api-smoke-consumer",
                "hidden": False,
                "node_kind": "unit_process",
                "mode": "balanced",
                "process_uuid": process["process_uuid"],
                "name": process["process_name"],
                "location": process.get("location") or "CN",
                "reference_product": reference_output["name"],
                "reference_product_flow_uuid": reference_output["flowUuid"],
                "reference_product_direction": "output",
                "inputs": input_ports,
                "outputs": output_ports,
            },
        ],
        "exchanges": [{
            "id": "api-smoke-provider-link",
            "fromNode": "api-smoke-provider",
            "toNode": "api-smoke-consumer",
            "sourceHandle": "out:provider-output",
            "targetHandle": f"in:{converted_port_id}",
            "sourcePortId": "provider-output",
            "targetPortId": converted_port_id,
            "flowUuid": target_flow_uuid,
            "consumerFlowUuid": converted_source["flow_uuid"],
            "flowName": provider_output.get("flow_name") or target_flow_uuid,
            "flow_name_en": provider_output.get("flow_name_en"),
            "quantityMode": "dual",
            "amount": target_amount,
            "providerAmount": float(provider_port["amount"]),
            "consumerAmount": target_amount,
            "unit": provider_port["unit"],
            "providerUnit": provider_port["unit"],
            "consumerUnit": converted_source.get("unit"),
            "type": "technosphere",
            "allocation": "none",
            "intermediateFlowLinkRuleId": confirmed_link["rule_id"],
            "intermediateFlowLinkFactor": confirmed_link["amount_factor"],
        }],
        "metadata": {
            "source": "api_acceptance_smoke",
            "project_preferences": {
                "target_product": {
                    "process_uuid": process["process_uuid"],
                    "flow_uuid": reference_output["flowUuid"],
                    "quantity_mode": "custom",
                    "quantity": 1,
                }
            },
        },
    }

    project_name = args.project_name or (
        "API TIDAS ecoinvent smoke "
        + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    )
    project = _request(
        api_base,
        "/projects",
        method="POST",
        body={
            "name": project_name,
            "functional_unit": graph["functionalUnit"],
            "source_policy": "explicit_mapped_mixed",
            "allowed_lcia_scope": "mapped_runtime",
        },
    )
    project_id = project["project_id"]
    version = _request(
        api_base,
        f"/projects/{urllib.parse.quote(project_id)}/versions?compile_pts_on_save=false",
        method="POST",
        body={"graph": graph},
    )
    saved = _request(api_base, f"/projects/{urllib.parse.quote(project_id)}/latest")
    saved_graph = saved.get("graph") or {}
    saved_links = [
        edge for edge in saved_graph.get("exchanges") or []
        if (
            edge.get("intermediateFlowLinkRuleId")
            or edge.get("intermediate_flow_link_rule_id")
        ) == confirmed_link["rule_id"]
    ]
    if len(saved_links) != 1:
        raise RuntimeError("Saved model readback did not preserve the provider association edge.")

    result = _request(
        api_base,
        "/model/run",
        method="POST",
        body={
            "graph": saved_graph,
            "model_version_id": f"{project_id}:{version['version']}",
            "project_id": project_id,
            "lcia_methods": ["EF v3.1"],
        },
        timeout=300,
        retries=0,
    )
    lci_result = result.get("lci_result") or {}
    values = lci_result.get("values") or []
    numeric_values = _flatten_numbers(values)
    nonzero_value_count = sum(1 for value in numeric_values if abs(value) > 0)
    if result.get("status") != "completed" or not numeric_values or nonzero_value_count == 0:
        raise RuntimeError("LCIA run completed without a non-zero result vector.")

    print(json.dumps({
        "ok": True,
        "project_id": project_id,
        "project_name": project_name,
        "version": version["version"],
        "run_id": result["run_id"],
        "source_process_uuid": synced["process_uuid"],
        "editable_process_uuid": process["process_uuid"],
        "converted_source_flow_uuid": converted_source["flow_uuid"],
        "converted_target_flow_uuid": target_flow_uuid,
        "mapping_level": confirmed_link["mapping_level"],
        "mapping_status": confirmed_link["status"],
        "amount_factor": confirmed_link["amount_factor"],
        "provider_process_uuid": provider["process_uuid"],
        "provider_location": provider.get("location"),
        "provider_vector_nnz": provider.get("vector_nnz"),
        "saved_provider_link_count": len(saved_links),
        "result_indicator_count": len(lci_result.get("indicator_index") or []),
        "result_value_count": len(numeric_values),
        "nonzero_value_count": nonzero_value_count,
        "missing_ef31_flow_count": len(lci_result.get("missing_ef31_flow_uuids") or []),
        "summary": result.get("summary") or {},
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
