"""HiQLCD API client for on-demand background LCI imports."""

from __future__ import annotations

import json
import uuid
from typing import Any
from urllib import error as url_error
from urllib import request as url_request

from .data_platform_connectors import (
    BaseDataPlatformConnector,
    ConnectorError,
    CredentialError,
    RemoteFlowDTO,
    RemotePageDTO,
    RemoteProcessDTO,
    RemoteProcessDetailDTO,
)


HIQLCD_API_BASE_URL = "https://x.hiqlcd.com"
_HIQLCD_NAMESPACE = uuid.UUID("5a5026a8-9c67-4f2c-b51b-8a6f0de17c74")


def _stable_uuid(kind: str, remote_id: str) -> str:
    return str(uuid.uuid5(_HIQLCD_NAMESPACE, f"{kind}:{remote_id}"))


def _as_object(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_text(value: Any) -> str:
    return str(value or "").strip()


class HiqlcdConnector(BaseDataPlatformConnector):
    """Adapter for HiQLCD's API-Key-protected dataset and LCI endpoints."""

    REQUEST_TIMEOUT_SECONDS = 30

    def _base_url(self) -> str:
        return _as_text(self.account.base_url).rstrip("/") or HIQLCD_API_BASE_URL

    def _api_key(self) -> str:
        api_key = _as_text(self.account.credential.get("api_key"))
        if not api_key:
            raise CredentialError("HiQLCD API Key is required.")
        return api_key

    def _post_json(self, path: str, payload: dict[str, Any]) -> Any:
        request = url_request.Request(
            f"{self._base_url()}{path}",
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            method="POST",
            headers={
                "Content-Type": "application/json",
                "X-API-Key": self._api_key(),
            },
        )
        try:
            with url_request.urlopen(request, timeout=self.REQUEST_TIMEOUT_SECONDS) as response:  # noqa: S310 - official fixed API endpoint
                raw = response.read().decode("utf-8")
        except url_error.HTTPError as exc:
            retry_after = None
            try:
                retry_after = int(exc.headers.get("Retry-After"))
            except (AttributeError, TypeError, ValueError):
                pass
            if exc.code in {401, 403}:
                message = "HiQLCD API Key was rejected."
            elif exc.code == 429:
                message = "HiQLCD request rate limit reached."
            else:
                message = f"HiQLCD request failed with HTTP {exc.code}."
            raise ConnectorError(message, status_code=exc.code, retry_after=retry_after) from exc
        except url_error.URLError as exc:
            raise ConnectorError(f"HiQLCD request failed: {_as_text(getattr(exc, 'reason', 'network error'))}") from exc
        try:
            body = json.loads(raw) if raw else {}
        except json.JSONDecodeError as exc:
            raise ConnectorError("HiQLCD returned invalid JSON.") from exc
        if not isinstance(body, dict):
            raise ConnectorError("HiQLCD response must be an object.")
        if body.get("success") is False:
            raise ConnectorError("HiQLCD rejected the request.")
        return body.get("data")

    def _dataset_rows(self, query: str, *, page: int, page_size: int, locale: str = "zh") -> tuple[list[dict[str, Any]], int]:
        data = self._post_json(
            "/xapi/datasets",
            {"query": query, "page": page, "pageSize": page_size, "locale": locale},
        )
        payload = _as_object(data)
        rows = [row for row in payload.get("items", []) if isinstance(row, dict)]
        total = int(payload.get("total") or len(rows))
        return rows, total

    @staticmethod
    def _process_from_dataset(dataset: dict[str, Any]) -> RemoteProcessDTO:
        remote_id = _as_text(dataset.get("id"))
        remote_uuid = _as_text(dataset.get("uuid")) or remote_id
        source = _as_object(dataset.get("source"))
        location = _as_object(dataset.get("location"))
        category = _as_object(dataset.get("category"))
        metadata = {
            "dataset_uuid": remote_uuid,
            "description": _as_text(dataset.get("description")),
            "location": _as_text(location.get("code") or location.get("name")),
            "source_name": _as_text(source.get("name")),
            "source_version": _as_text(source.get("version")),
            "category": _as_text(category.get("name")),
            "updated_at": _as_text(dataset.get("updatedAt")),
        }
        return RemoteProcessDTO(
            remote_id=remote_id,
            process_uuid=_stable_uuid("dataset", remote_uuid),
            process_name=_as_text(dataset.get("name")) or remote_id,
            process_type="lci_result",
            source="hiqlcd",
            remote_version=_as_text(dataset.get("version")) or None,
            metadata=metadata,
        )

    def test_connection(self) -> tuple[bool, str]:
        self._dataset_rows("a", page=1, page_size=1)
        return True, "HiQLCD API Key accepted."

    def search_lci_datasets(self, query: str, *, page: int = 1, page_size: int = 20, locale: str = "zh") -> RemotePageDTO:
        rows, total = self._dataset_rows(query, page=page, page_size=page_size, locale=locale)
        items = [self._process_from_dataset(row) for row in rows if _as_text(row.get("id"))]
        return RemotePageDTO(items=items, total=total, page=page, page_size=page_size, has_more=(page * page_size) < total)

    def get_lci_detail(self, remote_id: str, remote_version: str | None = None, *, locale: str = "zh") -> RemoteProcessDetailDTO:
        rows, _ = self._dataset_rows(remote_id, page=1, page_size=20, locale=locale)
        dataset = next((row for row in rows if _as_text(row.get("id")) == remote_id), None)
        if dataset is None:
            raise ConnectorError("HiQLCD dataset was not found by its dataset ID.", status_code=404)
        process = self._process_from_dataset(dataset)
        version = remote_version or process.remote_version
        request = {"datasetId": remote_id, "locale": locale}
        if version:
            request["version"] = version
        inputs = self._post_json("/xapi/lci/input", request)
        outputs = self._post_json("/xapi/lci/output", request)
        input_rows = [row for row in inputs if isinstance(row, dict)] if isinstance(inputs, list) else []
        output_rows = [row for row in outputs if isinstance(row, dict)] if isinstance(outputs, list) else []
        if len(output_rows) != 1:
            raise ConnectorError("HiQLCD LCI must contain exactly one reference product output.")
        reference = output_rows[0]
        reference_id = _as_text(reference.get("id"))
        reference_name = _as_text(reference.get("name"))
        reference_unit = _as_text(reference.get("unit"))
        if not reference_id or not reference_name or not reference_unit:
            raise ConnectorError("HiQLCD reference product is missing id, name, or unit.")
        reference_uuid = _stable_uuid("product-flow", reference_id)
        flows = [
            RemoteFlowDTO(
                remote_id=reference_id,
                flow_uuid=reference_uuid,
                flow_name=reference_name,
                flow_type="Product flow",
                default_unit=reference_unit,
                unit_group="HiQLCD",
                source="hiqlcd",
                remote_version=version,
                metadata={"category": _as_text(reference.get("category")), "dataset_id": remote_id},
            )
        ]
        vector_items: list[dict[str, Any]] = []
        exchanges = [{
            "flow_uuid": reference_uuid,
            "flow_name": reference_name,
            "flow_type": "Product flow",
            "direction": "output",
            "amount": reference.get("amount") if reference.get("amount") is not None else 1,
            "unit": reference_unit,
            "is_reference_flow": True,
            "isProduct": True,
        }]
        for item in input_rows:
            item_id = _as_text(item.get("id"))
            name = _as_text(item.get("name"))
            unit = _as_text(item.get("unit"))
            if not item_id or not name or not unit or item.get("amount") is None:
                raise ConnectorError("HiQLCD basic flow is missing id, name, amount, or unit.")
            flow_uuid = _stable_uuid("elementary-flow", item_id)
            category = _as_text(item.get("category"))
            flows.append(RemoteFlowDTO(
                remote_id=item_id,
                flow_uuid=flow_uuid,
                flow_name=name,
                flow_type="Elementary flow",
                default_unit=unit,
                unit_group="HiQLCD",
                source="hiqlcd",
                remote_version=version,
                metadata={"category": category, "dataset_id": remote_id},
            ))
            exchanges.append({
                "flow_uuid": flow_uuid,
                "flow_name": name,
                "flow_type": "Elementary flow",
                "direction": "input",
                "amount": item.get("amount"),
                "unit": unit,
                "category": category,
            })
            vector_items.append({
                "flow_uuid": flow_uuid,
                "direction": "input",
                "unit": unit,
                "amount": item.get("amount"),
                "compartment": category or None,
            })
        if not vector_items:
            raise ConnectorError("HiQLCD LCI has no basic flows to import.")
        process = RemoteProcessDTO(
            **{**process.__dict__, "reference_flow_uuid": reference_uuid, "remote_version": version},
        )
        process_json = {
            "process_uuid": process.process_uuid,
            "process_name": process.process_name,
            "process_type": "lci_dataset",
            "reference_flow_uuid": reference_uuid,
            "reference_product_id": reference_uuid,
            "reference_product": reference_name,
            "reference_product_unit": reference_unit,
            "reference_product_amount": reference.get("amount") if reference.get("amount") is not None else 1,
            "location": process.metadata.get("location"),
            "source": "hiqlcd",
            "source_dataset_id": remote_id,
            "source_dataset_uuid": process.metadata.get("dataset_uuid"),
            "source_dataset_version": version,
            "exchanges": exchanges,
        }
        return RemoteProcessDetailDTO(
            process=process,
            flows=flows,
            process_json=process_json,
            vector={"items": vector_items, "remote_version": version},
            import_report={"source": "hiqlcd", "dataset_id": remote_id, "reference_product": reference_name},
        )
