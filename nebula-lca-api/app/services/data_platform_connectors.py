"""Connector abstractions for external LCA data platforms."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib import error as url_error
from urllib import parse as url_parse
from urllib import request as url_request

from ..config import settings


class CredentialError(ValueError):
    pass


class ConnectorError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _credential_key() -> bytes:
    key = settings.data_platform_credential_key or settings.admin_token or ""
    if key:
        return key.encode("utf-8")

    key_file = Path(settings.data_platform_credential_key_file)
    if key_file.exists():
        stored = key_file.read_text(encoding="utf-8").strip()
        if stored:
            return stored.encode("utf-8")

    is_local_sqlite = str(settings.database_url).startswith("sqlite:///")
    if settings.debug or is_local_sqlite:
        key_file.parent.mkdir(parents=True, exist_ok=True)
        stored = secrets.token_urlsafe(48)
        key_file.write_text(stored, encoding="utf-8")
        try:
            os.chmod(key_file, 0o600)
        except OSError:
            pass
        return stored.encode("utf-8")

    if not key:
        raise CredentialError("DATA_PLATFORM_CREDENTIAL_KEY is required to store external platform credentials.")
    return key.encode("utf-8")


def _keystream(key: bytes, nonce: bytes, size: int) -> bytes:
    chunks: list[bytes] = []
    counter = 0
    while sum(len(chunk) for chunk in chunks) < size:
        chunks.append(hmac.new(key, nonce + counter.to_bytes(4, "big"), hashlib.sha256).digest())
        counter += 1
    return b"".join(chunks)[:size]


def encrypt_credential(payload: dict[str, Any] | None) -> str | None:
    if not payload:
        return None
    raw = json.dumps(payload, ensure_ascii=False, sort_keys=True).encode("utf-8")
    key = _credential_key()
    nonce = os.urandom(16)
    stream = _keystream(key, nonce, len(raw))
    cipher = bytes(a ^ b for a, b in zip(raw, stream))
    tag = hmac.new(key, nonce + cipher, hashlib.sha256).digest()
    return "v1." + base64.urlsafe_b64encode(nonce + tag + cipher).decode("ascii")


def decrypt_credential(ciphertext: str | None) -> dict[str, Any]:
    if not ciphertext:
        return {}
    if not ciphertext.startswith("v1."):
        raise CredentialError("Unsupported credential ciphertext version.")
    key = _credential_key()
    try:
        blob = base64.urlsafe_b64decode(ciphertext[3:].encode("ascii"))
    except Exception as exc:  # noqa: BLE001
        raise CredentialError("Credential ciphertext is not valid base64.") from exc
    if len(blob) < 48:
        raise CredentialError("Credential ciphertext is too short.")
    nonce, tag, cipher = blob[:16], blob[16:48], blob[48:]
    expected = hmac.new(key, nonce + cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(tag, expected):
        raise CredentialError("Credential ciphertext failed integrity check.")
    stream = _keystream(key, nonce, len(cipher))
    raw = bytes(a ^ b for a, b in zip(cipher, stream))
    try:
        decoded = json.loads(raw.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise CredentialError("Credential payload could not be decoded.") from exc
    return decoded if isinstance(decoded, dict) else {}


@dataclass(frozen=True)
class PlatformAccountContext:
    account_id: str
    platform: str
    alias: str
    base_url: str | None
    auth_type: str
    credential: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    session_ciphertext: str | None = None
    session_expires_at: datetime | None = None
    save_session: Callable[[str, datetime | None], None] | None = None


@dataclass(frozen=True)
class RemoteFlowDTO:
    remote_id: str
    flow_uuid: str
    flow_name: str
    flow_name_en: str | None = None
    flow_type: str = "Product flow"
    default_unit: str = "kg"
    unit_group: str = "Units of mass"
    source: str | None = None
    remote_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RemotePageDTO:
    items: list[Any]
    total: int
    page: int = 1
    page_size: int = 20
    has_more: bool = False


@dataclass(frozen=True)
class RemoteProcessDTO:
    remote_id: str
    process_uuid: str
    process_name: str
    process_type: str = "unit_process"
    reference_flow_uuid: str | None = None
    source: str | None = None
    remote_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RemoteModelDTO:
    remote_id: str
    model_uuid: str
    model_name: str
    source: str | None = None
    remote_version: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RemoteProcessDetailDTO:
    process: RemoteProcessDTO
    flows: list[RemoteFlowDTO] = field(default_factory=list)
    process_json: dict[str, Any] = field(default_factory=dict)
    import_report: dict[str, Any] = field(default_factory=dict)
    vector: dict[str, Any] | None = None


@dataclass(frozen=True)
class RemoteModelDetailDTO:
    model: RemoteModelDTO
    model_json: dict[str, Any] = field(default_factory=dict)
    lineage: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class RemoteUnitDefinitionDTO:
    unit_name: str
    factor_to_reference: float
    is_reference: bool = False


@dataclass(frozen=True)
class RemoteUnitGroupDTO:
    name: str
    reference_unit: str | None = None
    source_uuid: str | None = None
    source_version: str | None = None
    source_package_version: str | None = None
    definitions: list[RemoteUnitDefinitionDTO] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


class BaseDataPlatformConnector:
    def __init__(self, account: PlatformAccountContext):
        self.account = account

    def test_connection(self) -> tuple[bool, str]:
        raise NotImplementedError

    def search_flows(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, flow_type: str | None = None) -> RemotePageDTO:
        raise NotImplementedError

    def search_processes(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, process_type: str | None = None) -> RemotePageDTO:
        raise NotImplementedError

    def search_models(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100) -> RemotePageDTO:
        raise NotImplementedError

    def get_flow_detail(self, remote_flow_id: str, remote_version: str | None = None) -> RemoteFlowDTO:
        raise NotImplementedError

    def get_process_detail(self, remote_process_id: str, remote_version: str | None = None) -> RemoteProcessDetailDTO:
        raise NotImplementedError

    def get_model_detail(self, remote_model_id: str, remote_version: str | None = None) -> RemoteModelDetailDTO:
        raise NotImplementedError

    def get_flow_dependency_unit_groups(self, flow: RemoteFlowDTO) -> list[RemoteUnitGroupDTO]:
        return []


class MockDataPlatformConnector(BaseDataPlatformConnector):
    def test_connection(self) -> tuple[bool, str]:
        if self.account.metadata.get("fail_connection"):
            return False, "mock connection failed by account metadata"
        return True, "mock connection ok"

    def search_flows(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, flow_type: str | None = None) -> RemotePageDTO:
        if self.account.metadata.get("raise_search_error"):
            raise ConnectorError("mock search failed by account metadata")
        token = (query or "flow").strip() or "flow"
        items = [
            RemoteFlowDTO(
                remote_id=f"mock-flow-{idx}",
                flow_uuid=f"mock-flow-{idx}",
                flow_name=f"{token} flow {idx}",
                flow_type="Product flow" if idx % 2 else "Elementary flow",
                default_unit="kg",
                unit_group="Units of mass",
                source="mock",
                remote_version="v1",
            )
            for idx in range(1, min(page_size, 5) + 1)
        ]
        return RemotePageDTO(items=items, total=len(items), page=page, page_size=page_size, has_more=False)

    def search_processes(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, process_type: str | None = None) -> RemotePageDTO:
        token = (query or "process").strip() or "process"
        items = [
            RemoteProcessDTO(
                remote_id=f"mock-process-{idx}",
                process_uuid=f"mock-process-{idx}",
                process_name=f"{token} process {idx}",
                process_type="unit_process",
                reference_flow_uuid=f"mock-process-{idx}-product",
                source="mock",
                remote_version="v1",
            )
            for idx in range(1, min(page_size, 5) + 1)
        ]
        return RemotePageDTO(items=items, total=len(items), page=page, page_size=page_size, has_more=False)

    def search_models(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100) -> RemotePageDTO:
        token = (query or "model").strip() or "model"
        items = [
            RemoteModelDTO(
                remote_id=f"mock-model-{idx}",
                model_uuid=f"mock-model-{idx}",
                model_name=f"{token} model {idx}",
                source="mock",
                remote_version="v1",
            )
            for idx in range(1, min(page_size, 5) + 1)
        ]
        return RemotePageDTO(items=items, total=len(items), page=page, page_size=page_size, has_more=False)

    def get_flow_detail(self, remote_flow_id: str, remote_version: str | None = None) -> RemoteFlowDTO:
        remote_id = remote_flow_id.strip()
        if not remote_id:
            raise ConnectorError("remote_flow_id is required")
        return RemoteFlowDTO(remote_id=remote_id, flow_uuid=remote_id, flow_name=f"Mock flow {remote_id}", source="mock", remote_version=remote_version or "v1")

    def get_process_detail(self, remote_process_id: str, remote_version: str | None = None) -> RemoteProcessDetailDTO:
        remote_id = remote_process_id.strip()
        if not remote_id:
            raise ConnectorError("remote_process_id is required")
        product_uuid = f"{remote_id}-product"
        input_uuid = f"{remote_id}-input"
        process = RemoteProcessDTO(
            remote_id=remote_id,
            process_uuid=remote_id,
            process_name=f"Mock process {remote_id}",
            process_type="unit_process",
            reference_flow_uuid=product_uuid,
            source="mock",
            remote_version=remote_version or "v1",
        )
        flows = [
            RemoteFlowDTO(remote_id=product_uuid, flow_uuid=product_uuid, flow_name="Mock product", source="mock", remote_version=remote_version or "v1"),
            RemoteFlowDTO(remote_id=input_uuid, flow_uuid=input_uuid, flow_name="Mock input", source="mock", remote_version=remote_version or "v1"),
        ]
        process_json = {
            "process_uuid": remote_id,
            "process_name": process.process_name,
            "reference_flow_uuid": product_uuid,
            "reference_product": "Mock product",
            "reference_product_unit": "kg",
            "reference_product_amount": 1,
            "exchanges": [
                {"flow_uuid": input_uuid, "flow_name": "Mock input", "direction": "input", "amount": 2.0, "unit": "kg", "flow_type": "Product flow"},
                {"flow_uuid": product_uuid, "flow_name": "Mock product", "direction": "output", "amount": 1.0, "unit": "kg", "flow_type": "Product flow", "isProduct": True},
            ],
        }
        return RemoteProcessDetailDTO(process=process, flows=flows, process_json=process_json, import_report={"source": "mock", "synced_at": datetime.utcnow().isoformat()})

    def get_model_detail(self, remote_model_id: str, remote_version: str | None = None) -> RemoteModelDetailDTO:
        remote_id = remote_model_id.strip()
        if not remote_id:
            raise ConnectorError("remote_model_id is required")
        model = RemoteModelDTO(remote_id=remote_id, model_uuid=remote_id, model_name=f"Mock model {remote_id}", source="mock", remote_version=remote_version or "v1")
        graph = {
            "functionalUnit": "1 kg mock product",
            "nodes": [],
            "exchanges": [],
            "metadata": {"source_platform": "mock", "remote_model_id": remote_id},
        }
        return RemoteModelDetailDTO(model=model, model_json={"hybrid_graph": graph}, lineage={"source": "mock", "remote_model_id": remote_id})


class CustomHttpDataPlatformConnector(BaseDataPlatformConnector):
    def _json_get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        if not self.account.base_url:
            raise ConnectorError("base_url is required for custom platform connector")
        url = self.account.base_url.rstrip("/") + "/" + path.lstrip("/")
        if params:
            url = url + "?" + url_parse.urlencode(params)
        req = url_request.Request(url, headers=self._headers())
        try:
            with url_request.urlopen(req, timeout=10) as resp:  # noqa: S310 - user-configured connector endpoint
                return json.loads(resp.read().decode("utf-8"))
        except url_error.URLError as exc:
            raise ConnectorError(f"custom connector request failed: {exc}") from exc

    def _headers(self) -> dict[str, str]:
        credential = self.account.credential
        if self.account.auth_type == "bearer" and credential.get("token"):
            return {"Authorization": f"Bearer {credential['token']}"}
        if self.account.auth_type == "api_key" and credential.get("api_key"):
            return {"X-API-Key": str(credential["api_key"])}
        return {}

    def test_connection(self) -> tuple[bool, str]:
        payload = self._json_get("/health")
        return True, str(payload.get("message") if isinstance(payload, dict) else "custom connector ok")

    def search_flows(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, flow_type: str | None = None) -> RemotePageDTO:
        payload = self._json_get("/flows", {"q": query, "page": page, "page_size": page_size, "data_source": data_source, "state_code": state_code, "flow_type": flow_type})
        rows = payload.get("items", payload) if isinstance(payload, dict) else payload
        items = [_flow_from_mapping(row, self.account.platform) for row in rows if isinstance(row, dict)]
        total = int(payload.get("total") or len(items)) if isinstance(payload, dict) else len(items)
        return RemotePageDTO(items=items, total=total, page=page, page_size=page_size, has_more=(page * page_size) < total)

    def search_processes(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, process_type: str | None = None) -> RemotePageDTO:
        payload = self._json_get("/processes", {"q": query, "page": page, "page_size": page_size, "data_source": data_source, "state_code": state_code, "process_type": process_type})
        rows = payload.get("items", payload) if isinstance(payload, dict) else payload
        items = [_process_from_mapping(row, self.account.platform) for row in rows if isinstance(row, dict)]
        total = int(payload.get("total") or len(items)) if isinstance(payload, dict) else len(items)
        return RemotePageDTO(items=items, total=total, page=page, page_size=page_size, has_more=(page * page_size) < total)

    def search_models(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100) -> RemotePageDTO:
        payload = self._json_get("/models", {"q": query, "page": page, "page_size": page_size, "data_source": data_source, "state_code": state_code})
        rows = payload.get("items", payload) if isinstance(payload, dict) else payload
        items = [_model_from_mapping(row, self.account.platform) for row in rows if isinstance(row, dict)]
        total = int(payload.get("total") or len(items)) if isinstance(payload, dict) else len(items)
        return RemotePageDTO(items=items, total=total, page=page, page_size=page_size, has_more=(page * page_size) < total)

    def get_flow_detail(self, remote_flow_id: str, remote_version: str | None = None) -> RemoteFlowDTO:
        payload = self._json_get(f"/flows/{url_parse.quote(remote_flow_id, safe='')}", {"version": remote_version} if remote_version else None)
        if not isinstance(payload, dict):
            raise ConnectorError("custom flow detail response must be an object")
        return _flow_from_mapping(payload.get("flow") if isinstance(payload.get("flow"), dict) else payload, self.account.platform)

    def get_process_detail(self, remote_process_id: str, remote_version: str | None = None) -> RemoteProcessDetailDTO:
        payload = self._json_get(f"/processes/{url_parse.quote(remote_process_id, safe='')}")
        if not isinstance(payload, dict):
            raise ConnectorError("custom process detail response must be an object")
        process = _process_from_mapping(payload.get("process") if isinstance(payload.get("process"), dict) else payload, self.account.platform)
        flows_raw = payload.get("flows") if isinstance(payload.get("flows"), list) else []
        flows = [_flow_from_mapping(row, self.account.platform) for row in flows_raw if isinstance(row, dict)]
        process_json = payload.get("process_json") if isinstance(payload.get("process_json"), dict) else {}
        return RemoteProcessDetailDTO(
            process=process,
            flows=flows,
            process_json=process_json,
            import_report=payload.get("import_report") if isinstance(payload.get("import_report"), dict) else {},
            vector=payload.get("vector") if isinstance(payload.get("vector"), dict) else None,
        )

    def get_model_detail(self, remote_model_id: str, remote_version: str | None = None) -> RemoteModelDetailDTO:
        payload = self._json_get(f"/models/{url_parse.quote(remote_model_id, safe='')}", {"version": remote_version} if remote_version else None)
        if not isinstance(payload, dict):
            raise ConnectorError("custom model detail response must be an object")
        model = _model_from_mapping(payload.get("model") if isinstance(payload.get("model"), dict) else payload, self.account.platform)
        model_json = payload.get("model_json") if isinstance(payload.get("model_json"), dict) else payload
        return RemoteModelDetailDTO(model=model, model_json=model_json, lineage=payload.get("lineage") if isinstance(payload.get("lineage"), dict) else {})


class TianGongSupabaseConnector(BaseDataPlatformConnector):
    """On-demand TianGong connector backed by Supabase Auth, RPC, and tables."""

    SESSION_REFRESH_MARGIN_SECONDS = 60
    REQUEST_TIMEOUT_SECONDS = 90

    def __init__(self, account: PlatformAccountContext):
        super().__init__(account)
        self._runtime_access_token: str | None = None

    def _publishable_key(self) -> str:
        key = str(self.account.metadata.get("publishable_key") or "").strip()
        if not key:
            raise CredentialError("TianGong Supabase publishable key is required.")
        return key

    def _base_url(self) -> str:
        base_url = str(self.account.base_url or "").strip().rstrip("/")
        if not base_url:
            raise CredentialError("TianGong Supabase URL is required.")
        return base_url

    def _decode_api_key(self) -> dict[str, str]:
        raw_key = str(self.account.credential.get("api_key") or "").strip()
        if not raw_key:
            raise CredentialError("TianGong API Key is required.")
        try:
            padded = raw_key + ("=" * (-len(raw_key) % 4))
            payload = json.loads(base64.b64decode(padded.encode("utf-8")).decode("utf-8"))
        except Exception as exc:  # noqa: BLE001
            raise CredentialError("TianGong API Key must be Base64 JSON with email and password.") from exc
        if not isinstance(payload, dict):
            raise CredentialError("TianGong API Key payload must be a JSON object.")
        email = str(payload.get("email") or "").strip()
        password = str(payload.get("password") or "")
        if not email or not password:
            raise CredentialError("TianGong API Key payload must include email and password.")
        return {"email": email, "password": password}

    def _password_credentials(self) -> dict[str, str]:
        if self.account.auth_type == "basic":
            email = str(self.account.credential.get("username") or "").strip()
            password = str(self.account.credential.get("password") or "")
            if not email or not password:
                raise CredentialError("TianGong account login requires email and password.")
            return {"email": email, "password": password}
        return self._decode_api_key()

    def _request_json(self, method: str, path: str, *, headers: dict[str, str] | None = None, body: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url()}{path}"
        data = json.dumps(body or {}, ensure_ascii=False).encode("utf-8") if body is not None else None
        req = url_request.Request(url, data=data, headers=headers or {}, method=method)
        try:
            with url_request.urlopen(req, timeout=self.REQUEST_TIMEOUT_SECONDS) as resp:  # noqa: S310 - account-configured Supabase endpoint
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else None
        except url_error.HTTPError as exc:
            raise ConnectorError(f"TianGong Supabase request failed with HTTP {exc.code}.", status_code=exc.code) from exc
        except url_error.URLError as exc:
            reason = str(getattr(exc, "reason", "") or "").strip()
            message = f"TianGong Supabase request failed: {reason}" if reason else "TianGong Supabase request failed."
            raise ConnectorError(message) from exc

    def _session_payload(self) -> dict[str, Any]:
        if not self.account.session_ciphertext:
            return {}
        try:
            payload = decrypt_credential(self.account.session_ciphertext)
        except CredentialError:
            return {}
        return payload if isinstance(payload, dict) else {}

    def _is_session_usable(self) -> bool:
        expires_at = self.account.session_expires_at
        if expires_at is None:
            return True
        return expires_at > datetime.utcnow() + timedelta(seconds=self.SESSION_REFRESH_MARGIN_SECONDS)

    def _persist_session_payload(self, payload: dict[str, Any]) -> str:
        token = str(payload.get("access_token") or "").strip()
        if not token:
            raise ConnectorError("TianGong Supabase Auth did not return an access token.")
        self._runtime_access_token = token
        expires_at = _session_expires_at(payload)
        session_payload = {
            "access_token": token,
            "refresh_token": str(payload.get("refresh_token") or "").strip() or None,
            "token_type": str(payload.get("token_type") or "bearer").strip() or "bearer",
        }
        ciphertext = encrypt_credential(session_payload)
        if ciphertext and self.account.save_session:
            self.account.save_session(ciphertext, expires_at)
        return token

    def _password_grant_token(self) -> str:
        auth = self._password_credentials()
        payload = self._request_json(
            "POST",
            "/auth/v1/token?grant_type=password",
            headers={"apikey": self._publishable_key(), "Content-Type": "application/json"},
            body=auth,
        )
        return self._persist_session_payload(payload if isinstance(payload, dict) else {})

    def _refresh_grant_token(self, refresh_token: str) -> str:
        payload = self._request_json(
            "POST",
            "/auth/v1/token?grant_type=refresh_token",
            headers={"apikey": self._publishable_key(), "Content-Type": "application/json"},
            body={"refresh_token": refresh_token},
        )
        return self._persist_session_payload(payload if isinstance(payload, dict) else {})

    def _access_token(self) -> str:
        if self._runtime_access_token:
            return self._runtime_access_token
        if self.account.auth_type == "bearer":
            token = str(self.account.credential.get("token") or "").strip()
            if token:
                return token
            raise CredentialError("TianGong connector requires a bearer token.")
        if self.account.auth_type not in {"api_key", "basic"}:
            raise CredentialError("TianGong connector requires account login, api_key auth, or a bearer token.")
        cached = self._session_payload()
        cached_token = str(cached.get("access_token") or "").strip()
        if cached_token and self._is_session_usable():
            return cached_token
        refresh_token = str(cached.get("refresh_token") or "").strip()
        if refresh_token:
            try:
                return self._refresh_grant_token(refresh_token)
            except ConnectorError:
                pass
        return self._password_grant_token()

    def _current_user_id(self) -> str:
        token = self._access_token()
        parts = token.split(".")
        if len(parts) < 2:
            return ""
        try:
            padded = parts[1] + ("=" * (-len(parts[1]) % 4))
            payload = json.loads(base64.urlsafe_b64decode(padded.encode("utf-8")).decode("utf-8"))
        except Exception:  # noqa: BLE001
            return ""
        return str(payload.get("sub") or payload.get("user_id") or "").strip()

    def _headers(self) -> dict[str, str]:
        token = self._access_token()
        key = self._publishable_key()
        return {
            "apikey": key,
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

    def _rpc(self, name: str, payload: dict[str, Any]) -> Any:
        return self._request_json("POST", f"/rest/v1/rpc/{url_parse.quote(name, safe='')}", headers=self._headers(), body=payload)

    def _table_one(self, table: str, remote_id: str, remote_version: str | None = None) -> dict[str, Any]:
        params = {"select": "*", "id": f"eq.{remote_id}"}
        if remote_version:
            params["version"] = f"eq.{remote_version}"
        path = f"/rest/v1/{url_parse.quote(table, safe='')}?{url_parse.urlencode(params)}"
        payload = self._request_json("GET", path, headers=self._headers())
        rows = payload if isinstance(payload, list) else []
        if not rows:
            raise ConnectorError(f"TianGong {table} detail not found.")
        row = rows[0]
        if not isinstance(row, dict):
            raise ConnectorError(f"TianGong {table} detail row must be an object.")
        return row

    def _search_rpc(
        self,
        *,
        kind: str,
        query: str,
        page: int,
        page_size: int,
        data_source: str,
        state_code: int | None,
        flow_type: str | None = None,
        process_type: str | None = None,
    ) -> RemotePageDTO:
        if kind == "flow":
            new_rpc, latest_rpc, search_rpc, mapper = "pgroonga_search_flows_v1", "get_latest_flow_versions", "search_flows_latest", _tiangong_flow_from_row
        elif kind == "process":
            new_rpc, latest_rpc, search_rpc, mapper = "pgroonga_search_processes_v1", "get_latest_process_versions", "search_processes_latest", _tiangong_process_from_row
        else:
            new_rpc, latest_rpc, search_rpc, mapper = "pgroonga_search_lifecyclemodels_v1", "get_latest_lifecyclemodel_versions", "search_lifecyclemodels_latest", _tiangong_model_from_row
        user_id = self._current_user_id()
        filter_condition: dict[str, Any] = {}
        normalized_flow_type = str(flow_type or "").strip()
        normalized_process_type = str(process_type or "").strip()
        if kind == "flow" and normalized_flow_type and normalized_flow_type != "all":
            filter_condition["flowType"] = normalized_flow_type
        legacy_payload: dict[str, Any] = {
            "page_size": page_size,
            "page_current": page,
            "data_source": data_source,
            "this_user_id": user_id,
            "team_id_filter": None,
            "state_code_filter": state_code,
            "sort_by": "modified_at",
            "sort_direction": "desc",
        }
        if kind == "flow":
            legacy_payload["filter_condition"] = filter_condition
        if kind == "process":
            legacy_payload["type_of_data_set_filter"] = normalized_process_type if normalized_process_type and normalized_process_type != "all" else "all"
        if not query.strip():
            raw = self._rpc(latest_rpc, legacy_payload)
            rows, total = _tiangong_rows_and_total(raw)
            items = [mapper(row) for row in rows if isinstance(row, dict)]
            return RemotePageDTO(items=items, total=total if total is not None else len(items), page=page, page_size=page_size, has_more=(page * page_size) < (total if total is not None else len(items)))

        search_payload: dict[str, Any] = {
            "query_text": query.strip(),
            "filter_condition": filter_condition,
            "order_by": {},
            "page_size": page_size,
            "page_current": page,
            "data_source": data_source,
            "this_user_id": user_id,
            "team_id_filter": None,
            "state_code_filter": state_code,
        }
        if kind == "process":
            search_payload["type_of_data_set_filter"] = normalized_process_type if normalized_process_type and normalized_process_type != "all" else "all"
        tried = [search_rpc]
        try:
            raw = self._rpc(search_rpc, search_payload)
        except ConnectorError as search_exc:
            if search_exc.status_code not in {404, 500}:
                raise
            new_payload: dict[str, Any] = {
                "query_text": query.strip(),
                "filter_condition": filter_condition,
                "page_size": page_size,
                "page_current": page,
                "data_source": data_source,
                "order_by": {},
            }
            if state_code is not None:
                new_payload["state_code"] = state_code
            if kind == "process":
                new_payload["type_of_data_set"] = normalized_process_type if normalized_process_type and normalized_process_type != "all" else "all"
            tried.append(new_rpc)
            try:
                raw = self._rpc(new_rpc, new_payload)
            except ConnectorError as new_exc:
                raise ConnectorError(
                    f"TianGong Supabase search failed after trying RPCs: {', '.join(tried)}. Last error: {new_exc}",
                    status_code=new_exc.status_code,
                ) from new_exc
        rows, total = _tiangong_rows_and_total(raw)
        rows = _rerank_tiangong_rows(kind, rows, query)
        items = [mapper(row) for row in rows if isinstance(row, dict)]
        return RemotePageDTO(items=items, total=total if total is not None else len(items), page=page, page_size=page_size, has_more=(page * page_size) < (total if total is not None else len(items)))

    def test_connection(self) -> tuple[bool, str]:
        self._access_token()
        return True, "TianGong Supabase Auth ok."

    def search_flows(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, flow_type: str | None = None) -> RemotePageDTO:
        return self._search_rpc(kind="flow", query=query, page=page, page_size=page_size, data_source=data_source, state_code=state_code, flow_type=flow_type)

    def search_processes(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, process_type: str | None = None) -> RemotePageDTO:
        return self._search_rpc(kind="process", query=query, page=page, page_size=page_size, data_source=data_source, state_code=state_code, process_type=process_type)

    def search_models(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100) -> RemotePageDTO:
        return self._search_rpc(kind="model", query=query, page=page, page_size=page_size, data_source=data_source, state_code=state_code)

    def get_flow_detail(self, remote_flow_id: str, remote_version: str | None = None) -> RemoteFlowDTO:
        flow = _tiangong_flow_from_row(self._table_one("flows", remote_flow_id, remote_version))
        return self._resolve_flow_dependencies(flow)

    def get_process_detail(self, remote_process_id: str, remote_version: str | None = None) -> RemoteProcessDetailDTO:
        row = self._table_one("processes", remote_process_id, remote_version)
        process = _tiangong_process_from_row(row)
        flows = [_tiangong_flow_from_exchange(exchange) for exchange in _extract_process_exchanges(row) if _tiangong_flow_from_exchange(exchange) is not None]
        return RemoteProcessDetailDTO(
            process=process,
            flows=flows,
            process_json=_extract_json_payload(row),
            import_report={"source": "tiangong", "remote_id": process.remote_id, "remote_version": process.remote_version},
            vector=_extract_process_vector(row),
        )

    def get_model_detail(self, remote_model_id: str, remote_version: str | None = None) -> RemoteModelDetailDTO:
        row = self._table_one("lifecyclemodels", remote_model_id, remote_version)
        model = _tiangong_model_from_row(row)
        return RemoteModelDetailDTO(
            model=model,
            model_json=_extract_json_payload(row),
            lineage={"source": "tiangong", "remote_id": model.remote_id, "remote_version": model.remote_version, "row": row},
        )

    def get_flow_dependency_unit_groups(self, flow: RemoteFlowDTO) -> list[RemoteUnitGroupDTO]:
        deps = flow.metadata.get("dependencies") if isinstance(flow.metadata, dict) else None
        unit_group = deps.get("unit_group") if isinstance(deps, dict) else None
        parsed = _unit_group_from_payload(unit_group) if isinstance(unit_group, dict) else None
        return [parsed] if parsed is not None else []

    def _resolve_flow_dependencies(self, flow: RemoteFlowDTO) -> RemoteFlowDTO:
        flow_property_ref = _extract_flow_property_reference(flow.metadata.get("row") if isinstance(flow.metadata, dict) else {})
        if not flow_property_ref:
            return flow
        try:
            flow_property = self._table_one("flowproperties", flow_property_ref["id"], flow_property_ref.get("version"))
            unit_group_ref = _extract_unit_group_reference(flow_property)
            if not unit_group_ref:
                metadata = {**flow.metadata, "dependencies": {"flow_property": flow_property_ref, "flow_property_row": flow_property}}
                return replace(flow, metadata=metadata)
            unit_group_row = self._table_one("unitgroups", unit_group_ref["id"], unit_group_ref.get("version"))
            unit_group = _tiangong_unit_group_from_row(unit_group_row)
            default_unit = unit_group.reference_unit or flow.default_unit
            metadata = {
                **flow.metadata,
                "flow_property_id": flow_property_ref["id"],
                "flow_property_version": flow_property_ref.get("version"),
                "unit_group_id": unit_group_ref["id"],
                "unit_group_version": unit_group_ref.get("version"),
                "dependencies": {
                    "flow_property": flow_property_ref,
                    "flow_property_row": flow_property,
                    "unit_group_ref": unit_group_ref,
                    "unit_group": _unit_group_to_payload(unit_group),
                },
            }
            return replace(
                flow,
                default_unit=default_unit,
                unit_group=unit_group.name or flow.unit_group,
                metadata=metadata,
            )
        except ConnectorError as exc:
            metadata = {**flow.metadata, "dependency_error": str(exc), "flow_property_ref": flow_property_ref}
            return replace(flow, metadata=metadata)


class SkeletonDataPlatformConnector(BaseDataPlatformConnector):
    def test_connection(self) -> tuple[bool, str]:
        return False, f"{self.account.platform} connector skeleton is registered but not configured for live API calls yet"

    def search_flows(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, flow_type: str | None = None) -> RemotePageDTO:
        raise ConnectorError(f"{self.account.platform} live flow search is not implemented yet")

    def search_processes(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100, process_type: str | None = None) -> RemotePageDTO:
        raise ConnectorError(f"{self.account.platform} live process search is not implemented yet")

    def search_models(self, query: str, *, page: int = 1, page_size: int = 20, data_source: str = "tg", state_code: int | None = 100) -> RemotePageDTO:
        raise ConnectorError(f"{self.account.platform} live model search is not implemented yet")

    def get_flow_detail(self, remote_flow_id: str, remote_version: str | None = None) -> RemoteFlowDTO:
        raise ConnectorError(f"{self.account.platform} live flow sync is not implemented yet")

    def get_process_detail(self, remote_process_id: str, remote_version: str | None = None) -> RemoteProcessDetailDTO:
        raise ConnectorError(f"{self.account.platform} live process sync is not implemented yet")

    def get_model_detail(self, remote_model_id: str, remote_version: str | None = None) -> RemoteModelDetailDTO:
        raise ConnectorError(f"{self.account.platform} live model sync is not implemented yet")


def _nested_text(value: Any, *path: str) -> str:
    cur = value
    for key in path:
        if not isinstance(cur, dict):
            return ""
        cur = cur.get(key)
    if isinstance(cur, dict):
        return _localized_name(cur)
    if isinstance(cur, list):
        return _localized_name(cur)
    return str(cur or "").strip()


def _extract_json_payload(row: dict[str, Any]) -> dict[str, Any]:
    for key in ("json_tg", "json", "json_ordered", "payload"):
        value = row.get(key)
        if isinstance(value, dict):
            return value
    return row


def _tiangong_rows_and_total(payload: Any) -> tuple[list[dict[str, Any]], int | None]:
    if isinstance(payload, list):
        rows = [row for row in payload if isinstance(row, dict)]
        total = None
        if rows:
            try:
                total = int(rows[0].get("total_count")) if rows[0].get("total_count") is not None else None
            except (TypeError, ValueError):
                total = None
        return rows, total
    if not isinstance(payload, dict):
        return [], None
    rows = payload.get("items") or payload.get("data") or payload.get("rows") or payload.get("result")
    if isinstance(rows, dict):
        nested_rows = rows.get("items") or rows.get("data") or rows.get("rows")
        rows = nested_rows
    if not isinstance(rows, list):
        rows = [payload]
    total_raw = payload.get("total") or payload.get("count") or payload.get("total_count")
    try:
        total = int(total_raw) if total_raw is not None else None
    except (TypeError, ValueError):
        total = None
    return [row for row in rows if isinstance(row, dict)], total


def _row_version(row: dict[str, Any]) -> str | None:
    return str(row.get("version") or row.get("version_id") or row.get("data_version") or "").strip() or None


def _session_expires_at(payload: dict[str, Any]) -> datetime | None:
    expires_at_raw = payload.get("expires_at")
    if expires_at_raw is not None:
        try:
            return datetime.utcfromtimestamp(float(expires_at_raw))
        except (TypeError, ValueError):
            pass
    expires_in_raw = payload.get("expires_in")
    try:
        expires_in = int(expires_in_raw)
    except (TypeError, ValueError):
        expires_in = 3600
    return datetime.utcnow() + timedelta(seconds=max(60, expires_in))


def _localized_name(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, list):
        for item in value:
            text = _localized_name(item)
            if text:
                return text
    if isinstance(value, dict):
        for key in ("#text", "@value", "value", "text"):
            text = str(value.get(key) or "").strip()
            if text:
                return text
        for key in ("baseName", "common:baseName", "name", "common:name", "shortDescription", "common:shortDescription"):
            text = _localized_name(value.get(key))
            if text:
                return text
    return ""


def _as_list(value: Any) -> list[Any]:
    if value is None:
        return []
    return value if isinstance(value, list) else [value]


def _ref_dict(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        ref_id = str(value.get("@refObjectId") or value.get("refObjectId") or value.get("id") or "").strip()
        if ref_id:
            version = str(value.get("@version") or value.get("version") or "").strip() or None
            return {"id": ref_id, "version": version, "short_description": _localized_name(value.get("common:shortDescription") or value.get("shortDescription"))}
    return None


def _extract_flow_property_reference(row: dict[str, Any]) -> dict[str, Any] | None:
    payload = _extract_json_payload(row) if isinstance(row, dict) else {}
    flow_dataset = payload.get("flowDataSet") if isinstance(payload.get("flowDataSet"), dict) else payload
    flow_properties = flow_dataset.get("flowProperties") if isinstance(flow_dataset, dict) else None
    raw = flow_properties.get("flowProperty") if isinstance(flow_properties, dict) else None
    for item in _as_list(raw):
        if not isinstance(item, dict):
            continue
        ref = _ref_dict(item.get("referenceToFlowPropertyDataSet") or item.get("refObject") or item)
        if ref:
            ref["mean_value"] = item.get("meanValue")
            return ref
    return None


def _extract_unit_group_reference(flow_property_row: dict[str, Any]) -> dict[str, Any] | None:
    payload = _extract_json_payload(flow_property_row)
    dataset = payload.get("flowPropertyDataSet") if isinstance(payload.get("flowPropertyDataSet"), dict) else payload
    info = dataset.get("flowPropertiesInformation") if isinstance(dataset, dict) else None
    quant = info.get("quantitativeReference") if isinstance(info, dict) else None
    if not isinstance(quant, dict):
        return None
    return _ref_dict(quant.get("referenceToReferenceUnitGroup"))


def _tiangong_unit_group_from_row(row: dict[str, Any]) -> RemoteUnitGroupDTO:
    payload = _extract_json_payload(row)
    dataset = payload.get("unitGroupDataSet") if isinstance(payload.get("unitGroupDataSet"), dict) else payload
    info = dataset.get("unitGroupInformation") if isinstance(dataset, dict) else None
    data_info = info.get("dataSetInformation") if isinstance(info, dict) else None
    quant = info.get("quantitativeReference") if isinstance(info, dict) else None
    units_node = dataset.get("units") if isinstance(dataset, dict) else None
    raw_units = units_node.get("unit") if isinstance(units_node, dict) else None
    name = (
        str(row.get("name") or row.get("unit_group") or "").strip()
        or _localized_name(data_info.get("common:name") if isinstance(data_info, dict) else None)
        or _localized_name(data_info.get("name") if isinstance(data_info, dict) else None)
        or str(row.get("id") or "").strip()
    )
    reference_unit_id = str(quant.get("referenceToReferenceUnit") or "").strip() if isinstance(quant, dict) else ""
    definitions: list[RemoteUnitDefinitionDTO] = []
    reference_unit = ""
    for unit in _as_list(raw_units):
        if not isinstance(unit, dict):
            continue
        unit_name = _localized_name(unit.get("name")) or _localized_name(unit.get("common:name")) or str(unit.get("unitName") or "").strip()
        if not unit_name:
            continue
        internal_id = str(unit.get("@dataSetInternalID") or unit.get("dataSetInternalID") or "").strip()
        factor_raw = unit.get("meanValue") or unit.get("factorToReference") or unit.get("factor_to_reference") or 1
        try:
            factor = float(factor_raw)
        except (TypeError, ValueError):
            factor = 1.0
        is_reference = bool(reference_unit_id and internal_id == reference_unit_id)
        if is_reference:
            reference_unit = unit_name
            factor = 1.0
        definitions.append(RemoteUnitDefinitionDTO(unit_name=unit_name, factor_to_reference=factor, is_reference=is_reference))
    if not reference_unit and definitions:
        reference = next((item for item in definitions if item.is_reference), definitions[0])
        reference_unit = reference.unit_name
        definitions = [
            replace(item, is_reference=(item.unit_name == reference.unit_name), factor_to_reference=1.0 if item.unit_name == reference.unit_name else item.factor_to_reference)
            for item in definitions
        ]
    return RemoteUnitGroupDTO(
        name=name,
        reference_unit=reference_unit or None,
        source_uuid=str(row.get("id") or "").strip() or None,
        source_version=_row_version(row),
        source_package_version="tiangong",
        definitions=definitions,
        metadata={"row": row},
    )


def _unit_group_to_payload(item: RemoteUnitGroupDTO) -> dict[str, Any]:
    return {
        "name": item.name,
        "reference_unit": item.reference_unit,
        "source_uuid": item.source_uuid,
        "source_version": item.source_version,
        "source_package_version": item.source_package_version,
        "definitions": [
            {
                "unit_name": unit.unit_name,
                "factor_to_reference": unit.factor_to_reference,
                "is_reference": unit.is_reference,
            }
            for unit in item.definitions
        ],
        "metadata": item.metadata,
    }


def _unit_group_from_payload(payload: dict[str, Any]) -> RemoteUnitGroupDTO | None:
    name = str(payload.get("name") or "").strip()
    if not name:
        return None
    definitions: list[RemoteUnitDefinitionDTO] = []
    for raw in payload.get("definitions") if isinstance(payload.get("definitions"), list) else []:
        if not isinstance(raw, dict):
            continue
        unit_name = str(raw.get("unit_name") or "").strip()
        if not unit_name:
            continue
        try:
            factor = float(raw.get("factor_to_reference") or 1.0)
        except (TypeError, ValueError):
            factor = 1.0
        definitions.append(RemoteUnitDefinitionDTO(unit_name=unit_name, factor_to_reference=factor, is_reference=bool(raw.get("is_reference"))))
    return RemoteUnitGroupDTO(
        name=name,
        reference_unit=str(payload.get("reference_unit") or "").strip() or None,
        source_uuid=str(payload.get("source_uuid") or "").strip() or None,
        source_version=str(payload.get("source_version") or "").strip() or None,
        source_package_version=str(payload.get("source_package_version") or "").strip() or None,
        definitions=definitions,
        metadata=payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {},
    )


def _localized_values(value: Any) -> list[str]:
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if isinstance(value, list):
        values: list[str] = []
        for item in value:
            values.extend(_localized_values(item))
        return values
    if isinstance(value, dict):
        values = []
        for key in ("#text", "@value", "value", "text"):
            text = str(value.get(key) or "").strip()
            if text:
                values.append(text)
        for key in ("baseName", "common:baseName", "name", "common:name", "shortDescription", "common:shortDescription", "mixAndLocationTypes", "treatmentStandardsRoutes"):
            values.extend(_localized_values(value.get(key)))
        return values
    return []


def _classification_text(payload: dict[str, Any], kind: str) -> str:
    if kind == "flow":
        info = payload.get("flowDataSet", {}).get("flowInformation", {}).get("dataSetInformation", {}) if isinstance(payload.get("flowDataSet"), dict) else {}
    elif kind == "process":
        info = payload.get("processDataSet", {}).get("processInformation", {}).get("dataSetInformation", {}) if isinstance(payload.get("processDataSet"), dict) else {}
    else:
        info = payload.get("lifeCycleModelDataSet", {}).get("lifeCycleModelInformation", {}).get("dataSetInformation", {}) if isinstance(payload.get("lifeCycleModelDataSet"), dict) else {}
    if not isinstance(info, dict):
        return ""
    classification = info.get("classificationInformation")
    return " ".join(_localized_values(classification))


def _row_search_texts(kind: str, row: dict[str, Any]) -> tuple[list[str], list[str], str]:
    payload = _extract_json_payload(row)
    if kind == "flow":
        info = payload.get("flowDataSet", {}).get("flowInformation", {}).get("dataSetInformation", {}) if isinstance(payload.get("flowDataSet"), dict) else {}
        title_values = _localized_values(row.get("name") or row.get("flow_name")) + _localized_values(info.get("name") if isinstance(info, dict) else None)
        secondary = _localized_values(info.get("common:synonyms") if isinstance(info, dict) else None)
    elif kind == "process":
        info = payload.get("processDataSet", {}).get("processInformation", {}).get("dataSetInformation", {}) if isinstance(payload.get("processDataSet"), dict) else {}
        title_values = _localized_values(row.get("name") or row.get("process_name")) + _localized_values(info.get("name") if isinstance(info, dict) else None)
        secondary = _localized_values(info.get("common:generalComment") if isinstance(info, dict) else None)
    else:
        info = payload.get("lifeCycleModelDataSet", {}).get("lifeCycleModelInformation", {}).get("dataSetInformation", {}) if isinstance(payload.get("lifeCycleModelDataSet"), dict) else {}
        title_values = _localized_values(row.get("name") or row.get("model_name")) + _localized_values(info.get("name") if isinstance(info, dict) else None)
        secondary = _localized_values(info.get("common:generalComment") if isinstance(info, dict) else None)
    return title_values, secondary, _classification_text(payload, kind)


def _rank_tiangong_row(kind: str, row: dict[str, Any], query: str) -> int:
    token = query.strip().lower()
    if not token:
        return 0
    title_values, secondary_values, classification = _row_search_texts(kind, row)
    title_text = " ".join(title_values).lower()
    secondary_text = " ".join(secondary_values).lower()
    classification_text = classification.lower()
    if token in title_text:
        return 300
    if all(part and part in title_text for part in token.split()):
        return 240
    if token in secondary_text:
        return 120
    if token in classification_text:
        return 80
    return 0


def _rerank_tiangong_rows(kind: str, rows: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    if not query.strip() or not rows:
        return rows
    ranked = [(_rank_tiangong_row(kind, row, query), index, row) for index, row in enumerate(rows)]
    if any(score > 0 for score, _, _ in ranked):
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [row for _, _, row in ranked]
    return rows


def _tiangong_flow_from_row(row: dict[str, Any]) -> RemoteFlowDTO:
    payload = _extract_json_payload(row)
    flow_uuid = str(row.get("id") or row.get("flow_uuid") or row.get("uuid") or _nested_text(payload, "flowDataSet", "flowInformation", "dataSetInformation", "common:UUID")).strip()
    name = (
        str(row.get("name") or row.get("flow_name") or "").strip()
        or _nested_text(payload, "flowDataSet", "flowInformation", "dataSetInformation", "name")
        or flow_uuid
    )
    flow_type = str(row.get("flow_type") or row.get("type") or _nested_text(payload, "flowDataSet", "modellingAndValidation", "LCIMethod", "typeOfDataSet") or "Product flow").strip()
    unit = str(row.get("default_unit") or row.get("unit") or "kg").strip()
    unit_group = str(row.get("unit_group") or row.get("unitGroup") or "Units of mass").strip()
    metadata = {
        "row": row,
        "classification": _classification_text(payload, "flow"),
        "modified_at": row.get("modified_at"),
        "state_code": row.get("state_code"),
    }
    return RemoteFlowDTO(
        remote_id=str(row.get("id") or flow_uuid).strip(),
        flow_uuid=flow_uuid,
        flow_name=name,
        flow_name_en=str(row.get("name_en") or "").strip() or None,
        flow_type=flow_type,
        default_unit=unit,
        unit_group=unit_group,
        source="tiangong",
        remote_version=_row_version(row),
        metadata=metadata,
    )


def _tiangong_process_from_row(row: dict[str, Any]) -> RemoteProcessDTO:
    payload = _extract_json_payload(row)
    process_uuid = str(row.get("id") or row.get("process_uuid") or row.get("uuid") or _nested_text(payload, "processDataSet", "processInformation", "dataSetInformation", "common:UUID")).strip()
    name = (
        str(row.get("name") or row.get("process_name") or "").strip()
        or _nested_text(payload, "processDataSet", "processInformation", "dataSetInformation", "name")
        or process_uuid
    )
    process_type = str(
        row.get("process_type")
        or row.get("type")
        or _nested_text(payload, "processDataSet", "modellingAndValidation", "LCIMethodAndAllocation", "typeOfDataSet")
        or "unit_process"
    ).strip()
    metadata = {
        "row": row,
        "classification": _classification_text(payload, "process"),
        "modified_at": row.get("modified_at"),
        "state_code": row.get("state_code"),
    }
    return RemoteProcessDTO(
        remote_id=str(row.get("id") or process_uuid).strip(),
        process_uuid=process_uuid,
        process_name=name,
        process_type=process_type,
        reference_flow_uuid=str(row.get("reference_flow_uuid") or row.get("referenceFlowUuid") or "").strip() or None,
        source="tiangong",
        remote_version=_row_version(row),
        metadata=metadata,
    )


def _tiangong_model_from_row(row: dict[str, Any]) -> RemoteModelDTO:
    payload = _extract_json_payload(row)
    model_uuid = str(row.get("id") or row.get("model_uuid") or row.get("uuid") or payload.get("id") or _nested_text(payload, "lifeCycleModelDataSet", "lifeCycleModelInformation", "dataSetInformation", "common:UUID") or "").strip()
    name = (
        str(row.get("name") or row.get("model_name") or payload.get("name") or payload.get("title") or "").strip()
        or _nested_text(payload, "lifeCycleModelDataSet", "lifeCycleModelInformation", "dataSetInformation", "name")
        or model_uuid
    )
    return RemoteModelDTO(
        remote_id=str(row.get("id") or model_uuid).strip(),
        model_uuid=model_uuid,
        model_name=name,
        source="tiangong",
        remote_version=_row_version(row),
        metadata={"row": row, "classification": _classification_text(payload, "model"), "modified_at": row.get("modified_at"), "state_code": row.get("state_code")},
    )


def _extract_process_exchanges(row: dict[str, Any]) -> list[dict[str, Any]]:
    payload = _extract_json_payload(row)
    candidates = [payload.get("exchanges"), payload.get("exchange"), payload.get("process_json", {}).get("exchanges") if isinstance(payload.get("process_json"), dict) else None]
    for candidate in candidates:
        if isinstance(candidate, list):
            return [item for item in candidate if isinstance(item, dict)]
    return []


def _tiangong_flow_from_exchange(exchange: dict[str, Any]) -> RemoteFlowDTO | None:
    flow_uuid = str(exchange.get("flow_uuid") or exchange.get("flowUuid") or exchange.get("flow_id") or exchange.get("flowId") or "").strip()
    if not flow_uuid:
        return None
    return RemoteFlowDTO(
        remote_id=flow_uuid,
        flow_uuid=flow_uuid,
        flow_name=str(exchange.get("flow_name") or exchange.get("flowName") or flow_uuid).strip(),
        flow_type=str(exchange.get("flow_type") or exchange.get("flowType") or "Product flow").strip(),
        default_unit=str(exchange.get("unit") or exchange.get("default_unit") or "kg").strip(),
        unit_group=str(exchange.get("unit_group") or exchange.get("unitGroup") or "Units of mass").strip(),
        source="tiangong",
        remote_version=str(exchange.get("flow_version") or "").strip() or None,
        metadata={"exchange": exchange},
    )


def _extract_process_vector(row: dict[str, Any]) -> dict[str, Any] | None:
    payload = _extract_json_payload(row)
    vector = payload.get("vector") if isinstance(payload, dict) else None
    return vector if isinstance(vector, dict) else None


def _flow_from_mapping(row: dict[str, Any], platform: str) -> RemoteFlowDTO:
    flow_uuid = str(row.get("flow_uuid") or row.get("uuid") or row.get("id") or "").strip()
    return RemoteFlowDTO(
        remote_id=str(row.get("remote_id") or row.get("id") or flow_uuid).strip(),
        flow_uuid=flow_uuid,
        flow_name=str(row.get("flow_name") or row.get("name") or flow_uuid).strip(),
        flow_name_en=str(row.get("flow_name_en") or row.get("name_en") or "").strip() or None,
        flow_type=str(row.get("flow_type") or row.get("type") or "Product flow").strip(),
        default_unit=str(row.get("default_unit") or row.get("unit") or "kg").strip(),
        unit_group=str(row.get("unit_group") or row.get("unitGroup") or "Units of mass").strip(),
        source=str(row.get("source") or platform).strip(),
        remote_version=str(row.get("remote_version") or row.get("version") or "").strip() or None,
        metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
    )


def _process_from_mapping(row: dict[str, Any], platform: str) -> RemoteProcessDTO:
    process_uuid = str(row.get("process_uuid") or row.get("uuid") or row.get("id") or "").strip()
    return RemoteProcessDTO(
        remote_id=str(row.get("remote_id") or row.get("id") or process_uuid).strip(),
        process_uuid=process_uuid,
        process_name=str(row.get("process_name") or row.get("name") or process_uuid).strip(),
        process_type=str(row.get("process_type") or row.get("type") or "unit_process").strip(),
        reference_flow_uuid=str(row.get("reference_flow_uuid") or row.get("referenceFlowUuid") or "").strip() or None,
        source=str(row.get("source") or platform).strip(),
        remote_version=str(row.get("remote_version") or row.get("version") or "").strip() or None,
        metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
    )


def _model_from_mapping(row: dict[str, Any], platform: str) -> RemoteModelDTO:
    model_uuid = str(row.get("model_uuid") or row.get("uuid") or row.get("id") or "").strip()
    return RemoteModelDTO(
        remote_id=str(row.get("remote_id") or row.get("id") or model_uuid).strip(),
        model_uuid=model_uuid,
        model_name=str(row.get("model_name") or row.get("name") or model_uuid).strip(),
        source=str(row.get("source") or platform).strip(),
        remote_version=str(row.get("remote_version") or row.get("version") or "").strip() or None,
        metadata=row.get("metadata") if isinstance(row.get("metadata"), dict) else {},
    )


def connector_for_account(account: PlatformAccountContext) -> BaseDataPlatformConnector:
    platform = account.platform.strip().lower()
    if platform == "mock":
        return MockDataPlatformConnector(account)
    if platform == "custom":
        return CustomHttpDataPlatformConnector(account)
    if platform == "tiangong":
        return TianGongSupabaseConnector(account)
    if platform == "hiqlcd":
        return SkeletonDataPlatformConnector(account)
    raise ConnectorError(f"Unsupported data platform: {account.platform}")
