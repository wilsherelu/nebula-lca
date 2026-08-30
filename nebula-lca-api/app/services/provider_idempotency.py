from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from collections.abc import Callable
from datetime import datetime, timedelta

from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from ..config import settings
from ..models import ProviderSolveIdempotency
from ..provider_schemas import ProviderSolveRequest, ProviderSolveResponse
from .provider_contract import ProviderContractError


def _canonical_json(value: object) -> str:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def canonical_solve_request_hash(request: ProviderSolveRequest) -> str:
    payload = request.model_dump(mode="json", exclude={"idempotency_key"})
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _validate_completed_body(row: ProviderSolveIdempotency) -> str:
    body = str(row.response_body or "")
    try:
        payload = json.loads(body)
        response = ProviderSolveResponse.model_validate(payload)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ProviderContractError(
            503,
            "IDEMPOTENCY_RECEIPT_CORRUPT",
            "The stored idempotent solve receipt is incomplete or invalid.",
            idempotency_key=row.idempotency_key,
        ) from exc
    if (
        response.run_id != row.run_id
        or response.idempotency_key != row.idempotency_key
        or response.request_hash != row.request_hash
    ):
        raise ProviderContractError(
            503,
            "IDEMPOTENCY_RECEIPT_CORRUPT",
            "The stored idempotent solve receipt does not match its database identity.",
            idempotency_key=row.idempotency_key,
        )
    return body


def _raise_stored_failure(row: ProviderSolveIdempotency) -> None:
    value = row.error_json if isinstance(row.error_json, dict) else {}
    code = str(value.get("code") or "")
    message = str(value.get("message") or "")
    status_code = row.error_status
    if not code or not message or status_code is None or not 400 <= status_code < 500:
        raise ProviderContractError(
            503,
            "IDEMPOTENCY_RECEIPT_CORRUPT",
            "The stored idempotent failure receipt is incomplete or invalid.",
            idempotency_key=row.idempotency_key,
        )
    details = value.get("details") if isinstance(value.get("details"), dict) else {}
    raise ProviderContractError(status_code, code, message, **details)


def _start_heartbeat(
    db: Session,
    *,
    idempotency_key: str,
    claim_token: str,
    lease_seconds: float,
) -> tuple[threading.Event, threading.Thread]:
    stop = threading.Event()
    heartbeat_session = sessionmaker(
        bind=db.get_bind(),
        autoflush=False,
        autocommit=False,
        future=True,
    )

    def heartbeat() -> None:
        interval = max(0.1, lease_seconds / 3.0)
        while not stop.wait(interval):
            session = heartbeat_session()
            try:
                updated = (
                    session.query(ProviderSolveIdempotency)
                    .filter(
                        ProviderSolveIdempotency.idempotency_key == idempotency_key,
                        ProviderSolveIdempotency.status == "pending",
                        ProviderSolveIdempotency.claim_token == claim_token,
                    )
                    .update(
                        {
                            ProviderSolveIdempotency.lease_expires_at: (
                                datetime.utcnow() + timedelta(seconds=lease_seconds)
                            ),
                            ProviderSolveIdempotency.updated_at: datetime.utcnow(),
                        },
                        synchronize_session=False,
                    )
                )
                session.commit()
                if updated != 1:
                    return
            except SQLAlchemyError:
                session.rollback()
            finally:
                session.close()

    thread = threading.Thread(target=heartbeat, daemon=True)
    thread.start()
    return stop, thread


def _claim_or_replay(
    db: Session,
    *,
    idempotency_key: str,
    request_hash: str,
    runtime_fingerprint_factory: Callable[[], str],
) -> tuple[ProviderSolveIdempotency, bool]:
    deadline = time.monotonic() + settings.provider_idempotency_wait_seconds
    runtime_fingerprint: str | None = None

    def current_runtime_fingerprint() -> str:
        nonlocal runtime_fingerprint
        if runtime_fingerprint is None:
            runtime_fingerprint = runtime_fingerprint_factory()
        if len(runtime_fingerprint) != 64:
            raise ProviderContractError(
                503,
                "IDEMPOTENCY_RUNTIME_IDENTITY_UNAVAILABLE",
                "The provider runtime fingerprint is incomplete.",
            )
        return runtime_fingerprint

    while True:
        now = datetime.utcnow()
        db.expire_all()
        row = db.get(ProviderSolveIdempotency, idempotency_key)
        if row is None:
            claim_token = str(uuid.uuid4())
            candidate = ProviderSolveIdempotency(
                idempotency_key=idempotency_key,
                request_hash=request_hash,
                run_id=str(uuid.uuid4()),
                status="pending",
                runtime_fingerprint=current_runtime_fingerprint(),
                claim_token=claim_token,
                lease_expires_at=now + timedelta(seconds=settings.provider_idempotency_lease_seconds),
            )
            try:
                db.add(candidate)
                db.commit()
                db.refresh(candidate)
                return candidate, True
            except IntegrityError:
                db.rollback()
                continue
        if row.request_hash != request_hash:
            raise ProviderContractError(
                409,
                "IDEMPOTENCY_KEY_REQUEST_MISMATCH",
                "The idempotency key is already bound to a different canonical solve request.",
                idempotency_key=idempotency_key,
                stored_request_hash=row.request_hash,
                request_hash=request_hash,
            )
        if row.status == "completed":
            _validate_completed_body(row)
            return row, False
        if row.status == "failed":
            _raise_stored_failure(row)
        if row.status != "pending":
            raise ProviderContractError(
                503,
                "IDEMPOTENCY_RECEIPT_CORRUPT",
                "The idempotent solve receipt has an unknown status.",
                idempotency_key=idempotency_key,
                status=row.status,
            )
        if row.lease_expires_at <= now:
            current_fingerprint = current_runtime_fingerprint()
            if row.runtime_fingerprint != current_fingerprint:
                raise ProviderContractError(
                    409,
                    "IDEMPOTENCY_RUNTIME_DRIFT",
                    "The provider runtime changed before an interrupted solve could be recovered.",
                    idempotency_key=idempotency_key,
                    stored_runtime_fingerprint=row.runtime_fingerprint,
                    runtime_fingerprint=current_fingerprint,
                )
            claim_token = str(uuid.uuid4())
            updated = (
                db.query(ProviderSolveIdempotency)
                .filter(
                    ProviderSolveIdempotency.idempotency_key == idempotency_key,
                    ProviderSolveIdempotency.status == "pending",
                    ProviderSolveIdempotency.claim_token == row.claim_token,
                    ProviderSolveIdempotency.lease_expires_at <= now,
                )
                .update(
                    {
                        ProviderSolveIdempotency.claim_token: claim_token,
                        ProviderSolveIdempotency.lease_expires_at: (
                            now + timedelta(seconds=settings.provider_idempotency_lease_seconds)
                        ),
                        ProviderSolveIdempotency.updated_at: now,
                    },
                    synchronize_session=False,
                )
            )
            db.commit()
            if updated == 1:
                db.expire_all()
                claimed = db.get(ProviderSolveIdempotency, idempotency_key)
                if claimed is None:
                    raise ProviderContractError(
                        503,
                        "IDEMPOTENCY_RECEIPT_CORRUPT",
                        "The recovered idempotent solve receipt disappeared.",
                    )
                return claimed, True
            continue
        if time.monotonic() >= deadline:
            retry_after = max(1, int((row.lease_expires_at - now).total_seconds()))
            raise ProviderContractError(
                409,
                "IDEMPOTENCY_REQUEST_IN_PROGRESS",
                "The same idempotent solve request is still in progress.",
                idempotency_key=idempotency_key,
                run_id=row.run_id,
                retry_after_seconds=retry_after,
            )
        time.sleep(0.05)


def execute_idempotent_solve(
    db: Session,
    request: ProviderSolveRequest,
    *,
    runtime_fingerprint_factory: Callable[[], str],
    solve: Callable[[str], ProviderSolveResponse],
) -> str:
    idempotency_key = request.idempotency_key
    if idempotency_key is None:
        raise ValueError("execute_idempotent_solve requires idempotency_key")
    request_hash = canonical_solve_request_hash(request)
    row, owns_claim = _claim_or_replay(
        db,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        runtime_fingerprint_factory=runtime_fingerprint_factory,
    )
    if not owns_claim:
        return _validate_completed_body(row)

    stop, heartbeat = _start_heartbeat(
        db,
        idempotency_key=idempotency_key,
        claim_token=row.claim_token,
        lease_seconds=settings.provider_idempotency_lease_seconds,
    )
    try:
        try:
            response = solve(row.run_id)
        except ProviderContractError as exc:
            if not 400 <= exc.status_code < 500:
                raise
            error_json = {
                "code": exc.code,
                "message": exc.message,
                "details": exc.details,
            }
            updated = (
                db.query(ProviderSolveIdempotency)
                .filter(
                    ProviderSolveIdempotency.idempotency_key == idempotency_key,
                    ProviderSolveIdempotency.status == "pending",
                    ProviderSolveIdempotency.claim_token == row.claim_token,
                )
                .update(
                    {
                        ProviderSolveIdempotency.status: "failed",
                        ProviderSolveIdempotency.error_json: error_json,
                        ProviderSolveIdempotency.error_status: exc.status_code,
                        ProviderSolveIdempotency.finished_at: datetime.utcnow(),
                        ProviderSolveIdempotency.updated_at: datetime.utcnow(),
                    },
                    synchronize_session=False,
                )
            )
            db.commit()
            if updated != 1:
                raise ProviderContractError(
                    503,
                    "IDEMPOTENCY_CLAIM_LOST",
                    "The provider lost ownership before freezing a deterministic failure.",
                ) from exc
            raise

        response.idempotency_key = idempotency_key
        response.request_hash = request_hash
        response.provenance.idempotency_key = idempotency_key
        response.provenance.request_hash = request_hash
        body = _canonical_json(response.model_dump(mode="json"))
        updated = (
            db.query(ProviderSolveIdempotency)
            .filter(
                ProviderSolveIdempotency.idempotency_key == idempotency_key,
                ProviderSolveIdempotency.status == "pending",
                ProviderSolveIdempotency.claim_token == row.claim_token,
            )
            .update(
                {
                    ProviderSolveIdempotency.status: "completed",
                    ProviderSolveIdempotency.response_body: body,
                    ProviderSolveIdempotency.finished_at: datetime.utcnow(),
                    ProviderSolveIdempotency.updated_at: datetime.utcnow(),
                },
                synchronize_session=False,
            )
        )
        db.commit()
        if updated != 1:
            raise ProviderContractError(
                503,
                "IDEMPOTENCY_CLAIM_LOST",
                "The provider lost ownership before freezing the solve response.",
            )
        return body
    finally:
        stop.set()
        heartbeat.join(timeout=1.0)
