"""Create one minimal TianGong personal-draft Flow and read it back.

The script never prints credentials or raw session material. It chooses a local
kg Flow only as a source for a resolvable TianGong Flow-property reference.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from uuid import uuid4


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path, help="Nebula desktop SQLite database")
    parser.add_argument("--account-id", help="Active TianGong account ID; defaults to the only active account")
    return parser.parse_args()


def _report(payload: dict) -> None:
    print(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True))


def main() -> int:
    args = _parse_args()
    database = args.database.resolve()
    if not database.is_file():
        _report({"ok": False, "stage": "configuration", "error": "Database file was not found."})
        return 2
    os.environ["DATABASE_URL"] = f"sqlite:///{database.as_posix()}"
    credential_key_candidates = [
        database.parent / "runtime" / "secrets" / "data_platform_credential.key",
        database.parent / "data_platform_credential.key",
    ]
    credential_key_file = next((item for item in credential_key_candidates if item.is_file()), None)
    if credential_key_file is not None:
        os.environ.setdefault("DATA_PLATFORM_CREDENTIAL_KEY_FILE", str(credential_key_file))

    from app.api.data_platforms import _account_context, _flow_json_ordered, _flow_publish_preflight
    from app.database import SessionLocal
    from app.models import DataPlatformAccount, FlowRecord
    from app.services.data_platform_connectors import ConnectorError, connector_for_account

    db = SessionLocal()
    try:
        query = db.query(DataPlatformAccount).filter(
            DataPlatformAccount.platform == "tiangong",
            DataPlatformAccount.status == "active",
        )
        if args.account_id:
            query = query.filter(DataPlatformAccount.id == args.account_id)
        accounts = query.all()
        if len(accounts) != 1:
            _report({"ok": False, "stage": "account", "error": "Expected exactly one active TianGong account.", "account_count": len(accounts)})
            return 2
        account = accounts[0]
        connector = connector_for_account(_account_context(account, db))

        template = None
        reference = None
        for candidate in (
            db.query(FlowRecord)
            .filter(
                FlowRecord.tidas_flow_property_uuid.isnot(None),
                FlowRecord.tidas_flow_property_uuid != "",
                FlowRecord.default_unit == "kg",
            )
            .order_by(FlowRecord.flow_uuid)
            .yield_per(100)
        ):
            try:
                resolved = connector.resolve_flow_publish_reference(candidate.tidas_flow_property_uuid)
            except ConnectorError:
                continue
            if any(unit.unit_name == "kg" for unit in resolved.unit_group.definitions):
                template = candidate
                reference = resolved
                break
        if template is None or reference is None:
            _report({"ok": False, "stage": "dependency", "error": "No local Flow property resolved to a TianGong kg unit group."})
            return 2

        flow = FlowRecord(
            flow_uuid=str(uuid4()),
            flow_name="Nebula API smoke test flow",
            flow_name_en="Nebula API smoke test flow",
            flow_type="Product flow",
            default_unit="kg",
            unit_group=reference.unit_group.name,
            source="api_smoke",
            is_custom=True,
            tidas_compatible=True,
            tidas_unit_group=reference.unit_group.name,
            tidas_flow_property_uuid=reference.flow_property_uuid,
            tidas_reference_source="tiangong_open_data",
        )
        _flow_publish_preflight(flow, reference)
        remote = connector.publish_flow(
            flow_uuid=flow.flow_uuid,
            json_ordered=_flow_json_ordered(flow, reference),
            rule_verification=False,
            overwrite=False,
        )
        version = str(remote.get("version") or "01.01.000")
        readback = connector.get_flow_detail(flow.flow_uuid, version)
        readback_row = readback.metadata.get("row") if isinstance(readback.metadata, dict) else {}
        db.commit()
        _report(
            {
                "ok": True,
                "stage": "readback",
                "account_id": account.id,
                "flow_uuid": flow.flow_uuid,
                "version": version,
                "flow_type": readback.flow_type,
                "reference_unit": readback.default_unit,
                "unit_group": reference.unit_group.name,
                "unit_group_uuid": reference.unit_group.source_uuid,
                "unit_group_version": reference.unit_group.source_version,
                "flow_property_uuid": reference.flow_property_uuid,
                "flow_property_version": reference.flow_property_version,
                "state_code": remote.get("state_code") or (readback_row.get("state_code") if isinstance(readback_row, dict) else None),
            }
        )
        return 0
    except ConnectorError as exc:
        db.rollback()
        _report({"ok": False, "stage": "remote", "status_code": exc.status_code, "error": str(exc)})
        return 1
    except Exception as exc:  # noqa: BLE001
        db.rollback()
        _report({"ok": False, "stage": "local", "error": str(exc)})
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
