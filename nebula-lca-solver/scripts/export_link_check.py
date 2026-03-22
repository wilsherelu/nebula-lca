import argparse
import csv
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.core.matrix_builder import load_snapshot


def main() -> None:
    parser = argparse.ArgumentParser(description="Export link input amounts and product outputs.")
    parser.add_argument("--snapshot", required=True, help="Path to snapshot JSON.")
    parser.add_argument(
        "--output-dir",
        default="exports/link_check",
        help="Output directory for CSV files.",
    )
    args = parser.parse_args()

    snapshot = load_snapshot(args.snapshot)
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    processes = snapshot.get("processes", []) or []
    flows = snapshot.get("flows", []) or []
    exchanges = snapshot.get("exchanges", []) or []
    links = snapshot.get("links", []) or []

    process_by_uuid = {p.get("process_uuid", ""): p for p in processes if p.get("process_uuid")}
    flow_by_uuid = {f.get("flow_uuid", ""): f for f in flows if f.get("flow_uuid")}

    exchanges_by_process = {}
    for ex in exchanges:
        proc_uuid = ex.get("process_uuid", "")
        if not proc_uuid:
            continue
        exchanges_by_process.setdefault(proc_uuid, []).append(ex)

    _write_link_inputs(
        out_dir / "links_input_amounts.csv",
        links,
        process_by_uuid,
        flow_by_uuid,
        exchanges_by_process,
    )
    _write_process_products(
        out_dir / "process_products.csv",
        process_by_uuid,
        flow_by_uuid,
        exchanges_by_process,
    )
    _write_process_elementary_flows(
        out_dir / "process_elementary_flows.csv",
        process_by_uuid,
        flow_by_uuid,
        exchanges_by_process,
    )

    print(f"wrote {out_dir / 'links_input_amounts.csv'}")
    print(f"wrote {out_dir / 'process_products.csv'}")
    print(f"wrote {out_dir / 'process_elementary_flows.csv'}")


def _write_link_inputs(path, links, process_by_uuid, flow_by_uuid, exchanges_by_process) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "consumer_process_uuid",
                "consumer_process_name",
                "provider_process_uuid",
                "provider_process_name",
                "flow_uuid",
                "flow_name",
                "input_exchange_ids",
                "input_amount",
            ]
        )
        for link in links:
            consumer = link.get("consumer_process_uuid", "")
            provider = link.get("provider_process_uuid", "")
            flow_uuid = link.get("flow_uuid", "")
            consumer_name = process_by_uuid.get(consumer, {}).get("process_name", "")
            provider_name = process_by_uuid.get(provider, {}).get("process_name", "")
            flow_name = flow_by_uuid.get(flow_uuid, {}).get("flow_name", "")

            exchange_ids = []
            amount_total = 0.0
            for ex in exchanges_by_process.get(consumer, []):
                if ex.get("flow_uuid") != flow_uuid:
                    continue
                if str(ex.get("direction", "")).lower() != "input":
                    continue
                exchange_ids.append(str(ex.get("exchange_id", "")))
                try:
                    amount_total += float(ex.get("amount", 0) or 0)
                except (TypeError, ValueError):
                    pass

            writer.writerow(
                [
                    consumer,
                    consumer_name,
                    provider,
                    provider_name,
                    flow_uuid,
                    flow_name,
                    ";".join(exchange_ids),
                    amount_total if exchange_ids else "",
                ]
            )


def _write_process_products(path, process_by_uuid, flow_by_uuid, exchanges_by_process) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "process_uuid",
                "process_name",
                "flow_uuid",
                "flow_name",
                "flow_type",
                "unit_group_uuid",
                "exchange_id",
                "amount",
                "allocation_fraction",
            ]
        )
        for proc_uuid, proc in process_by_uuid.items():
            proc_name = proc.get("process_name", "")
            outputs = []
            for ex in exchanges_by_process.get(proc_uuid, []):
                if str(ex.get("direction", "")).lower() != "output":
                    continue
                if ex.get("allocation_fraction") is None:
                    continue
                outputs.append(ex)

            # Fallback to reference exchange when no allocation outputs exist.
            if not outputs:
                ref_exchange_id = str(proc.get("reference_product_flow_uuid", "")).strip()
                if ref_exchange_id:
                    for ex in exchanges_by_process.get(proc_uuid, []):
                        if str(ex.get("exchange_id", "")).strip() == ref_exchange_id:
                            outputs.append(ex)
                            break

            for ex in outputs:
                flow_uuid = ex.get("flow_uuid", "")
                flow = flow_by_uuid.get(flow_uuid, {})
                writer.writerow(
                    [
                        proc_uuid,
                        proc_name,
                        flow_uuid,
                        flow.get("flow_name", ""),
                        flow.get("flow_type", ""),
                        flow.get("unit_group_uuid", ""),
                        ex.get("exchange_id", ""),
                        ex.get("amount", ""),
                        ex.get("allocation_fraction", ""),
                    ]
                )


def _write_process_elementary_flows(
    path,
    process_by_uuid,
    flow_by_uuid,
    exchanges_by_process,
) -> None:
    with open(path, "w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "process_uuid",
                "process_name",
                "flow_uuid",
                "flow_name",
                "flow_type",
                "direction",
                "exchange_id",
                "amount",
                "unit_group_uuid",
            ]
        )
        for proc_uuid, proc in process_by_uuid.items():
            proc_name = proc.get("process_name", "")
            for ex in exchanges_by_process.get(proc_uuid, []):
                flow_uuid = ex.get("flow_uuid", "")
                flow = flow_by_uuid.get(flow_uuid, {})
                if flow.get("flow_type") != "Elementary flow":
                    continue
                writer.writerow(
                    [
                        proc_uuid,
                        proc_name,
                        flow_uuid,
                        flow.get("flow_name", ""),
                        flow.get("flow_type", ""),
                        ex.get("direction", ""),
                        ex.get("exchange_id", ""),
                        ex.get("amount", ""),
                        flow.get("unit_group_uuid", ""),
                    ]
                )


if __name__ == "__main__":
    main()
