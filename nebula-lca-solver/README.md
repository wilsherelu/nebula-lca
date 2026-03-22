# Tiangong LCIA Solver

Tiangong LCIA Solver is a high-performance life cycle impact assessment (LCIA) engine built on matrix-based methods.  
It provides a Python FastAPI service for large-scale LCA computation, supporting:

- Construction and solution of technology and intervention matrices (A/B/C)
- Life cycle inventory (LCI) and multi-indicator LCIA calculations
- Hotspot analysis and contribution tracing for complex process networks
- Scalable computation for large LCA databases and industrial systems

The solver is designed as a standalone computational backend that can be integrated with platforms such as Tiangong LCA, while remaining flexible for reuse in other LCA and sustainability analysis workflows.

## Snapshot allocation (current)

For Tiangong snapshot inputs, matrix construction currently supports allocation by **consistent unit group**:

- Any output `exchange` with `allocation_fraction != null` is treated as a product output.
- Allocation is enabled per process when such outputs exist; `is_reference_product` is ignored.
- The functional unit denominator is the **sum of amounts** of those product outputs.
- Allocation outputs must be **non-elementary** flows.
- When allocation is enabled, all product outputs must share the **same unit group**.
- Other allocation bases (energy, stoichiometric, economic) are not implemented yet.
- If a process has no allocation outputs, the reference exchange (by `reference_product_flow_uuid`) is used as the product fallback.

## B/C matrix scope (demo)

- B matrix rows are built from **elementary flows that appear in exchanges** (not full flow list).
- B matrix values do **not** apply input/output sign; the exchange amount is used as-is.
- C matrix is expected to map a **subset** of elementary flows to CFs (e.g., from EF3.1).
- Future extension may expand B to **all elementary flows** in the snapshot for full coverage.

## Engineering main path

Primary flow for integration:

JSON (or payload) -> MMR (pb, immutable) -> in-memory A/B/C -> LCIA -> result output.

CSV exports are for auditing/debug only and are not part of the main pipeline.

### What is MMR?

MMR (Minimal Matrix Representation) is a **minimal, immutable** protobuf record that captures only what is needed
to rebuild A/B matrices:

- `process_index`: ordered list of process UUIDs (matrix indices)
- `elementary_flow_index`: ordered list of elementary flow UUIDs
- sparse A/B entries (row, col, value)

It does **not** include UI fields, units, allocation rules, or LCIA factors.  
MMR is used for **audit, reproducibility, and storage**: generate it once from a snapshot, store it as a stable artifact,
then rebuild A/B on demand for computation.

## Runtime configuration

Environment variables:

- `TIANGONG_OUTPUT_DIR`: output directory for generated files (default: `exports`).
- `TIANGONG_EF31_DIR`: EF3.1 data directory for LCIA (default: `data/EF3.1`).

## Dependencies

Minimum Python packages:

- `numpy`
- `pydantic`
- `protobuf`

For API service:

- `fastapi`
- `uvicorn`

For generating Python classes from `.proto` (only when needed):

- `grpcio-tools`

Install example:

```powershell
py -m pip install numpy pydantic protobuf fastapi uvicorn
```

Optional (proto tooling):

```powershell
py -m pip install grpcio-tools
```

Or install all:

```powershell
py -m pip install -r requirements.txt
```

## CLI usage

Main pipeline:

```powershell
py scripts/main.py --snapshot test.json --ef31 data/EF3.1
```

Outputs:

- `exports/mmr_YYYYMMDDThhmmssZ.pb`
- `exports/LCIA_results_YYYYMMDDThhmmssZ.csv`

Output directory can be customized via `TIANGONG_OUTPUT_DIR`.


MMR only (timestamped by default):

```powershell
py scripts/export_mmr.py --snapshot test.json
```

LCIA from MMR (timestamped by default):

```powershell
py scripts/run_lcia.py --mmr exports/mmr.pb --ef31 data/EF3.1
```

## API usage

POST `/v1/lcia` with JSON body:

```json
{
  "snapshot": { "...": "..." }
}
```

Returns LCIA matrix and writes `mmr.pb` to `TIANGONG_OUTPUT_DIR`.

## Snapshot payload contract

This section documents the current runtime contract actually consumed by `/v1/lcia`.
If payload semantics disagree with these rules, matrix build may succeed while LCIA
results are still wrong.

### 1. `flow_type` literals are case-sensitive

Runtime matrix construction currently recognizes these exact flow type strings:

- `Elementary flow`
- `Product flow`
- `Waste flow`

Do not send lowercase or snake_case variants such as:

- `elementary_flow`
- `product_flow`
- `waste_flow`

If an environmental flow is not labeled exactly as `Elementary flow`, it will not be
picked up into the B matrix and downstream LCIA will be wrong even if the A matrix
looks correct.

### 2. `reference_product_flow_uuid` currently points to an `exchange_id`

For each process, `reference_product_flow_uuid` is matched against the process
reference output `exchange_id` during matrix construction.

Practical implication:

- send the reference output exchange id here
- do not send a flow uuid unless the runtime is changed accordingly

If allocation outputs are absent, the solver falls back to this reference exchange to
derive the process denominator.

### 3. Process denominator rule

Technosphere and biosphere coefficients are normalized by the consumer process
denominator.

Current denominator priority:

1. Sum of `amount` for all `output` exchanges where `allocation_fraction != null`
2. Fallback to the reference output exchange matched by `reference_product_flow_uuid`
3. Fallback to `1.0` only when data is invalid

All allocation outputs for one process should share the same non-elementary unit group.

### 4. Link semantics

Each technosphere link must contain:

- `consumer_process_uuid`
- `provider_process_uuid`
- `flow_uuid`
- `quantity_mode`

Current runtime interpretation:

- `dual`: use `consumer_amount`
- `single`: use `amount`

For one link, the A-matrix coefficient is built as:

```text
coeff(provider, consumer) = - input_amount / consumer_denominator
```

Where `input_amount` is:

- `consumer_amount` for `quantity_mode = "dual"`
- `amount` for `quantity_mode = "single"`

### 5. How to write `single` links

For `quantity_mode = "single"`:

- `amount` must mean the consumer-side input amount
- it should match the consumer process input exchange for the same `flow_uuid`
- `consumer_amount` and `provider_amount` are not used by the current `/v1/lcia`
  matrix builder

Do not treat `single.amount` as a UI edge weight or provider-side output amount.
It must mean: "how much of this flow the consumer process consumes for its own batch."

### 6. How to write `dual` links

For `quantity_mode = "dual"`:

- `consumer_amount` is the runtime value used for matrix construction
- `provider_amount` is not used by the current A-matrix builder
- `amount` is not the preferred runtime field for `dual`

Use `dual` for market allocation style links. Use `single` for ordinary
process-to-process technosphere links, including recycle and feedback loops.

### 7. `flow_uuid` alignment rule

`links[*].flow_uuid` should match the corresponding consumer-side input exchange
`flow_uuid`.

Even when link amounts are present, keeping links and exchanges aligned is required
for auditability and for fallback/debug logic.

### 8. Minimal recycle loop example

The example below expresses a two-process loop:

- atmospheric-vacuum unit -> FCC: 200
- FCC -> atmospheric-vacuum unit: 100

```json
{
  "snapshot": {
    "processes": [
      {
        "process_uuid": "proc_atm_vac",
        "process_name": "Atmospheric-Vacuum Unit",
        "reference_product_flow_uuid": "ex_atm_out"
      },
      {
        "process_uuid": "proc_fcc",
        "process_name": "FCC",
        "reference_product_flow_uuid": "ex_fcc_out"
      }
    ],
    "flows": [
      {
        "flow_uuid": "flow_feed_to_fcc",
        "flow_name": "Feed to FCC",
        "flow_type": "Product flow",
        "unit_group_uuid": "ug_ton"
      },
      {
        "flow_uuid": "flow_recycle_to_atm",
        "flow_name": "Recycle to Atmospheric-Vacuum Unit",
        "flow_type": "Product flow",
        "unit_group_uuid": "ug_ton"
      },
      {
        "flow_uuid": "flow_climate",
        "flow_name": "Carbon dioxide, fossil",
        "flow_type": "Elementary flow",
        "unit_group_uuid": "ug_kg"
      }
    ],
    "exchanges": [
      {
        "exchange_id": "ex_atm_out",
        "process_uuid": "proc_atm_vac",
        "flow_uuid": "flow_feed_to_fcc",
        "direction": "output",
        "amount": 200,
        "allocation_fraction": 1.0
      },
      {
        "exchange_id": "ex_fcc_out",
        "process_uuid": "proc_fcc",
        "flow_uuid": "flow_recycle_to_atm",
        "direction": "output",
        "amount": 100,
        "allocation_fraction": 1.0
      },
      {
        "exchange_id": "ex_fcc_input",
        "process_uuid": "proc_fcc",
        "flow_uuid": "flow_feed_to_fcc",
        "direction": "input",
        "amount": 200
      },
      {
        "exchange_id": "ex_atm_input",
        "process_uuid": "proc_atm_vac",
        "flow_uuid": "flow_recycle_to_atm",
        "direction": "input",
        "amount": 100
      },
      {
        "exchange_id": "ex_atm_co2",
        "process_uuid": "proc_atm_vac",
        "flow_uuid": "flow_climate",
        "direction": "output",
        "amount": 10
      }
    ],
    "links": [
      {
        "consumer_process_uuid": "proc_fcc",
        "provider_process_uuid": "proc_atm_vac",
        "flow_uuid": "flow_feed_to_fcc",
        "quantity_mode": "single",
        "amount": 200,
        "consumer_amount": 200,
        "provider_amount": 200
      },
      {
        "consumer_process_uuid": "proc_atm_vac",
        "provider_process_uuid": "proc_fcc",
        "flow_uuid": "flow_recycle_to_atm",
        "quantity_mode": "single",
        "amount": 100,
        "consumer_amount": 100,
        "provider_amount": 100
      }
    ]
  }
}
```

This yields technosphere coefficients:

- `A(proc_atm_vac, proc_fcc) = -200 / 100 = -2.0`
- `A(proc_fcc, proc_atm_vac) = -100 / 200 = -0.5`

### 9. Common payload mistakes

- Sending `flow_type` as `elementary_flow` or `product_flow`
- Writing `reference_product_flow_uuid` as a flow uuid instead of the reference
  output exchange id
- Writing `single.amount` as provider output instead of consumer input
- Omitting the matching consumer input exchange for a recycle link
- Expecting `/v1/lcia` to solve one custom demand vector; current endpoint returns
  the full LCIA matrix for process columns

### API example (Python)

```python
import json
from pathlib import Path
from urllib import request

snapshot = json.loads(Path("cycle_test.json").read_text(encoding="utf-8"))
payload = json.dumps({"snapshot": snapshot}).encode("utf-8")

req = request.Request(
    "http://127.0.0.1:8000/v1/lcia",
    data=payload,
    headers={"Content-Type": "application/json"},
    method="POST",
)
with request.urlopen(req) as resp:
    print(resp.read().decode("utf-8"))
```

### Response fields

- `indicator_index`: list of indicator indices (rows)
- `process_index`: list of process UUIDs (columns)
- `values`: LCIA matrix (indicator x process)
- `mmr_path`: local path where the MMR was saved
- `issues`: warnings collected during matrix build

Start API (PowerShell):

```powershell
.\scripts\run_api.ps1
```
