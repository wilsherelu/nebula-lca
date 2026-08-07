import { describe, expect, it } from "vitest";

import { parseImportedRows } from "./UnitProcessImportDialog";


describe("parseImportedRows", () => {
  it("preserves co-products while also keeping the reference product", () => {
    const [process] = parseImportedRows(
      {
        imported_processes: [
          {
            process_uuid: "process-1",
            process_name: "Process 1",
            reference_flow_uuid: "flow-a",
            outputs: [
              {
                flow_uuid: "flow-a",
                flow_name: "Product A",
                unit: "kg",
                direction: "output",
                is_product: true,
                allocation_factor: 0.25,
                allocation_basis: { method: "manual_factor" },
              },
              {
                flow_uuid: "flow-b",
                flow_name: "Product B",
                unit: "kg",
                direction: "output",
                is_product: true,
                allocation_factor: 0.75,
                allocation_basis: { method: "manual_factor" },
              },
            ],
          },
        ],
      },
      "locked",
      "zh",
    );

    expect(process.outputs.filter((port) => port.isProduct)).toHaveLength(2);
    expect(process.outputs.map((port) => port.allocationFactor)).toEqual([0.25, 0.75]);
  });
});
