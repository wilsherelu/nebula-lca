import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { ProviderCandidate } from "../../services/intermediateFlowLinks";
import { BackgroundLciPickerDialog } from "./BackgroundLciPickerDialog";

const provider = (index: number, overrides: Partial<ProviderCandidate> = {}): ProviderCandidate => ({
  process_uuid: `process-${index}`,
  process_name: `production process ${index}`,
  location: index % 2 === 0 ? "CN" : "GLO",
  reference_product_name: "electricity, low voltage",
  reference_product_unit: "kWh",
  has_lci_vector: true,
  vector_nnz: 100 + index,
  ...overrides,
});

describe("BackgroundLciPickerDialog", () => {
  it("filters providers and selects one from the table", () => {
    const onSelect = vi.fn();
    render(
      <BackgroundLciPickerDialog
        open
        busy={false}
        language="zh"
        providers={[
          provider(1, { process_name: "market for electricity, low voltage", location: "CN" }),
          provider(2, { process_name: "electricity production, photovoltaic", location: "GLO" }),
        ]}
        onClose={vi.fn()}
        onSelect={onSelect}
      />,
    );

    fireEvent.change(screen.getByLabelText("过程类型"), { target: { value: "market" } });
    expect(screen.getByText("market for electricity, low voltage")).toBeTruthy();
    expect(screen.queryByText("electricity production, photovoltaic")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "关联" }));
    expect(onSelect).toHaveBeenCalledWith(expect.objectContaining({ process_uuid: "process-1" }));
  });

  it("paginates large provider lists", () => {
    render(
      <BackgroundLciPickerDialog
        open
        busy={false}
        language="zh"
        providers={Array.from({ length: 25 }, (_, index) => provider(index + 1))}
        onClose={vi.fn()}
        onSelect={vi.fn()}
      />,
    );

    expect(screen.getByText("production process 1")).toBeTruthy();
    expect(screen.queryByText("production process 25")).toBeNull();
    fireEvent.click(screen.getByRole("button", { name: "下一页" }));
    expect(screen.getByText("production process 25")).toBeTruthy();
  });
});
