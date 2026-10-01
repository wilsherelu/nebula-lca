import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { FlowPickerDialog } from "./FlowPickerDialog";

describe("FlowPickerDialog", () => {
  it("shows explicit labels for the elementary-flow filters", () => {
    render(
      <FlowPickerDialog
        language="zh"
        elementary
        flows={[]}
        loading={false}
        error=""
        searchInput=""
        category=""
        categories={[]}
        source=""
        compatibleOnly={false}
        page={1}
        totalPages={1}
        total={0}
        refreshingFlowUuid=""
        onClose={vi.fn()}
        onSearchInputChange={vi.fn()}
        onSearch={vi.fn()}
        onCategoryChange={vi.fn()}
        onSourceChange={vi.fn()}
        onCompatibleOnlyChange={vi.fn()}
        onCreate={vi.fn()}
        onUse={vi.fn()}
        onRefresh={vi.fn()}
        onPageChange={vi.fn()}
        displayFlowType={(value) => value}
      />,
    );

    expect(screen.getByLabelText("按流名称检索")).toBeTruthy();
    expect(screen.getByLabelText("分类")).toBeTruthy();
    expect(screen.getByLabelText("来源")).toBeTruthy();
    expect(screen.getByLabelText("仅显示可转换流")).toBeTruthy();
  });

  it("warns when a catalog elementary flow has no exact source identity", () => {
    render(
      <FlowPickerDialog
        language="zh" elementary
        flows={[{flow_uuid: "flow-1", flow_name: "排放流", flow_type: "elementary_flow", default_unit: "kg", unit_group: "mass", source: "ef3.1"}]}
        loading={false} error="" searchInput="" category="" categories={[]} source=""
        compatibleOnly={false} page={1} totalPages={1} total={1} refreshingFlowUuid=""
        onClose={vi.fn()} onSearchInputChange={vi.fn()} onSearch={vi.fn()}
        onCategoryChange={vi.fn()} onSourceChange={vi.fn()} onCompatibleOnlyChange={vi.fn()}
        onCreate={vi.fn()} onUse={vi.fn()} onRefresh={vi.fn()} onPageChange={vi.fn()}
        displayFlowType={(value) => value}
      />,
    );
    expect(screen.getByText("缺少精确来源版本，不能直接用于 LCIA")).toBeTruthy();
  });
});
