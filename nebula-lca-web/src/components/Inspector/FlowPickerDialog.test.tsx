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

    expect(screen.getByLabelText("关键词")).toBeTruthy();
    expect(screen.getByLabelText("分类")).toBeTruthy();
    expect(screen.getByLabelText("来源")).toBeTruthy();
    expect(screen.getByLabelText("仅显示可转换流")).toBeTruthy();
  });
});
