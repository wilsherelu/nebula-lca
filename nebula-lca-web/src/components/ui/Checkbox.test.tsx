import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { Checkbox } from "./Checkbox";

describe("Checkbox", () => {
  it("renders a controlled checkbox and reports changes", () => {
    const onCheckedChange = vi.fn();
    render(
      <Checkbox
        checked={false}
        ariaLabel="Define as product"
        onCheckedChange={onCheckedChange}
      />,
    );

    const checkbox = screen.getByRole("checkbox", { name: "Define as product" }) as HTMLInputElement;
    expect(checkbox.checked).toBe(false);

    fireEvent.click(checkbox);
    expect(onCheckedChange).toHaveBeenCalledWith(true);
  });

  it("renders the native input as disabled", () => {
    const onCheckedChange = vi.fn();
    render(
      <Checkbox
        checked
        disabled
        ariaLabel="Define as product"
        onCheckedChange={onCheckedChange}
      />,
    );

    const checkbox = screen.getByRole("checkbox", { name: "Define as product" }) as HTMLInputElement;
    expect(checkbox.disabled).toBe(true);
    expect(checkbox.checked).toBe(true);
  });
});
