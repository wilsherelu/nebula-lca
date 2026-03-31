import { describe, it, expect } from "vitest";

describe("简单测试", () => {
  it("应该通过", () => {
    expect(1 + 1).toBe(2);
  });

  it("字符串测试", () => {
    expect("hello").toBe("hello");
  });
});
