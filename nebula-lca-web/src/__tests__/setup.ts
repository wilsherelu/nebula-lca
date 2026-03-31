// Mock localStorage
const localStorageMock = {
  store: {} as Record<string, string>,
  clear() {
    this.store = {};
  },
  getItem(key: string) {
    return this.store[key] || null;
  },
  setItem(key: string, value: string) {
    this.store[key] = String(value);
  },
  removeItem(key: string) {
    delete this.store[key];
  },
};

Object.defineProperty(window, "localStorage", {
  value: localStorageMock,
});

// Mock crypto.randomUUID
if (!global.crypto.randomUUID) {
  Object.defineProperty(global.crypto, "randomUUID", {
    value: () => "test-uuid-" + Math.random().toString(36).slice(2),
    writable: true,
  });
}

// Mock performance
const originalPerformance = global.performance;
if (!global.performance?.mark) {
  Object.defineProperty(global, "performance", {
    value: {
      mark: () => { },
      measure: () => { },
      clearMarks: () => { },
      clearMeasures: () => { },
      now: () => Date.now(),
      ...originalPerformance,
    },
    writable: true,
  });
}
