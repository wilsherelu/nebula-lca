type UiLanguage = "zh" | "en";

const TEXT_KEYS = ["#text", "text", "value", "@value"] as const;

const pickFromStructuredValue = (value: unknown, language: UiLanguage): string => {
  if (value === null || value === undefined) return "";
  if (typeof value === "string" || typeof value === "number") return String(value).trim();
  if (Array.isArray(value)) {
    const preferred = value.find((item) => {
      if (!item || typeof item !== "object") return false;
      const lang = String((item as Record<string, unknown>)["@xml:lang"] ?? "").toLowerCase();
      return lang.startsWith(language);
    });
    return pickFromStructuredValue(preferred, language)
      || value.map((item) => pickFromStructuredValue(item, language)).find(Boolean)
      || "";
  }
  if (typeof value === "object") {
    const record = value as Record<string, unknown>;
    for (const key of TEXT_KEYS) {
      const text = pickFromStructuredValue(record[key], language);
      if (text) return text;
    }
  }
  return "";
};

const pickFromSerializedValue = (value: string, language: UiLanguage): string => {
  const trimmed = value.trim();
  if (!trimmed.startsWith("{") && !trimmed.startsWith("[")) return trimmed;
  try {
    return pickFromStructuredValue(JSON.parse(trimmed), language) || trimmed;
  } catch {
    const matches = Array.from(
      trimmed.matchAll(/["']#text["']\s*:\s*(["'])(.*?)\1\s*(?=,|})/g),
    );
    if (matches.length === 0) return trimmed;
    const languageMatch = matches.find((match) => {
      const tail = trimmed.slice(match.index ?? 0, (match.index ?? 0) + match[0].length + 80);
      return new RegExp(`["']@xml:lang["']\\s*:\\s*["']${language}`, "i").test(tail);
    });
    return (languageMatch?.[2] ?? matches[0]?.[2] ?? trimmed).trim();
  }
};

export const getLocalizedText = (
  value: unknown,
  language: UiLanguage,
  fallback = "",
): string => {
  const text = typeof value === "string"
    ? pickFromSerializedValue(value, language)
    : pickFromStructuredValue(value, language);
  return text || fallback;
};
