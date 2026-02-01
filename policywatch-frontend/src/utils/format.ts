// src/utils/format.ts

const SMALL_WORDS = new Set([
  "a","an","and","as","at","but","by","for","from","if","in","into","nor",
  "of","on","or","per","the","to","via","with"
]);

function capWord(w: string) {
  if (!w) return w;

  // keep common acronyms
  const upper = w.toUpperCase();
  if (["US", "U.S.", "USA", "AI", "COVID-19", "HHS", "DOJ", "DHS", "EPA", "FDA"].includes(upper)) {
    return upper === "US" ? "U.S." : upper;
  }

  // handle possessives / punctuation safely
  return w.charAt(0).toUpperCase() + w.slice(1).toLowerCase();
}

export function titleCase(input: string) {
  const s = (input ?? "").trim();
  if (!s) return s;

  const words = s.split(/\s+/);
  return words
    .map((word, i) => {
      const clean = word.replace(/^[("']+|[)"'.:,;!?]+$/g, "");
      const punctPrefix = word.match(/^[("']+/)?.[0] ?? "";
      const punctSuffix = word.match(/[)"'.:,;!?]+$/)?.[0] ?? "";

      const lower = clean.toLowerCase();
      const isSmall = i !== 0 && i !== words.length - 1 && SMALL_WORDS.has(lower);

      const cased = isSmall ? lower : capWord(clean);
      return `${punctPrefix}${cased}${punctSuffix}`;
    })
    .join(" ");
}

// white-house -> White House, north_carolina -> North Carolina
export function humanizeKey(key: string) {
  return titleCase(String(key ?? "").replace(/[-_]+/g, " ").trim());
}

// "White House — Briefings & Statements" -> "White House · Briefings & Statements"
// also fixes accidental ALL CAPS
export function prettySourceLine(name: string) {
  const s = String(name ?? "").trim();
  if (!s) return s;

  // normalize separators to a clean bullet-dot
  const normalizedSep = s.replace(/\s*[—–-]\s*/g, " · ");
  return titleCase(normalizedSep);
}
