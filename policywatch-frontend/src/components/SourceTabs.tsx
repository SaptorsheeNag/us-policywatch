// src/components/SourceTabs.tsx
import React from "react";
import type { SourceKey } from "../types";

const LABELS: { key: SourceKey; label: string }[] = [
  { key: "white-house", label: "White House" },
  { key: "florida", label: "Florida" },
  { key: "texas", label: "Texas" },
  { key: "new-york", label: "New York" },
  { key: "pennsylvania", label: "Pennsylvania" },
  { key: "illinois", label: "Illinois" },
  { key: "massachusetts", label: "Massachusetts" },
  { key: "washington", label: "Washington" },
  { key: "california", label: "California" },

  { key: "utah", label: "Utah" },
  { key: "ohio", label: "Ohio" },
  { key: "vermont", label: "Vermont" },
  { key: "arizona", label: "Arizona" },
  { key: "virginia", label: "Virginia" },
  { key: "georgia", label: "Georgia" },
  { key: "hawaii", label: "Hawaii" },
  { key: "alaska", label: "Alaska" },
  { key: "new-jersey", label: "New Jersey" },
  { key: "maryland", label: "Maryland" },
  { key: "colorado", label: "Colorado" },
  { key: "minnesota", label: "Minnesota" },
  { key: "oregon", label: "Oregon" },
  { key: "michigan", label: "Michigan" },

  // ⭐ High-priority additions
  { key: "north-carolina", label: "North Carolina" },
  { key: "wisconsin", label: "Wisconsin" },
  { key: "nevada", label: "Nevada" },
  { key: "tennessee", label: "Tennessee" },

  // 🟡 Second wave
  { key: "south-carolina", label: "South Carolina" },
  { key: "iowa", label: "Iowa" },
  { key: "missouri", label: "Missouri" },
  { key: "kansas", label: "Kansas" },
  { key: "new-mexico", label: "New Mexico" },
];

interface Props {
  active: SourceKey;
  onChange: (source: SourceKey) => void;
  allowedKeys?: SourceKey[];
}

export const SourceTabs: React.FC<Props> = ({ active, onChange, allowedKeys }) => {
  const list = allowedKeys?.length ? LABELS.filter(x => allowedKeys.includes(x.key)) : LABELS;

  return (
    <nav className="tabs">
      {list.map((s) => (
        <button
          key={s.key}
          className={`tab ${active === s.key ? "tab--active" : ""}`}
          onClick={() => onChange(s.key)}
        >
          {s.label}
        </button>
      ))}
    </nav>
  );
};
