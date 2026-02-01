import React, { useMemo, useState } from "react";
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
  { key: "north-carolina", label: "North Carolina" },
  { key: "wisconsin", label: "Wisconsin" },
  { key: "nevada", label: "Nevada" },
  { key: "tennessee", label: "Tennessee" },
  { key: "south-carolina", label: "South Carolina" },
  { key: "iowa", label: "Iowa" },
  { key: "missouri", label: "Missouri" },
  { key: "kansas", label: "Kansas" },
  { key: "new-mexico", label: "New Mexico" },
];

type Props = {
  open: boolean;
  fullName: string;
  initial: string[];
  onClose: () => void;
  onSave: (sources: string[]) => Promise<void>;
};

export const PreferencesModal: React.FC<Props> = ({ open, fullName, initial, onClose, onSave }) => {
  const [selected, setSelected] = useState<string[]>(initial ?? []);
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  React.useEffect(() => {
    if (open) {
      setSelected(initial ?? []);
      setErr(null);
    }
  }, [open, initial]);

  const toggle = (k: string) => {
    setSelected((prev) => (prev.includes(k) ? prev.filter((x) => x !== k) : [...prev, k]));
  };

  const prettyName = useMemo(() => (fullName?.trim() ? fullName.trim() : "there"), [fullName]);

  if (!open) return null;

  const save = async () => {
    setErr(null);
    setBusy(true);
    try {
      // ✅ empty list means "show everything" (default)
      await onSave(selected);
      onClose();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to save preferences.");
    } finally {
      setBusy(false);
    }
  };


  return (
    <div className="glassOverlay" role="dialog" aria-modal="true" onMouseDown={() => !busy && onClose()}>
      <div className="glassModal" onMouseDown={(e) => e.stopPropagation()}>
        <div className="glassModal__bgBlob" aria-hidden="true" />
        <div className="glassModal__top">
          <div className="glassModal__title">Welcome, {prettyName} ✦</div>
          <button className="glassModal__close" onClick={onClose} disabled={busy} aria-label="Close">
            ×
          </button>
        </div>

        <div className="glassModal__subtitle">
          Choose what you want on your home feed. You can change this anytime.
        </div>

        {err && <div className="glassNotice glassNotice--error">{err}</div>}

        <div className="prefGrid">
          {LABELS.map((s) => {
            const on = selected.includes(s.key);
            return (
              <button
                key={s.key}
                className={`prefChip ${on ? "is-on" : ""}`}
                onClick={() => toggle(s.key)}
                disabled={busy}
                type="button"
              >
                {s.label}
              </button>
            );
          })}
        </div>

        <div className="glassModal__actions">
          <button className="glassBtn" onClick={save} disabled={busy}>
            Save preferences
          </button>
        </div>
      </div>
    </div>
  );
};
