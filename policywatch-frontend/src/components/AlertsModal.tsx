import React, { useEffect, useMemo, useRef, useState } from "react";
import type { SourceKey } from "../types";
import { createAlert, deleteAlert, fetchStatuses, listAlerts, updateAlert, type AlertDTO } from "../api";

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
  onClose: () => void;
};

type MuteChoice = "1h" | "8h" | "1d" | "1w" | "1mo" | "always";

const LS_UNTIL = (alertId: string) => `pw_alert_mute_until_${alertId}`;

function nowMs() {
  return Date.now();
}

function getMuteUntil(alertId: string): number | null {
  const raw = window.localStorage.getItem(LS_UNTIL(alertId));
  if (!raw) return null;
  const n = Number(raw);
  if (!Number.isFinite(n) || n <= 0) return null;
  if (n <= nowMs()) {
    window.localStorage.removeItem(LS_UNTIL(alertId));
    return null;
  }
  return n;
}

function setMuteUntil(alertId: string, untilMs: number) {
  window.localStorage.setItem(LS_UNTIL(alertId), String(untilMs));
}

function clearMuteUntil(alertId: string) {
  window.localStorage.removeItem(LS_UNTIL(alertId));
}

function choiceToUntil(choice: Exclude<MuteChoice, "always">): number {
  const base = nowMs();
  const H = 60 * 60 * 1000;
  switch (choice) {
    case "1h":
      return base + 1 * H;
    case "8h":
      return base + 8 * H;
    case "1d":
      return base + 24 * H;
    case "1w":
      return base + 7 * 24 * H;
    case "1mo":
      return base + 30 * 24 * H; // good enough
  }
}

function fmtUntil(tsMs: number): string {
  try {
    return new Date(tsMs).toLocaleString();
  } catch {
    return "later";
  }
}

export const AlertsModal: React.FC<Props> = ({ open, fullName, onClose }) => {
  const [busy, setBusy] = useState(false);
  const [err, setErr] = useState<string | null>(null);

  const [alerts, setAlerts] = useState<AlertDTO[]>([]);
  const [sourceKey, setSourceKey] = useState<SourceKey>("white-house");
  const [availableStatuses, setAvailableStatuses] = useState<string[]>([]);
  const [pickedStatuses, setPickedStatuses] = useState<string[]>([]);

  // mute picker UI
  const [muteOpenFor, setMuteOpenFor] = useState<AlertDTO | null>(null);

  // ✅ Inline mute panel scroll ref
  const mutePanelRef = useRef<HTMLDivElement | null>(null);

  // ✅ Auto-scroll to mute panel when opened
  useEffect(() => {
    if (!muteOpenFor) return;

    // let layout paint first
    window.setTimeout(() => {
      mutePanelRef.current?.scrollIntoView({
        behavior: "smooth",
        block: "nearest",
      });
    }, 0);
  }, [muteOpenFor]);

  const prettyName = useMemo(() => (fullName?.trim() ? fullName.trim() : "there"), [fullName]);

  const reloadAlerts = async () => {
    const a = await listAlerts();
    setAlerts(a);
  };

  useEffect(() => {
    if (!open) return;
    let cancelled = false;

    (async () => {
      try {
        setErr(null);
        setBusy(true);
        const a = await listAlerts();
        if (!cancelled) setAlerts(a);
      } catch (e: any) {
        if (!cancelled) setErr(e?.message ?? "Failed to load alerts.");
      } finally {
        if (!cancelled) setBusy(false);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [open]);

  useEffect(() => {
    if (!open) return;
    let cancelled = false;

    (async () => {
      try {
        const st = await fetchStatuses(sourceKey);
        if (cancelled) return;
        setAvailableStatuses(st);
        setPickedStatuses([]); // reset when source changes
      } catch {
        if (!cancelled) {
          setAvailableStatuses([]);
          setPickedStatuses([]);
        }
      }
    })();

    return () => {
      cancelled = true;
    };
  }, [open, sourceKey]);

  if (!open) return null;

  const toggleStatus = (s: string) => {
    setPickedStatuses((prev) => (prev.includes(s) ? prev.filter((x) => x !== s) : [...prev, s]));
  };

  // ✅ IMPORTANT FIX:
  // If user selects multiple statuses, we create multiple alerts
  // so you get one “latest item” stream per status.
  const addAlert = async () => {
    setErr(null);
    setBusy(true);
    try {
      const base = {
        source_key: sourceKey,
        categories: [],
        enabled: true,
        muted: false,
      };

      if (pickedStatuses.length > 0) {
        // one alert per status
        await Promise.all(
          pickedStatuses.map((st) =>
            createAlert({
              ...(base as any),
              statuses: [st],
            } as any)
          )
        );
      } else {
        // “All types” alert
        await createAlert({
          ...(base as any),
          statuses: [],
        } as any);
      }

      await reloadAlerts();
      setPickedStatuses([]);
    } catch (e: any) {
      setErr(e?.message ?? "Failed to create alert.");
    } finally {
      setBusy(false);
    }
  };

  const remove = async (id: string) => {
    setErr(null);
    setBusy(true);
    try {
      await deleteAlert(id);
      await reloadAlerts();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to delete alert.");
    } finally {
      setBusy(false);
    }
  };

  const unmute = async (a: AlertDTO) => {
    setErr(null);
    setBusy(true);
    try {
      clearMuteUntil(a.id);
      await updateAlert(a.id, {
        source_key: a.source_key,
        statuses: a.statuses,
        categories: a.categories,
        enabled: a.enabled,
        muted: false,
      } as any);
      await reloadAlerts();
    } catch (e: any) {
      setErr(e?.message ?? "Failed to unmute alert.");
    } finally {
      setBusy(false);
    }
  };

  const applyMute = async (a: AlertDTO, choice: MuteChoice) => {
    setErr(null);
    setBusy(true);
    try {
      if (choice === "always") {
        clearMuteUntil(a.id);
        await updateAlert(a.id, {
          source_key: a.source_key,
          statuses: a.statuses,
          categories: a.categories,
          enabled: a.enabled,
          muted: true,
        } as any);
      } else {
        // timed mute (client-side)
        const until = choiceToUntil(choice);
        setMuteUntil(a.id, until);

        // ensure backend isn't permanently muted
        if (a.muted) {
          await updateAlert(a.id, {
            source_key: a.source_key,
            statuses: a.statuses,
            categories: a.categories,
            enabled: a.enabled,
            muted: false,
          } as any);
        }
      }

      await reloadAlerts();
      setMuteOpenFor(null);
    } catch (e: any) {
      setErr(e?.message ?? "Failed to mute alert.");
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="glassOverlay" role="dialog" aria-modal="true" onMouseDown={() => !busy && onClose()}>
      <div className="glassModal glassModal--wide" onMouseDown={(e) => e.stopPropagation()}>
        <div className="glassModal__bgBlob" aria-hidden="true" />
        <div className="glassModal__top">
          <div className="glassModal__title">Alerts for {prettyName}</div>
          <button className="glassModal__close" onClick={onClose} disabled={busy} aria-label="Close">
            ×
          </button>
        </div>

        <div className="glassModal__subtitle">
          Pick a state/source, then choose feed types. We’ll notify you when a new item appears.
          <br />
          <b>Tip:</b> Selecting multiple feed types creates <b>multiple alerts</b> so you get one notification per group.
        </div>

        {err && <div className="glassNotice glassNotice--error">{err}</div>}

        <div className="alertBuilder">
          <div className="alertBuilder__col">
            <div className="glassLabel">Source</div>
            <div className="prefGrid prefGrid--compact">
              {LABELS.map((s) => (
                <button
                  key={s.key}
                  className={`prefChip ${sourceKey === s.key ? "is-on" : ""}`}
                  onClick={() => setSourceKey(s.key)}
                  disabled={busy}
                  type="button"
                >
                  {s.label}
                </button>
              ))}
            </div>
          </div>

          <div className="alertBuilder__col">
            <div className="glassLabel">Feed types</div>
            {availableStatuses.length === 0 ? (
              <div className="glassHint">No feed types found for this source yet.</div>
            ) : (
              <div className="prefGrid prefGrid--compact">
                {availableStatuses.map((s) => {
                  const on = pickedStatuses.includes(s);
                  return (
                    <button
                      key={s}
                      className={`prefChip ${on ? "is-on" : ""}`}
                      onClick={() => toggleStatus(s)}
                      disabled={busy}
                      type="button"
                      title="This maps to items.status"
                    >
                      {s}
                    </button>
                  );
                })}
              </div>
            )}

            <div className="glassModal__actions glassModal__actions--row">
              <button className="glassBtn" onClick={addAlert} disabled={busy}>
                Add alert{pickedStatuses.length > 1 ? "s" : ""}
              </button>
            </div>
          </div>
        </div>

        <div className="glassDivider" />

        <div className="glassLabel">Your alerts</div>

        {alerts.length === 0 ? (
          <div className="glassHint">No alerts yet. Add one above.</div>
        ) : (
          <div className="alertList">
            {alerts.map((a) => {
              const until = getMuteUntil(a.id);
              const mutedLabel = a.muted
                ? "Muted (always)"
                : until
                ? `Muted until ${fmtUntil(until)}`
                : "Active";

              const showUnmute = a.muted || until;

              return (
                <div key={a.id} className="alertRow">
                  <div className="alertRow__main">
                    <div className="alertRow__title">
                      <span className="alertTag">{a.source_key}</span>
                      {a.statuses?.length ? (
                        a.statuses.map((s) => (
                          <span key={s} className="alertTag alertTag--soft">
                            {s}
                          </span>
                        ))
                      ) : (
                        <span className="alertTag alertTag--soft">All types</span>
                      )}
                    </div>
                    <div className="alertRow__meta">{mutedLabel}</div>
                  </div>

                  <div className="alertRow__actions">
                    {showUnmute ? (
                      <button className="glassBtn glassBtn--ghost" onClick={() => unmute(a)} disabled={busy}>
                        Unmute
                      </button>
                    ) : (
                      <button className="glassBtn glassBtn--ghost" onClick={() => setMuteOpenFor(a)} disabled={busy}>
                        Mute
                      </button>
                    )}

                    <button className="glassBtn glassBtn--danger" onClick={() => remove(a.id)} disabled={busy}>
                      Delete
                    </button>
                  </div>
                </div>
              );
            })}
          </div>
        )}

        {/* ✅ Inline Mute panel (no nested modal) */}
        {muteOpenFor && (
          <div ref={mutePanelRef} className="mutePanel">
            <div className="mutePanel__bgBlob" aria-hidden="true" />

            <div className="mutePanel__top">
              <div className="mutePanel__title">Mute alert</div>
              <button
                className="mutePanel__close"
                onClick={() => setMuteOpenFor(null)}
                disabled={busy}
                aria-label="Close"
              >
                ×
              </button>
            </div>

            <div className="mutePanel__subtitle">
              Choose how long to mute this alert. “Always” mutes on the server. Others are timed and will auto-unmute.
            </div>

            <div className="prefGrid">
              <button className="prefChip" disabled={busy} onClick={() => applyMute(muteOpenFor, "1h")}>1 hour</button>
              <button className="prefChip" disabled={busy} onClick={() => applyMute(muteOpenFor, "8h")}>8 hours</button>
              <button className="prefChip" disabled={busy} onClick={() => applyMute(muteOpenFor, "1d")}>1 day</button>
              <button className="prefChip" disabled={busy} onClick={() => applyMute(muteOpenFor, "1w")}>1 week</button>
              <button className="prefChip" disabled={busy} onClick={() => applyMute(muteOpenFor, "1mo")}>1 month</button>
              <button className="prefChip is-on" disabled={busy} onClick={() => applyMute(muteOpenFor, "always")}>Always</button>
            </div>
          </div>
        )}

      </div>
    </div>
  );
};
