import React, { useEffect, useMemo, useRef, useState } from "react";

type AlertToastItem = {
  alert: { id: string; source_key: string; statuses: string[]; categories: string[]; muted?: boolean };
  delivery: { id: string; delivered_at: string; acknowledged_at: string | null };
  item: {
    title: string;
    summary: string;
    url: string;
    source_name?: string | null;
    jurisdiction?: string | null;
    status?: string | null;
    categories?: string[] | null;

    ai_impact_score?: number | null;
    ai_impact_status?: string | null;
  };
};

type MuteChoice = "1h" | "8h" | "1d" | "1w" | "1mo" | "always";

type Props = {
  n: AlertToastItem;

  // ✅ Got it = ACK (do not show again until new item ingested for this alert stream)
  onGotIt: (deliveryId: string) => Promise<void> | void;

  // ✅ Close = TEMP dismiss only (will come back next poll)
  onCloseTemp: (alertId: string) => void;

  // mute/unmute
  isMuted: (alertId: string) => boolean;
  onMute: (alertId: string, choice: MuteChoice) => Promise<void> | void;
  onUnmute: (alertId: string) => Promise<void> | void;
};

function fmtScore(x: unknown): string {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(2);
}

function scoreClass(score: number): string {
  if (score < -0.2) return "toastImpactChip toastImpactChip--neg";
  if (score > 0.2) return "toastImpactChip toastImpactChip--pos";
  return "toastImpactChip toastImpactChip--neu";
}

export const AlertToast: React.FC<Props> = ({ n, onGotIt, onCloseTemp, isMuted, onMute, onUnmute }) => {
  const [leaving, setLeaving] = useState(false);
  const leaveTimer = useRef<number | null>(null);

  const [muteOpen, setMuteOpen] = useState(false);

  const item = n.item;
  const deliveryId = n.delivery.id;
  const alertId = n.alert.id;

  const muted = isMuted(alertId);

  const where = useMemo(() => {
    return (item.jurisdiction || item.source_name || n.alert.source_key || "").toString();
  }, [item.jurisdiction, item.source_name, n.alert.source_key]);

  const status = (item.status || "").toString();
  const cats = Array.isArray(item.categories) ? item.categories : [];

  const impactScore = typeof item.ai_impact_score === "number" ? item.ai_impact_score : null;
  const impactStatus = (item.ai_impact_status || "").toString();

  const closeTemp = () => {
    if (leaving) return;
    setLeaving(true);
    leaveTimer.current = window.setTimeout(() => {
      onCloseTemp(alertId);
    }, 340);
  };

  const gotIt = async () => {
    if (leaving) return;
    setLeaving(true);
    leaveTimer.current = window.setTimeout(async () => {
      await onGotIt(deliveryId);
    }, 340);
  };

  useEffect(() => {
    return () => {
      if (leaveTimer.current) window.clearTimeout(leaveTimer.current);
    };
  }, []);

  return (
    <div className={`alertToast ${leaving ? "is-leaving" : ""}`}>
      <div className="alertToast__bgBlob" aria-hidden="true" />

      <div className="alertToast__top">
        <div className="alertToast__kicker">
          <span className="alertToast__kickerLabel">NEW ALERT</span>
          <span className="alertToast__kickerSource">{where}</span>

          {status ? <span className="alertToast__pill">{status}</span> : null}
          {cats?.[0] ? <span className="alertToast__pill alertToast__pill--soft">{cats[0]}</span> : null}
        </div>

        {/* ✅ Close = temp dismiss only */}
        <button className="alertToast__close" onClick={closeTemp} aria-label="Close">
          ×
        </button>
      </div>

      <a className="alertToast__title" href={item.url} target="_blank" rel="noreferrer">
        {item.title}
      </a>

      {item.summary ? <div className="alertToast__summary">{item.summary}</div> : null}

      <div className="alertToast__impactRow">
        <div className="alertToast__impactLabel">AI impact</div>

        {impactScore != null ? (
          <div className={scoreClass(impactScore)} title="AI impact score in [-1, 1]">
            {fmtScore(impactScore)}
          </div>
        ) : (
          <div className="toastImpactChip toastImpactChip--muted">pending</div>
        )}

        {impactStatus ? <div className="alertToast__impactStatus">{impactStatus}</div> : null}

        <div className="alertToast__spacer" />

        {muted ? (
          <button className="alertToast__btn" onClick={() => onUnmute(alertId)}>
            Unmute
          </button>
        ) : (
          <button className="alertToast__btn" onClick={() => setMuteOpen((v) => !v)}>
            Mute
          </button>
        )}

        {/* ✅ Got it = ACK */}
        <button className="alertToast__btn alertToast__btn--primary" onClick={gotIt}>
          Got it
        </button>
      </div>

      {/* Inline mute chooser */}
      {!muted && muteOpen && (
        <div style={{ marginTop: 10, display: "flex", gap: 8, flexWrap: "wrap" }}>
          <button className="alertToast__btn" onClick={() => onMute(alertId, "1h")}>1 hour</button>
          <button className="alertToast__btn" onClick={() => onMute(alertId, "8h")}>8 hours</button>
          <button className="alertToast__btn" onClick={() => onMute(alertId, "1d")}>1 day</button>
          <button className="alertToast__btn" onClick={() => onMute(alertId, "1w")}>1 week</button>
          <button className="alertToast__btn" onClick={() => onMute(alertId, "1mo")}>1 month</button>
          <button className="alertToast__btn alertToast__btn--primary" onClick={() => onMute(alertId, "always")}>
            Always
          </button>
        </div>
      )}
    </div>
  );
};
