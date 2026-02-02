// src/App.tsx
import React, { useEffect, useMemo, useRef, useState } from "react";
import "./App.css";
import { SourceTabs } from "./components/SourceTabs";
import { ItemCard } from "./components/ItemCard";
import type { PolicyItem, SourceKey, WhatsNewEntry } from "./types";
import { fetchItems, fetchStatuses, fetchWhatsNew, type SortMode } from "./api";
import { WhatsNewCarousel } from "./components/WhatsNewCarousel";
import usFlag from "./assets/us-flag.jpg";
import type { Session, User } from "@supabase/supabase-js";
import { supabase } from "./supabaseClient";
import { AuthModal } from "./components/AuthModal";
import { PreferencesModal } from "./components/PreferencesModal";
import { AlertsModal } from "./components/AlertsModal";
import { AlertToast } from "./components/AlertToast";
import {
  ackDelivery,
  fetchPreferences,
  pollAlerts,
  savePreferences,
  listAlerts,
  updateAlert,
} from "./api";
import { humanizeKey } from "./utils/format";

const DEFAULT_SOURCE: SourceKey = "white-house";

export const App: React.FC = () => {
  const [activeSource, setActiveSource] = useState<SourceKey>(DEFAULT_SOURCE);
  const [items, setItems] = useState<PolicyItem[]>([]);
  const [sourceName, setSourceName] = useState<string>("White House");
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const [whatsNew, setWhatsNew] = useState<WhatsNewEntry[]>([]);

  // pagination state
  const [page, setPage] = useState<number>(1);
  const [pageSize, setPageSize] = useState<number>(40);
  const [total, setTotal] = useState<number>(0);

  const totalPages = Math.max(1, pageSize > 0 ? Math.ceil(total / pageSize) : 1);

  // ✅ NEW: sort + group-by(status) + date range filters
  const [sortMode, setSortMode] = useState<SortMode>("desc"); // default latest→oldest
  const [statuses, setStatuses] = useState<string[]>([]);
  const [statusFilter, setStatusFilter] = useState<string>(""); // "" = all

  // use input type=date (YYYY-MM-DD)
  const [dateFrom, setDateFrom] = useState<string>("");
  const [dateTo, setDateTo] = useState<string>("");

  // pagination chunking (groups of 10 pages)
  const CHUNK_SIZE = 10;
  const [pageChunk, setPageChunk] = useState<number>(0);

  const [session, setSession] = useState<Session | null>(null);
  void session; // keep for later (alerts/preferences will use access_token)
  const [user, setUser] = useState<User | null>(null);

  const [authOpen, setAuthOpen] = useState(false);
  const [authMode, setAuthMode] = useState<"login" | "signup" | "forgot" | "updatePassword">("login");

  const [prefsOpen, setPrefsOpen] = useState(false);
  const [alertsOpen, setAlertsOpen] = useState(false);
  const [preferredSources, setPreferredSources] = useState<string[] | null>(null);
  const [toastQueue, setToastQueue] = useState<any[]>([]);

  // --------------------
  // Alerts: mute/dismiss/sound + queue handling
  // --------------------
  type MuteChoice = "1h" | "8h" | "1d" | "1w" | "1mo" | "always";

  const LS_UNTIL = (alertId: string) => `pw_alert_mute_until_${alertId}`;
  const DISMISS_UNTIL = (alertId: string) => `pw_alert_dismiss_until_${alertId}`;

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
        return base + 30 * 24 * H;
    }
  }

  function getTempDismissed(alertId: string): boolean {
    const raw = window.localStorage.getItem(DISMISS_UNTIL(alertId));
    if (!raw) return false;
    const n = Number(raw);
    if (!Number.isFinite(n) || n <= 0) return false;
    if (n <= nowMs()) {
      window.localStorage.removeItem(DISMISS_UNTIL(alertId));
      return false;
    }
    return true;
  }

  function setTempDismissed(alertId: string, minutes: number) {
    window.localStorage.setItem(
      DISMISS_UNTIL(alertId),
      String(nowMs() + minutes * 60 * 1000)
    );
  }

  // --- helper: sound ---
  const audioCtxRef = useRef<AudioContext | null>(null);

  // ✅ Date input refs (so clicking the icon can open the picker)
  const dateFromRef = useRef<HTMLInputElement | null>(null);
  const dateToRef = useRef<HTMLInputElement | null>(null);

  const openDatePicker = (ref: React.RefObject<HTMLInputElement | null>) => {
    const el = ref.current;
    if (!el) return;

    // ✅ Best case (Chrome/Edge/Safari modern)
    const anyEl = el as any;
    if (typeof anyEl.showPicker === "function") {
      anyEl.showPicker();
      return;
    }

    // ✅ Fallback
    el.focus();
    el.click();
  };

  const ensureAudioUnlocked = async () => {
    const AudioCtx = window.AudioContext || (window as any).webkitAudioContext;
    if (!AudioCtx) return;

    if (!audioCtxRef.current) {
      audioCtxRef.current = new AudioCtx();
    }
    const ctx = audioCtxRef.current;

    // browsers often start suspended until a gesture
    if (ctx.state === "suspended") {
      try {
        await ctx.resume();
      } catch {
        // ignore
      }
    }
  };

  // Unlock audio on first user interaction (click/tap/key)
  useEffect(() => {
    const unlock = () => {
      void ensureAudioUnlocked();
      window.removeEventListener("pointerdown", unlock);
      window.removeEventListener("keydown", unlock);
    };
    window.addEventListener("pointerdown", unlock, { once: true });
    window.addEventListener("keydown", unlock, { once: true });

    return () => {
      window.removeEventListener("pointerdown", unlock);
      window.removeEventListener("keydown", unlock);
    };
  }, []);

  const playNotif = async () => {
    try {
      await ensureAudioUnlocked();
      const ctx = audioCtxRef.current;
      if (!ctx) return;

      const o = ctx.createOscillator();
      const g = ctx.createGain();

      o.type = "sine";
      o.frequency.setValueAtTime(880, ctx.currentTime);

      // quick envelope to avoid click
      g.gain.setValueAtTime(0.0001, ctx.currentTime);
      g.gain.exponentialRampToValueAtTime(0.05, ctx.currentTime + 0.01);
      g.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.18);

      o.connect(g);
      g.connect(ctx.destination);

      o.start();
      o.stop(ctx.currentTime + 0.2);
    } catch {
      // ignore
    }
  };

  // mute check (timed mute OR server mute if present)
  const isMuted = (alertId: string, serverMuted?: boolean) => {
    if (serverMuted) return true; // backend “always”
    const until = getMuteUntil(alertId);
    return until != null;
  };

  // Mute / Unmute actions (used by Toast)
  const onMuteAlert = async (alertId: string, choice: MuteChoice) => {
    if (choice === "always") {
      // server-side mute
      const all = await listAlerts();
      const a = all.find((x) => x.id === alertId);
      if (!a) return;

      await updateAlert(alertId, {
        source_key: a.source_key,
        statuses: a.statuses,
        categories: a.categories,
        enabled: a.enabled,
        muted: true,
      } as any);

      // remove any queued toasts for this alert immediately
      setToastQueue((prev) => prev.filter((x) => x.alert.id !== alertId));
      return;
    }

    // timed mute (client-side)
    setMuteUntil(alertId, choiceToUntil(choice));
    setToastQueue((prev) => prev.filter((x) => x.alert.id !== alertId));
  };

  const onUnmuteAlert = async (alertId: string) => {
    clearMuteUntil(alertId);

    // if server-muted, unmute there too
    try {
      const all = await listAlerts();
      const a = all.find((x) => x.id === alertId);
      if (!a) return;
      if (!a.muted) return;

      await updateAlert(alertId, {
        source_key: a.source_key,
        statuses: a.statuses,
        categories: a.categories,
        enabled: a.enabled,
        muted: false,
      } as any);
    } catch {
      // ignore
    }
  };

  // Close (×) = temporary dismiss only (no ACK)
  const onCloseTemp = (alertId: string) => {
    setTempDismissed(alertId, 5);
    setToastQueue((prev) => prev.filter((x) => x.alert.id !== alertId));
  };

  // Got it = ACK
  const onGotIt = async (deliveryId: string) => {
    try {
      await ackDelivery(deliveryId);
    } finally {
      setToastQueue((prev) => prev.filter((x) => x.delivery.id !== deliveryId));
    }
  };


  const openLogin = () => {
    setAuthMode("login");
    setAuthOpen(true);
  };

  const openSignup = () => {
    setAuthMode("signup");
    setAuthOpen(true);
  };

  const openPrefs = () => (user ? setPrefsOpen(true) : openLogin());
  const openAlerts = () => (user ? setAlertsOpen(true) : openLogin());

  useEffect(() => {
    let mounted = true;

    (async () => {
      const { data } = await supabase.auth.getSession();
      if (!mounted) return;
      setSession(data.session ?? null);
      setUser(data.session?.user ?? null);
    })();

    const { data: sub } = supabase.auth.onAuthStateChange((event, newSession) => {
      setSession(newSession ?? null);
      setUser(newSession?.user ?? null);

      // If user clicks reset-password link, Supabase triggers PASSWORD_RECOVERY
      if (event === "PASSWORD_RECOVERY") {
        setAuthMode("updatePassword");
        setAuthOpen(true);
      }
    });

    return () => {
      mounted = false;
      sub.subscription.unsubscribe();
    };
  }, []);

  // ✅ Load preferences whenever user changes (sign-in/sign-out)
  useEffect(() => {
    if (!user) {
      setPreferredSources(null);
      setPrefsOpen(false);
      setAlertsOpen(false);
      setToastQueue([]);
      return;
    }

    let cancelled = false;

    // ✅ show onboarding only once per sign-in session
    // (resets when user.id changes)
    const seenKey = `pw_prefs_seen_${user.id}`;
    const hasSeen = window.sessionStorage.getItem(seenKey) === "1";

    (async () => {
      try {
        const p = await fetchPreferences();
        if (cancelled) return;

        const prefRow = p.preferences ?? null;   // row exists?
        const sources = prefRow?.sources ?? [];  // [] is allowed = "show all"

        setPreferredSources(sources);

        // ✅ only force onboarding if there is NO row yet,
        // and only once per sign-in (no tab-switch annoyance)
        if (!prefRow && !hasSeen) {
          setPrefsOpen(true);
          window.sessionStorage.setItem(seenKey, "1");
          return;
        }

        // ✅ only enforce activeSource when prefs are restrictive
        if (sources.length > 0 && !sources.includes(activeSource)) {
          setActiveSource(sources[0] as any);
        }
      } catch {
        // ignore
      }
    })();

    return () => {
      cancelled = true;
    };
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user]);


  // ✅ Poll alerts every 5 minutes (signed-in only) + queue one-by-one
  useEffect(() => {
    if (!user) return;

    let cancelled = false;

    const run = async () => {
      try {
        const notes = await pollAlerts();
        if (cancelled) return;

        const filtered = (notes || []).filter((n: any) => {
          if (!n?.alert?.id || !n?.delivery?.id) return false;
          if (isMuted(n.alert.id, !!n.alert.muted)) return false;
          if (getTempDismissed(n.alert.id)) return false;
          return true;
        });

        if (!filtered.length) return;

        setToastQueue((prev) => {
          const have = new Set(prev.map((x: any) => x.delivery.id));
          const nextAdds = filtered.filter((x: any) => !have.has(x.delivery.id));
          if (!nextAdds.length) return prev;

          void playNotif();
          return [...prev, ...nextAdds];
        });
      } catch {
        // ignore
      }
    };

    void run(); // run immediately
    const t = window.setInterval(run, 5 * 60 * 1000);

    return () => {
      cancelled = true;
      window.clearInterval(t);
    };
  }, [user]);


  const logout = async () => {
    await supabase.auth.signOut();
  };

  const requestDeleteAccount = async () => {
    const ok = window.confirm("Delete your account permanently? This cannot be undone.");
    if (!ok) return;

    const { data } = await supabase.auth.getSession();
    const token = data.session?.access_token;

    if (!token) {
      alert("Not signed in.");
      return;
    }

    try {
      const res = await fetch(`${import.meta.env.VITE_API_BASE}/me`, {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      });

      if (!res.ok) {
        const txt = await res.text();
        throw new Error(txt || "Delete failed");
      }

      // after backend deletes auth user, local session may still exist briefly
      await supabase.auth.signOut();
      alert("Account deleted.");
    } catch (e: any) {
      alert(e?.message ?? "Failed to delete account.");
    }
  };



  useEffect(() => {
    setPageChunk(0);
  }, [activeSource]);

  useEffect(() => {
    const maxChunk = Math.max(0, Math.floor((totalPages - 1) / CHUNK_SIZE));
    setPageChunk((c) => Math.min(c, maxChunk));
  }, [totalPages]);

  useEffect(() => {
    const newChunk = Math.floor((page - 1) / CHUNK_SIZE);
    setPageChunk(newChunk);
  }, [page]);

  const chunkStart = pageChunk * CHUNK_SIZE + 1;
  const chunkEnd = Math.min(totalPages, chunkStart + CHUNK_SIZE - 1);

  const visiblePages = Array.from(
    { length: Math.max(0, chunkEnd - chunkStart + 1) },
    (_, i) => chunkStart + i
  );

  const canChunkPrev = pageChunk > 0;
  const canChunkNext = chunkEnd < totalPages;

  const goToPage = (p: number) => {
    const next = Math.min(Math.max(1, p), totalPages);
    setPage(next);
  };

  // ✅ Load statuses when source changes
  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        const st = await fetchStatuses(activeSource);
        if (cancelled) return;
        setStatuses(st);

        // If current statusFilter isn't available for this state, reset it.
        if (statusFilter && !st.includes(statusFilter)) {
          setStatusFilter("");
        }
      } catch (e) {
        // non-fatal; just show "All"
        if (!cancelled) setStatuses([]);
      }
    })();

    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeSource]);

  useEffect(() => {
    let cancelled = false;

    (async () => {
      try {
        const data = await fetchWhatsNew();
        if (cancelled) return;
        setWhatsNew(data.items ?? []);
      } catch (e) {
        // optional section — don’t break the rest of the UI
        console.warn("whats-new failed", e);
      }
    })();

    return () => {
      cancelled = true;
    };
  }, []);

  // ✅ Tabs allowed by saved preferences (signed-in only)
  const allowedKeys = useMemo(() => {
    if (!user) return undefined; // not signed in => show all
    if (!preferredSources?.length) return undefined;
    return preferredSources as any; // source keys already match SourceKey strings
  }, [user, preferredSources]);


  const fetchOpts = useMemo(() => {
    return {
      sort: sortMode,
      status: statusFilter ? statusFilter : null,
      dateFrom: dateFrom ? dateFrom : null,
      dateTo: dateTo ? dateTo : null,
    };
  }, [sortMode, statusFilter, dateFrom, dateTo]);

  const loadItems = async (source: SourceKey, pageNum: number) => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchItems(source, pageNum, fetchOpts);
      setItems(data.items);
      setSourceName(humanizeKey(data.source));
      setPageSize(data.page_size);
      setTotal(data.total);
    } catch (err: any) {
      console.error(err);
      setError(err.message ?? "Failed to load items");
      setItems([]);
    } finally {
      setLoading(false);
    }
  };

  // ✅ refetch when source/page OR filters change
  useEffect(() => {
    loadItems(activeSource, page);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeSource, page, fetchOpts]);

  // ✅ when filters change, reset to page 1 (keeps UI consistent)
  useEffect(() => {
    setPage(1);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeSource, sortMode, statusFilter, dateFrom, dateTo]);

  // ✅ Save preferences from modal
  const handleSavePrefs = async (sources: string[]) => {
    await savePreferences(sources);
    setPreferredSources(sources);

    if (sources.length && !sources.includes(activeSource)) {
      setActiveSource(sources[0] as any);
    }
  };


  const handleSourceChange = (src: SourceKey) => {
    if (src === activeSource) return;
    setActiveSource(src);
    setPage(1);
  };

  const handlePrevPage = () => {
    setPage((prev) => (prev > 1 ? prev - 1 : prev));
  };

  const handleNextPage = () => {
    setPage((prev) => (prev < totalPages ? prev + 1 : prev));
  };

  const handlePrevChunk = () => {
    setPageChunk((c) => Math.max(0, c - 1));
  };

  const handleNextChunk = () => {
    setPageChunk((c) => c + 1);
  };

  const clearFilters = () => {
    setSortMode("desc");
    setStatusFilter("");
    setDateFrom("");
    setDateTo("");
  };

  return (
    <div className="page">
      <div className="app">
        {/* ✅ Live status pill (top-left) — now scrolls away */}
        <div
          className="livePill"
          role="status"
          aria-label="Fetching American political updates in real time"
        >
          <span className="liveDots" aria-hidden="true">
            <span className="liveDot liveDot--blue" />
            <span className="liveDot liveDot--white" />
            <span className="liveDot liveDot--red" />
          </span>
          <span className="liveText">Fetching American Political Updates in Real time</span>
          <span className="liveSheen" aria-hidden="true" />
        </div>
        {/* ✅ Auth buttons (top-right) */}
        <div className="app__authbar">
          {user ? (
            <>
              <div className="app__authchip" title={user.email ?? ""}>
                <span className="app__authchipLabel">Signed in</span>
                <span className="app__authchipEmail">{user.email}</span>

                {/* ✅ Delete inside the signed-in box, under the email */}
                <button className="authPill authPill--danger authPill--compact" onClick={requestDeleteAccount}>
                  Delete account
                </button>
              </div>

              <button className="authPill" onClick={logout}>
                Logout
              </button>
            </>
          ) : (
            <>
              <button className="authPill" onClick={openLogin}>
                Login
              </button>
              <button className="authPill authPill--primary" onClick={openSignup}>
                Sign up
              </button>
            </>
          )}
        </div>
        <header className="app__header">
          <div className="headerGrain" />
          <div className="app__topbar">
            <div className="topbarGlass">
              <div className="app__topbar-center">
                <div className="madeby">
                  <span className="madeby__label">Made by</span>
                  <span className="madeby__name">Saptorshee Nag</span>
                </div>

                <div className="socials">
                  <a
                    className="socialbtn"
                    href="https://github.com/SaptorsheeNag"
                    target="_blank"
                    rel="noreferrer"
                    aria-label="GitHub"
                    title="GitHub"
                  >
                    <svg viewBox="0 0 24 24" className="socialbtn__icon" aria-hidden="true">
                      <path d="M12 .5a11.5 11.5 0 0 0-3.64 22.4c.58.1.79-.25.79-.56v-2.1c-3.22.7-3.9-1.38-3.9-1.38-.53-1.34-1.3-1.7-1.3-1.7-1.06-.73.08-.72.08-.72 1.17.08 1.78 1.2 1.78 1.2 1.04 1.78 2.73 1.27 3.4.97.11-.75.41-1.27.74-1.56-2.57-.29-5.28-1.29-5.28-5.74 0-1.27.45-2.31 1.2-3.13-.12-.3-.52-1.52.12-3.17 0 0 .97-.31 3.18 1.2a11 11 0 0 1 5.8 0c2.2-1.51 3.18-1.2 3.18-1.2.64 1.65.24 2.87.12 3.17.74.82 1.2 1.86 1.2 3.13 0 4.46-2.71 5.44-5.3 5.73.42.36.8 1.08.8 2.18v3.22c0 .31.2.67.8.56A11.5 11.5 0 0 0 12 .5z" />
                    </svg>
                  </a>

                  <a
                    className="socialbtn"
                    href="https://www.linkedin.com/in/saptorshee-nag-588294220/"
                    target="_blank"
                    rel="noreferrer"
                    aria-label="LinkedIn"
                    title="LinkedIn"
                  >
                    <svg viewBox="0 0 24 24" className="socialbtn__icon" aria-hidden="true">
                      <path d="M20.45 20.45h-3.55v-5.57c0-1.33-.02-3.05-1.86-3.05-1.86 0-2.14 1.45-2.14 2.95v5.67H9.36V9h3.41v1.56h.05c.48-.9 1.65-1.86 3.4-1.86 3.64 0 4.31 2.4 4.31 5.52v6.23zM5.34 7.43a2.06 2.06 0 1 1 0-4.12 2.06 2.06 0 0 1 0 4.12zM3.56 20.45h3.56V9H3.56v11.45z" />
                    </svg>
                  </a>
                </div>
              </div>
            </div>
          </div>

          <div className="app__header-inner">
            <div className="app__brand">
              <img className="app__logo" src={usFlag} alt="US flag logo" />
              <h1 className="app__title title-font">US Policywatch</h1>
            </div>
            <div className="taglineWrap taglineWrap--full">
              <div className="taglineBox">
                <p className="app__tagline app__tagline--glass">
                  <span className="taglineSheen" aria-hidden="true" />
                  <span className="taglineText">
                    Welcome to US Policywatch, your live stream of U.S. policy updates from the White House and state governors. Each item is AI-polished into a clear summary and includes an AI Impact view that highlights affected industries, sentiment, and the “why” behind the change.
                    Select a state below to explore its latest orders, announcements, and policy signals.
                  </span>
                </p>
              </div>
            </div>
          </div>
        </header>

        <SourceTabs active={activeSource} onChange={handleSourceChange} allowedKeys={allowedKeys} />

        <WhatsNewCarousel
          entries={whatsNew}
          intervalMs={3500}
          rightActions={
            user ? (
              <div className="whatsActionRow">
                <button className="whatsActionBtn" onClick={openPrefs}>
                  Set preferences
                </button>
                <button
                  className="whatsActionBtn whatsActionBtn--primary"
                  onClick={openAlerts}
                >
                  Alerts
                </button>
              </div>
            ) : null
          }
        />

        <main className="app__main">
          <section className="app__feed">
            <div className="app__feed-header">
              <div className="feedHead">
                <div className="feedHead__kicker">Policy feed</div>
                <h2 className="feedHead__title">{sourceName}</h2>
                <div className="feedHead__sub">Orders, proclamations, releases — summarized + impact-tagged.</div>
              </div>

              {/* ✅ Controls go here (between title and count) */}
              <div className="feed-controls">
                {/* Sort By */}
                <label className="feed-controls__field" title="Sort by date">
                  <span className="feed-controls__label">Sort</span>
                  <select
                    className="feed-controls__select"
                    value={sortMode}
                    onChange={(e) => setSortMode(e.target.value as SortMode)}
                    disabled={loading}
                  >
                    <option value="desc">Latest → Oldest</option>
                    <option value="asc">Oldest → Latest</option>
                  </select>
                </label>

                {/* Group By (status filter) */}
                <label className="feed-controls__field" title="Filter by status">
                  <span className="feed-controls__label">Group</span>
                  <select
                    className="feed-controls__select"
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    disabled={loading}
                  >
                    <option value="">All</option>
                    {statuses.map((s) => (
                      <option key={s} value={s}>
                        {s}
                      </option>
                    ))}
                  </select>
                </label>

                {/* Date range */}
                <div className="feed-controls__field" title="Filter by date range">
                  <span className="feed-controls__label">Dates</span>
                  <div className="feed-controls__dates">
                    <div className="dateWrap">
                      <input
                        ref={dateFromRef}
                        className="feed-controls__date"
                        type="date"
                        value={dateFrom}
                        onChange={(e) => setDateFrom(e.target.value)}
                        disabled={loading}
                      />
                      <svg
                        className="dateIcon"
                        viewBox="0 0 24 24"
                        aria-hidden="true"
                        onPointerDown={(e) => {
                          e.preventDefault();
                          openDatePicker(dateFromRef);
                        }}
                      >
                        <path d="M7 2a1 1 0 0 1 1 1v1h8V3a1 1 0 1 1 2 0v1h1a3 3 0 0 1 3 3v13a3 3 0 0 1-3 3H5a3 3 0 0 1-3-3V7a3 3 0 0 1 3-3h1V3a1 1 0 0 1 1-1Zm14 8H3v10a1 1 0 0 0 1 1h16a1 1 0 0 0 1-1V10Z"/>
                      </svg>
                    </div>

                    <span className="feed-controls__dash">—</span>

                    <div className="dateWrap">
                      <input
                        ref={dateToRef}
                        className="feed-controls__date"
                        type="date"
                        value={dateTo}
                        onChange={(e) => setDateTo(e.target.value)}
                        disabled={loading}
                      />
                      <svg
                        className="dateIcon"
                        viewBox="0 0 24 24"
                        aria-hidden="true"
                        onPointerDown={(e) => {
                          e.preventDefault();
                          openDatePicker(dateToRef);
                        }}
                      >
                        <path d="M7 2a1 1 0 0 1 1 1v1h8V3a1 1 0 1 1 2 0v1h1a3 3 0 0 1 3 3v13a3 3 0 0 1-3 3H5a3 3 0 0 1-3-3V7a3 3 0 0 1 3-3h1V3a1 1 0 0 1 1-1Zm14 8H3v10a1 1 0 0 0 1 1h16a1 1 0 0 0 1-1V10Z"/>
                      </svg>
                    </div>
                  </div>
                </div>

                {/* Clear */}
                <button
                  className="feed-controls__clear"
                  onClick={clearFilters}
                  disabled={loading}
                  title="Reset filters"
                >
                  Clear
                </button>
              </div>

              <span className="app__feed-count">
                {items.length > 0 && `${items.length} items · page ${page} of ${totalPages}`}
              </span>
            </div>

            {loading && <div className="app__status">Loading latest items…</div>}

            {error && !loading && <div className="app__status app__status--error">{error}</div>}

            {!loading && !error && items.length === 0 && (
              <div className="app__status">No items yet for this source.</div>
            )}

            {!loading && !error && items.length > 0 && (
              <>
                <div className="item-list">
                  {items.map((item) => (
                    <ItemCard key={item.id} item={item} />
                  ))}
                </div>

                <div className="pagination">
                  <button
                    className="pagination__button"
                    onClick={handlePrevPage}
                    disabled={page <= 1 || loading}
                    aria-label="Previous page"
                  >
                    Previous
                  </button>

                  {totalPages > CHUNK_SIZE && (
                    <button
                      className="pagination__button"
                      onClick={handlePrevChunk}
                      disabled={!canChunkPrev || loading}
                      aria-label="Previous page group"
                      title="Previous 10 pages"
                    >
                      ‹
                    </button>
                  )}

                  <div className="pagination__pages">
                    {chunkStart > 1 && <span className="pagination__ellipsis">…</span>}

                    {visiblePages.map((p) => (
                      <button
                        key={p}
                        className={`pagination__page ${p === page ? "pagination__page--active" : ""}`}
                        onClick={() => goToPage(p)}
                        disabled={loading}
                        aria-current={p === page ? "page" : undefined}
                      >
                        {p}
                      </button>
                    ))}

                    {chunkEnd < totalPages && <span className="pagination__ellipsis">…</span>}
                  </div>

                  {totalPages > CHUNK_SIZE && (
                    <button
                      className="pagination__button"
                      onClick={handleNextChunk}
                      disabled={!canChunkNext || loading}
                      aria-label="Next page group"
                      title="Next 10 pages"
                    >
                      ›
                    </button>
                  )}

                  <button
                    className="pagination__button"
                    onClick={handleNextPage}
                    disabled={page >= totalPages || loading}
                    aria-label="Next page"
                  >
                    Next
                  </button>
                </div>
              </>
            )}
          </section>
        </main>
        {/* ✅ Toasts (bottom-right) — show ONE at a time */}
        {user && (
          <div className="toastStack">
            {toastQueue[0] ? (
              <AlertToast
                key={toastQueue[0].delivery.id}
                n={toastQueue[0]}
                onGotIt={onGotIt}
                onCloseTemp={onCloseTemp}
                isMuted={(alertId) => isMuted(alertId, !!toastQueue[0]?.alert?.muted)}
                onMute={onMuteAlert}
                onUnmute={onUnmuteAlert}
              />
            ) : null}
          </div>
        )}

        {/* ✅ Preferences onboarding + editor */}
        <PreferencesModal
          open={prefsOpen}
          fullName={(user?.user_metadata as any)?.full_name ?? (user?.email ?? "")}
          initial={preferredSources ?? []}
          onClose={() => setPrefsOpen(false)}
          onSave={handleSavePrefs}
        />

        {/* ✅ Alerts manager */}
        <AlertsModal
          open={alertsOpen}
          fullName={(user?.user_metadata as any)?.full_name ?? (user?.email ?? "")}
          onClose={() => setAlertsOpen(false)}
        />
        <AuthModal
          open={authOpen}
          onClose={() => setAuthOpen(false)}
          initialMode={authMode}
        />
      </div>
    </div>
  );
};
