// src/components/WhatsNewCarousel.tsx
import React, { useEffect, useMemo, useState } from "react";
import type { PolicyItem, WhatsNewEntry, ImpactIndustry } from "../types";

function formatDate(value: string | null): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" });
}

function scoreClass(score: number): string {
  if (score < -0.2) return "impact-chip impact-chip--neg";
  if (score > 0.2) return "impact-chip impact-chip--pos";
  return "impact-chip impact-chip--neu";
}

function fmt01(x: unknown): string {
  const n = typeof x === "number" ? x : Number(x);
  if (!Number.isFinite(n)) return "—";
  return n.toFixed(2);
}

type Props = {
  entries: WhatsNewEntry[];
  intervalMs?: number;
  rightActions?: React.ReactNode;
};

export const WhatsNewCarousel: React.FC<Props> = ({ entries, intervalMs = 3500, rightActions }) => {
  const [index, setIndex] = useState(0);
  const safeEntries = useMemo(() => entries ?? [], [entries]);

  useEffect(() => {
    if (!safeEntries.length) return;
    const t = window.setInterval(() => {
      setIndex((i) => (i + 1) % safeEntries.length);
    }, intervalMs);
    return () => window.clearInterval(t);
  }, [safeEntries.length, intervalMs]);

  useEffect(() => {
    if (index >= safeEntries.length) setIndex(0);
  }, [safeEntries.length, index]);

  if (!safeEntries.length) return null;

  return (
    <section className="whatsnew">
      <div className="whatsnew__header">
        <div className="sectionHead">
          <div className="sectionHead__kicker">Live</div>
          <h3 className="sectionHead__title">
            What’s new <span className="sectionHead__spark" aria-hidden="true">✦</span>
          </h3>
          <div className="sectionHead__sub">
            Fresh policy signals, AI-polished for clarity.
          </div>
        </div>

        <div className="whatsnew__right">
          {rightActions ? <div className="whatsnew__actions whatsnew__actions--stack">{rightActions}</div> : null}

          <div className="whatsnew__dots" aria-label="Slideshow progress">
            {safeEntries.map((_, i) => (
              <span key={i} className={`whatsnew__dot ${i === index ? "is-active" : ""}`} />
            ))}
          </div>
        </div>
      </div>

      <div className="whatsnew__viewport">
        <div className="whatsnew__track" style={{ transform: `translateX(-${index * 100}%)` }}>
          {safeEntries.map((e) => (
            <WhatsNewSlide key={e.source_key} item={e.item} />
          ))}
        </div>
      </div>
    </section>
  );
};

const WhatsNewSlide: React.FC<{ item: PolicyItem }> = ({ item }) => {
  const cats = Array.isArray(item.categories) ? item.categories : [];

  const score = typeof item.ai_impact_score === "number" ? item.ai_impact_score : null;
  const impact = item.ai_impact ?? null;

  const industries: ImpactIndustry[] = Array.isArray(impact?.industries)
    ? (impact!.industries as ImpactIndustry[])
    : [];

  const overallWhy = typeof impact?.overall_why === "string" ? impact.overall_why : "";

  const hasImpact = score != null || industries.length > 0 || !!overallWhy;

  return (
    <article className="whatsnew__card">
      <header className="whatsnew__meta">
        <span className="whatsnew__jur">{(item.jurisdiction || item.source_name) ?? "Unknown"}</span>
        <span className="whatsnew__pills">
          {item.status && <span className="whatsnew__pill">{item.status}</span>}
          {cats.slice(0, 2).map((c) => (
            <span key={c} className="whatsnew__pill whatsnew__pill--soft">{c}</span>
          ))}
          {formatDate(item.published_at) && <span className="whatsnew__date">{formatDate(item.published_at)}</span>}
        </span>
      </header>

      <a className="whatsnew__headline" href={item.url} target="_blank" rel="noreferrer">
        {item.title}
      </a>

      {item.summary && <p className="whatsnew__summary">{item.summary}</p>}

      {/* ✅ Impact block (now matches ItemCard: industries + overall why + same hierarchy) */}
      {hasImpact && (
        <div className="impact impact--slide">
          <div className="impact__top">
            <div className="impact__label">Impact Outlook</div>

            {typeof score === "number" && (
              <div className={scoreClass(score)} title="AI impact score in [-1, 1]">
                {score.toFixed(2)}
              </div>
            )}

            <div className="impact__status">
              {item.ai_impact_status ? item.ai_impact_status : ""}
            </div>
          </div>

          {industries.length > 0 && (
            <div className="impact__section">
              <div className="impact__subtitle">Industries Affected</div>

              <ul className="impact__industries">
                {industries.slice(0, 4).map((ind, i) => (
                  <li key={`${ind.name}-${i}`} className="impact__industry">
                    <div className="impact__industryRow">
                      <span className="impact__industryName">{ind.name}</span>

                      <span className={`impact__dir impact__dir--${ind.direction ?? "mixed"}`}>
                        {ind.direction}
                      </span>

                      <span className="impact__metric impact__metric--mag" title="Magnitude (0..1)">
                        <span className="impact__metricLabel">Magnitude</span> {fmt01(ind.magnitude)}
                      </span>

                      <span className="impact__metric impact__metric--conf" title="Confidence (0..1)">
                        <span className="impact__metricLabel">Confidence</span> {fmt01(ind.confidence)}
                      </span>
                    </div>

                    {ind.why && <div className="impact__why">{ind.why}</div>}
                  </li>
                ))}
              </ul>
            </div>
          )}

          {overallWhy && (
            <div className="impact__section">
              <div className="impact__subtitle impact__subtitle--overall">Overall Why</div>

              {/* ✅ same “card” treatment as ItemCard */}
              <div className="impact__overallCard">
                <div className="impact__overall">{overallWhy}</div>
              </div>
            </div>
          )}
        </div>
      )}
    </article>
  );
};
