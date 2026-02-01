// src/components/ItemCard.tsx
import React from "react";
import type { PolicyItem, ImpactIndustry } from "../types";

interface Props {
  item: PolicyItem;
}

function formatDate(value: string | null): string {
  if (!value) return "";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  });
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

function ImpactBlock({ item, variant }: { item: PolicyItem; variant?: "card" | "slide" }) {
  const score = typeof item.ai_impact_score === "number" ? item.ai_impact_score : null;
  const impact = item.ai_impact ?? null;

  const industries: ImpactIndustry[] = Array.isArray(impact?.industries)
    ? (impact!.industries as ImpactIndustry[])
    : [];

  const overallWhy = typeof impact?.overall_why === "string" ? impact.overall_why : "";

  // If there is no impact at all, don’t show the block.
  // (If you WANT to show "Impact: pending" then remove this early return and render status.)
  if (score == null && industries.length === 0 && !overallWhy) return null;

  return (
    <div className={variant === "slide" ? "impact impact--slide" : "impact"}>
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
            {industries.slice(0, 5).map((ind, i) => (
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

          {/* ✅ make it look like an industry card */}
          <div className="impact__overallCard">
            <div className="impact__overall">{overallWhy}</div>
          </div>
        </div>
      )}
    </div>
  );
}

export const ItemCard: React.FC<Props> = ({ item }) => {
  // Only for California: avoid showing the primary status twice
  const cats = Array.isArray(item.categories) ? item.categories : [];
  const displayCats =
    (item.jurisdiction ?? "").toLowerCase() === "california"
      ? cats.filter((c) => c !== item.status)
      : cats;

  return (
    <article className="item-card">
      <header className="item-card__header">
        <span className="item-card__source">{item.source_name}</span>
        <span className="item-card__meta">
          {item.jurisdiction && (
            <span className="item-card__pill">{item.jurisdiction}</span>
          )}

          {item.status && (
            <span className="item-card__pill item-card__pill--soft">
              {item.status}
            </span>
          )}

          {displayCats.map((cat) => (
            <span
              key={cat}
              className="item-card__pill item-card__pill--soft"
              title="Category"
            >
              {cat}
            </span>
          ))}

          {formatDate(item.published_at) && (
            <span className="item-card__date">
              {formatDate(item.published_at)}
            </span>
          )}
        </span>
      </header>

      <a
        href={item.url}
        target="_blank"
        rel="noreferrer"
        className="item-card__title"
      >
        {item.title}
      </a>

      {item.summary && <p className="item-card__summary">{item.summary}</p>}

      {/* ✅ NEW impact block */}
      <ImpactBlock item={item} />
    </article>
  );
};
