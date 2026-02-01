// src/types.ts

export type SourceKey =
  | "white-house"
  | "florida"
  | "texas"
  | "new-york"
  | "pennsylvania"
  | "illinois"
  | "massachusetts"
  | "washington"
  | "california"
  | "utah"
  | "ohio"
  | "vermont"
  | "arizona"
  | "virginia"
  | "georgia"
  | "hawaii"
  | "alaska"
  | "new-jersey"
  | "maryland"
  | "colorado"
  | "minnesota"
  | "oregon"
  | "michigan"
  | "north-carolina"
  | "wisconsin"
  | "nevada"
  | "tennessee"
  | "south-carolina"
  | "iowa"
  | "missouri"
  | "kansas"
  | "new-mexico";

export interface ImpactIndustry {
  name: string;
  direction: "positive" | "negative" | "mixed";
  magnitude: number;   // 0..1
  confidence: number;  // 0..1
  why?: string;
}

export interface AIImpact {
  score?: number;
  tags?: string[];
  industries?: ImpactIndustry[];
  overall_why?: string;
}

export interface PolicyItem {
  id: string;
  title: string;
  summary: string;
  url: string;
  source_name: string;
  published_at: string | null;
  jurisdiction?: string | null;
  status?: string | null;
  categories?: string[] | null;

  ai_impact_score?: number | null;
  ai_impact?: AIImpact | null;
  ai_impact_status?: string | null;
}

export interface WhatsNewEntry {
  source_key: SourceKey;
  item: PolicyItem;
}

export interface WhatsNewResponse {
  updated_at: string;
  count: number;
  items: WhatsNewEntry[];
}
