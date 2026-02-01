// src/api.ts
import type { PolicyItem, SourceKey } from "./types";

import type { WhatsNewResponse } from "./types";

export const API_BASE =
  import.meta.env.VITE_API_BASE ?? "http://127.0.0.1:8000";

export interface ItemsResponse {
  source: string;
  page: number;
  page_size: number;
  total: number;
  items: PolicyItem[];
}

export type SortMode = "desc" | "asc"; // desc=latest→oldest, asc=oldest→latest

export interface FetchItemsOptions {
  sort?: SortMode;
  status?: string | null;
  dateFrom?: string | null; // YYYY-MM-DD
  dateTo?: string | null;   // YYYY-MM-DD
}

export async function fetchItems(
  source: SourceKey,
  page: number = 1,
  opts: FetchItemsOptions = {}
): Promise<ItemsResponse> {
  const params = new URLSearchParams({
    source,
    page: String(page),
    page_size: "40",
  });

  // ✅ NEW query params
  if (opts.sort) params.set("sort", opts.sort);
  if (opts.status) params.set("status", opts.status);
  if (opts.dateFrom) params.set("date_from", opts.dateFrom);
  if (opts.dateTo) params.set("date_to", opts.dateTo);

  const res = await fetch(`${API_BASE}/frontend/items?${params.toString()}`);
  if (!res.ok) throw new Error(`API error ${res.status}`);

  const data = (await res.json()) as ItemsResponse;

  // normalize items (fix CA categories + ensure id)
  data.items = data.items.map((item, idx) => {
    let categories = (item as any).categories;

    if (source === "california" && typeof categories === "string") {
      try {
        const parsed = JSON.parse(categories);
        if (Array.isArray(parsed)) categories = parsed;
      } catch {
        const s = categories.trim();
        if (s.startsWith("{") && s.endsWith("}")) {
          const inner = s.slice(1, -1).trim();
          categories =
            inner.length === 0
              ? []
              : inner
                  .split(",")
                  .map((x: string) => x.trim().replace(/^"(.*)"$/, "$1"))
                  .filter(Boolean);
        } else if (s.includes(",")) {
          categories = s.split(",").map((x: string) => x.trim()).filter(Boolean);
        }
      }
    }

    // ✅ ADD THIS BLOCK (RIGHT HERE, after categories normalization)
    // ✅ Normalize ai_impact (may come as object or JSON string)
    let aiImpact = (item as any).ai_impact ?? null;
    if (typeof aiImpact === "string") {
      try {
        aiImpact = JSON.parse(aiImpact);
      } catch {
        aiImpact = null;
      }
    }

    let aiImpactScore = (item as any).ai_impact_score;
    if (typeof aiImpactScore === "string") {
      const n = Number(aiImpactScore);
      aiImpactScore = Number.isFinite(n) ? n : null;
    }

    return {
      ...item,
      categories,
      ai_impact: aiImpact,
      ai_impact_score: (aiImpactScore ?? null) as any,
      ai_impact_status: (item as any).ai_impact_status ?? null,
      id: (item as any).id ?? (item as any).external_id ?? String(idx),
    };
  });

  return data;
}

// ✅ NEW: fetch valid statuses for Group By dropdown
export async function fetchStatuses(source: SourceKey): Promise<string[]> {
  const params = new URLSearchParams({ source });
  const res = await fetch(`${API_BASE}/frontend/statuses?${params.toString()}`);
  if (!res.ok) throw new Error(`API error ${res.status}`);

  const data = (await res.json()) as { source: string; statuses: string[] };
  return Array.isArray(data.statuses) ? data.statuses : [];
}

export async function fetchWhatsNew(): Promise<WhatsNewResponse> {
  const res = await fetch(`${API_BASE}/frontend/whats-new`);
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return (await res.json()) as WhatsNewResponse;
}

export async function authHeaders(): Promise<HeadersInit> {
  const { supabase } = await import("./supabaseClient");
  const { data } = await supabase.auth.getSession();
  const token = data.session?.access_token;
  return token ? { Authorization: `Bearer ${token}` } : {};
}

export async function fetchPreferences() {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/preferences`, { headers });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json() as Promise<{ preferences: { sources: string[] } | null }>;
}

export async function savePreferences(sources: string[]) {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/preferences`, {
    method: "PUT",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify({ sources }),
  });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

export type AlertDTO = {
  id: string;
  source_key: string;
  statuses: string[];
  categories: string[];
  enabled: boolean;
  muted: boolean;
  created_at: string;
};

export async function listAlerts(): Promise<AlertDTO[]> {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/alerts`, { headers });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  const data = (await res.json()) as { alerts: AlertDTO[] };
  return data.alerts ?? [];
}

export async function createAlert(payload: Omit<AlertDTO, "id" | "created_at">) {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/alerts`, {
    method: "POST",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

export async function updateAlert(alertId: string, payload: Omit<AlertDTO, "id" | "created_at">) {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/alerts/${alertId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json", ...headers },
    body: JSON.stringify(payload),
  });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

export async function deleteAlert(alertId: string) {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/alerts/${alertId}`, {
    method: "DELETE",
    headers,
  });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}

export type AlertNotification = {
  alert: {
    id: string;
    source_key: string;
    statuses: string[];
    categories: string[];
    muted?: boolean;
    enabled?: boolean;
  };
  delivery: { id: string; delivered_at: string; acknowledged_at: string | null };
  item: PolicyItem & { item_uuid?: string };
};

export async function pollAlerts(): Promise<AlertNotification[]> {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/alerts/poll`, { headers });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  const data = (await res.json()) as { notifications: AlertNotification[] };
  return data.notifications ?? [];
}

export async function ackDelivery(deliveryId: string) {
  const headers = await authHeaders();
  const res = await fetch(`${API_BASE}/me/alerts/deliveries/${deliveryId}/ack`, {
    method: "POST",
    headers,
  });
  if (!res.ok) throw new Error(`API error ${res.status}`);
  return res.json();
}
