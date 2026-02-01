# US-Policywatch

**US PolicyWatch is a full-stack policy intelligence platform designed to track, ingest, summarize, and analyze U.S. federal and state-level government activity in near-real time. It consolidates executive orders, proclamations, press releases, regulatory updates, and official newsroom announcements into a single searchable interface, making complex policy changes easier to understand, monitor, and act upon.**

**The platform is built as a production-grade system, not a demo.** Its backend uses **FastAPI and Python** with an asynchronous ingestion architecture to reliably fetch content from dozens of official sources, including the White House, Federal Register, governor newsrooms, and state agencies. Ingestion pipelines are designed to be **cron-safe and idempotent, ensuring duplicate-free storage and resilient recovery from partial failures**. All content is persisted in **PostgreSQL (Supabase) using schemas optimized for policy metadata, timestamps, jurisdictions, and document types**.

To enhance usability, US PolicyWatch integrates **AI-powered summarization and impact analysis**. The system supports multiple AI providers **(OpenAI and Hugging Face)** and applies configurable **safety rails, rate limits, and timeouts**. Long-form policy documents and PDFs are **normalized, summarized, and optionally “polished”** into clear, readable explanations while preserving factual accuracy. An **AI impact layer** categorizes potential effects across industries and policy domains.

The frontend is built with **React, TypeScript, and Vite**, providing a fast, modern, and responsive interface. **Users can browse policy updates by source or jurisdiction, track what’s new through a dynamic feed, and manage alerts and preferences. Authentication and data access are integrated with Supabase, and the UI is designed for scalability and future personalization.**

US PolicyWatch showcases real-world **full-stack engineering, async data pipelines, AI integration, and deployment workflows—demonstrating** how modern web technologies can make **government policy more accessible, transparent, and actionable.**

---

## ✨ What US PolicyWatch Does

### 📥 Policy Ingestion

* Continuously ingests official documents from:

  * State governors’ offices
  * White House releases
  * Federal Register & agencies
* Supports:

  * Press releases
  * Executive orders
  * Proclamations
  * PDF‑based notices
* Handles pagination, duplicate detection, and source normalization

### 🧠 AI Summaries & Impact Analysis

* Automatically generates:

  * Clean, readable summaries of long policy documents
  * AI‑based **impact analysis** (policy intent, affected domains, sentiment)
* Uses LLMs with fallbacks and safe‑guards to ensure reliability

### 🔔 Alerts & Monitoring

* Users can:

  * Subscribe to specific states or feeds
  * Receive alerts when new policy items are published
  * Mute alerts for custom durations
  * Re‑surface alerts automatically after mute expiry

### 🔐 Authentication & Preferences

* Secure authentication (JWT‑based)
* User‑specific preferences:

  * States to track
  * Feed types to monitor
  * Alert behavior

### 📊 Clean, Modern UI

* Fast, responsive frontend
* Focused on readability and signal over noise
* Built for long‑term extensibility

---

## 🏗️ Architecture Overview

```
us-policywatch/
├── policywatch-backend/   # FastAPI backend
│   ├── app/
│   │   ├── ingest_*       # Ingestion pipelines
│   │   ├── ai_*           # AI summarization & impact
│   │   ├── auth.py        # Authentication
│   │   ├── db.py          # Database access
│   │   ├── main.py        # FastAPI app
│   │   └── run.py         # Uvicorn entrypoint
│   └── requirements.txt
│
├── policywatch-frontend/  # React + Vite frontend
│   ├── src/
│   │   ├── components/
│   │   ├── utils/
│   │   ├── api.ts
│   │   └── App.tsx
│   └── package.json
│
└── README.md
```

---

## 🧩 Backend Tech Stack

* **Python 3.11**
* **FastAPI** – REST API framework
* **Uvicorn** – ASGI server
* **PostgreSQL / Supabase** – Persistent storage
* **Playwright** – Headless browser scraping
* **httpx / asyncio** – Async networking
* **pdfminer / pypdf** – PDF parsing
* **python‑jose** – JWT authentication
* **OpenAI / HuggingFace** – AI summarization & impact analysis

### Backend Responsibilities

* Data ingestion & normalization
* Deduplication & versioning
* AI processing
* Auth & user preferences
* Alert scheduling logic

---

## 🎨 Frontend Tech Stack

* **React (TypeScript)**
* **Vite** – Build tooling
* **Modern CSS** (custom + utility patterns)
* **Supabase client** – Auth/session handling

### Frontend Responsibilities

* Policy feed browsing
* Alerts & notifications UI
* User preferences & settings
* Auth modals & session state

---

## ⏱️ Background Jobs & Scheduling

US PolicyWatch relies on **scheduled ingestion jobs** to stay up‑to‑date.

Recommended setup:

* **GitHub Actions** → Cron‑based ingestion triggers
* **Render** → Always‑on backend API

Scheduled jobs include:

* State‑level ingestion
* Federal & White House ingestion
* AI impact backfills

This separation ensures:

* Reliability
* Cost efficiency
* Clear observability

---

## 🚀 Deployment Strategy

* **Backend**: Render (FastAPI service)
* **Frontend**: Render / Vercel / Netlify
* **Cron jobs**: GitHub Actions
* **Secrets**: Render environment variables + GitHub secrets

---

## 🧪 Local Development

### Backend

```bash
cd policywatch-backend
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements.txt
python -m app.run
```

### Frontend

```bash
cd policywatch-frontend
npm install
npm run dev
```

---

## 🎯 Project Vision

US PolicyWatch is built for:

* Students & researchers
* Policy analysts
* Journalists
* Founders tracking regulatory risk
* Anyone who wants **signal, not noise**

This project prioritizes:

* Real data
* Real infrastructure
* Real‑world engineering trade‑offs

---

## 📌 Status

🚧 Actively developed

Upcoming:

* Production cron pipelines
* Advanced alert rules
* Public dashboards
* Search & tagging improvements

---

**Built with intent, not tutorials.**

