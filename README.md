# India Exchange Circulars

Automated near-real-time scraper and searchable library for NSE, BSE, and MCX circulars.

## Live site

[https://venkatezh-13.github.io/circulars/](https://venkatezh-13.github.io/circulars/)

## What it does

- Polls NSE, BSE, and MCX every **15 minutes** during market hours (9 AM – 6 PM IST, Mon–Sat)
- Deduplicates on every run — safe to poll frequently, no double entries
- Stores all circulars as JSON files (no archiving)
- Rebuilds `search_index.json` once per cycle after all scrapers finish
- Generates **RSS feeds** (today's circulars only) after every index rebuild
- Backfills historical data one week per exchange per day (back to 2020)
- Frontend: search by keyword, filter by exchange and date range

## Repo structure

```
circulars/
├── .github/workflows/
│   └── circulars_all_in_one.yml  # single workflow: poll + backfill + index
├── scrapers/
│   ├── nse_circulars.py          # NSE scraper core
│   ├── bse_circulars.py          # BSE scraper core
│   ├── mcx_circulars.py          # MCX scraper core
│   ├── run_nse.py                # today's runner (called by workflow)
│   ├── run_bse.py
│   └── run_mcx.py
├── scripts/
│   ├── build_index.py            # rebuilds search_index.json + triggers RSS
│   ├── generate_rss.py           # generates RSS feeds from search_index.json
│   ├── backfill.py               # historical backfill with state tracking
│   └── backfill_state.json       # tracks backfill progress (auto-updated)
├── data/
│   ├── nse/raw/                  # daily JSONs
│   ├── bse/raw/
│   └── mcx/raw/
└── docs/
    ├── index.html                # frontend (GitHub Pages)
    ├── search_index.json         # flat search index (auto-generated)
    └── rss/
        ├── all.xml               # all exchanges — today only (auto-generated)
        ├── nse.xml               # NSE only   — today only (auto-generated)
        ├── bse.xml               # BSE only   — today only (auto-generated)
        └── mcx.xml               # MCX only   — today only (auto-generated)
```

## RSS feeds

RSS feeds are generated automatically after every index rebuild. Each feed contains **only today's circulars** — the file is fully overwritten each day. Your RSS reader accumulates history on its end.

### Feed URLs

| Feed | URL |
|------|-----|
| All exchanges | `https://venkatezh-13.github.io/circulars/rss/all.xml` |
| NSE only | `https://venkatezh-13.github.io/circulars/rss/nse.xml` |
| BSE only | `https://venkatezh-13.github.io/circulars/rss/bse.xml` |
| MCX only | `https://venkatezh-13.github.io/circulars/rss/mcx.xml` |

### How to subscribe

**RSS reader (Feedly, Inoreader, etc.)**
Add any of the feed URLs above to your reader. New circulars appear within minutes of the 15-min poll cycle completing.

**Slack**
Run this in any channel to get circulars posted automatically:
```
/feed subscribe https://venkatezh-13.github.io/circulars/rss/all.xml
```
For a specific exchange:
```
/feed subscribe https://venkatezh-13.github.io/circulars/rss/nse.xml
```

**Zapier / Make**
1. Create a new zap with trigger: **RSS by Zapier → New Item in Feed**
2. Paste the feed URL
3. Connect to any action — Slack message, email, Microsoft Teams, webhook, etc.
4. Optional: add a filter step to match specific keywords in the item title

**n8n (self-hosted)**
Use the **RSS Feed Trigger** node with any feed URL. Connect to HTTP Request, Slack, or any other node.

**No keyword filtering needed on your end** — the feed already contains only today's circulars. If you want to filter by keyword, do it in Zapier/Make's filter step or in your RSS reader's rule engine.

---

## How the workflow runs

All automation lives in a single file: `.github/workflows/circulars_all_in_one.yml`.

It has three jobs that run in sequence:

```
poll  ──┐
        ├──▶  rebuild-index  (rebuilds index + RSS feeds)
backfill─┘
```

**poll** — scrapes today's circulars from all three exchanges. Commits only `data/` if new entries are found. Runs every 15 minutes during market hours.

**backfill** — scrapes one week of historical data per exchange per run. Commits only `data/` and `backfill_state.json`. Runs once daily at 8:30 AM IST.

**rebuild-index** — runs after both jobs finish. Rebuilds `search_index.json`, generates RSS feeds, and commits. This is the only job that touches `search_index.json` and `docs/rss/`, which avoids merge conflicts.

## Schedule (all times IST)

| Job | Frequency | Window | Days |
|-----|-----------|--------|------|
| Poll (NSE + BSE + MCX) | Every 15 min | 6:00 AM – 1:00 AM | Daily |
| Backfill (historical) | Once daily | 8:30 AM | Daily |
| Index + RSS rebuild | After each poll/backfill | — | Daily |

> GitHub Actions cron has a minimum interval of 5 minutes. The workflow uses 15 min to stay well within free-tier limits and avoid exchange rate limits. Change `*/15` to `*/5` in the cron if you want faster polling.

## GitHub Actions free tier usage

Public repos get **unlimited** Actions minutes. Private repos get **2,000 min/month**.

At 15-min polling (9 AM–6 PM IST, Mon–Sat):
- ~36 poll runs/day × ~1 min each = ~36 min/day
- ~1,000 min/month for polling + ~30 min/month for backfill
- Well within the 2,000 min/month private repo limit

## Setup

### 1. Create the repo and push

```bash
git init
git add .
git commit -m "Initial commit"
git remote add origin https://github.com/<username>/circulars.git
git push -u origin main
```

### 2. Enable GitHub Pages

Go to **Settings → Pages → Source: Deploy from branch → Branch: main, Folder: /docs**

### 3. Enable GitHub Actions write permissions

Go to **Settings → Actions → General → Workflow permissions → Read and write permissions**

This is required so the workflow can commit new data back to the repo.

### 4. Seed today's data (first run)

```bash
pip install httpx requests beautifulsoup4
python scrapers/run_nse.py
python scrapers/run_bse.py
python scrapers/run_mcx.py
python scripts/build_index.py   # also generates docs/rss/*.xml
git add . && git commit -m "Seed initial data" && git push
```

### 5. Trigger backfill manually (optional)

Go to **Actions → Circulars — Poll & Backfill → Run workflow** and set mode to `backfill`.

Or run locally:

```bash
python scripts/backfill.py
# specific exchange and date range:
python scripts/backfill.py --exchange nse --from 2025-01-01 --to 2025-01-07
```

## Manual triggers

The workflow supports `workflow_dispatch` with these inputs:

| Input | Options | Description |
|-------|---------|-------------|
| `mode` | `poll`, `backfill`, `poll+backfill` | What to run |
| `exchanges` | `all`, `nse`, `bse`, `mcx` | Which exchanges to poll |
| `exchange` | `all`, `nse`, `bse`, `mcx` | Which exchange to backfill |
| `from_date` | YYYY-MM-DD | Backfill start (optional override) |
| `to_date` | YYYY-MM-DD | Backfill end (optional override) |

## Run locally

```bash
pip install httpx requests beautifulsoup4

# Scrape today
python scrapers/run_nse.py
python scrapers/run_bse.py
python scrapers/run_mcx.py

# Build search index + generate RSS feeds
python scripts/build_index.py

# Run backfill (auto mode — reads backfill_state.json)
python scripts/backfill.py

# Serve the frontend
cd docs && python -m http.server 8080
# Open http://localhost:8080
```

## Notes on exchange rate limits

All three scrapers rotate User-Agent strings, use real browser headers, and include random delays between requests (2–6 seconds). The backfill scrapes one week per exchange per run to avoid triggering rate limits on historical endpoints.

GitHub Actions runs on Azure IP ranges, which exchanges generally tolerate. If you start seeing empty responses or 403s during market hours, that is a sign of IP-based throttling — in that case, reduce polling frequency or add a longer sleep inside the scraper.
