# India Exchange Circulars

Automated near-real-time scraper and searchable library for NSE, BSE, MCX, SEBI, and MSEI circulars.

## Live site

[https://venkatezh-13.github.io/circulars/](https://venkatezh-13.github.io/circulars/)

## What it does

- Polls NSE, BSE, MCX, SEBI, and MSEI every **15 minutes** during market hours (9 AM – 6 PM IST, Mon–Sat)
- Deduplicates on every run — safe to poll frequently, no double entries
- Stores all circulars as JSON files
- Rebuilds `search_index.json` once per cycle after all scrapers finish
- Generates **RSS feeds** (today's circulars only) after every index rebuild
- Frontend: search by keyword, filter by exchange and date range

## Repo structure

```
circulars/
├── .github/workflows/
│   └── circulars_all_in_one.yml  # daily polling workflow
├── scrapers/
│   ├── nse_circulars.py          # NSE scraper core
│   ├── bse_circulars.py          # BSE scraper core
│   ├── mcx_circulars.py          # MCX scraper core
│   ├── sebi_circulars.py         # SEBI scraper core
│   ├── msei_circulars.py         # MSEI scraper core
│   ├── run_nse.py                # today's runner (called by workflow)
│   ├── run_bse.py
│   ├── run_mcx.py
│   ├── run_sebi.py
│   └── run_msei.py               # MSEI runner
├── scripts/
│   ├── build_index.py            # rebuilds search_index.json + triggers RSS
│   ├── generate_rss.py           # generates RSS feeds from search_index.json
│   └── split_sebi_monthly.py     # SEBI monthly file splitter
├── data/
│   ├── nse/raw/                  # daily JSONs
│   ├── bse/raw/
│   ├── mcx/raw/
│   ├── sebi/raw/
│   └── msei/raw/                 # MSEI daily JSONs
└── docs/
    ├── index.html                # frontend (GitHub Pages)
    ├── search_index.json         # flat search index (auto-generated)
    └── rss/
        ├── all.xml               # all exchanges — today only (auto-generated)
        ├── nse.xml               # NSE only   — today only (auto-generated)
        ├── bse.xml               # BSE only   — today only (auto-generated)
        ├── mcx.xml               # MCX only   — today only (auto-generated)
        ├── sebi.xml              # SEBI only  — today only (auto-generated)
        └── msei.xml              # MSEI only  — today only (auto-generated)
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
| SEBI only | `https://venkatezh-13.github.io/circulars/rss/sebi.xml` |
| MSEI only | `https://venkatezh-13.github.io/circulars/rss/msei.xml` |

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

It runs a single unified job (`poll-and-index`):

```
poll (NSE, BSE, MCX, SEBI) ──▶ rebuild-index & RSS ──▶ single commit & push
```

- **Polls** all four exchanges (NSE, BSE, MCX, SEBI) in parallel/sequence.
- **Rebuilds** `search_index.json` and updates RSS feeds immediately in the same runner.
- **Commits** data, index, and RSS updates in a single git commit.
- **Keepalive**: Includes `keepalive-workflow` protection to prevent GitHub from disabling scheduled runs after 60 days of inactivity.
- **Execution Time**: ~35 seconds per run (down from ~3 minutes).

## Bypassing GitHub Free Tier Schedule Delays (100% Reliable 15-Min Cron)

GitHub Actions cron (`schedule`) on Free Tier repositories can suffer from **queue delays (30-120 minutes)** during peak traffic periods.

To guarantee execution **precisely every 15 minutes without delays**, set up a free external trigger via [cron-job.org](https://cron-job.org):

### Setup Instructions for Cron-Job.org (Free):

1. **Generate a GitHub Personal Access Token (PAT)**:
   - Go to **GitHub Settings → Developer Settings → Personal Access Tokens → Fine-grained tokens**.
   - Create a token for this repository with permission: **Contents: Read and write** (or **Repository permissions → Actions: Read & write**).
2. **Create a Job in [cron-job.org](https://cron-job.org)**:
   - **URL**: `https://api.github.com/repos/venkatezh-13/circulars/dispatches`
   - **HTTP Method**: `POST`
   - **Schedule**: Every 15 minutes (or custom interval)
   - **Headers**:
     - `Authorization`: `Bearer ghp_YOUR_PERSONAL_ACCESS_TOKEN`
     - `Accept`: `application/vnd.github.v3+json`
     - `User-Agent`: `CronJob-Client`
   - **Request Body (JSON)**:
     ```json
     {"event_type": "poll_circulars"}
     ```
3. Save the cron job. When cron-job.org fires, GitHub Actions will trigger `repository_dispatch` **immediately** without waiting on GitHub's cron queue!

## GitHub Actions free tier usage

Public repos get **unlimited** Actions minutes. Private repos get **2,000 min/month**.

With our single-job refactor (~35 seconds per run):
- ~36 poll runs/day × ~0.6 min each = ~21 min/day
- ~630 min/month for polling (down from 1,600+ min/month)
- Uses < 32% of private repo limits and run-times are ultra fast.

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
python scrapers/run_sebi.py
python scripts/build_index.py   # also generates docs/rss/*.xml
git add . && git commit -m "Seed initial data" && git push
```

## Manual triggers

The workflow supports `workflow_dispatch` with these inputs:

| Input | Options | Description |
|-------|---------|-------------|
| `exchanges` | `all`, `nse`, `bse`, `mcx`, `sebi` | Which exchanges to poll |

## Run locally

```bash
pip install httpx requests beautifulsoup4

# Scrape today
python scrapers/run_nse.py
python scrapers/run_bse.py
python scrapers/run_mcx.py
python scrapers/run_sebi.py

# Build search index + generate RSS feeds
python scripts/build_index.py

# Serve the frontend
cd docs && python -m http.server 8080
# Open http://localhost:8080
```

## Notes on exchange rate limits

All four scrapers (NSE, BSE, MCX, SEBI) rotate User-Agent strings, use real browser headers, and include random delays between requests (2–6 seconds). SEBI scraper handles malformed HTML with unclosed `<td>` tags gracefully.

GitHub Actions runs on Azure IP ranges, which exchanges generally tolerate. If you start seeing empty responses or 403s during market hours, that is a sign of IP-based throttling — in that case, reduce polling frequency or add a longer sleep inside the scraper.
