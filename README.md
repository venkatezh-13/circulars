# India Exchange Circulars

Automated near-real-time scraper, archive aggregator, and searchable library for **NSE**, **BSE**, **MCX**, and **SEBI** circulars.

## Live site

🌐 **[https://venkatezh-13.github.io/circulars/](https://venkatezh-13.github.io/circulars/)**

---

## What it does

- **22,500+ Circulars Indexed**: Seamlessly combines historical circulars with real-time daily updates.
- **Ultra-Lightweight Architecture**: 0 raw data files stored in Git — the entire search index is compiled directly in memory, keeping repository clone size under 5 MB.
- **Automated Live Polling**: Scrapes NSE, BSE, MCX, and SEBI every **15 minutes** during market hours (6:00 AM – 1:00 AM IST) via GitHub Actions.
- **Fast Interactive Search**: Filter by Exchange (All, NSE, BSE, MCX, SEBI), keyword search, date range presets (1D, 7D, 1M, 3M, 6M, 1Y, Custom), and column sorting.
- **Live RSS Feeds**: Generated automatically after every index rebuild for instant feed reader & webhook notifications.

---

## Dataset & Exchanges

| Exchange | Current Records | Coverage | Source |
|:---|:---:|:---|:---|
| **BSE** (Bombay Stock Exchange) | **13,467+** | Notices & Debt / Equity Circulars | Live XML API + Archive |
| **NSE** (National Stock Exchange) | **6,220+** | Listing, Trading, Compliance, Clearing | Live API + Archive |
| **SEBI** (Securities & Exchange Board) | **2,828+** | Regulatory Orders & Circulars | Live HTML Scraping + Archive |
| **MCX** (Multi Commodity Exchange) | **Live** | Circulars & Trading Parameters | Live API Endpoint |
| **TOTAL** | **22,517+** | Comprehensive Market Coverage | Consolidated Search Index |

---

## Repo Structure

```
circulars/
├── .github/workflows/
│   └── circulars_all_in_one.yml  # Daily automated poll & deploy workflow
├── scrapers/
│   ├── nse_circulars.py          # NSE scraper core
│   ├── bse_circulars.py          # BSE scraper core
│   ├── mcx_circulars.py          # MCX scraper core
│   ├── sebi_circulars.py         # SEBI scraper core
│   ├── run_nse.py                # Today's NSE runner
│   ├── run_bse.py                # Today's BSE runner
│   ├── run_mcx.py                # Today's MCX runner
│   └── run_sebi.py               # Today's SEBI runner
├── scripts/
│   ├── build_index.py            # Compiles search_index.json & RSS feeds
│   └── generate_rss.py           # RSS generator
└── docs/
    ├── index.html                # Interactive frontend (GitHub Pages)
    ├── search_index.json         # Flat lightweight search index (~6 MB)
    └── rss/
        ├── all.xml               # All exchanges RSS feed
        ├── nse.xml               # NSE only RSS feed
        ├── bse.xml               # BSE only RSS feed
        ├── mcx.xml               # MCX only RSS feed
        └── sebi.xml              # SEBI only RSS feed
```

---

## RSS Feeds

RSS feeds are generated after every index rebuild.

### Feed URLs

| Feed | URL |
|:---|:---|
| **All Exchanges** | `https://venkatezh-13.github.io/circulars/rss/all.xml` |
| **NSE only** | `https://venkatezh-13.github.io/circulars/rss/nse.xml` |
| **BSE only** | `https://venkatezh-13.github.io/circulars/rss/bse.xml` |
| **MCX only** | `https://venkatezh-13.github.io/circulars/rss/mcx.xml` |
| **SEBI only** | `https://venkatezh-13.github.io/circulars/rss/sebi.xml` |

### How to Subscribe

- **RSS Reader (Feedly, Inoreader, NetNewsWire)**: Paste any of the XML URLs above into your reader.
- **Slack**: Run `/feed subscribe https://venkatezh-13.github.io/circulars/rss/all.xml` in any channel.
- **Zapier / Make / n8n**: Use the **RSS Trigger** node to push alerts to Telegram, Discord, or Email.

---

## Automation & GitHub Actions

The automation runs through `.github/workflows/circulars_all_in_one.yml`:

```
poll-and-index (scrape live circulars + merge archive in memory)
     │
     ▼
deploy-pages (fast 15-second GitHub Pages deployment)
```

1. **Scrapes Live Circulars**: Runs today's scrapers for NSE, BSE, MCX, and SEBI.
2. **Aggregates Archive**: Clones external archive in memory without saving raw files to Git.
3. **Rebuilds Index**: Generates `docs/search_index.json` and RSS feeds.
4. **Deploys to Pages**: Publishes updated frontend immediately.

### Schedule (IST)

| Job | Frequency | Window | Days |
|:---|:---|:---|:---|
| **Poll & Deploy** | Every 15 min | 6:00 AM – 1:00 AM IST | Daily |

---

## Running Locally

```bash
# 1. Install Python dependencies
pip install httpx requests beautifulsoup4

# 2. Run scrapers for today's circulars
python scrapers/run_nse.py
python scrapers/run_bse.py
python scrapers/run_mcx.py
python scrapers/run_sebi.py

# 3. Build search index & RSS feeds
python scripts/build_index.py

# 4. Start local development server
python -m http.server 8080 --directory docs
# Open http://localhost:8080 in your browser
```

---

## License & Attribution

Historical dataset integration powered in part by [rhnvrm/stock-market-circulars](https://github.com/rhnvrm/stock-market-circulars).
All circular content is owned by their respective regulatory authorities and exchanges (NSE India, BSE India, MCX India, SEBI).
