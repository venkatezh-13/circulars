"""
generate_rss.py — Generates RSS feeds from docs/search_index.json.

Reads:
  - docs/search_index.json   (built by build_index.py)

Writes:
  - docs/rss/all.xml         (all exchanges, last 7 days)
  - docs/rss/nse.xml         (NSE only, last 7 days)
  - docs/rss/bse.xml         (BSE only, last 7 days)
  - docs/rss/mcx.xml         (MCX only, last 7 days)

Feeds contain the last 7 days of circulars — resilient to workflow failures.
Files are fully overwritten on every run.
RSS readers accumulate history on their end.
"""

import json
import os
from datetime import date, datetime, timedelta
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

REPO_ROOT     = os.path.join(os.path.dirname(__file__), "..")
SEARCH_INDEX  = os.path.join(REPO_ROOT, "docs", "search_index.json")
RSS_DIR       = os.path.join(REPO_ROOT, "docs", "rss")
BASE_URL      = "https://venkatezh-13.github.io/circulars"
EXCHANGES     = ["NSE", "BSE", "MCX"]
WINDOW_DAYS   = 7


# ── Feed builder ──────────────────────────────────────────────────────────────

def build_feed(title: str, feed_url: str, description: str, items: list) -> str:
    rss = Element("rss", version="2.0")
    rss.set("xmlns:atom", "http://www.w3.org/2005/Atom")

    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text       = title
    SubElement(channel, "link").text        = BASE_URL
    SubElement(channel, "description").text = description
    SubElement(channel, "language").text    = "en-in"
    SubElement(channel, "lastBuildDate").text = datetime.utcnow().strftime(
        "%a, %d %b %Y %H:%M:%S +0000"
    )

    atom_link = SubElement(channel, "atom:link")
    atom_link.set("href", feed_url)
    atom_link.set("rel", "self")
    atom_link.set("type", "application/rss+xml")

    for item in items:
        entry = SubElement(channel, "item")

        SubElement(entry, "title").text = item.get("subject", "No title")
        SubElement(entry, "link").text  = item.get("link", BASE_URL)
        SubElement(entry, "guid", isPermaLink="false").text = (
            item.get("link") or item.get("ref") or item.get("subject", "")
        )
        SubElement(entry, "description").text = (
            f"[{item.get('exchange', '')}] "
            f"{item.get('ref', '')} — "
            f"{item.get('subject', '')}"
        )
        SubElement(entry, "category").text = item.get("category", "")

        # pubDate from date_iso (YYYY-MM-DD)
        try:
            d = datetime.strptime(item["date_iso"], "%Y-%m-%d")
            SubElement(entry, "pubDate").text = d.strftime(
                "%a, %d %b %Y 07:30:00 +0530"
            )
        except Exception:
            pass

    # Pretty-print XML
    raw = tostring(rss, encoding="unicode")
    pretty = minidom.parseString(raw).toprettyxml(indent="  ")
    # Replace minidom's declaration with a clean UTF-8 one
    lines = pretty.split("\n")
    lines[0] = '<?xml version="1.0" encoding="UTF-8"?>'
    return "\n".join(lines)


# ── Main ──────────────────────────────────────────────────────────────────────

def generate_rss():
    if not os.path.exists(SEARCH_INDEX):
        print("generate_rss: search_index.json not found, skipping")
        return

    with open(SEARCH_INDEX, encoding="utf-8") as f:
        all_items = json.load(f)

    cutoff = (date.today() - timedelta(days=WINDOW_DAYS)).isoformat()  # "2026-03-15"
    recent_items = [i for i in all_items if i.get("date_iso", "") >= cutoff]

    if not recent_items:
        print(f"generate_rss: no items in the last {WINDOW_DAYS} days, skipping")
        return

    os.makedirs(RSS_DIR, exist_ok=True)

    # Combined feed — all exchanges
    _write_feed(
        title       = "India Exchange Circulars — All",
        feed_url    = f"{BASE_URL}/rss/all.xml",
        description = f"NSE, BSE and MCX circulars — last {WINDOW_DAYS} days",
        items       = recent_items,
        filename    = "all.xml",
    )

    # Per-exchange feeds
    for exchange in EXCHANGES:
        items = [i for i in recent_items if i.get("exchange", "").upper() == exchange]
        if not items:
            print(f"generate_rss: no {exchange} items in last {WINDOW_DAYS} days, skipping {exchange.lower()}.xml")
            continue
        _write_feed(
            title       = f"India Exchange Circulars — {exchange}",
            feed_url    = f"{BASE_URL}/rss/{exchange.lower()}.xml",
            description = f"{exchange} circulars — last {WINDOW_DAYS} days",
            items       = items,
            filename    = f"{exchange.lower()}.xml",
        )

    print(f"generate_rss: feeds written (last {WINDOW_DAYS} days, {len(recent_items)} items, cutoff {cutoff})")


def _write_feed(title, feed_url, description, items, filename):
    xml = build_feed(title, feed_url, description, items)
    path = os.path.join(RSS_DIR, filename)
    with open(path, "w", encoding="utf-8") as f:
        f.write(xml)
    print(f"  wrote {path} ({len(items)} items)")


if __name__ == "__main__":
    generate_rss()
