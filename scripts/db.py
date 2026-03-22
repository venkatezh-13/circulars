"""
db.py — SQLite database module for circulars data.

Provides a single-file SQLite database for efficient storage, querying,
and full-text search of exchange circulars.

Schema:
  - circulars: Main table with all circular metadata
  - circulars_fts: Full-text search virtual table
"""

import sqlite3
import os
from datetime import datetime
from contextlib import contextmanager

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
DB_PATH = os.path.join(REPO_ROOT, "data", "circulars.db")


def get_connection() -> sqlite3.Connection:
    """Get a database connection with row factory enabled."""
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


@contextmanager
def get_db():
    """Context manager for database connections."""
    conn = get_connection()
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initialize database schema."""
    with get_db() as conn:
        conn.executescript("""
            -- Main circulars table
            CREATE TABLE IF NOT EXISTS circulars (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                exchange TEXT NOT NULL,
                date_iso TEXT NOT NULL,
                ref TEXT NOT NULL,
                subject TEXT NOT NULL,
                category TEXT,
                link TEXT,
                -- Exchange-specific fields
                segment TEXT,
                department TEXT,
                -- Metadata
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(exchange, ref, date_iso)
            );

            -- Indexes for common queries
            CREATE INDEX IF NOT EXISTS idx_circulars_date ON circulars(date_iso DESC);
            CREATE INDEX IF NOT EXISTS idx_circulars_exchange ON circulars(exchange);
            CREATE INDEX IF NOT EXISTS idx_circulars_ref ON circulars(ref);
            CREATE INDEX IF NOT EXISTS idx_circulars_category ON circulars(category);

            -- Full-text search virtual table
            CREATE VIRTUAL TABLE IF NOT EXISTS circulars_fts USING fts5(
                ref,
                subject,
                category,
                content='circulars',
                content_rowid='id'
            );

            -- Triggers to keep FTS in sync
            CREATE TRIGGER IF NOT EXISTS circulars_ai AFTER INSERT ON circulars BEGIN
                INSERT INTO circulars_fts(rowid, ref, subject, category)
                VALUES (new.id, new.ref, new.subject, new.category);
            END;

            CREATE TRIGGER IF NOT EXISTS circulars_ad AFTER DELETE ON circulars BEGIN
                INSERT INTO circulars_fts(circulars_fts, rowid, ref, subject, category)
                VALUES ('delete', old.id, old.ref, old.subject, old.category);
            END;

            CREATE TRIGGER IF NOT EXISTS circulars_au AFTER UPDATE ON circulars BEGIN
                INSERT INTO circulars_fts(circulars_fts, rowid, ref, subject, category)
                VALUES ('delete', old.id, old.ref, old.subject, old.category);
                INSERT INTO circulars_fts(rowid, ref, subject, category)
                VALUES (new.id, new.ref, new.subject, new.category);
            END;
        """)
        conn.commit()
    print(f"Database initialized: {DB_PATH}")


def insert_circular(conn, exchange: str, date_iso: str, ref: str, 
                    subject: str, category: str = None, link: str = None,
                    segment: str = None, department: str = None) -> bool:
    """
    Insert a circular record. Returns True if inserted, False if duplicate.
    """
    try:
        conn.execute("""
            INSERT INTO circulars (exchange, date_iso, ref, subject, category, link, segment, department)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (exchange, date_iso, ref, subject, category, link, segment, department))
        return True
    except sqlite3.IntegrityError:
        return False


def bulk_insert_circulars(records: list[dict]) -> int:
    """
    Bulk insert circular records. Returns count of inserted records.
    Each record should have: exchange, date_iso, ref, subject, category, link
    """
    inserted = 0
    with get_db() as conn:
        for r in records:
            if insert_circular(conn, **r):
                inserted += 1
        conn.commit()
    return inserted


def search_circulars(query: str = None, exchange: str = None, 
                     date_from: str = None, date_to: str = None,
                     limit: int = None, offset: int = 0) -> list[dict]:
    """
    Search circulars with optional filters.
    Returns list of dictionaries with all fields.
    """
    with get_db() as conn:
        sql = "SELECT * FROM circulars WHERE 1=1"
        params = []

        if exchange and exchange != "ALL":
            sql += " AND exchange = ?"
            params.append(exchange)

        if date_from:
            sql += " AND date_iso >= ?"
            params.append(date_from)

        if date_to:
            sql += " AND date_iso <= ?"
            params.append(date_to)

        if query:
            # Use FTS for full-text search
            sql += " AND id IN (SELECT rowid FROM circulars_fts WHERE circulars_fts MATCH ?)"
            params.append(query)

        sql += " ORDER BY date_iso DESC"

        if limit:
            sql += f" LIMIT {limit} OFFSET {offset}"

        rows = conn.execute(sql, params).fetchall()
        return [dict(row) for row in rows]


def get_all_circulars() -> list[dict]:
    """Get all circulars sorted by date (newest first)."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT exchange, date_iso, ref, subject, category, link 
            FROM circulars 
            ORDER BY date_iso DESC
        """).fetchall()
        return [dict(row) for row in rows]


def get_stats() -> dict:
    """Get database statistics."""
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM circulars").fetchone()[0]
        by_exchange = conn.execute("""
            SELECT exchange, COUNT(*) as count 
            FROM circulars 
            GROUP BY exchange
        """).fetchall()
        date_range = conn.execute("""
            SELECT MIN(date_iso) as min_date, MAX(date_iso) as max_date 
            FROM circulars
        """).fetchone()
        
        return {
            "total": total,
            "by_exchange": {row["exchange"]: row["count"] for row in by_exchange},
            "date_range": {
                "min": date_range["min_date"],
                "max": date_range["max_date"]
            }
        }


def get_db_size() -> int:
    """Get database file size in bytes."""
    if os.path.exists(DB_PATH):
        return os.path.getsize(DB_PATH)
    return 0


if __name__ == "__main__":
    init_db()
    stats = get_stats()
    print(f"\nDatabase stats:")
    print(f"  Total records: {stats['total']:,}")
    for ex, count in stats['by_exchange'].items():
        print(f"  {ex}: {count:,}")
    print(f"  Date range: {stats['date_range']['min']} to {stats['date_range']['max']}")
    print(f"  File size: {get_db_size() / 1024:.1f} KB")
