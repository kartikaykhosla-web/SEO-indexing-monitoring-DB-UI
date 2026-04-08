from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS url_state (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    property_key TEXT NOT NULL,
    url TEXT NOT NULL,
    sitemap_published_date TEXT NOT NULL,
    discovered_at TEXT NOT NULL,
    first_checked_at TEXT,
    last_checked_at TEXT,
    check_count INTEGER NOT NULL DEFAULT 0,
    current_status TEXT NOT NULL DEFAULT 'Pending',
    first_indexed_seen_at TEXT,
    last_non_indexed_seen_at TEXT,
    estimated_indexed_at TEXT,
    indexing_latency_minutes INTEGER,
    gsc_coverage_state TEXT,
    gsc_page_fetch_state TEXT,
    google_last_crawl_at TEXT,
    next_check_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(property_key, url)
);

CREATE TABLE IF NOT EXISTS check_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    property_key TEXT NOT NULL,
    url TEXT NOT NULL,
    checked_at TEXT NOT NULL,
    status TEXT NOT NULL,
    verdict TEXT,
    coverage_state TEXT,
    indexing_state TEXT,
    page_fetch_state TEXT,
    robots_state TEXT,
    last_crawl_time TEXT,
    error TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS property_state (
    property_key TEXT PRIMARY KEY,
    last_sitemap_check_at TEXT,
    gsc_hour_bucket TEXT,
    gsc_checks_this_hour INTEGER NOT NULL DEFAULT 0,
    gsc_quota_backoff_until TEXT,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_url_state_property_status ON url_state(property_key, current_status);
CREATE INDEX IF NOT EXISTS idx_url_state_property_published ON url_state(property_key, sitemap_published_date);
CREATE INDEX IF NOT EXISTS idx_check_log_property_checked ON check_log(property_key, checked_at);
"""


def connect(db_path: str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)
    conn.commit()


def reset_db(conn: sqlite3.Connection) -> None:
    conn.execute("DELETE FROM check_log")
    conn.execute("DELETE FROM url_state")
    conn.execute("DELETE FROM property_state")
    conn.commit()


def get_property_state(conn: sqlite3.Connection, property_key: str) -> Dict[str, Any]:
    row = conn.execute(
        "SELECT * FROM property_state WHERE property_key = ?",
        (property_key,),
    ).fetchone()
    if row:
        return dict(row)
    return {
        "property_key": property_key,
        "last_sitemap_check_at": "",
        "gsc_hour_bucket": "",
        "gsc_checks_this_hour": 0,
        "gsc_quota_backoff_until": "",
        "updated_at": "",
    }


def upsert_property_state(conn: sqlite3.Connection, state: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO property_state (
            property_key,
            last_sitemap_check_at,
            gsc_hour_bucket,
            gsc_checks_this_hour,
            gsc_quota_backoff_until,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(property_key) DO UPDATE SET
            last_sitemap_check_at=excluded.last_sitemap_check_at,
            gsc_hour_bucket=excluded.gsc_hour_bucket,
            gsc_checks_this_hour=excluded.gsc_checks_this_hour,
            gsc_quota_backoff_until=excluded.gsc_quota_backoff_until,
            updated_at=excluded.updated_at
        """,
        (
            state.get("property_key", ""),
            state.get("last_sitemap_check_at", ""),
            state.get("gsc_hour_bucket", ""),
            int(state.get("gsc_checks_this_hour", 0) or 0),
            state.get("gsc_quota_backoff_until", ""),
            state.get("updated_at", ""),
        ),
    )
    conn.commit()


def upsert_discovered_urls(
    conn: sqlite3.Connection,
    property_key: str,
    rows: Iterable[Tuple[str, str, str, str]],
) -> int:
    """Rows: (url, sitemap_published_date, discovered_at, date)"""
    count = 0
    for url, sitemap_published_date, discovered_at, date_value in rows:
        cursor = conn.execute(
            """
            INSERT INTO url_state (
                date, property_key, url, sitemap_published_date, discovered_at,
                current_status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, 'Pending', ?, ?)
            ON CONFLICT(property_key, url) DO NOTHING
            """,
            (
                date_value,
                property_key,
                url,
                sitemap_published_date,
                discovered_at,
                discovered_at,
                discovered_at,
            ),
        )
        if cursor.rowcount > 0:
            count += 1
    conn.commit()
    return count


def fetch_property_urls(conn: sqlite3.Connection, property_key: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        "SELECT * FROM url_state WHERE property_key = ? ORDER BY sitemap_published_date DESC, id DESC",
        (property_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_due_candidates(conn: sqlite3.Connection, property_key: str) -> List[Dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT *
        FROM url_state
        WHERE property_key = ?
          AND current_status != 'Indexed'
        ORDER BY
          CASE WHEN check_count = 0 THEN 0 ELSE 1 END,
          sitemap_published_date DESC,
          id DESC
        """,
        (property_key,),
    ).fetchall()
    return [dict(row) for row in rows]


def update_url_state(conn: sqlite3.Connection, row_id: int, fields: Dict[str, Any]) -> None:
    keys = sorted(fields.keys())
    if not keys:
        return
    assignments = ", ".join([f"{key} = ?" for key in keys])
    values = [fields[key] for key in keys]
    values.append(row_id)
    conn.execute(f"UPDATE url_state SET {assignments} WHERE id = ?", values)
    conn.commit()


def insert_check_log(conn: sqlite3.Connection, payload: Dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO check_log (
            property_key, url, checked_at, status, verdict, coverage_state,
            indexing_state, page_fetch_state, robots_state, last_crawl_time,
            error, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            payload.get("property_key", ""),
            payload.get("url", ""),
            payload.get("checked_at", ""),
            payload.get("status", ""),
            payload.get("verdict", ""),
            payload.get("coverage_state", ""),
            payload.get("indexing_state", ""),
            payload.get("page_fetch_state", ""),
            payload.get("robots_state", ""),
            payload.get("last_crawl_time", ""),
            payload.get("error", ""),
            payload.get("checked_at", ""),
        ),
    )
    conn.commit()


def fetch_all_summary(conn: sqlite3.Connection, property_key: Optional[str] = None) -> List[Dict[str, Any]]:
    if property_key:
        rows = conn.execute(
            "SELECT * FROM url_state WHERE property_key = ? ORDER BY sitemap_published_date DESC, id DESC",
            (property_key,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM url_state ORDER BY property_key ASC, sitemap_published_date DESC, id DESC"
        ).fetchall()
    return [dict(row) for row in rows]


def fetch_logs(
    conn: sqlite3.Connection,
    property_key: Optional[str] = None,
    url: Optional[str] = None,
    limit: int = 1000,
) -> List[Dict[str, Any]]:
    query = "SELECT * FROM check_log WHERE 1=1"
    params: List[Any] = []
    if property_key:
        query += " AND property_key = ?"
        params.append(property_key)
    if url:
        query += " AND url = ?"
        params.append(url)
    query += " ORDER BY checked_at DESC, id DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    return [dict(row) for row in rows]


def summary_counts(conn: sqlite3.Connection, property_key: Optional[str] = None) -> Dict[str, int]:
    params: List[Any] = []
    where = ""
    if property_key:
        where = "WHERE property_key = ?"
        params.append(property_key)

    total = conn.execute(f"SELECT COUNT(*) FROM url_state {where}", params).fetchone()[0]

    indexed = conn.execute(
        f"SELECT COUNT(*) FROM url_state {where + (' AND' if where else ' WHERE')} current_status = 'Indexed'",
        params,
    ).fetchone()[0]

    pending = conn.execute(
        f"SELECT COUNT(*) FROM url_state {where + (' AND' if where else ' WHERE')} current_status = 'Pending'",
        params,
    ).fetchone()[0]

    errors = conn.execute(
        f"SELECT COUNT(*) FROM url_state {where + (' AND' if where else ' WHERE')} current_status IN ('Error', 'Quota Exceeded')",
        params,
    ).fetchone()[0]

    return {
        "total": int(total),
        "indexed": int(indexed),
        "pending": int(pending),
        "errors": int(errors),
    }
