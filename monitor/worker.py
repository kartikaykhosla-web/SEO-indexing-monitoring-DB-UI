from __future__ import annotations

import datetime as dt
from typing import Dict, List, Optional, Tuple

import requests

from . import db
from .config import MonitorConfig, PropertyConfig
from .export import export_all_json
from .gsc import inspect_url, is_quota_exceeded_error, status_bucket
from .sitemap import fetch_sitemap_urls
from .time_utils import (
    IST,
    hour_bucket_ist,
    indexing_latency_minutes,
    midpoint_ist_iso,
    now_utc,
    parse_iso_datetime,
    parse_publication_datetime,
    to_ist_iso,
)


def property_discovery_due(state: Dict[str, str], interval_minutes: int, now: dt.datetime) -> bool:
    last = parse_iso_datetime(state.get("last_sitemap_check_at", ""))
    if not last:
        return True
    return (now - last) >= dt.timedelta(minutes=interval_minutes)


def quota_backoff_due(state: Dict[str, str], now: dt.datetime) -> bool:
    blocked_until = parse_iso_datetime(state.get("gsc_quota_backoff_until", ""))
    if not blocked_until:
        return True
    return now >= blocked_until


def remaining_hourly_capacity(property_cfg: PropertyConfig, state: Dict[str, str], now: dt.datetime) -> Optional[int]:
    hourly = property_cfg.max_gsc_checks_per_hour
    if not hourly:
        return None
    bucket = hour_bucket_ist(now)
    current_bucket = state.get("gsc_hour_bucket", "")
    count = int(state.get("gsc_checks_this_hour", 0) or 0)
    if current_bucket != bucket:
        return hourly
    return max(0, hourly - count)


def can_run_gsc(property_cfg: PropertyConfig, state: Dict[str, str], now: dt.datetime) -> bool:
    if not quota_backoff_due(state, now):
        return False
    capacity = remaining_hourly_capacity(property_cfg, state, now)
    return capacity is None or capacity > 0


def increment_hourly_count(property_cfg: PropertyConfig, state: Dict[str, str], now: dt.datetime) -> Dict[str, str]:
    updated = dict(state)
    bucket = hour_bucket_ist(now)
    if updated.get("gsc_hour_bucket", "") != bucket:
        updated["gsc_hour_bucket"] = bucket
        updated["gsc_checks_this_hour"] = 0
    updated["gsc_checks_this_hour"] = int(updated.get("gsc_checks_this_hour", 0) or 0) + 1
    updated["updated_at"] = to_ist_iso(now)
    return updated


def set_quota_backoff(state: Dict[str, str], now: dt.datetime, minutes: int = 60) -> Dict[str, str]:
    updated = dict(state)
    updated["gsc_quota_backoff_until"] = to_ist_iso(now + dt.timedelta(minutes=minutes))
    updated["updated_at"] = to_ist_iso(now)
    return updated


def next_poll_interval_minutes(published_dt: Optional[dt.datetime], now: dt.datetime) -> int:
    if not published_dt:
        return 240
    if now - published_dt <= dt.timedelta(hours=1):
        return 10
    return 240


def row_due_for_gsc(row: Dict[str, str], now: dt.datetime, single_check_per_day: bool = False) -> bool:
    if row.get("current_status") == "Indexed":
        return False

    next_check = parse_iso_datetime(row.get("next_check_at", "") or "")
    if next_check and now < next_check:
        return False

    last_checked = parse_iso_datetime(row.get("last_checked_at", "") or "")
    if single_check_per_day and last_checked and last_checked.astimezone(IST).date() == now.astimezone(IST).date():
        return False

    if int(row.get("check_count", 0) or 0) == 0:
        return True

    if not last_checked:
        return True

    published_dt = parse_publication_datetime(row.get("sitemap_published_date", ""))
    interval = next_poll_interval_minutes(published_dt, now)
    return (now - last_checked) >= dt.timedelta(minutes=interval)


def discover_new_rows(
    session: requests.Session,
    property_cfg: PropertyConfig,
    existing_urls: set[str],
    cutoff_datetime: dt.datetime,
    discovered_at_ist: str,
    max_new_rows: Optional[int],
) -> List[Tuple[str, str, str, str]]:
    discovered: Dict[str, dt.datetime] = {}

    for sitemap_url in property_cfg.sitemap_urls:
        for url, published_raw in fetch_sitemap_urls(
            session,
            sitemap_url,
            allow_lastmod_fallback=property_cfg.allow_lastmod_fallback,
        ):
            published_dt = parse_publication_datetime(published_raw)
            if not published_dt:
                continue
            if published_dt < cutoff_datetime:
                continue
            if url in existing_urls:
                continue
            discovered[url] = published_dt

    items = sorted(discovered.items(), key=lambda x: x[1], reverse=True)

    effective_limit = property_cfg.max_new_urls_per_run
    if max_new_rows is not None:
        effective_limit = min(effective_limit, max_new_rows) if effective_limit is not None else max_new_rows
    if effective_limit:
        items = items[:effective_limit]

    rows: List[Tuple[str, str, str, str]] = []
    for url, published_dt in items:
        published_ist = to_ist_iso(published_dt)
        date_value = published_dt.astimezone(dt.timezone(dt.timedelta(hours=5, minutes=30))).date().isoformat()
        rows.append((url, published_ist, discovered_at_ist, date_value))
    return rows


def run_property_discovery(
    conn,
    session: requests.Session,
    property_cfg: PropertyConfig,
    cutoff_datetime: dt.datetime,
    now: dt.datetime,
) -> int:
    state = db.get_property_state(conn, property_cfg.key)
    if not property_discovery_due(state, property_cfg.discovery_interval_minutes, now):
        return 0

    state["property_key"] = property_cfg.key
    state["last_sitemap_check_at"] = to_ist_iso(now)
    state["updated_at"] = to_ist_iso(now)

    # If quota backoff is active or cap exhausted, skip discovery to prevent pending explosion.
    if not quota_backoff_due(state, now):
        db.upsert_property_state(conn, state)
        return 0

    capacity = remaining_hourly_capacity(property_cfg, state, now)
    if capacity is not None and capacity <= 0:
        db.upsert_property_state(conn, state)
        return 0

    existing = db.fetch_property_urls(conn, property_cfg.key)
    existing_urls = {row["url"] for row in existing}

    rows = discover_new_rows(
        session=session,
        property_cfg=property_cfg,
        existing_urls=existing_urls,
        cutoff_datetime=cutoff_datetime,
        discovered_at_ist=to_ist_iso(now),
        max_new_rows=capacity,
    )
    inserted = db.upsert_discovered_urls(conn, property_cfg.key, rows)
    db.upsert_property_state(conn, state)
    return inserted


def run_property_gsc(
    conn,
    gsc_service,
    property_cfg: PropertyConfig,
    cutoff_datetime: dt.datetime,
    now: dt.datetime,
) -> Dict[str, int]:
    state = db.get_property_state(conn, property_cfg.key)
    metrics = {"checked": 0, "indexed_now": 0}

    candidates = db.fetch_due_candidates(conn, property_cfg.key)
    due_rows: List[Dict[str, str]] = []
    for row in candidates:
        published_dt = parse_publication_datetime(row.get("sitemap_published_date", ""))
        if not published_dt or published_dt < cutoff_datetime:
            continue
        if row_due_for_gsc(row, now, property_cfg.single_gsc_check_per_day):
            due_rows.append(row)

    for row in due_rows:
        if not can_run_gsc(property_cfg, state, now):
            break
        if property_cfg.max_gsc_checks_per_run and metrics["checked"] >= property_cfg.max_gsc_checks_per_run:
            break

        checked_at = to_ist_iso(now_utc())
        result = inspect_url(gsc_service, row["url"], property_cfg.gsc_site_url)

        check_count = int(row.get("check_count", 0) or 0) + 1
        current_status = status_bucket(result.get("status", ""), result.get("error", ""))
        first_checked = row.get("first_checked_at") or checked_at
        last_non_indexed = row.get("last_non_indexed_seen_at", "")
        first_indexed = row.get("first_indexed_seen_at", "")
        estimated_indexed = row.get("estimated_indexed_at", "")

        if current_status == "Indexed":
            if not first_indexed:
                first_indexed = checked_at
            estimated_indexed = midpoint_ist_iso(last_non_indexed, checked_at) if last_non_indexed else checked_at
        else:
            last_non_indexed = checked_at

        published = row.get("sitemap_published_date", "")
        crawl = result.get("last_crawl_time", "") or row.get("google_last_crawl_at", "")
        latency = indexing_latency_minutes(published, crawl)

        next_check_at = ""
        if current_status != "Indexed":
            interval = next_poll_interval_minutes(parse_publication_datetime(published), now)
            next_check_at = to_ist_iso(now + dt.timedelta(minutes=interval))

        db.update_url_state(
            conn,
            row["id"],
            {
                "first_checked_at": first_checked,
                "last_checked_at": checked_at,
                "check_count": check_count,
                "current_status": current_status,
                "first_indexed_seen_at": first_indexed,
                "last_non_indexed_seen_at": last_non_indexed,
                "estimated_indexed_at": estimated_indexed,
                "google_last_crawl_at": crawl,
                "indexing_latency_minutes": latency,
                "gsc_coverage_state": result.get("coverage_state", ""),
                "gsc_page_fetch_state": result.get("page_fetch_state", ""),
                "next_check_at": next_check_at,
                "updated_at": checked_at,
            },
        )

        db.insert_check_log(
            conn,
            {
                "property_key": property_cfg.key,
                "url": row["url"],
                "checked_at": checked_at,
                "status": current_status,
                "verdict": result.get("verdict", ""),
                "coverage_state": result.get("coverage_state", ""),
                "indexing_state": result.get("indexing_state", ""),
                "page_fetch_state": result.get("page_fetch_state", ""),
                "robots_state": result.get("robots_state", ""),
                "last_crawl_time": result.get("last_crawl_time", ""),
                "error": result.get("error", ""),
            },
        )

        metrics["checked"] += 1
        if current_status == "Indexed":
            metrics["indexed_now"] += 1

        state["last_crawled_at"] = checked_at
        state["updated_at"] = checked_at
        state = increment_hourly_count(property_cfg, state, now)

        if is_quota_exceeded_error(result.get("error", "")):
            state = set_quota_backoff(state, now)
            db.upsert_property_state(conn, state)
            break

        db.upsert_property_state(conn, state)

    return metrics


def run_monitor(
    conn,
    config: MonitorConfig,
    properties: List[PropertyConfig],
    cutoff_datetime: dt.datetime,
    run_discovery: bool,
    run_gsc_checks: bool,
    run_export: bool,
    gsc_service,
) -> List[str]:
    now = now_utc()
    session = requests.Session()
    session.headers.update({"User-Agent": "SEOIndexingMonitorLocal/1.0"})

    summaries: List[str] = []

    for prop in properties:
        discovered = 0
        checked = 0
        indexed_now = 0
        state = db.get_property_state(conn, prop.key)
        run_started_at = to_ist_iso(now_utc())
        state.update(
            {
                "property_key": prop.key,
                "current_status": "running",
                "current_run_started_at": run_started_at,
                "updated_at": run_started_at,
            }
        )
        db.upsert_property_state(conn, state)

        try:
            if run_discovery:
                try:
                    discovered = run_property_discovery(conn, session, prop, cutoff_datetime, now)
                except Exception as exc:  # noqa: BLE001
                    summaries.append(f"{prop.key}: discovery_error={exc}")
                    continue

            if run_gsc_checks:
                try:
                    metrics = run_property_gsc(conn, gsc_service, prop, cutoff_datetime, now)
                    checked = metrics["checked"]
                    indexed_now = metrics["indexed_now"]
                except Exception as exc:  # noqa: BLE001
                    summaries.append(f"{prop.key}: gsc_error={exc}")
                    continue

            if run_export:
                export_all_json(conn, config.exports_dir, property_key=prop.key)

            summaries.append(f"{prop.key}: discovered={discovered} checked={checked} indexed_now={indexed_now}")
        finally:
            final_state = db.get_property_state(conn, prop.key)
            finished_at = to_ist_iso(now_utc())
            final_state.update(
                {
                    "property_key": prop.key,
                    "current_status": "idle",
                    "last_run_finished_at": finished_at,
                    "updated_at": finished_at,
                }
            )
            db.upsert_property_state(conn, final_state)

    return summaries
