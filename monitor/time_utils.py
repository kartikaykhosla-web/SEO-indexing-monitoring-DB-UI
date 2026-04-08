from __future__ import annotations

import datetime as dt
from typing import Optional

UTC = dt.timezone.utc
IST = dt.timezone(dt.timedelta(hours=5, minutes=30))


def now_utc() -> dt.datetime:
    return dt.datetime.now(tz=UTC)


def now_ist() -> dt.datetime:
    return dt.datetime.now(tz=IST)


def today_ist_midnight(reference: Optional[dt.datetime] = None) -> dt.datetime:
    current = reference.astimezone(IST) if reference else now_ist()
    return current.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_iso_datetime(value: str) -> Optional[dt.datetime]:
    raw = (value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def parse_publication_datetime(value: str) -> Optional[dt.datetime]:
    parsed = parse_iso_datetime(value)
    if parsed:
        return parsed
    raw = (value or "").strip()
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            temp = dt.datetime.strptime(raw, fmt).replace(tzinfo=IST)
            return temp.astimezone(UTC)
        except ValueError:
            continue
    return None


def parse_cutoff_datetime(value: str) -> dt.datetime:
    raw = (value or "").strip()
    if not raw or raw.lower() in {"today", "today_ist", "today-only", "today_only"}:
        return today_ist_midnight().astimezone(UTC)

    parsed = parse_iso_datetime(raw)
    if parsed:
        return parsed

    for fmt in ("%Y-%m-%d %H:%M", "%d-%m-%Y %H:%M", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            temp = dt.datetime.strptime(raw, fmt).replace(tzinfo=IST)
            return temp.astimezone(UTC)
        except ValueError:
            continue

    raise ValueError(f"Unsupported cutoff date format: {value}")


def to_ist_iso(value: dt.datetime) -> str:
    return value.astimezone(IST).replace(microsecond=0).isoformat()


def display_as_ist(value: str) -> str:
    parsed = parse_iso_datetime(value)
    if parsed:
        return to_ist_iso(parsed)
    return value


def midpoint_ist_iso(start_value: str, end_value: str) -> str:
    start_dt = parse_iso_datetime(start_value)
    end_dt = parse_iso_datetime(end_value)
    if not start_dt or not end_dt:
        return display_as_ist(end_value)
    midpoint = start_dt + (end_dt - start_dt) / 2
    return to_ist_iso(midpoint)


def hour_bucket_ist(reference: dt.datetime) -> str:
    bucket = reference.astimezone(IST).replace(minute=0, second=0, microsecond=0)
    return to_ist_iso(bucket)


def indexing_latency_minutes(published_value: str, google_last_crawl_value: str) -> Optional[int]:
    published_dt = parse_publication_datetime(published_value)
    crawl_dt = parse_iso_datetime(google_last_crawl_value)
    if not published_dt or not crawl_dt:
        return None
    latency = int((crawl_dt - published_dt).total_seconds() // 60)
    return max(latency, 0)
