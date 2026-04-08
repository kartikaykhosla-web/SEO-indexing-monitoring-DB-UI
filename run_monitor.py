#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from monitor import db
from monitor.config import load_config
from monitor.gsc import build_gsc_service
from monitor.time_utils import parse_cutoff_datetime, today_ist_midnight
from monitor.worker import run_monitor


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Local SEO indexing monitor (SQLite + JSON exports)")
    parser.add_argument(
        "--config",
        default="config.local.json",
        help="Path to config JSON",
    )
    parser.add_argument(
        "--property",
        default="",
        help="Run only one property key",
    )
    parser.add_argument(
        "--mode",
        choices=["all", "discover", "poll", "export"],
        default="all",
        help="Run mode",
    )
    parser.add_argument(
        "--cutoff-datetime",
        default="",
        help="Optional runtime cutoff override, e.g. 2026-04-08T18:10:00+05:30",
    )
    parser.add_argument(
        "--reset-db",
        action="store_true",
        help="Clear all DB tables before running",
    )
    parser.add_argument(
        "--skip-gsc",
        action="store_true",
        help="Skip GSC checks even in mode=all/poll",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    cfg = load_config(config_path)
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    if args.reset_db:
        db.reset_db(conn)

    cutoff_raw = args.cutoff_datetime.strip() or cfg.cutoff_datetime
    cutoff = parse_cutoff_datetime(cutoff_raw)
    # Safety guard: never process dates before today's IST midnight unless explicit older date is passed.
    if not args.cutoff_datetime.strip() and cfg.cutoff_datetime in {"today", "today_ist", "today-only", "today_only"}:
        cutoff = max(cutoff, today_ist_midnight().astimezone(cutoff.tzinfo))

    properties = cfg.properties
    if args.property:
        properties = [p for p in properties if p.key == args.property]
        if not properties:
            raise ValueError(f"Property not found in config: {args.property}")

    run_discovery = args.mode in {"all", "discover"}
    run_gsc_checks = args.mode in {"all", "poll"} and not args.skip_gsc
    run_export = args.mode in {"all", "export"}

    gsc_service = None
    if run_gsc_checks:
        gsc_service = build_gsc_service(cfg.service_account_json_path)

    summaries = run_monitor(
        conn=conn,
        config=cfg,
        properties=properties,
        cutoff_datetime=cutoff,
        run_discovery=run_discovery,
        run_gsc_checks=run_gsc_checks,
        run_export=run_export,
        gsc_service=gsc_service,
    )

    print("Run completed")
    for item in summaries:
        print(item)

    conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
