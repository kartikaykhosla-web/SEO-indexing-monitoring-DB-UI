from __future__ import annotations

import datetime as dt
import tempfile
import unittest
from pathlib import Path

from monitor import db
from monitor.gsc import normalize_result
from monitor.config import PropertyConfig
from monitor.worker import order_due_rows_for_gsc, row_due_for_gsc


class GscStatusTests(unittest.TestCase):
    def test_submitted_and_indexed_is_indexed_even_when_verdict_is_neutral(self) -> None:
        result = normalize_result(
            {
                "verdict": "NEUTRAL",
                "coverageState": "Submitted and indexed",
                "indexingState": "INDEXING_ALLOWED",
                "pageFetchState": "SUCCESSFUL",
                "robotsTxtState": "ALLOWED",
            }
        )

        self.assertEqual(result["status"], "Indexed")

    def test_indexed_not_submitted_in_sitemap_is_indexed(self) -> None:
        result = normalize_result(
            {
                "verdict": "NEUTRAL",
                "coverageState": "Indexed, not submitted in sitemap",
                "indexingState": "INDEXING_ALLOWED",
                "pageFetchState": "SUCCESSFUL",
                "robotsTxtState": "ALLOWED",
            }
        )

        self.assertEqual(result["status"], "Indexed")

    def test_excluded_row_with_google_last_crawl_can_be_rechecked(self) -> None:
        row = {
            "current_status": "Excluded",
            "google_last_crawl_at": "2026-07-13T11:48:06+05:30",
            "next_check_at": "2026-07-13T11:56:10+05:30",
            "last_checked_at": "2026-07-13T11:51:51+05:30",
            "first_checked_at": "2026-07-13T11:32:50+05:30",
            "check_count": 3,
        }
        now = dt.datetime.fromisoformat("2026-07-13T12:00:00+05:30")

        self.assertTrue(row_due_for_gsc(row, now))

    def test_retry_candidates_are_prioritized_before_new_urls(self) -> None:
        db_path = Path(tempfile.gettempdir()) / "seo_indexing_monitor_retry_order_test.db"
        db_path.unlink(missing_ok=True)
        conn = db.connect(str(db_path))
        db.init_db(conn)
        db.upsert_discovered_urls(
            conn,
            "jagran.com",
            [
                (
                    "https://www.jagran.com/new-url.html",
                    "2026-07-13T18:00:00+05:30",
                    "2026-07-13T18:00:10+05:30",
                    "2026-07-13",
                ),
                (
                    "https://www.jagran.com/excluded-retry.html",
                    "2026-07-13T16:00:00+05:30",
                    "2026-07-13T16:00:10+05:30",
                    "2026-07-13",
                ),
            ],
        )
        rows = {row["url"]: row for row in db.fetch_property_urls(conn, "jagran.com")}
        db.update_url_state(
            conn,
            rows["https://www.jagran.com/excluded-retry.html"]["id"],
            {
                "current_status": "Excluded",
                "check_count": 2,
                "first_checked_at": "2026-07-13T16:10:00+05:30",
                "last_checked_at": "2026-07-13T16:20:00+05:30",
                "next_check_at": "2026-07-13T16:25:00+05:30",
            },
        )

        candidates = db.fetch_due_candidates(conn, "jagran.com")

        self.assertEqual(candidates[0]["url"], "https://www.jagran.com/excluded-retry.html")
        self.assertEqual(candidates[1]["url"], "https://www.jagran.com/new-url.html")

    def test_capped_runs_reserve_slots_for_new_pending_urls(self) -> None:
        property_cfg = PropertyConfig(
            key="jagran.com",
            gsc_site_url="https://www.jagran.com/",
            sitemap_urls=[],
            max_gsc_checks_per_hour=25,
            max_gsc_checks_per_run=25,
        )
        state = {
            "gsc_hour_bucket": "2026-07-13T18:00:00+05:30",
            "gsc_checks_this_hour": 0,
        }
        retry_rows = [
            {
                "url": f"https://www.jagran.com/retry-{index}.html",
                "check_count": 2,
                "next_check_at": "2026-07-13T18:10:00+05:30",
            }
            for index in range(25)
        ]
        new_rows = [
            {
                "url": f"https://www.jagran.com/new-{index}.html",
                "check_count": 0,
                "next_check_at": "",
            }
            for index in range(10)
        ]
        now = dt.datetime.fromisoformat("2026-07-13T18:30:00+05:30")

        ordered = order_due_rows_for_gsc(property_cfg, state, retry_rows + new_rows, now)
        first_run_urls = [row["url"] for row in ordered[:25]]

        self.assertEqual(sum("/retry-" in url for url in first_run_urls), 20)
        self.assertEqual(sum("/new-" in url for url in first_run_urls), 5)


if __name__ == "__main__":
    unittest.main()
