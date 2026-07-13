from __future__ import annotations

import datetime as dt
import unittest

from monitor.gsc import normalize_result
from monitor.worker import row_due_for_gsc


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


if __name__ == "__main__":
    unittest.main()
