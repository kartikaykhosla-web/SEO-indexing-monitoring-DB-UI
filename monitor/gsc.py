from __future__ import annotations

import json
import os
from typing import Any, Dict, Optional

from .time_utils import display_as_ist


def is_quota_exceeded_error(error_message: str) -> bool:
    text = (error_message or "").lower()
    return "quota exceeded" in text or "httperror 429" in text or "429" in text


def status_bucket(status: str, error: str) -> str:
    if is_quota_exceeded_error(error):
        return "Quota Exceeded"
    if error:
        return "Error"
    return (status or "Unknown").strip() or "Unknown"


def build_gsc_service(service_account_path: str):
    if not service_account_path and not os.environ.get("GSC_SERVICE_ACCOUNT_JSON", "").strip():
        return None

    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    scopes = ["https://www.googleapis.com/auth/webmasters.readonly"]

    if service_account_path:
        creds = service_account.Credentials.from_service_account_file(service_account_path, scopes=scopes)
    else:
        payload = json.loads(os.environ["GSC_SERVICE_ACCOUNT_JSON"])
        creds = service_account.Credentials.from_service_account_info(payload, scopes=scopes)

    return build("searchconsole", "v1", credentials=creds, cache_discovery=False)


def normalize_result(index_status: Dict[str, Any]) -> Dict[str, str]:
    verdict = str(index_status.get("verdict", "") or "")
    coverage_state = str(index_status.get("coverageState", "") or "")
    indexing_state = str(index_status.get("indexingState", "") or "")
    page_fetch_state = str(index_status.get("pageFetchState", "") or "")
    robots_state = str(index_status.get("robotsTxtState", "") or "")

    if robots_state == "DISALLOWED" or page_fetch_state == "BLOCKED_ROBOTS_TXT":
        status = "Blocked by robots.txt"
    elif indexing_state in ("BLOCKED_BY_META_TAG", "BLOCKED_BY_HTTP_HEADER"):
        status = "Blocked by noindex"
    elif verdict == "PASS":
        status = "Indexed"
    elif verdict == "NEUTRAL":
        status = "Excluded"
    elif verdict == "FAIL":
        status = "Error"
    else:
        status = "Unknown"

    return {
        "status": status,
        "verdict": verdict,
        "coverage_state": coverage_state,
        "indexing_state": indexing_state,
        "page_fetch_state": page_fetch_state,
        "robots_state": robots_state,
        "last_crawl_time": display_as_ist(str(index_status.get("lastCrawlTime", "") or "")),
        "error": "",
    }


def inspect_url(gsc_service, inspection_url: str, site_url: str) -> Dict[str, str]:
    if gsc_service is None:
        return {
            "status": "Error",
            "verdict": "",
            "coverage_state": "",
            "indexing_state": "",
            "page_fetch_state": "",
            "robots_state": "",
            "last_crawl_time": "",
            "error": "GSC client not configured",
        }

    try:
        response = (
            gsc_service.urlInspection()
            .index()
            .inspect(
                body={
                    "inspectionUrl": inspection_url,
                    "siteUrl": site_url,
                    "languageCode": "en-US",
                }
            )
            .execute()
        )
        index_status = response.get("inspectionResult", {}).get("indexStatusResult", {})
        if not isinstance(index_status, dict):
            return {
                "status": "Error",
                "verdict": "",
                "coverage_state": "",
                "indexing_state": "",
                "page_fetch_state": "",
                "robots_state": "",
                "last_crawl_time": "",
                "error": "No index status returned by GSC",
            }
        return normalize_result(index_status)
    except Exception as exc:  # noqa: BLE001
        return {
            "status": "Error",
            "verdict": "",
            "coverage_state": "",
            "indexing_state": "",
            "page_fetch_state": "",
            "robots_state": "",
            "last_crawl_time": "",
            "error": str(exc),
        }
