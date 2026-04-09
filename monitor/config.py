from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional


@dataclass
class PropertyConfig:
    key: str
    gsc_site_url: str
    sitemap_urls: List[str]
    discovery_interval_minutes: int = 5
    max_gsc_checks_per_hour: Optional[int] = None
    max_new_urls_per_run: Optional[int] = None
    max_gsc_checks_per_run: Optional[int] = None
    allow_lastmod_fallback: bool = False


@dataclass
class MonitorConfig:
    db_path: str
    exports_dir: str
    cutoff_datetime: str
    service_account_json_path: str
    login_history_spreadsheet_id: str
    login_history_worksheet_name: str
    properties: List[PropertyConfig]


def load_config(path: Path) -> MonitorConfig:
    payload = json.loads(path.read_text(encoding="utf-8"))
    props = []
    for item in payload.get("properties", []):
        props.append(
            PropertyConfig(
                key=str(item["key"]),
                gsc_site_url=str(item["gsc_site_url"]),
                sitemap_urls=[str(x) for x in item.get("sitemap_urls", [])],
                discovery_interval_minutes=int(item.get("discovery_interval_minutes", 5)),
                max_gsc_checks_per_hour=(
                    int(item["max_gsc_checks_per_hour"])
                    if item.get("max_gsc_checks_per_hour") is not None
                    else None
                ),
                max_new_urls_per_run=(
                    int(item["max_new_urls_per_run"])
                    if item.get("max_new_urls_per_run") is not None
                    else None
                ),
                max_gsc_checks_per_run=(
                    int(item["max_gsc_checks_per_run"])
                    if item.get("max_gsc_checks_per_run") is not None
                    else None
                ),
                allow_lastmod_fallback=bool(item.get("allow_lastmod_fallback", False)),
            )
        )
    return MonitorConfig(
        db_path=str(payload.get("db_path", "./data/indexing_monitor.db")),
        exports_dir=str(payload.get("exports_dir", "./exports")),
        cutoff_datetime=str(payload.get("cutoff_datetime", "today_ist")),
        service_account_json_path=str(payload.get("service_account_json_path", "")),
        login_history_spreadsheet_id=str(payload.get("login_history_spreadsheet_id", "")),
        login_history_worksheet_name=str(payload.get("login_history_worksheet_name", "login_history")),
        properties=props,
    )


def property_map(config: MonitorConfig) -> Dict[str, PropertyConfig]:
    return {item.key: item for item in config.properties}
