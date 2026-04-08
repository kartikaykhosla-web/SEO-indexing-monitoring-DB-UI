from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from . import db


def export_property_json(conn, exports_dir: str, property_key: str) -> None:
    target = Path(exports_dir)
    target.mkdir(parents=True, exist_ok=True)

    summary_rows = db.fetch_all_summary(conn, property_key=property_key)
    logs = db.fetch_logs(conn, property_key=property_key, limit=20000)

    (target / f"summary_{property_key}.json").write_text(
        json.dumps(summary_rows, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    with (target / f"checks_{property_key}.jsonl").open("w", encoding="utf-8") as handle:
        for row in logs:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def export_all_json(conn, exports_dir: str, property_key: Optional[str] = None) -> None:
    if property_key:
        export_property_json(conn, exports_dir, property_key)
        return

    all_rows = db.fetch_all_summary(conn, property_key=None)
    keys = sorted({row["property_key"] for row in all_rows})
    for key in keys:
        export_property_json(conn, exports_dir, key)
