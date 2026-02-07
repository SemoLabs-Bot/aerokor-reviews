from __future__ import annotations

import datetime as dt
import json
import os
from typing import Any, Dict, List, Optional

from review_hub.sheets_client import GogSheetsClient

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def kst_now() -> dt.datetime:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def ensure_error_tab_header(*, client: GogSheetsClient, tab: str) -> None:
    # Write header if A1 is empty
    v = client.get(f"{tab}!A1:H1")
    if v and v[0] and str(v[0][0]).strip():
        return
    header = [[
        "collected_at",
        "run_id",
        "stage",
        "brand",
        "platform",
        "url",
        "status",
        "error",
    ]]
    client.update(f"{tab}!A1:H1", header)


def log_errors(
    *,
    client: GogSheetsClient,
    tab: str,
    run_id: str,
    stage: str,
    items: List[Dict[str, Any]],
) -> int:
    """Append error rows into a dedicated error tab.

    Each item expects keys: brand, platform, url, status, error.
    """
    if not items:
        return 0

    ensure_error_tab_header(client=client, tab=tab)
    now = kst_now().isoformat()

    rows: List[List[object]] = []
    for it in items:
        rows.append([
            now,
            run_id,
            stage,
            str(it.get("brand") or ""),
            str(it.get("platform") or ""),
            str(it.get("url") or ""),
            str(it.get("status") or ""),
            str(it.get("error") or ""),
        ])

    # Dedicated tab: safe to use append heuristic.
    client.append(f"{tab}!A2:H", rows)
    return len(rows)
