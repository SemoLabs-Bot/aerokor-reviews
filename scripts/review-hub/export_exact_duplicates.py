#!/usr/bin/env python3
"""Export exact duplicate reviews into a separate Google Sheets tab.

Goal
- Detect *exactly identical* reviews in main_review and write them into a new tab.
- This is for diagnosis/QA (dashboard currently shows duplicates).

Definition (exact duplicate)
- We consider rows identical if the following fields match exactly ("loose-exact"):
  brand, platform, product_name, author, review_date, rating, title, body_hash

Rationale
- Exports/collectors sometimes omit or vary product_url/review_id while still referring to the same review.

Notes
- collected_at/collected_date are intentionally NOT part of the key because the
  same review may be re-collected at different times.

Output tab
- `exact_duplicates` (created if missing)
- Overwrites the tab each run (clear + write fresh snapshot)

Env
- REVIEW_HUB_MAX_ROWS (default 60000)
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, List, Tuple

# Ensure workspace root is on sys.path so `import review_hub` works.
import sys

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, WORKSPACE_ROOT)

from review_hub.sheets_admin import ensure_tab_row_capacity, ensure_tabs_exist
from review_hub.sheets_client import GogSheetsClient


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def norm(x: Any) -> str:
    return "" if x is None else str(x).strip()


def load_sink() -> Dict[str, Any]:
    with open(os.path.join(WORKSPACE_ROOT, "config", "review-hub", "google-sheets.sink.json"), "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    sink = load_sink()
    account = sink["account"]
    sheet_id = sink["sheetId"]
    tab_main = sink.get("tab") or "main_review"
    tab_out = "exact_duplicates"

    max_rows = int(os.environ.get("REVIEW_HUB_MAX_ROWS") or "60000")

    # Ensure output tab exists
    creds_path = os.path.expanduser("~/Library/Application Support/gogcli/credentials.json")
    ensure_tabs_exist(
        spreadsheet_id=sheet_id,
        account_email=account,
        titles=[tab_out],
        credentials_path=creds_path,
    )

    client = GogSheetsClient(account=account, spreadsheet_id=sheet_id)

    # Read required columns from main_review
    start = 3
    end = max_rows

    # Columns in main_review (A:O):
    # A collected_date
    # B collected_at
    # C brand
    # D platform
    # E product_name
    # F product_url
    # G review_id
    # H review_date
    # I rating
    # J author
    # K title
    # L body
    # M body_hash
    # N dedup_key
    # O source_url

    # We only need C:N + some row id; but easiest is to pull A:O.
    rows = client.get(f"{tab_main}!A{start}:O{end}")

    # Track duplicates
    seen: Dict[str, int] = {}
    groups: Dict[str, List[int]] = {}

    def key_for(r: List[str]) -> str:
        brand = norm(r[2] if len(r) > 2 else "")
        platform = norm(r[3] if len(r) > 3 else "")
        product_name = norm(r[4] if len(r) > 4 else "")
        review_date = norm(r[7] if len(r) > 7 else "")
        rating = norm(r[8] if len(r) > 8 else "")
        author = norm(r[9] if len(r) > 9 else "")
        title = norm(r[10] if len(r) > 10 else "")
        body_hash = norm(r[12] if len(r) > 12 else "")
        mat = "|".join([brand, platform, product_name, author, review_date, rating, title, body_hash])
        return sha256_hex(mat)

    for i, r in enumerate(rows):
        rownum = start + i
        collected_date = norm(r[0] if len(r) > 0 else "")
        if not collected_date:
            continue
        k = key_for(r)
        if k in seen:
            groups.setdefault(k, [seen[k]]).append(rownum)
        else:
            seen[k] = rownum

    # Build output
    header = [
        "group_key",
        "occurrence_index",
        "rownum",
        "group_count",
        "brand",
        "platform",
        "product_name",
        "product_url",
        "review_id",
        "review_date",
        "rating",
        "author",
        "title",
        "body_hash",
        "dedup_key",
        "source_url",
    ]

    out: List[List[object]] = [header]

    for gk, rownums in sorted(groups.items(), key=lambda x: len(x[1]), reverse=True):
        rownums_sorted = sorted(rownums)
        group_count = len(rownums_sorted)
        for idx0, rn in enumerate(rownums_sorted, start=1):
            # locate row in cached rows array
            r = rows[rn - start]
            brand = norm(r[2] if len(r) > 2 else "")
            platform = norm(r[3] if len(r) > 3 else "")
            product_name = norm(r[4] if len(r) > 4 else "")
            product_url = norm(r[5] if len(r) > 5 else "")
            review_id = norm(r[6] if len(r) > 6 else "")
            review_date = norm(r[7] if len(r) > 7 else "")
            rating = norm(r[8] if len(r) > 8 else "")
            author = norm(r[9] if len(r) > 9 else "")
            title = norm(r[10] if len(r) > 10 else "")
            body_hash = norm(r[12] if len(r) > 12 else "")
            dedup_key = norm(r[13] if len(r) > 13 else "")
            source_url = norm(r[14] if len(r) > 14 else "")

            out.append(
                [
                    gk,
                    idx0,
                    rn,
                    group_count,
                    brand,
                    platform,
                    product_name,
                    product_url,
                    review_id,
                    review_date,
                    rating,
                    author,
                    title,
                    body_hash,
                    dedup_key,
                    source_url,
                ]
            )

    # Ensure tab has capacity
    ensure_tab_row_capacity(
        spreadsheet_id=sheet_id,
        account_email=account,
        tab_title=tab_out,
        min_rows=max(1000, len(out) + 20),
        credentials_path=creds_path,
    )

    # Clear then write
    # We clear a wide-ish range to avoid leftover.
    client.update(f"{tab_out}!A1:P50000", [[""] * 16 for _ in range(2000)])

    # Write fresh (limit payload size with moderate chunk)
    chunk = 400
    for i in range(0, len(out), chunk):
        part = out[i : i + chunk]
        start_row_out = 1 + i
        end_row_out = start_row_out + len(part) - 1
        client.update(f"{tab_out}!A{start_row_out}:P{end_row_out}", part)

    print(
        json.dumps(
            {
                "ok": True,
                "groups": len(groups),
                "rows_written": len(out) - 1,
                "tab": tab_out,
            },
            ensure_ascii=False,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
