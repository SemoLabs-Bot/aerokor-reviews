#!/usr/bin/env python3
"""Deduplicate main_review tab by dedup_key (col N).

- Keeps the first occurrence of each dedup_key.
- Clears duplicate rows (A:O) by writing empty strings.

Usage:
  python3 scripts/review-hub/dedupe_main_review.py

Env:
  REVIEW_HUB_DEDUPE_MAX_ROWS (default 60000)
  REVIEW_HUB_DEDUPE_BATCH (default 200)  # number of rows cleared per Sheets update
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple

from review_hub.sheets_client import GogSheetsClient


def load_sink() -> Dict:
    with open("config/review-hub/google-sheets.sink.json", "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> int:
    sink = load_sink()
    tab = sink.get("tab") or "main_review"

    max_rows = int(os.environ.get("REVIEW_HUB_DEDUPE_MAX_ROWS") or "60000")
    batch = int(os.environ.get("REVIEW_HUB_DEDUPE_BATCH") or "200")
    batch = max(1, min(batch, 1000))

    client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])

    # Read dedup_key column N and also col A (sentinel) to know which rows are real.
    # Data starts at row 3.
    start = 3
    end = max_rows

    colA = client.get(f"{tab}!A{start}:A{end}")
    colN = client.get(f"{tab}!N{start}:N{end}")

    # Flatten with row indices.
    seen: Dict[str, int] = {}
    dup_rows: List[int] = []

    for i in range(0, max(len(colA), len(colN))):
        rownum = start + i
        a = (colA[i][0] if i < len(colA) and colA[i] else "").strip()
        k = (colN[i][0] if i < len(colN) and colN[i] else "").strip()

        if not a:
            continue
        if not k:
            # if missing key, treat as ignorable for now
            continue
        if k in seen:
            dup_rows.append(rownum)
        else:
            seen[k] = rownum

    if not dup_rows:
        print(json.dumps({"ok": True, "duplicates_found": 0}, ensure_ascii=False))
        return 0

    # Clear duplicate rows.
    empty_row = [""] * 15  # A:O

    cleared = 0
    for i in range(0, len(dup_rows), batch):
        chunk = dup_rows[i : i + batch]
        # Build a compact values_2d of empty rows; but ranges must be contiguous for update.
        # We'll do contiguous runs.
        chunk.sort()
        runs: List[Tuple[int, int]] = []
        s = e = chunk[0]
        for r in chunk[1:]:
            if r == e + 1:
                e = r
            else:
                runs.append((s, e))
                s = e = r
        runs.append((s, e))

        for s, e in runs:
            values = [empty_row[:] for _ in range(e - s + 1)]
            client.update(f"{tab}!A{s}:O{e}", values)
            cleared += (e - s + 1)

    print(
        json.dumps(
            {
                "ok": True,
                "duplicates_found": len(dup_rows),
                "duplicates_cleared": cleared,
                "unique_keys": len(seen),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
