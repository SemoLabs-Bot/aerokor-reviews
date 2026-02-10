#!/usr/bin/env python3
"""Deduplicate main_review tab by (1) review_id and (2) exact body_hash.

Rules (conservative):
- If review_id (col G) is present: keep the first row for each review_id.
  - BUT if source_url (col O) differs between rows that share the same review_id,
    we DO NOT clear those rows (they may refer to different pages/contexts).
- If body_hash (col M) is present: keep the first row for each body_hash.
  - Same safety rule: if source_url differs, we do not clear.

We clear duplicates by writing empty strings to A:O for the duplicate row.

Usage:
  python3 scripts/review-hub/dedupe_main_review_reviewid_or_bodyhash.py

Env:
  REVIEW_HUB_DEDUPE_MAX_ROWS (default 120000)
  REVIEW_HUB_DEDUPE_BATCH (default 200)

Output:
  JSON summary to stdout.
"""

from __future__ import annotations

import json
import os
from typing import Dict, List, Tuple

import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from review_hub.sheets_client import GogSheetsClient


def load_sink() -> Dict:
    with open("config/review-hub/google-sheets.sink.json", "r", encoding="utf-8") as f:
        return json.load(f)


def norm(x: str) -> str:
    return str(x or "").strip()


def main() -> int:
    sink = load_sink()
    tab = sink.get("tab") or "main_review"

    max_rows = int(os.environ.get("REVIEW_HUB_DEDUPE_MAX_ROWS") or "120000")
    batch = int(os.environ.get("REVIEW_HUB_DEDUPE_BATCH") or "200")
    batch = max(1, min(batch, 1000))

    client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])

    start = 3
    end = max_rows

    # We'll scan in pages to avoid Sheets timeouts for very large ranges.
    scan_chunk = int(os.environ.get("REVIEW_HUB_DEDUPE_SCAN_CHUNK") or "8000")
    scan_chunk = max(1000, min(scan_chunk, 20000))
    retries = int(os.environ.get("REVIEW_HUB_DEDUPE_RETRIES") or "4")

    def get_range(a1: str) -> List[List[str]]:
        last_err: Exception | None = None
        for attempt in range(retries):
            try:
                return client.get(a1)
            except Exception as e:
                last_err = e
                # exponential-ish backoff
                import time

                time.sleep(1.5 * (attempt + 1))
        assert last_err is not None
        raise last_err

    # Track first occurrence row per key and its source_url.
    seen_review_id: Dict[str, Tuple[int, str]] = {}
    seen_body_hash: Dict[str, Tuple[int, str]] = {}

    dup_rows: List[int] = []
    skipped_due_to_url_mismatch = 0

    cur = start
    while cur <= end:
        cur_end = min(end, cur + scan_chunk - 1)

        colA = get_range(f"{tab}!A{cur}:A{cur_end}")
        colG = get_range(f"{tab}!G{cur}:G{cur_end}")  # review_id
        colM = get_range(f"{tab}!M{cur}:M{cur_end}")  # body_hash
        colO = get_range(f"{tab}!O{cur}:O{cur_end}")  # source_url

        n = max(len(colA), len(colG), len(colM), len(colO))
        for i in range(n):
            rownum = cur + i
            a = norm(colA[i][0] if i < len(colA) and colA[i] else "")
            if not a:
                continue

            src = norm(colO[i][0] if i < len(colO) and colO[i] else "")
            rid = norm(colG[i][0] if i < len(colG) and colG[i] else "")
            bh = norm(colM[i][0] if i < len(colM) and colM[i] else "")

            cleared = False

            if rid:
                if rid in seen_review_id:
                    _, src0 = seen_review_id[rid]
                    if src0 and src and src0 != src:
                        skipped_due_to_url_mismatch += 1
                    else:
                        dup_rows.append(rownum)
                        cleared = True
                else:
                    seen_review_id[rid] = (rownum, src)

            # Only consider body_hash if we didn't already mark this row as duplicate.
            if (not cleared) and bh:
                if bh in seen_body_hash:
                    _, src0 = seen_body_hash[bh]
                    if src0 and src and src0 != src:
                        skipped_due_to_url_mismatch += 1
                    else:
                        dup_rows.append(rownum)
                else:
                    seen_body_hash[bh] = (rownum, src)

        # Heuristic: if colA returned fewer rows than we requested, we're likely at the end.
        if len(colA) < (cur_end - cur + 1):
            break

        cur += scan_chunk

    dup_rows = sorted(set(dup_rows))

    if not dup_rows:
        print(
            json.dumps(
                {
                    "ok": True,
                    "duplicates_found": 0,
                    "duplicates_cleared": 0,
                    "unique_review_id": len(seen_review_id),
                    "unique_body_hash": len(seen_body_hash),
                    "skipped_due_to_url_mismatch": skipped_due_to_url_mismatch,
                },
                ensure_ascii=False,
            )
        )
        return 0

    empty_row = [""] * 15  # A:O

    def to_runs(rows: List[int]) -> List[Tuple[int, int]]:
        rows = sorted(rows)
        runs: List[Tuple[int, int]] = []
        s = e = rows[0]
        for r in rows[1:]:
            if r == e + 1:
                e = r
            else:
                runs.append((s, e))
                s = e = r
        runs.append((s, e))
        return runs

    cleared = 0
    for i in range(0, len(dup_rows), batch):
        chunk = dup_rows[i : i + batch]
        for s, e in to_runs(chunk):
            values = [empty_row[:] for _ in range(e - s + 1)]
            client.update(f"{tab}!A{s}:O{e}", values)
            cleared += (e - s + 1)

    print(
        json.dumps(
            {
                "ok": True,
                "duplicates_found": len(dup_rows),
                "duplicates_cleared": cleared,
                "unique_review_id": len(seen_review_id),
                "unique_body_hash": len(seen_body_hash),
                "skipped_due_to_url_mismatch": skipped_due_to_url_mismatch,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
