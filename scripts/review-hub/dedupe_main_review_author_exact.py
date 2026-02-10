#!/usr/bin/env python3
"""Additional dedupe pass: author-based exact duplicates (URL-sensitive).

User request
- "동일한 author 기준으로 해서 완전 똑같은것들" 추가 중복 정리
- 단, source_url 이 다르면 일단 보류(=삭제/클리어 하지 않음)

Heuristic (same review)
- Same: brand, platform, product_name, author, review_date, rating, body_hash
- And ALSO same source_url (including both empty)

Action
- Keep first occurrence, clear subsequent duplicate rows (A:O) by writing empty strings.

Env
- REVIEW_HUB_DEDUPE_MAX_ROWS (default 90000)
- REVIEW_HUB_DEDUPE_BATCH (default 200)
"""

from __future__ import annotations

import hashlib
import json
import os
from typing import Any, Dict, List, Tuple

import sys

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, WORKSPACE_ROOT)

from review_hub.sheets_client import GogSheetsClient


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def norm(x: Any) -> str:
    return "" if x is None else str(x).strip()


def load_sink() -> Dict[str, Any]:
    with open(os.path.join(WORKSPACE_ROOT, "config", "review-hub", "google-sheets.sink.json"), "r", encoding="utf-8") as f:
        return json.load(f)


def key_for_row(r: List[str]) -> str:
    # Columns in main_review (A:O):
    # C brand(2) D platform(3) E product_name(4) H review_date(7) I rating(8) J author(9) M body_hash(12) O source_url(14)
    brand = norm(r[2] if len(r) > 2 else "")
    platform = norm(r[3] if len(r) > 3 else "")
    product_name = norm(r[4] if len(r) > 4 else "")
    review_date = norm(r[7] if len(r) > 7 else "")
    rating = norm(r[8] if len(r) > 8 else "")
    author = norm(r[9] if len(r) > 9 else "")
    body_hash = norm(r[12] if len(r) > 12 else "")
    source_url = norm(r[14] if len(r) > 14 else "")
    mat = "|".join([brand, platform, product_name, author, review_date, rating, body_hash, source_url])
    return sha256_hex(mat)


def main() -> int:
    sink = load_sink()
    tab = sink.get("tab") or "main_review"

    max_rows = int(os.environ.get("REVIEW_HUB_DEDUPE_MAX_ROWS") or "90000")
    batch = int(os.environ.get("REVIEW_HUB_DEDUPE_BATCH") or "200")
    batch = max(1, min(batch, 1000))

    client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])

    start = 3
    end = max_rows

    # Read A (sentinel) + A:O (we need O for url). We'll pull A:O once.
    rows = client.get(f"{tab}!A{start}:O{end}")

    seen: Dict[str, int] = {}
    dup_rows: List[int] = []

    for i, r in enumerate(rows):
        rownum = start + i
        a = norm(r[0] if len(r) > 0 else "")
        if not a:
            continue
        k = key_for_row(r)
        # If body_hash missing, skip (can't prove exact)
        if not k:
            continue
        if k in seen:
            dup_rows.append(rownum)
        else:
            seen[k] = rownum

    if not dup_rows:
        print(json.dumps({"ok": True, "duplicates_found": 0}, ensure_ascii=False))
        return 0

    empty_row = [""] * 15

    cleared = 0
    for i in range(0, len(dup_rows), batch):
        chunk = sorted(dup_rows[i : i + batch])
        # contiguous runs
        runs: List[Tuple[int, int]] = []
        s = e = chunk[0]
        for rn in chunk[1:]:
            if rn == e + 1:
                e = rn
            else:
                runs.append((s, e))
                s = e = rn
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
                "unique_groups": len(seen),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
