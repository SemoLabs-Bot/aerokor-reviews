from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from typing import Any, Dict, List

from review_hub.sheets_client import GogSheetsClient
from review_hub.state import TextSet
from review_hub.sheets_admin import ensure_tab_row_capacity

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kst_now() -> dt.datetime:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def _within_lookback(review_date: str, *, now: dt.datetime, lookback_days: int) -> bool:
    if lookback_days <= 0:
        return True
    s = (review_date or "").strip()
    if not s:
        return True

    cutoff = (now - dt.timedelta(days=max(lookback_days - 1, 0))).date()

    # Ohou often uses YYYY.MM.DD
    for fmt in ["%Y.%m.%d", "%Y-%m-%d", "%Y/%m/%d"]:
        try:
            d = dt.datetime.strptime(s, fmt).date()
            return d >= cutoff
        except Exception:
            pass

    # Unknown format -> keep
    return True


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_reviews(payload: Any) -> List[Dict[str, Any]]:
    """Accepts either:
    - { items: [ {productionId, reviews:[...]}, ... ] }
    - { reviews: [...] }
    - [ ... ]
    and returns flat list of review dicts.
    """
    if isinstance(payload, list):
        # could already be review objects
        out: List[Dict[str, Any]] = []
        for x in payload:
            if isinstance(x, dict):
                out.append(x)
        return out

    if not isinstance(payload, dict):
        return []

    if isinstance(payload.get("reviews"), list):
        return [x for x in payload["reviews"] if isinstance(x, dict)]

    items = payload.get("items")
    if isinstance(items, list):
        out2: List[Dict[str, Any]] = []
        for it in items:
            if not isinstance(it, dict):
                continue
            for r in (it.get("reviews") or []):
                if isinstance(r, dict):
                    out2.append(r)
        return out2

    return []


def main(in_path: str):
    sink = load_sink_config()
    lookback_days = int(sink.get("reviewLookbackDays") or 0)

    client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])
    dedup_state = TextSet(os.path.join(WORKSPACE_ROOT, sink["dedup"]["localStatePath"]))
    seen = dedup_state.load()

    payload = json.load(open(in_path, "r", encoding="utf-8"))
    reviews = iter_reviews(payload)

    now = kst_now()
    collected_date = now.date().isoformat()
    collected_at = now.isoformat()

    rows_to_append: List[List[object]] = []
    new_keys: List[str] = []
    rows_to_update: List[tuple[int, List[object]]] = []

    for r in reviews:
        product_url = str(r.get("product_url") or r.get("productUrl") or r.get("source_url") or r.get("sourceUrl") or "")
        source_url = str(r.get("source_url") or r.get("sourceUrl") or product_url)
        platform = str(r.get("platform") or "ohou")

        review_date = str(r.get("review_date") or r.get("reviewDate") or "")
        if not _within_lookback(review_date, now=now, lookback_days=lookback_days):
            continue

        body = str(r.get("body") or "")
        body_hash = sha256(body)
        author = str(r.get("author") or "")

        # Default goods URL if productionId present
        if not product_url:
            pid = r.get("productionId") or r.get("production_id")
            if pid:
                product_url = f"https://store.ohou.se/goods/{pid}"
                if not source_url:
                    source_url = product_url

        dedup_key = sha256("|".join([
            platform,
            product_url,
            author,
            review_date,
            body_hash,
        ]))
        # We UPSERT by dedup_key: if it already exists on the sheet, update that row;
        # otherwise append a new row.
        row_payload = [
            collected_date,
            collected_at,
            str(r.get("brand") or ""),
            platform,
            str(r.get("product_name") or r.get("productName") or ""),
            product_url,
            str(r.get("review_id") or r.get("reviewId") or ""),
            review_date,
            r.get("rating") if r.get("rating") is not None else "",
            author,
            str(r.get("title") or ""),
            body,
            body_hash,
            dedup_key,
            source_url,
        ]

        rows_to_append.append(row_payload)

    reviews_appended = 0
    reviews_updated = 0

    if rows_to_append:
        # WARNING: gog passes values as a single CLI argument (--values-json ...).
        # For large batches (especially with long review bodies), this can exceed OS argv limits.
        # SheetsClient.update() already has a curl+API fallback for large payloads.
        tab = sink.get("tab") or "main_review"

        # Ensure the destination tab has enough rows (Google Sheets grid limit).
        creds_path = os.path.expanduser("~/Library/Application Support/gogcli/credentials.json")
        ensure_tab_row_capacity(
            spreadsheet_id=sink["sheetId"],
            account_email=sink["account"],
            tab_title=tab,
            min_rows=50000,
            credentials_path=creds_path,
        )

        batch_size = int(os.environ.get("REVIEW_HUB_SHEETS_BATCH") or "20")
        batch_size = max(1, min(batch_size, 200))

        # Build an index of existing dedup_key -> row number (single scan).
        import re

        max_scan_row = int(os.environ.get("REVIEW_HUB_SHEETS_MAX_SCAN_ROW") or "50000")
        colA = client.get(f"{tab}!A3:A{max_scan_row}")
        colN = client.get(f"{tab}!N3:N{max_scan_row}")

        key_to_row: Dict[str, int] = {}
        pat_date = re.compile(r"^\d{4}-\d{2}-\d{2}$")
        last = 2
        for i in range(0, max(len(colA), len(colN))):
            rownum = 3 + i
            a = (colA[i][0] if i < len(colA) and colA[i] else "").strip()
            k = (colN[i][0] if i < len(colN) and colN[i] else "").strip()
            if a and isinstance(a, str) and pat_date.search(a.strip()):
                last = rownum
            if a and k:
                # keep first occurrence
                if k not in key_to_row:
                    key_to_row[k] = rownum

        # Split incoming rows into updates vs appends.
        to_update: List[tuple[int, List[object]]] = []
        to_append: List[List[object]] = []

        for row in rows_to_append:
            k = str(row[13] or "").strip()
            if k and k in key_to_row:
                to_update.append((key_to_row[k], row))
            else:
                to_append.append(row)
                if k:
                    new_keys.append(k)

        # UPSERT updates (group contiguous rows to reduce API calls).
        if to_update:
            to_update.sort(key=lambda x: x[0])
            run_start = None
            run_rows: List[List[object]] = []
            prev = None

            def flush_run():
                nonlocal reviews_updated, run_start, run_rows
                if run_start is None or not run_rows:
                    return
                end = run_start + len(run_rows) - 1
                client.update(f"{tab}!A{run_start}:O{end}", run_rows)
                reviews_updated += len(run_rows)
                run_start = None
                run_rows = []

            for rownum, payload_row in to_update:
                if run_start is None:
                    run_start = rownum
                    run_rows = [payload_row]
                    prev = rownum
                    continue
                if rownum == prev + 1:
                    run_rows.append(payload_row)
                    prev = rownum
                else:
                    flush_run()
                    run_start = rownum
                    run_rows = [payload_row]
                    prev = rownum

            flush_run()

        # Append new rows at the end (fast).
        if to_append:
            next_row = last + 1
            for i in range(0, len(to_append), batch_size):
                chunk = to_append[i : i + batch_size]
                end_row = next_row + len(chunk) - 1
                write_range = f"{tab}!A{next_row}:O{end_row}"
                client.update(write_range, chunk)
                reviews_appended += len(chunk)

                # Persist dedup keys for appended rows only.
                dedup_state.add_many(new_keys[i : i + len(chunk)])

                next_row = end_row + 1

        print(
            json.dumps(
                {
                    "upsert": True,
                    "updated": reviews_updated,
                    "appended": reviews_appended,
                    "append_batches": (len(to_append) + batch_size - 1) // batch_size if to_append else 0,
                },
                ensure_ascii=False,
            )
        )

    print(
        json.dumps(
            {
                "reviews_seen": len(reviews),
                "reviews_appended": reviews_appended,
                "reviews_updated": reviews_updated,
                "dedup_added": len(new_keys),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("in_path")
    args = ap.parse_args()
    main(args.in_path)
