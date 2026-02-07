from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from typing import Any, Dict, List

from review_hub.sheets_client import GogSheetsClient
from review_hub.state import TextSet

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kst_now() -> dt.datetime:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def iter_reviews(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    if isinstance(payload.get("reviews"), list):
        return [x for x in payload["reviews"] if isinstance(x, dict)]
    return []


def main(in_path: str) -> None:
    sink = load_sink_config()

    client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])
    dedup_state = TextSet(os.path.join(WORKSPACE_ROOT, sink["dedup"]["localStatePath"]))
    seen = dedup_state.load()

    payload = json.load(open(in_path, "r", encoding="utf-8"))
    reviews = iter_reviews(payload)

    now = kst_now()
    collected_date = now.date().isoformat()
    collected_at = now.isoformat()

    rows: List[List[object]] = []
    new_keys: List[str] = []

    for r in reviews:
        platform = str(r.get("platform") or "coupang")
        brand = str(r.get("brand") or "")
        product_name = str(r.get("product_name") or r.get("productName") or "")
        product_url = str(r.get("product_url") or r.get("productUrl") or "")
        source_url = str(r.get("source_url") or r.get("sourceUrl") or product_url)

        review_id = str(r.get("review_id") or r.get("reviewId") or "")
        review_date = str(r.get("review_date") or r.get("reviewDate") or "")
        author = str(r.get("author") or "")
        title = str(r.get("title") or "")
        body = str(r.get("body") or "")

        # normalize rating
        rating = r.get("rating")
        if rating is None:
            rating_cell: object = ""
        else:
            try:
                rating_cell = float(rating)
            except Exception:
                rating_cell = str(rating)

        body_hash = sha256(body)
        dedup_key = sha256("|".join([
            platform,
            product_url,
            author,
            review_date,
            body_hash,
        ]))
        if dedup_key in seen:
            continue
        seen.add(dedup_key)
        new_keys.append(dedup_key)

        rows.append([
            collected_date,
            collected_at,
            brand,
            platform,
            product_name,
            product_url,
            review_id,
            review_date,
            rating_cell,
            author,
            title,
            body,
            body_hash,
            dedup_key,
            source_url,
        ])

    if rows:
        client.append_fixed(
            tab=sink.get("tab") or "시트1",
            start_row=3,
            start_col="A",
            end_col="O",
            values_2d=rows,
            sentinel_col="A",
            sentinel_regex=r"^\d{4}-\d{2}-\d{2}$",
            scan_max_rows=20000,
        )
        dedup_state.add_many(new_keys)

    print(json.dumps({"reviews_seen": len(reviews), "reviews_appended": len(rows), "dedup_added": len(new_keys)}, ensure_ascii=False))


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("in_path")
    args = ap.parse_args()
    main(args.in_path)
