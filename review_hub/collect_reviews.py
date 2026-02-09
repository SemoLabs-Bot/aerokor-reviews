from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
from typing import Dict, List
from urllib.parse import urlparse

from review_hub.collect_imweb_reviews import collect_imweb_reviews
from review_hub.sheets_client import GogSheetsClient
from review_hub.state import TextSet
from review_hub.errors import log_errors


def infer_brand_from_url(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "olly" in host:
        return "OLLY"
    if "soleusair" in host:
        return "Soleusair"
    if "naimolii" in host or "naimoli" in host:
        return "Naimolii"
    if "millkorea" in host or host.startswith("mill"):
        return "Mill"
    if "hanilshop" in host or "domokor" in host:
        return "Hanil"
    return ""

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kst_now():
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _parse_kst_now() -> dt.datetime:
    return kst_now()


def _within_lookback(review_date: str, *, now: dt.datetime, lookback_days: int) -> bool:
    """Best-effort date filter.

    If parsing fails (unknown format), keep the row (fail-open).
    """
    if lookback_days <= 0:
        return True
    s = (review_date or "").strip()
    if not s:
        return True

    # "최근 N일"을 오늘 포함 N일로 해석 (예: 7일 => 오늘~6일전)
    cutoff = (now - dt.timedelta(days=max(lookback_days - 1, 0))).date()

    # Relative formats commonly seen in KR sites.
    try:
        if "시간전" in s:
            return True
        if "분전" in s:
            return True
        if "방금" in s:
            return True
        if s.endswith("일전"):
            n = int("".join([c for c in s if c.isdigit()]) or "0")
            return (now - dt.timedelta(days=n)).date() >= cutoff
    except Exception:
        pass

    # ISO-ish
    for fmt in [
        "%Y-%m-%d",
        "%Y.%m.%d",
        "%Y.%m.%d.",
        "%Y/%m/%d",
    ]:
        try:
            d = dt.datetime.strptime(s, fmt).date()
            return d >= cutoff
        except Exception:
            pass

    # Full ISO with timezone
    try:
        d2 = dt.datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(now.tzinfo).date()
        return d2 >= cutoff
    except Exception:
        pass

    # Unknown format -> keep
    return True


def main(max_products: int = 200):
    sink = load_sink_config()
    client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])
    lookback_days = int(sink.get("reviewLookbackDays") or 0)

    seen_products_path = os.path.join(WORKSPACE_ROOT, "state/review-hub/seen-product-urls.txt")
    product_urls = sorted(TextSet(seen_products_path).load())

    # Only run imweb for now
    imweb_all = [u for u in product_urls if ("imweb.me" in u or "olly-korea.co.kr" in u)]

    # Cursor-based batching so we continue where we left off (avoid re-checking the same first N URLs forever)
    cursor_path = os.path.join(WORKSPACE_ROOT, "state/review-hub/imweb-cursor.txt")
    try:
        cursor = int((open(cursor_path, "r", encoding="utf-8").read() or "0").strip())
    except Exception:
        cursor = 0

    if not imweb_all:
        imweb_urls = []
    else:
        cursor = max(cursor, 0) % len(imweb_all)
        n = min(max_products, len(imweb_all))
        chunk = imweb_all[cursor:cursor + n]
        if len(chunk) < n:
            chunk += imweb_all[0:(n - len(chunk))]
        imweb_urls = chunk
        next_cursor = (cursor + n) % len(imweb_all)
        os.makedirs(os.path.dirname(cursor_path), exist_ok=True)
        with open(cursor_path, "w", encoding="utf-8") as f:
            f.write(str(next_cursor))

    dedup_state = TextSet(os.path.join(WORKSPACE_ROOT, "state/review-hub/dedup-keys.txt"))
    seen_keys = dedup_state.load()

    rows: List[List[object]] = []
    new_keys: List[str] = []
    errs: List[dict] = []
    review_err_rows: List[dict] = []

    # Parallelize per-product collection to speed up.
    try:
        workers = int(os.environ.get("IMWEB_WORKERS") or "6")
    except Exception:
        workers = 6
    workers = max(1, min(workers, 16))

    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _work(u: str):
        product_name, reviews, err = collect_imweb_reviews(u)
        return u, product_name, reviews, err

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_work, u) for u in imweb_urls]
        for fut in as_completed(futs):
            u, product_name, reviews, err = fut.result()
            if err:
                eo = {"brand": infer_brand_from_url(u), "platform": "imweb", "url": u, "status": "", "error": err}
                errs.append({"url": u, "error": err})
                review_err_rows.append(eo)
                continue

            now = kst_now()
            collected_date = now.date().isoformat()
            collected_at = now.isoformat()
            brand = infer_brand_from_url(u)

            for r in reviews:
                # Lookback filter (fail-open)
                if not _within_lookback(str(r.get("review_date") or ""), now=now, lookback_days=lookback_days):
                    continue

                body = r["body"]
                body_hash = r["body_hash"]
                dedup_key = sha256("|".join([
                    "imweb",
                    u,
                    str(r.get("author") or ""),
                    str(r.get("review_date") or ""),
                    body_hash,
                ]))

                if dedup_key in seen_keys:
                    continue
                seen_keys.add(dedup_key)
                new_keys.append(dedup_key)

                rows.append([
                    collected_date,
                    collected_at,
                    brand,
                    "imweb",
                    r.get("product_name") or product_name or "",
                    u,
                    r.get("review_id") or "",
                    r.get("review_date") or "",
                    r.get("rating") if r.get("rating") is not None else "",
                    r.get("author") or "",
                    r.get("title") or "",
                    body,
                    body_hash,
                    dedup_key,
                    r.get("source_url") or u,
                ])

    if rows:
        # Avoid Sheets 'append' heuristics: compute next row and write A..O explicitly.
        client.append_fixed(
            tab=sink.get("tab") or "main_review",
            start_row=3,
            start_col="A",
            end_col="O",
            values_2d=rows,
            sentinel_col="A",
            sentinel_regex=r"^\d{4}-\d{2}-\d{2}$",
            scan_max_rows=20000,
        )
        dedup_state.add_many(new_keys)

    # Write review-collection errors into a dedicated sheet tab.
    try:
        log_errors(client=client, tab="errors_reviews", run_id="collect_reviews", stage="reviews", items=review_err_rows)
    except Exception:
        pass

    # write a local log
    os.makedirs(os.path.join(WORKSPACE_ROOT, "logs/review-hub"), exist_ok=True)
    out = {
        "when": kst_now().isoformat(),
        "max_products": max_products,
        "platform": "imweb",
        "lookback_days": lookback_days,
        "products_checked": len(imweb_urls),
        "reviews_appended": len(rows),
        "dedup_keys_added": len(new_keys),
        "errors": errs[:50],
    }
    with open(os.path.join(WORKSPACE_ROOT, "logs/review-hub/collect-reviews-imweb.json"), "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    print(json.dumps({"reviews_appended": len(rows), "dedup_added": len(new_keys), "errors": len(errs)}, ensure_ascii=False))


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=20)
    args = ap.parse_args()

    main(max_products=args.max_products)
