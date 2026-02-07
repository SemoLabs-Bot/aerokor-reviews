from __future__ import annotations

import datetime as dt
import json
import os
import uuid
from dataclasses import dataclass
from typing import Dict, List, Tuple

import yaml

from review_hub.fetch import fetch_html
from review_hub.extract import extract_links, classify_product_links
from review_hub.sheets_client import GogSheetsClient
from review_hub.state import TextSet
from review_hub.errors import log_errors

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@dataclass
class SinkConfig:
    account: str
    sheet_id: str
    append_range: str


def load_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_sink_config(path: str) -> SinkConfig:
    with open(path, "r", encoding="utf-8") as f:
        d = json.load(f)
    return SinkConfig(account=d["account"], sheet_id=d["sheetId"], append_range=d["appendRange"])


def utc_now_iso() -> str:
    return dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc).isoformat()


def kst_now_iso() -> str:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst).isoformat()


def main():
    cfg_path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    src_path = os.path.join(WORKSPACE_ROOT, "config/review-hub/brands-platform-urls.yaml")

    sink = load_sink_config(cfg_path)
    src = load_yaml(src_path)

    client = GogSheetsClient(account=sink.account, spreadsheet_id=sink.sheet_id)

    run_id = f"run_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
    started_at = kst_now_iso()

    # local state
    seen_products = TextSet(os.path.join(WORKSPACE_ROOT, "state/review-hub/seen-product-urls.txt"))

    counts: Dict[str, int] = {
        "sources_total": 0,
        "sources_fetched": 0,
        "sources_failed": 0,
        "product_urls_found": 0,
        "product_urls_new": 0,
    }
    errors: List[dict] = []
    discovery_err_rows: List[dict] = []

    # NOTE: We intentionally avoid Sheets 'append' for in-tab tables because it can
    # shift columns when multiple sections exist. We only write a final run_log row
    # at the end using append_fixed.

    rows_platform_products: List[List[str]] = []

    brands = (src or {}).get("brands", {})
    for brand, b in brands.items():
        platforms = (b or {}).get("platforms", {})
        for platform_key, p in platforms.items():
            url = (p or {}).get("url")
            if not url:
                continue
            counts["sources_total"] += 1

            # Fetch brand/store page
            fr = fetch_html(url)
            if fr.text is None or fr.status_code is None or fr.status_code >= 400:
                counts["sources_failed"] += 1
                err_obj = {
                    "brand": brand,
                    "platform": platform_key,
                    "url": url,
                    "status": fr.status_code,
                    "error": fr.error or "http_error",
                }
                errors.append(err_obj)
                discovery_err_rows.append(err_obj)
                continue

            counts["sources_fetched"] += 1
            # Extract product links (best-effort)
            try:
                links = extract_links(fr.url, fr.text)
                product_links = classify_product_links(platform_key, links)
            except Exception as e:
                counts["sources_failed"] += 1
                err_obj = {"brand": brand, "platform": platform_key, "url": fr.url, "status": "", "error": f"parse_error: {e}"}
                errors.append(err_obj)
                discovery_err_rows.append(err_obj)
                continue

            if not product_links:
                # still record that we touched it
                continue

            counts["product_urls_found"] += len(product_links)

            # Build rows for platform_products table (unmapped product_key for now)
            now = kst_now_iso()
            for pu in product_links[:200]:  # safety cap per source for first run
                rows_platform_products.append(["", platform_key, pu, "", now])

    # Dedup product URLs locally before appending
    existing = seen_products.load()
    new_rows = []
    new_urls = []
    for r in rows_platform_products:
        pu = r[2]
        if pu not in existing:
            new_rows.append(r)
            new_urls.append(pu)

    counts["product_urls_new"] = seen_products.add_many(new_urls)

    # Append to sheet (platform_products section: P..T)
    if new_rows:
        try:
            client.append_fixed(
                tab="시트1",
                start_row=3,
                start_col="P",
                end_col="T",
                values_2d=new_rows,
                sentinel_col="R",  # product_url
                sentinel_regex=r"^https?://",
                scan_max_rows=20000,
            )
        except Exception as e:
            errors.append({"stage": "append_platform_products", "error": str(e), "rows": len(new_rows)})

    ended_at = kst_now_iso()
    status = "ok" if not errors else "partial"
    notes = "product discovery best-effort; reviews collection not yet implemented (next step)."

    # Write discovery errors into a dedicated sheet tab.
    try:
        log_errors(client=client, tab="errors_discovery", run_id=run_id, stage="discovery", items=discovery_err_rows)
    except Exception as e:
        errors.append({"stage": "errors_discovery_tab", "error": str(e)})

    # log end (run_log section: U..Z)
    # Columns: run_id, started_at, ended_at, status, counts_json, errors_json
    try:
        client.append_fixed(
            tab="시트1",
            start_row=3,
            start_col="U",
            end_col="Z",
            values_2d=[[
                run_id,
                started_at,
                ended_at,
                status,
                json.dumps({"counts": counts, "notes": notes}, ensure_ascii=False),
                json.dumps(errors, ensure_ascii=False),
            ]],
            sentinel_col="U",
            sentinel_regex=r"^run_",
            scan_max_rows=5000,
        )
    except Exception as e:
        errors.append({"stage": "run_log_end", "error": str(e)})

    # also write local run artifact
    os.makedirs(os.path.join(WORKSPACE_ROOT, "logs/review-hub"), exist_ok=True)
    with open(os.path.join(WORKSPACE_ROOT, f"logs/review-hub/{run_id}.json"), "w", encoding="utf-8") as f:
        json.dump({"run_id": run_id, "started_at": started_at, "ended_at": ended_at, "status": status, "counts": counts, "errors": errors}, f, ensure_ascii=False, indent=2)

    print(json.dumps({"run_id": run_id, "status": status, "counts": counts, "errors": errors[:3]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
