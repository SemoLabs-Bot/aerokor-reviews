from __future__ import annotations

import json
import os
from glob import glob

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _count_lines(path: str) -> int:
    if not os.path.exists(path):
        return 0
    with open(path, "r", encoding="utf-8") as f:
        return sum(1 for _ in f if _.strip())


def main():
    seen_products = os.path.join(WORKSPACE_ROOT, "state/review-hub/seen-product-urls.txt")
    dedup = os.path.join(WORKSPACE_ROOT, "state/review-hub/dedup-keys.txt")

    logs = sorted(glob(os.path.join(WORKSPACE_ROOT, "logs/review-hub/*.json")), key=os.path.getmtime)
    last_log = logs[-1] if logs else None
    last = None
    if last_log:
        try:
            last = json.load(open(last_log, "r", encoding="utf-8"))
        except Exception:
            last = None

    out = {
        "seen_product_urls": _count_lines(seen_products),
        "dedup_keys": _count_lines(dedup),
        "last_log": os.path.basename(last_log) if last_log else None,
        "last": last,
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
