#!/usr/bin/env python3
"""Review Hub imweb attack worker (2m cap + flock lock).

Designed to be called from OpenClaw cron payloads.
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

# Ensure repo root is on sys.path when run as a script
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.abspath(os.path.join(_HERE, "..", ".."))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from review_hub.lock import file_lock


def main() -> int:
    repo = os.environ.get("REVIEW_HUB_REPO", "/Users/semobot/Desktop/workspace/My-Assistant")
    lock_path = os.environ.get("IMWEB_LOCK", "state/review-hub/imweb-run.lock")
    workers = os.environ.get("IMWEB_WORKERS", "24")
    max_products = os.environ.get("IMWEB_MAX_PRODUCTS", "6000")

    # hard cap: ~2 minutes to avoid overlap; leave a few seconds for cleanup
    timeout_s = float(os.environ.get("IMWEB_TIMEOUT_S", "115"))

    cmd = f"IMWEB_WORKERS={workers} python3 -m review_hub.collect_reviews --max-products {max_products}"

    os.chdir(repo)

    started = time.time()
    with file_lock(lock_path):
        try:
            subprocess.run(
                ["bash", "-lc", cmd],
                check=False,
                timeout=timeout_s,
            )
            return 0
        except subprocess.TimeoutExpired:
            elapsed = time.time() - started
            print(f"[imweb_worker_attack_2m] TIMEOUT after {elapsed:.1f}s", file=sys.stderr)
            return 124


if __name__ == "__main__":
    raise SystemExit(main())
