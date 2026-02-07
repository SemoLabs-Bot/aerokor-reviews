from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from typing import List, Optional

from review_hub.lock import file_lock

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_LOCK = os.path.join(WORKSPACE_ROOT, "state/review-hub/browser-queue.lock")


@dataclass
class QueueConfig:
    browser_profile_ohou: str = "openclaw-ohou"
    browser_profile_coupang: str = "openclaw-coupang"
    browser_profile_naver_brand: str = "openclaw-naver-brand"
    browser_profile_smartstore: str = "openclaw-smartstore"
    browser_profile_wadiz: str = "openclaw-wadiz"

    # how long a single invocation should run (keeps cron runs bounded)
    max_minutes: int = 55

    # pacing (avoid hammering too hard; user asked for speed, so small)
    sleep_s_between_collectors: float = 2.0


def _run(cmd: str, *, timeout_s: int) -> int:
    # Use bash -lc so env + multi-line commands work.
    p = subprocess.run(
        ["bash", "-lc", cmd],
        cwd=WORKSPACE_ROOT,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout_s,
        check=False,
    )
    return int(p.returncode)


def run_one_round(cfg: QueueConfig) -> List[str]:
    """Run one pass over the browser-based collectors in a fixed order."""
    cmds: List[str] = [
        # 오늘의집
        (
            "python3 -m review_hub.collect_ohou_browser --all-brands "
            f"--browser-profile {cfg.browser_profile_ohou} "
            "--max-goods 8000 --max-scrolls 80 --scroll-wait-ms 600 "
            "--per-page 100 --order recent --max-pages-per-goods 80 --max-reviews-per-goods 1200"
        ),
        # 쿠팡
        (
            "python3 -m review_hub.collect_coupang_browser --all-brands "
            f"--browser-profile {cfg.browser_profile_coupang} "
            "--max-brand-pages 20 --max-products-per-brand 160 "
            "--max-pages-per-product 10 --max-reviews-per-product 150 --order latest"
        ),
        # 네이버 브랜드스토어
        (
            "python3 -m review_hub.collect_naver_brand_browser --all-brands "
            f"--browser-profile {cfg.browser_profile_naver_brand} "
            "--max-products-per-brand 200 --max-scrolls 80 "
            "--max-review-pages-per-product 10 --max-reviews-per-product 200 --order latest"
        ),
        # 스마트스토어
        (
            "python3 -m review_hub.collect_smartstore_browser --all-brands "
            f"--browser-profile {cfg.browser_profile_smartstore}"
        ),
        # 와디즈
        (
            "python3 -m review_hub.collect_wadiz_qa --urls-file state/review-hub/wadiz-qa-urls.txt "
            f"--browser-profile {cfg.browser_profile_wadiz}"
        ),
    ]

    completed: List[str] = []
    for c in cmds:
        # Hard cap each collector so the whole queue doesn't get stuck.
        # (These collectors already have internal max limits; this is a safety net.)
        _run(c, timeout_s=1800)
        completed.append(c.split(" ")[3])  # module name after -m
        time.sleep(cfg.sleep_s_between_collectors)
    return completed


def main(argv: Optional[List[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Run browser collectors sequentially (ohou -> coupang -> naver -> smartstore -> wadiz)")
    ap.add_argument("--max-minutes", type=int, default=55)
    ap.add_argument("--sleep", type=float, default=2.0)

    ap.add_argument("--browser-profile-ohou", default="openclaw-ohou")
    ap.add_argument("--browser-profile-coupang", default="openclaw-coupang")
    ap.add_argument("--browser-profile-naver-brand", default="openclaw-naver-brand")
    ap.add_argument("--browser-profile-smartstore", default="openclaw-smartstore")
    ap.add_argument("--browser-profile-wadiz", default="openclaw-wadiz")

    args = ap.parse_args(argv)

    cfg = QueueConfig(
        browser_profile_ohou=args.browser_profile_ohou,
        browser_profile_coupang=args.browser_profile_coupang,
        browser_profile_naver_brand=args.browser_profile_naver_brand,
        browser_profile_smartstore=args.browser_profile_smartstore,
        browser_profile_wadiz=args.browser_profile_wadiz,
        max_minutes=int(args.max_minutes),
        sleep_s_between_collectors=float(args.sleep),
    )

    deadline = time.time() + cfg.max_minutes * 60

    with file_lock(DEFAULT_LOCK):
        # Loop rounds until time is up. Next cron tick can pick up again.
        while time.time() < deadline:
            run_one_round(cfg)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
