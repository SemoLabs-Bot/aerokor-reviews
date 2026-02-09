from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import tempfile
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import yaml

from review_hub.lock import file_lock
from review_hub.state import TextSet

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_SOURCES_YAML = os.path.join(WORKSPACE_ROOT, "config/review-hub/brands-platform-urls.yaml")
DEFAULT_DEDUP_PATH = os.path.join(WORKSPACE_ROOT, "state/review-hub/dedup-keys.txt")
DEFAULT_OUT_DIR = os.path.join(WORKSPACE_ROOT, "out/review-hub/ohou")
DEFAULT_COLLECTOR_LOCK = os.path.join(WORKSPACE_ROOT, "state/review-hub/ohou-collector.lock")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kst_now() -> dt.datetime:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def _slug(s: str) -> str:
    s = (s or "").strip()
    if not s:
        return "unknown"
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9A-Za-z가-힣_\-]+", "", s)
    return s[:80] or "unknown"


def load_sources_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def iter_ohou_brand_sources(cfg: dict, *, only_brands: Optional[Set[str]] = None) -> List[Tuple[str, str]]:
    brands = (cfg or {}).get("brands") or {}
    out: List[Tuple[str, str]] = []
    for brand, bcfg in brands.items():
        if only_brands and brand not in only_brands:
            continue
        platforms = (bcfg or {}).get("platforms") or {}
        ohou = platforms.get("ohou")
        if isinstance(ohou, dict) and ohou.get("url"):
            out.append((brand, str(ohou["url"]).strip()))
    return out


def _run(cmd: Sequence[str], *, timeout_s: int = 60, check: bool = True) -> str:
    p = subprocess.run(
        list(cmd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        encoding="utf-8",
        timeout=timeout_s,
        check=False,
    )
    if check and p.returncode != 0:
        raise RuntimeError(f"command failed ({p.returncode}): {' '.join(cmd)}\n{p.stdout}")
    return p.stdout


def _run_json(cmd: Sequence[str], *, timeout_s: int = 60) -> dict:
    out = _run(cmd, timeout_s=timeout_s, check=True)
    # openclaw sometimes prints extra non-JSON lines; try to find the last JSON object.
    out = out.strip()
    if not out:
        return {}
    lines = [ln for ln in out.splitlines() if ln.strip()]
    # Try parsing whole output first.
    try:
        return json.loads(out)
    except Exception:
        pass

    # Find last line that parses as JSON.
    for ln in reversed(lines):
        try:
            return json.loads(ln)
        except Exception:
            continue

    # As a fallback, try parsing from the first '{' of the last JSON-ish chunk.
    blob = "\n".join(lines)
    i = blob.rfind("{")
    if i >= 0:
        try:
            return json.loads(blob[i:])
        except Exception:
            pass

    raise RuntimeError(f"failed to parse JSON from command: {' '.join(cmd)}\n---\n{out[:2000]}")


@dataclass
class OhouCollectorConfig:
    sources_yaml: str = DEFAULT_SOURCES_YAML
    out_dir: str = DEFAULT_OUT_DIR
    dedup_path: str = DEFAULT_DEDUP_PATH
    browser_profile: str = "openclaw"

    # Goods discovery
    max_goods: int = 5000
    max_scrolls: int = 40
    scroll_wait_ms: int = 1200
    stable_scroll_iters: int = 3

    # Reviews API
    per_page: int = 100
    order: str = "recent"  # best|recent|worst
    max_pages_per_goods: int = 50
    max_reviews_per_goods: int = 800
    sleep_s_between_goods: float = 0.3

    # Behavior
    try_fast_browser_use: bool = True
    dry_run: bool = False


FAST_BROWSER_USE_BIN_DEFAULT = os.path.join(
    WORKSPACE_ROOT, "skills/fast-browser-use/target/release/fast-browser-use"
)


def discover_goods_ids_fastbrowser(url: str, *, max_goods: int) -> List[int]:
    """Best-effort goods id discovery using fast-browser-use.

    Returns [] if fast-browser-use is unavailable or blocked.
    """
    bin_path = os.environ.get("FAST_BROWSER_USE_BIN") or FAST_BROWSER_USE_BIN_DEFAULT
    if not os.path.exists(bin_path):
        return []

    with tempfile.TemporaryDirectory(prefix="reviewhub_ohou_fbu_") as td:
        md_path = os.path.join(td, "page.md")
        try:
            _run([bin_path, "markdown", "--url", url, "--output", md_path], timeout_s=90, check=True)
            txt = open(md_path, "r", encoding="utf-8").read()
        except Exception:
            return []

    ids = [int(x) for x in re.findall(r"/goods/(\d+)", txt)]
    # Dedup preserve order
    seen: Set[int] = set()
    out: List[int] = []
    for i in ids:
        if i in seen:
            continue
        seen.add(i)
        out.append(i)
        if len(out) >= max_goods:
            break
    return out


class OpenClawBrowser:
    def __init__(self, *, profile: str = "openclaw"):
        self.profile = profile

    def start(self):
        # start prints human text, not JSON
        _run(["openclaw", "browser", "start", "--browser-profile", self.profile, "--timeout", "60000"], timeout_s=70)

    def open(self, url: str) -> str:
        j = _run_json(
            [
                "openclaw",
                "browser",
                "open",
                url,
                "--browser-profile",
                self.profile,
                "--json",
                "--timeout",
                "60000",
            ],
            timeout_s=70,
        )
        tid = str(j.get("targetId") or "")
        if not tid:
            raise RuntimeError(f"openclaw browser open returned no targetId: {j}")
        return tid

    def focus(self, target_id: str):
        _run(["openclaw", "browser", "focus", target_id, "--browser-profile", self.profile, "--timeout", "60000"], timeout_s=60)

    def wait_ms(self, ms: int, *, target_id: Optional[str] = None):
        cmd = [
            "openclaw",
            "browser",
            "wait",
            "--browser-profile",
            self.profile,
            "--timeout",
            "60000",
            "--time",
            str(int(ms)),
        ]
        if target_id:
            cmd += ["--target-id", target_id]
        _run(cmd, timeout_s=70)

    def evaluate(self, fn: str, *, timeout_s: int = 60, target_id: Optional[str] = None) -> Any:
        cmd = [
            "openclaw",
            "browser",
            "evaluate",
            "--browser-profile",
            self.profile,
            "--json",
            "--timeout",
            str(int(timeout_s * 1000)),
            "--fn",
            fn,
        ]
        if target_id:
            cmd += ["--target-id", target_id]
        j = _run_json(cmd, timeout_s=timeout_s + 10)
        if not j.get("ok"):
            raise RuntimeError(f"openclaw evaluate failed: {j}")
        return j.get("result")


def discover_goods_ids_openclaw(brand_url: str, cfg: OhouCollectorConfig) -> List[int]:
    b = OpenClawBrowser(profile=cfg.browser_profile)
    b.start()
    tid = b.open(brand_url)
    b.focus(tid)
    b.wait_ms(2500, target_id=tid)

    def get_ids() -> List[int]:
        hrefs = b.evaluate(
            "() => Array.from(document.querySelectorAll('a[href]')).map(a => a.getAttribute('href') || '')",
            target_id=tid,
        )
        ids: List[int] = []
        if isinstance(hrefs, list):
            for h in hrefs:
                if not isinstance(h, str):
                    continue
                m = re.search(r"/goods/(\d+)", h)
                if m:
                    ids.append(int(m.group(1)))
        # Dedup preserve order
        seen: Set[int] = set()
        out: List[int] = []
        for i in ids:
            if i in seen:
                continue
            seen.add(i)
            out.append(i)
            if len(out) >= cfg.max_goods:
                break
        return out

    best: List[int] = []
    stable = 0
    for _ in range(max(1, cfg.max_scrolls)):
        cur = get_ids()
        if len(cur) > len(best):
            best = cur
            stable = 0
        else:
            stable += 1
        if len(best) >= cfg.max_goods:
            break
        if stable >= cfg.stable_scroll_iters:
            break
        # scroll
        b.evaluate(
            "() => { window.scrollTo(0, document.body.scrollHeight); return {y: window.scrollY, h: document.body.scrollHeight}; }",
            target_id=tid,
        )
        b.wait_ms(cfg.scroll_wait_ms, target_id=tid)

    return best


def _dedup_key_for_review(
    *,
    platform: str,
    product_url: str,
    author: str,
    review_date: str,
    body: str,
) -> str:
    body_hash = sha256(body or "")
    return sha256("|".join([platform, product_url, author or "", review_date or "", body_hash]))


def fetch_reviews_for_goods_ids_openclaw(
    *,
    brand: str,
    brand_url: str,
    goods_ids: List[int],
    cfg: OhouCollectorConfig,
    seen_keys: Set[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    b = OpenClawBrowser(profile=cfg.browser_profile)
    b.start()
    tid = b.open(brand_url)
    b.focus(tid)
    b.wait_ms(2500, target_id=tid)

    all_reviews: List[Dict[str, Any]] = []
    stats = {
        "goods_total": len(goods_ids),
        "goods_with_reviews": 0,
        "reviews_fetched": 0,
        "reviews_new_estimate": 0,
        "stopped_early_seen": 0,
    }

    for idx, gid in enumerate(goods_ids):
        # simple throttle
        if idx and cfg.sleep_s_between_goods > 0:
            time.sleep(cfg.sleep_s_between_goods)

        product_url = f"https://store.ohou.se/goods/{gid}"

        fetched_for_goods = 0
        new_for_goods = 0
        page = 1
        while True:
            if page > cfg.max_pages_per_goods:
                break
            if new_for_goods >= cfg.max_reviews_per_goods:
                break

            fn = (
                "async () => {"
                f"  const url = `/api/goods/reviews?page={page}&productionId={gid}&per={cfg.per_page}&order={cfg.order}&stars=&option=`;"
                "  const r = await fetch(url, {credentials: 'include'});"
                "  const j = await r.json();"
                "  return {status: r.status, url, json: j};"
                "}"
            )
            resp = b.evaluate(fn, timeout_s=60, target_id=tid)
            if not isinstance(resp, dict):
                break
            if int(resp.get("status") or 0) != 200:
                break
            payload = resp.get("json")
            if not isinstance(payload, dict):
                break
            reviews = payload.get("reviews")
            if not isinstance(reviews, list) or not reviews:
                break

            new_in_page = 0
            for r in reviews:
                if not isinstance(r, dict):
                    continue

                review_id = str(r.get("id") or "")
                review_date = str(r.get("createdAt") or "")
                writer = str(r.get("writerNickname") or "")

                review_obj = r.get("review") if isinstance(r.get("review"), dict) else {}
                rating = review_obj.get("starAvg")
                try:
                    rating = float(rating) if rating is not None else None
                except Exception:
                    rating = None
                body = str(review_obj.get("comment") or r.get("comment") or r.get("content") or "")

                prod = r.get("productionInformation") if isinstance(r.get("productionInformation"), dict) else {}
                product_name = str(prod.get("name") or "")

                dedup_key = _dedup_key_for_review(
                    platform="ohou",
                    product_url=product_url,
                    author=writer,
                    review_date=review_date,
                    body=body,
                )
                if dedup_key in seen_keys:
                    continue

                # strict dedup: only emit never-seen reviews
                seen_keys.add(dedup_key)
                new_for_goods += 1
                new_in_page += 1

                all_reviews.append(
                    {
                        "platform": "ohou",
                        "brand": brand,
                        "productionId": gid,
                        "product_name": product_name,
                        "product_url": product_url,
                        "review_id": review_id,
                        "review_date": review_date,
                        "rating": rating,
                        "author": writer,
                        "title": "",
                        "body": body,
                        "source_url": product_url,
                    }
                )

            fetched_for_goods += len(reviews)
            stats["reviews_fetched"] += len(reviews)

            if len(reviews) < cfg.per_page:
                break

            # Stop early heuristic: if this page yields 0 new reviews, further pages are almost
            # certainly already-ingested (order=recent).
            if new_in_page == 0:
                stats["stopped_early_seen"] += 1
                break

            page += 1

        if fetched_for_goods > 0:
            stats["goods_with_reviews"] += 1
        stats["reviews_new_estimate"] += new_for_goods

    return all_reviews, stats


def collect_one_brand(brand: str, brand_url: str, cfg: OhouCollectorConfig) -> str:
    os.makedirs(cfg.out_dir, exist_ok=True)

    dedup_state = TextSet(cfg.dedup_path)
    seen_keys = dedup_state.load()

    goods_ids: List[int] = []
    if cfg.try_fast_browser_use:
        goods_ids = discover_goods_ids_fastbrowser(brand_url, max_goods=cfg.max_goods)

    if not goods_ids:
        goods_ids = discover_goods_ids_openclaw(brand_url, cfg)

    reviews, stats = fetch_reviews_for_goods_ids_openclaw(
        brand=brand,
        brand_url=brand_url,
        goods_ids=goods_ids,
        cfg=cfg,
        seen_keys=seen_keys,
    )

    now = kst_now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(cfg.out_dir, f"{_slug(brand)}_{ts}.json")

    payload = {
        "platform": "ohou",
        "brand": brand,
        "brand_url": brand_url,
        "collected_at": now.isoformat(),
        "goods_ids": goods_ids,
        "stats": stats,
        "reviews": reviews,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    if not cfg.dry_run:
        _run(["python3", "-m", "review_hub.ingest_ohou_json", out_path], timeout_s=180, check=True)

    return out_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Collect 오늘의집(ohou) reviews via OpenClaw managed browser")
    ap.add_argument("--sources-yaml", default=DEFAULT_SOURCES_YAML)
    ap.add_argument("--brand", action="append", help="Brand key from sources YAML (repeatable)")
    ap.add_argument("--all-brands", action="store_true", help="Collect for all brands that have platforms.ohou.url")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--dedup-path", default=DEFAULT_DEDUP_PATH)
    ap.add_argument("--browser-profile", default="openclaw", help="OpenClaw browser profile (recommended: openclaw)")

    ap.add_argument("--no-fast-browser-use", action="store_true", help="Disable fast-browser-use attempt")
    ap.add_argument("--dry-run", action="store_true", help="Collect + write JSON but skip ingest")

    ap.add_argument("--max-goods", type=int, default=5000)
    ap.add_argument("--max-scrolls", type=int, default=40)
    ap.add_argument("--scroll-wait-ms", type=int, default=1200)

    ap.add_argument("--per-page", type=int, default=100)
    ap.add_argument("--order", default="recent", choices=["best", "recent", "worst"])
    ap.add_argument("--max-pages-per-goods", type=int, default=50)
    ap.add_argument("--max-reviews-per-goods", type=int, default=800)

    args = ap.parse_args(list(argv) if argv is not None else None)

    only_brands: Optional[Set[str]]
    if args.all_brands:
        only_brands = None
    else:
        brands = set(args.brand or [])
        if not brands:
            ap.error("Provide --all-brands or at least one --brand")
        only_brands = brands

    cfg = OhouCollectorConfig(
        sources_yaml=args.sources_yaml,
        out_dir=args.out_dir,
        dedup_path=args.dedup_path,
        browser_profile=args.browser_profile,
        try_fast_browser_use=not args.no_fast_browser_use,
        dry_run=args.dry_run,
        max_goods=int(args.max_goods),
        max_scrolls=int(args.max_scrolls),
        scroll_wait_ms=int(args.scroll_wait_ms),
        per_page=int(args.per_page),
        order=str(args.order),
        max_pages_per_goods=int(args.max_pages_per_goods),
        max_reviews_per_goods=int(args.max_reviews_per_goods),
    )

    sources_cfg = load_sources_yaml(cfg.sources_yaml)
    sources = iter_ohou_brand_sources(sources_cfg, only_brands=only_brands)
    if not sources:
        raise SystemExit("No ohou brand sources found")

    with file_lock(DEFAULT_COLLECTOR_LOCK):
        out_paths: List[str] = []
        for brand, url in sources:
            out_paths.append(collect_one_brand(brand, url, cfg))
        print(json.dumps({"ok": True, "outputs": out_paths}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
