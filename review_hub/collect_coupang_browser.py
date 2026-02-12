from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import yaml

from review_hub.errors import log_errors
from review_hub.lock import file_lock
from review_hub.sheets_client import GogSheetsClient
from review_hub.state import TextSet

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DEFAULT_SOURCES_YAML = os.path.join(WORKSPACE_ROOT, "config/review-hub/brands-platform-urls.yaml")
DEFAULT_DEDUP_PATH = os.path.join(WORKSPACE_ROOT, "state/review-hub/dedup-keys.txt")
DEFAULT_OUT_DIR = os.path.join(WORKSPACE_ROOT, "out/review-hub/coupang")
DEFAULT_COLLECTOR_LOCK = os.path.join(WORKSPACE_ROOT, "state/review-hub/coupang-collector.lock")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kst_now() -> dt.datetime:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def _slug(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^0-9A-Za-z가-힣_\-]+", "", s)
    return (s[:80] or "unknown")


def load_sources_yaml(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def iter_coupang_brandshop_sources(cfg: dict, *, only_brands: Optional[Set[str]] = None) -> List[Tuple[str, str]]:
    brands = (cfg or {}).get("brands") or {}
    out: List[Tuple[str, str]] = []
    for brand, bcfg in brands.items():
        if only_brands and brand not in only_brands:
            continue
        platforms = (bcfg or {}).get("platforms") or {}
        cp = platforms.get("coupang_brandshop")
        if isinstance(cp, dict) and cp.get("url"):
            out.append((brand, str(cp["url"]).strip()))
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
    out = (out or "").strip()
    if not out:
        return {}

    lines = [ln for ln in out.splitlines() if ln.strip()]
    try:
        return json.loads(out)
    except Exception:
        pass

    for ln in reversed(lines):
        try:
            return json.loads(ln)
        except Exception:
            continue

    blob = "\n".join(lines)
    i = blob.rfind("{")
    if i >= 0:
        try:
            return json.loads(blob[i:])
        except Exception:
            pass

    raise RuntimeError(f"failed to parse JSON from command: {' '.join(cmd)}\n---\n{out[:2000]}")


class OpenClawBrowser:
    def __init__(self, *, profile: str = "openclaw"):
        self.profile = profile

    def start(self):
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
        _run(
            ["openclaw", "browser", "focus", target_id, "--browser-profile", self.profile, "--timeout", "60000"],
            timeout_s=60,
        )

    def navigate(self, url: str):
        _run(
            [
                "openclaw",
                "browser",
                "navigate",
                url,
                "--browser-profile",
                self.profile,
                "--timeout",
                "60000",
            ],
            timeout_s=70,
        )

    def wait_ms(self, ms: int):
        _run(
            [
                "openclaw",
                "browser",
                "wait",
                "--browser-profile",
                self.profile,
                "--timeout",
                "60000",
                "--time",
                str(int(ms)),
            ],
            timeout_s=70,
        )

    def wait_for_selector(self, selector: str, *, timeout_ms: int = 20000):
        _run(
            [
                "openclaw",
                "browser",
                "wait",
                "--browser-profile",
                self.profile,
                "--timeout",
                "60000",
                "--timeout-ms",
                str(int(timeout_ms)),
                selector,
            ],
            timeout_s=max(10, int(timeout_ms / 1000) + 15),
        )

    def evaluate(self, fn: str, *, timeout_s: int = 60) -> Any:
        j = _run_json(
            [
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
            ],
            timeout_s=timeout_s + 10,
        )
        if not j.get("ok"):
            raise RuntimeError(f"openclaw evaluate failed: {j}")
        return j.get("result")


@dataclass
class CoupangCollectorConfig:
    sources_yaml: str = DEFAULT_SOURCES_YAML
    out_dir: str = DEFAULT_OUT_DIR
    dedup_path: str = DEFAULT_DEDUP_PATH
    browser_profile: str = "openclaw"

    # Brandshop discovery
    max_brand_pages: int = 10
    max_products_per_brand: int = 50
    max_scrolls_per_brand_page: int = 6
    scroll_wait_ms: int = 900
    stable_scroll_iters: int = 2

    # Reviews
    order: str = "latest"  # latest|best
    max_pages_per_product: int = 5
    max_reviews_per_product: int = 80

    # Throttle
    sleep_s_between_products: float = 0.6
    sleep_ms_between_review_pages: int = 900

    # Behavior
    dry_run: bool = False
    log_errors_to_sheet: bool = True


def normalize_coupang_product_url(url: str) -> str:
    """Normalize to stable product URL: https://www.coupang.com/vp/products/<pid>

    Coupang product URLs often contain itemId/vendorItemId; reviews are shared across
    vendors for the same productId.
    """
    s = (url or "").strip()
    if not s:
        return s
    m = re.search(r"/vp/products/(\d+)", s)
    if not m:
        return s
    return f"https://www.coupang.com/vp/products/{m.group(1)}"


def add_or_replace_query(url: str, **params: object) -> str:
    pr = urlparse(url)
    q = dict(parse_qsl(pr.query, keep_blank_values=True))
    for k, v in params.items():
        if v is None:
            q.pop(k, None)
        else:
            q[k] = str(v)
    new_q = urlencode(q, doseq=True)
    return urlunparse((pr.scheme, pr.netloc, pr.path, pr.params, new_q, pr.fragment))


def detect_blocked_page(b: OpenClawBrowser) -> Optional[str]:
    """Return error string if AccessDenied/captcha-like page detected."""
    try:
        info = b.evaluate(
            "() => ({title: document.title || '', url: location.href || '', text: (document.body && document.body.innerText || '').slice(0, 2000)})",
            timeout_s=20,
        )
    except Exception as e:
        return f"evaluate_error: {e}"

    if not isinstance(info, dict):
        return None

    title = str(info.get("title") or "")
    txt = str(info.get("text") or "")

    needles = [
        "Access Denied",
        "접근이 거부",
        "로봇이 아닙니다",
        "자동 입력 방지",
        "자동입력",
        "captcha",
        "보안 문자",
        "비정상적인 트래픽",
    ]
    if any(n.lower() in title.lower() for n in needles):
        return f"blocked: title={title}"
    if any(n.lower() in txt.lower() for n in needles):
        return f"blocked: {needles}"

    # Heuristic selectors
    try:
        has = b.evaluate(
            "() => ({captcha: !!document.querySelector('input[name*=captcha], #captcha, iframe[src*=captcha]'), denied: !!document.querySelector('h1') && /Denied/i.test(document.querySelector('h1').innerText || '')})",
            timeout_s=20,
        )
        if isinstance(has, dict) and (has.get("captcha") or has.get("denied")):
            return f"blocked: captcha_or_denied_selector"
    except Exception:
        pass

    return None


def discover_product_urls_for_brandshop(
    *,
    brand: str,
    brandshop_url: str,
    cfg: CoupangCollectorConfig,
    b: OpenClawBrowser,
    target_id: str,
) -> List[str]:
    """Discover product URLs from a Coupang brandshop page by visiting up to N pages."""

    b.focus(target_id)

    all_urls: List[str] = []
    seen: Set[str] = set()

    for page in range(1, cfg.max_brand_pages + 1):
        page_url = add_or_replace_query(brandshop_url, page=page)
        b.navigate(page_url)
        b.wait_ms(1800)

        blocked = detect_blocked_page(b)
        if blocked:
            raise RuntimeError(blocked)

        best: List[str] = []
        stable = 0
        for _ in range(max(1, cfg.max_scrolls_per_brand_page)):
            hrefs = b.evaluate(
                "() => Array.from(new Set(Array.from(document.querySelectorAll('a[href]')).map(a => a.href).filter(h => h && h.includes('/vp/products/'))))",
                timeout_s=25,
            )
            cur: List[str] = []
            if isinstance(hrefs, list):
                for h in hrefs:
                    if isinstance(h, str) and "/vp/products/" in h:
                        cur.append(h)
            # preserve order + normalize
            out: List[str] = []
            s2: Set[str] = set()
            for u in cur:
                nu = normalize_coupang_product_url(u)
                if nu in s2:
                    continue
                s2.add(nu)
                out.append(nu)
            if len(out) > len(best):
                best = out
                stable = 0
            else:
                stable += 1
            if stable >= cfg.stable_scroll_iters:
                break
            b.evaluate("() => { window.scrollTo(0, document.body.scrollHeight); return {y: window.scrollY}; }")
            b.wait_ms(cfg.scroll_wait_ms)

        # merge
        new_this_page = 0
        for u in best:
            if u in seen:
                continue
            seen.add(u)
            all_urls.append(u)
            new_this_page += 1
            if len(all_urls) >= cfg.max_products_per_brand:
                break

        if len(all_urls) >= cfg.max_products_per_brand:
            break

        # If page yielded nothing new, assume end.
        if page > 1 and new_this_page == 0:
            break

    return all_urls


def _dedup_key_for_review(*, product_url: str, author: str, review_date: str, body: str) -> str:
    return sha256("|".join([
        "coupang",
        product_url,
        author or "",
        review_date or "",
        sha256(body or ""),
    ]))


def _ensure_review_section_and_sort(b: OpenClawBrowser, *, order: str):
    """Ensure the page is at the review section.

    Coupang PDPs often have a visible tab/button labeled "상품평" that reveals the
    review section. In some variants there is also an anchor link to #sdpReview.
    This function clicks whichever is available and scrolls into view.
    """

    r = b.evaluate(
        "() => {\n"
        "  const norm = (s) => (s || '').replace(/\\s+/g,' ').trim();\n"
        "  const isVisible = (el) => {\n"
        "    if (!el) return false;\n"
        "    const r = el.getBoundingClientRect();\n"
        "    return r.width > 0 && r.height > 0;\n"
        "  };\n"
        "\n"
        "  // 1) Try direct anchor\n"
        "  const a = document.querySelector('a[href=\\"#sdpReview\\"], a[href*=\\"#sdpReview\\"]');\n"
        "  if (a && isVisible(a)) { try { a.click(); } catch(e) {} }\n"
        "\n"
        "  // 2) Try tab/button with text '상품평'\n"
        "  const cand = Array.from(document.querySelectorAll('a,button,li,div,span'))\n"
        "    .filter(el => isVisible(el))\n"
        "    .find(el => {\n"
        "      const t = norm(el.innerText);\n"
        "      return t === '상품평' || t.startsWith('상품평 ');\n"
        "    });\n"
        "  if (cand) { try { cand.click(); } catch(e) {} }\n"
        "\n"
        "  // 3) Scroll to review root if present\n"
        "  const root = document.querySelector('#sdpReview');\n"
        "  if (root) root.scrollIntoView({block: 'start'});\n"
        "\n"
        "  return {clickedAnchor: !!(a && isVisible(a)), clickedTab: !!cand, hasRoot: !!root};\n"
        "}",
        timeout_s=25,
    )
    # Minimal debug signal (stdout)
    try:
        print(f"[coupang] ensure_review_section: {r}")
    except Exception:
        pass

    # Wait a bit for lazy rendering
    b.wait_ms(1500)

    if order == "latest":
        # Best-effort click '최신순'
        rr = b.evaluate(
            "() => {\n"
            "  const root = document.querySelector('#sdpReview');\n"
            "  if (!root) return {clicked:false, reason:'no_root'};\n"
            "  const btn = Array.from(root.querySelectorAll('button, a'))\n"
            "    .find(x => (x.innerText || '').trim() === '최신순');\n"
            "  if (btn) { try { btn.click(); } catch(e) {} }\n"
            "  return {clicked: !!btn};\n"
            "}",
            timeout_s=25,
        )
        try:
            print(f"[coupang] sort_latest: {rr}")
        except Exception:
            pass
        b.wait_ms(900)


def _extract_reviews_visible(b: OpenClawBrowser) -> List[Dict[str, Any]]:
    res = b.evaluate(
        "() => {\n"
        "  const root = document.querySelector('#sdpReview');\n"
        "  if (!root) return [];\n"
        "  const articles = Array.from(root.querySelectorAll('article'))\n"
        "    .filter(a => a.querySelector('.js_reviewArticleHelpfulContainer, .sdp-review__article__list__help'));\n"
        "  const out = [];\n"
        "  for (const a of articles) {\n"
        "    // Expand truncated bodies within each article\n"
        "    for (const b of Array.from(a.querySelectorAll('button'))) {\n"
        "      if ((b.innerText || '').trim() === '더보기') {\n"
        "        try { b.click(); } catch (e) {}\n"
        "      }\n"
        "    }\n"
        "\n"
        "    const helpfulEl = a.querySelector('.js_reviewArticleHelpfulContainer, .sdp-review__article__list__help');\n"
        "    const reviewId = (helpfulEl && (helpfulEl.getAttribute('data-review-id') || helpfulEl.getAttribute('data-reviewid'))) || '';\n"
        "\n"
        "    const textEls = Array.from(a.querySelectorAll('span,div,strong'))\n"
        "      .map(e => (e.innerText || '').trim())\n"
        "      .filter(Boolean);\n"
        "\n"
        "    const author = textEls.find(t => t.includes('*') && t.length <= 12) || '';\n"
        "    const reviewDate = textEls.find(t => /\\d{4}[./-]\\d{2}[./-]\\d{2}/.test(t)) || '';\n"
        "\n"
        "    const sellerLine = textEls.find(t => t.startsWith('판매자:')) || '';\n"
        "    const seller = sellerLine ? sellerLine.replace('판매자:', '').trim() : '';\n"
        "\n"
        "    const titleEl = Array.from(a.querySelectorAll('div,span,strong'))\n"
        "      .find(e => {\n"
        "        const t = (e.innerText || '').trim();\n"
        "        if (!t) return false;\n"
        "        if (t === author) return false;\n"
        "        if (t.startsWith('판매자:')) return false;\n"
        "        if (/\\d{4}[./-]\\d{2}[./-]\\d{2}/.test(t)) return false;\n"
        "        if (t === '신고하기') return false;\n"
        "        return (e.className || '').includes('font-bold') && t.length <= 40;\n"
        "      });\n"
        "    const title = titleEl ? (titleEl.innerText || '').trim() : '';\n"
        "\n"
        "    const bodyEl = a.querySelector('.sdp-review__article__list__review__content')\n"
        "      || a.querySelector('div.twc-break-all')\n"
        "      || a.querySelector('span.twc-bg-white');\n"
        "    const body = bodyEl ? (bodyEl.innerText || '').trim() : '';\n"
        "\n"
        "    const full = a.querySelectorAll(\"i[class*='full-star']\").length;\n"
        "    const half = a.querySelectorAll(\"i[class*='half-star']\").length;\n"
        "    const rating = full + (half ? 0.5 * half : 0);\n"
        "\n"
        "    const helpfulText = helpfulEl ? ((helpfulEl.innerText || '').trim()) : '';\n"
        "    let helpful = null;\n"
        "    const hm = helpfulText.match(/(\\d+)/);\n"
        "    if (hm) {\n"
        "      try { helpful = parseInt(hm[1], 10); } catch (e) {}\n"
        "    }\n"
        "\n"
        "    out.push({\n"
        "      review_id: reviewId,\n"
        "      author,\n"
        "      review_date: reviewDate,\n"
        "      rating,\n"
        "      title,\n"
        "      body,\n"
        "      helpful,\n"
        "      seller,\n"
        "    });\n"
        "  }\n"
        "  return out;\n"
        "}",
        timeout_s=30,
    )

    if not isinstance(res, list):
        return []
    return [x for x in res if isinstance(x, dict)]


def _click_review_page(b: OpenClawBrowser, *, next_page: int) -> bool:
    # Click page number if visible. If not visible, try to advance with a right-arrow button.
    r = b.evaluate(
        "() => {\n"
        "  const root = document.querySelector('#sdpReview');\n"
        "  if (!root) return {ok:false};\n"
        "  const want = String(%d);\n"
        "  const digit = Array.from(root.querySelectorAll('button')).find(x => (x.innerText || '').trim() === want);\n"
        "  if (digit) { digit.click(); return {ok:true, clicked:'digit'}; }\n"
        "  const arrows = Array.from(root.querySelectorAll('button'))\n"
        "    .filter(b => !((b.innerText||'').trim()) && b.querySelector('svg') && (b.className||'').includes('twc-w-[38px]'));\n"
        "  const nextArrow = arrows.find(a => !a.disabled) || null;\n"
        "  if (nextArrow) { nextArrow.click(); return {ok:true, clicked:'arrow'}; }\n"
        "  return {ok:false};\n"
        "}" % next_page,
        timeout_s=25,
    )
    return bool(isinstance(r, dict) and r.get("ok"))


def collect_reviews_for_product(
    *,
    b: OpenClawBrowser,
    target_id: str,
    brand: str,
    product_url: str,
    cfg: CoupangCollectorConfig,
    seen_keys: Set[str],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    b.focus(target_id)
    b.navigate(product_url)
    b.wait_ms(2200)

    blocked = detect_blocked_page(b)
    if blocked:
        raise RuntimeError(blocked)

    # product name
    product_name = b.evaluate(
        "() => (document.querySelector('h1') && document.querySelector('h1').innerText || document.title || '').trim()",
        timeout_s=20,
    )
    if not isinstance(product_name, str):
        product_name = ""

    _ensure_review_section_and_sort(b, order=cfg.order)

    out: List[Dict[str, Any]] = []
    stats: Dict[str, Any] = {
        "product_url": product_url,
        "pages_visited": 0,
        "reviews_seen": 0,
        "reviews_new": 0,
        "stopped_early_seen": 0,
    }

    page = 1
    while True:
        if page > cfg.max_pages_per_product:
            break
        if len(out) >= cfg.max_reviews_per_product:
            break

        # ensure review root exists
        try:
            b.wait_for_selector("#sdpReview", timeout_ms=15000)
        except Exception as e:
            try:
                print(f"[coupang] wait_for #sdpReview failed: {e}")
            except Exception:
                pass

        # extract visible
        reviews = _extract_reviews_visible(b)
        stats["pages_visited"] += 1
        stats["reviews_seen"] += len(reviews)

        new_in_page = 0
        for r in reviews:
            author = str(r.get("author") or "")
            review_date = str(r.get("review_date") or "")
            body = str(r.get("body") or "")
            dedup_key = _dedup_key_for_review(
                product_url=normalize_coupang_product_url(product_url),
                author=author,
                review_date=review_date,
                body=body,
            )
            if dedup_key in seen_keys:
                continue

            seen_keys.add(dedup_key)
            new_in_page += 1
            stats["reviews_new"] += 1

            out.append(
                {
                    "platform": "coupang",
                    "brand": brand,
                    "product_name": product_name,
                    "product_url": normalize_coupang_product_url(product_url),
                    "review_id": str(r.get("review_id") or ""),
                    "review_date": review_date,
                    "rating": r.get("rating") if r.get("rating") is not None else None,
                    "author": author,
                    "title": str(r.get("title") or ""),
                    "body": body,
                    "helpful": r.get("helpful"),
                    "seller": str(r.get("seller") or ""),
                    "source_url": normalize_coupang_product_url(product_url),
                    "raw": r,
                }
            )

            if len(out) >= cfg.max_reviews_per_product:
                break

        if page >= 1 and new_in_page == 0:
            stats["stopped_early_seen"] += 1
            try:
                print(f"[coupang] no new reviews on page={page}; stop early")
            except Exception:
                pass
            break

        page += 1
        clicked = _click_review_page(b, next_page=page)
        if not clicked:
            break
        b.wait_ms(cfg.sleep_ms_between_review_pages)

    return out, stats


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def collect_one_brand(brand: str, brandshop_url: str, cfg: CoupangCollectorConfig) -> str:
    os.makedirs(cfg.out_dir, exist_ok=True)

    dedup_state = TextSet(cfg.dedup_path)
    seen_keys = dedup_state.load()

    b = OpenClawBrowser(profile=cfg.browser_profile)
    b.start()
    tid = b.open("about:blank")
    b.focus(tid)
    b.wait_ms(500)

    # Error logging sink (best-effort)
    sink = None
    client = None
    if cfg.log_errors_to_sheet:
        try:
            sink = load_sink_config()
            client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])
        except Exception:
            client = None

    err_rows: List[dict] = []

    try:
        product_urls = discover_product_urls_for_brandshop(
            brand=brand,
            brandshop_url=brandshop_url,
            cfg=cfg,
            b=b,
            target_id=tid,
        )
    except Exception as e:
        msg = str(e)
        if client is not None:
            err_rows.append({"brand": brand, "platform": "coupang", "url": brandshop_url, "status": "blocked", "error": msg})
            try:
                log_errors(client=client, tab="errors_reviews", run_id="collect_coupang_browser", stage="brandshop", items=err_rows)
            except Exception:
                pass
        raise

    all_reviews: List[Dict[str, Any]] = []
    per_product_stats: List[dict] = []

    for idx, pu in enumerate(product_urls):
        if idx and cfg.sleep_s_between_products > 0:
            time.sleep(cfg.sleep_s_between_products)
        try:
            reviews, st = collect_reviews_for_product(
                b=b,
                target_id=tid,
                brand=brand,
                product_url=pu,
                cfg=cfg,
                seen_keys=seen_keys,
            )
            per_product_stats.append(st)
            all_reviews.extend(reviews)
        except Exception as e:
            msg = str(e)
            # Access denied / captcha errors should be logged
            if client is not None and ("blocked" in msg.lower() or "access" in msg.lower() or "captcha" in msg.lower()):
                err_rows.append({"brand": brand, "platform": "coupang", "url": pu, "status": "blocked", "error": msg})
            continue

    # write errors (best-effort)
    if client is not None and err_rows:
        try:
            log_errors(client=client, tab="errors_reviews", run_id="collect_coupang_browser", stage="reviews", items=err_rows)
        except Exception:
            pass

    now = kst_now()
    ts = now.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(cfg.out_dir, f"{_slug(brand)}_{ts}.json")

    payload = {
        "platform": "coupang",
        "brand": brand,
        "brandshop_url": brandshop_url,
        "collected_at": now.isoformat(),
        "product_urls": product_urls,
        "stats": {
            "products_total": len(product_urls),
            "reviews_collected": len(all_reviews),
            "per_product": per_product_stats,
            "errors": err_rows[:50],
        },
        "reviews": all_reviews,
    }

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    if not cfg.dry_run:
        _run(["python3", "-m", "review_hub.ingest_coupang_json", out_path], timeout_s=240, check=True)

    return out_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Collect Coupang public reviews via OpenClaw managed browser")
    ap.add_argument("--sources-yaml", default=DEFAULT_SOURCES_YAML)
    ap.add_argument("--brand", action="append", help="Brand key from sources YAML (repeatable)")
    ap.add_argument("--all-brands", action="store_true", help="Collect for all brands that have platforms.coupang_brandshop.url")

    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--dedup-path", default=DEFAULT_DEDUP_PATH)
    ap.add_argument("--browser-profile", default="openclaw")

    ap.add_argument("--max-brand-pages", type=int, default=10)
    ap.add_argument("--max-products-per-brand", type=int, default=50)
    ap.add_argument("--max-pages-per-product", type=int, default=5)
    ap.add_argument("--max-reviews-per-product", type=int, default=80)

    ap.add_argument("--order", default="latest", choices=["latest", "best"], help="Review ordering")

    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--no-sheet-error-log", action="store_true", help="Disable logging blocked/captcha errors to errors_reviews tab")

    args = ap.parse_args(list(argv) if argv is not None else None)

    only_brands: Optional[Set[str]]
    if args.all_brands:
        only_brands = None
    else:
        brands = set(args.brand or [])
        if not brands:
            ap.error("Provide --all-brands or at least one --brand")
        only_brands = brands

    cfg = CoupangCollectorConfig(
        sources_yaml=args.sources_yaml,
        out_dir=args.out_dir,
        dedup_path=args.dedup_path,
        browser_profile=args.browser_profile,
        max_brand_pages=int(args.max_brand_pages),
        max_products_per_brand=int(args.max_products_per_brand),
        max_pages_per_product=int(args.max_pages_per_product),
        max_reviews_per_product=int(args.max_reviews_per_product),
        order=str(args.order),
        dry_run=bool(args.dry_run),
        log_errors_to_sheet=not bool(args.no_sheet_error_log),
    )

    sources_cfg = load_sources_yaml(cfg.sources_yaml)
    sources = iter_coupang_brandshop_sources(sources_cfg, only_brands=only_brands)
    if not sources:
        raise SystemExit("No coupang_brandshop sources found")

    with file_lock(DEFAULT_COLLECTOR_LOCK):
        out_paths: List[str] = []
        for brand, url in sources:
            out_paths.append(collect_one_brand(brand, url, cfg))
        print(json.dumps({"ok": True, "outputs": out_paths}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
