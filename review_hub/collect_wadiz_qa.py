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
from urllib.parse import urlparse

from review_hub.errors import log_errors
from review_hub.lock import file_lock
from review_hub.sheets_client import GogSheetsClient
from review_hub.state import TextSet

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

DEFAULT_URLS_PATH = os.path.join(WORKSPACE_ROOT, "state/review-hub/wadiz-qa-urls.txt")
DEFAULT_OUT_DIR = os.path.join(WORKSPACE_ROOT, "out/review-hub/wadiz")
DEFAULT_COLLECTOR_LOCK = os.path.join(WORKSPACE_ROOT, "state/review-hub/wadiz-collector.lock")


def sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def kst_now() -> dt.datetime:
    kst = dt.timezone(dt.timedelta(hours=9))
    return dt.datetime.now(tz=kst)


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


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
        _run(["openclaw", "browser", "focus", target_id, "--browser-profile", self.profile, "--timeout", "60000"], timeout_s=60)

    def navigate(self, url: str, *, target_id: Optional[str] = None):
        cmd = ["openclaw", "browser", "navigate", url, "--browser-profile", self.profile, "--timeout", "60000"]
        if target_id:
            cmd += ["--target-id", target_id]
        _run(cmd, timeout_s=70)

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


def _parse_wadiz_project_no(url: str) -> Optional[str]:
    """Extract numeric projectNo from known Wadiz URLs."""
    s = (url or "").strip()
    if not s:
        return None

    # Paths like: /web/campaign/detail/qa/160013[/...]
    m = re.search(r"/web/campaign/detail/qa/(\d+)", s)
    if m:
        return m.group(1)

    # Paths like: /web/campaign/detail/160013
    m2 = re.search(r"/web/campaign/detail/(\d+)", s)
    if m2:
        return m2.group(1)

    # Global funding URLs sometimes exist
    m3 = re.search(r"/funding/(\d+)", s)
    if m3:
        return m3.group(1)

    # Last-resort: any long-ish number in path
    pr = urlparse(s)
    m4 = re.search(r"/(\d{5,})\b", pr.path)
    if m4:
        return m4.group(1)

    return None


def wadiz_campaign_url(project_no: str) -> str:
    return f"https://www.wadiz.kr/web/campaign/detail/{project_no}"


def wadiz_qa_root_url(project_no: str) -> str:
    return f"https://www.wadiz.kr/web/campaign/detail/qa/{project_no}"


def _normalize_korean_date_to_iso(s: str) -> str:
    """Convert e.g. '2022년 10월 26일' -> '2022-10-26'. Fail-open."""
    s = (s or "").strip()
    if not s:
        return ""
    m = re.search(r"(\d{4})\s*년\s*(\d{1,2})\s*월\s*(\d{1,2})\s*일", s)
    if not m:
        return s
    y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
    try:
        return dt.date(y, mo, d).isoformat()
    except Exception:
        return s


def detect_blocked_or_login(b: OpenClawBrowser, *, target_id: str) -> Optional[str]:
    """Best-effort detection of login wall/captcha/blocked pages."""
    try:
        info = b.evaluate(
            "() => ({title: document.title || '', url: location.href || '', text: (document.body && document.body.innerText || '').slice(0, 2000)})",
            timeout_s=20,
            target_id=target_id,
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
        "captcha",
        "보안 문자",
        "비정상적인 트래픽",
        "로그인",
        "회원가입",
    ]
    if any(n.lower() in title.lower() for n in needles):
        return f"blocked_or_login: title={title}"
    if any(n.lower() in txt.lower() for n in needles):
        # Don't treat mere presence of header login button as blocked: require stronger signal.
        if "비정상적인 트래픽" in txt or "captcha" in txt.lower() or "로봇" in txt:
            return "blocked_or_login: captcha"

    return None


def _click_all_more_buttons(b: OpenClawBrowser, *, target_id: str) -> int:
    res = b.evaluate(
        "() => {\n"
        "  const btns = Array.from(document.querySelectorAll('button')).filter(b => (b.innerText || '').trim() === '더보기');\n"
        "  let clicked = 0;\n"
        "  for (const bt of btns) { try { bt.click(); clicked++; } catch (e) {} }\n"
        "  return {clicked, total: btns.length};\n"
        "}",
        timeout_s=30,
        target_id=target_id,
    )
    if isinstance(res, dict):
        try:
            return int(res.get("clicked") or 0)
        except Exception:
            return 0
    return 0


def _count_comment_items(b: OpenClawBrowser, *, target_id: str) -> int:
    res = b.evaluate(
        "() => document.querySelectorAll('[class*=\"CommentItem_commentItem__\"]').length",
        timeout_s=20,
        target_id=target_id,
    )
    try:
        return int(res or 0)
    except Exception:
        return 0


def _scroll_bottom(b: OpenClawBrowser, *, target_id: str) -> None:
    b.evaluate(
        "() => { window.scrollTo(0, document.body.scrollHeight); return {y: window.scrollY, h: document.body.scrollHeight}; }",
        timeout_s=20,
        target_id=target_id,
    )


def load_all_items_by_scrolling(
    b: OpenClawBrowser,
    *,
    target_id: str,
    max_scrolls: int,
    stable_scroll_iters: int,
    scroll_wait_ms: int,
    max_items: int,
) -> Dict[str, Any]:
    best = 0
    stable = 0
    total_clicked_more = 0

    for _ in range(max(1, max_scrolls)):
        total_clicked_more += _click_all_more_buttons(b, target_id=target_id)
        n = _count_comment_items(b, target_id=target_id)
        if n > best:
            best = n
            stable = 0
        else:
            stable += 1

        if best >= max_items:
            break
        if stable >= stable_scroll_iters:
            break

        _scroll_bottom(b, target_id=target_id)
        b.wait_ms(scroll_wait_ms, target_id=target_id)

    # final expand
    total_clicked_more += _click_all_more_buttons(b, target_id=target_id)

    return {"items_loaded": best, "stable": stable, "clicked_more": total_clicked_more}


def extract_comment_items(b: OpenClawBrowser, *, target_id: str) -> List[Dict[str, Any]]:
    res = b.evaluate(
        "() => {\n"
        "  const items = Array.from(document.querySelectorAll('[class*=\\\"CommentItem_commentItem__\\\"]'));\n"
        "  const out = [];\n"
        "  for (const it of items) {\n"
        "    const badgeEl = it.querySelector('[class*=\\\"LabelBadge_badge__\\\"]');\n"
        "    const badge = badgeEl ? (badgeEl.innerText || '').trim() : '';\n"
        "    const authorEl = it.querySelector('[class*=\\\"CommentProfile_nickName__\\\"]');\n"
        "    const author = authorEl ? (authorEl.innerText || '').trim() : '';\n"
        "    const dateEl = it.querySelector('[class*=\\\"CommentProfile_date__\\\"]');\n"
        "    const date = dateEl ? (dateEl.innerText || '').trim() : '';\n"
        "    const optionEl = it.querySelector('[class*=\\\"SatisfactionContentHeader_options__\\\"]');\n"
        "    const option = optionEl ? (optionEl.innerText || '').trim() : '';\n"
        "    const ratingEl = it.querySelector('[class*=\\\"SatisfactionContentHeader_score__\\\"]');\n"
        "    const ratingText = ratingEl ? (ratingEl.innerText || '').trim() : '';\n"
        "    const bodyEl = it.querySelector('[class*=\\\"CommentContentArea_fullComment__\\\"],[class*=\\\"CommentContentArea_comment__\\\"]');\n"
        "    const body = bodyEl ? (bodyEl.innerText || '').trim() : '';\n"
        "    out.push({badge, author, date, option, ratingText, body});\n"
        "  }\n"
        "  return out;\n"
        "}",
        timeout_s=40,
        target_id=target_id,
    )

    if not isinstance(res, list):
        return []
    return [x for x in res if isinstance(x, dict)]


@dataclass
class WadizCollectorConfig:
    out_dir: str = DEFAULT_OUT_DIR
    dedup_path: str = os.path.join(WORKSPACE_ROOT, "state/review-hub/dedup-keys.txt")
    browser_profile: str = "openclaw"

    # Only ingest items whose review_date >= min_review_date (KST date).
    # Helps avoid re-ingesting old historical QA/comments daily.
    min_review_date: Optional[dt.date] = None

    include_satisfaction: bool = True
    include_comment: bool = True
    include_signature: bool = False

    max_scrolls: int = 30
    stable_scroll_iters: int = 3
    scroll_wait_ms: int = 1200
    max_items_per_page: int = 400

    sleep_s_between_pages: float = 0.3

    dry_run: bool = False
    log_errors_to_sheet: bool = True


def _page_urls(project_no: str, cfg: WadizCollectorConfig) -> List[Tuple[str, str]]:
    base = wadiz_qa_root_url(project_no)
    pages: List[Tuple[str, str]] = []
    if cfg.include_satisfaction:
        pages.append(("satisfaction", base + "/satisfaction"))
    if cfg.include_comment:
        pages.append(("comment", base + "/comment"))
    if cfg.include_signature:
        pages.append(("signature", base + "/signature"))
    return pages


def collect_for_project_no(
    project_no: str,
    *,
    cfg: WadizCollectorConfig,
    seen_keys: Set[str],
    client: Optional[GogSheetsClient],
) -> Dict[str, Any]:
    os.makedirs(cfg.out_dir, exist_ok=True)

    b = OpenClawBrowser(profile=cfg.browser_profile)
    b.start()
    tid = b.open("about:blank")
    b.focus(tid)
    b.wait_ms(600, target_id=tid)

    campaign_url = wadiz_campaign_url(project_no)
    pages = _page_urls(project_no, cfg)

    errors: List[dict] = []
    collected: List[Dict[str, Any]] = []

    campaign_title = ""

    for page_kind, page_url in pages:
        if cfg.sleep_s_between_pages:
            time.sleep(cfg.sleep_s_between_pages)

        try:
            b.navigate(page_url, target_id=tid)
            b.wait_ms(2300, target_id=tid)

            blocked = detect_blocked_or_login(b, target_id=tid)
            if blocked:
                raise RuntimeError(blocked)

            if not campaign_title:
                t = b.evaluate("() => (document.title || '').trim()", timeout_s=20, target_id=tid)
                if isinstance(t, str):
                    campaign_title = t.strip()

            stats = load_all_items_by_scrolling(
                b,
                target_id=tid,
                max_scrolls=cfg.max_scrolls,
                stable_scroll_iters=cfg.stable_scroll_iters,
                scroll_wait_ms=cfg.scroll_wait_ms,
                max_items=cfg.max_items_per_page,
            )

            items = extract_comment_items(b, target_id=tid)
            for it in items:
                badge = str(it.get("badge") or "").strip()
                author = str(it.get("author") or "").strip()
                review_date_raw = str(it.get("date") or "").strip()
                review_date = _normalize_korean_date_to_iso(review_date_raw)

                # Date cutoff: skip old items (based on review_date) to keep daily runs focused on new content.
                if cfg.min_review_date:
                    try:
                        d = dt.date.fromisoformat(str(review_date)[:10]) if review_date else None
                    except Exception:
                        d = None
                    # If date is missing/unparseable, skip (fail-closed) to avoid noisy backfills.
                    if not d or d < cfg.min_review_date:
                        continue

                rating = None
                rt = str(it.get("ratingText") or "").strip()
                if rt:
                    try:
                        rating = float(rt)
                    except Exception:
                        rating = None

                option = str(it.get("option") or "").strip()
                body = str(it.get("body") or "").strip()
                body = re.sub(r"\n{3,}", "\n\n", body).strip()

                if not body:
                    continue

                if badge == "만족도 리뷰":
                    title = option or badge
                else:
                    title = badge

                collected.append(
                    {
                        "platform": "wadiz_qa",
                        "brand": "Wadiz",
                        "product_name": campaign_title,
                        "product_url": campaign_url,
                        "review_id": "",
                        "review_date": review_date,
                        "rating": rating,
                        "author": author,
                        "title": title,
                        "body": body,
                        "source_url": page_url,
                        "page_kind": page_kind,
                        "badge": badge,
                        "option": option,
                        "ratingText": rt,
                    }
                )

        except Exception as e:
            err = str(e)
            errors.append({"project_no": project_no, "page": page_kind, "url": page_url, "error": err})
            if client is not None and cfg.log_errors_to_sheet:
                try:
                    log_errors(
                        client=client,
                        tab="errors_reviews",
                        run_id="collect_wadiz_qa",
                        stage=page_kind,
                        items=[{"brand": "Wadiz", "platform": "wadiz_qa", "url": page_url, "status": "error", "error": err}],
                    )
                except Exception:
                    pass
            continue

    # Dedup + append rows (best-effort)
    appended = 0
    dedup_added = 0
    out_rows: List[List[object]] = []
    new_keys: List[str] = []

    now = kst_now()
    collected_date = now.date().isoformat()
    collected_at = now.isoformat()

    for r in collected:
        platform = str(r.get("platform") or "wadiz_qa")
        brand = str(r.get("brand") or "")
        product_name = str(r.get("product_name") or "")
        product_url = str(r.get("product_url") or "")
        source_url = str(r.get("source_url") or product_url)

        review_id = str(r.get("review_id") or "")
        review_date = str(r.get("review_date") or "")
        author = str(r.get("author") or "")
        title = str(r.get("title") or "")
        body = str(r.get("body") or "")

        rating = r.get("rating")
        if rating is None:
            rating_cell: object = ""
        else:
            try:
                rating_cell = float(rating)
            except Exception:
                rating_cell = str(rating)

        body_hash = sha256(body)
        dedup_key = sha256("|".join([platform, product_url, author, review_date, body_hash]))
        if dedup_key in seen_keys:
            continue

        seen_keys.add(dedup_key)
        new_keys.append(dedup_key)

        out_rows.append(
            [
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
            ]
        )

    if out_rows and not cfg.dry_run:
        sink = load_sink_config()
        tab = sink.get("tab") or "시트1"
        if client is None:
            client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])

        client.append_fixed(
            tab=tab,
            start_row=3,
            start_col="A",
            end_col="O",
            values_2d=out_rows,
            sentinel_col="A",
            sentinel_regex=r"^\d{4}-\d{2}-\d{2}$",
            scan_max_rows=20000,
        )
        appended = len(out_rows)

    if new_keys and not cfg.dry_run:
        TextSet(cfg.dedup_path).add_many(new_keys)
        dedup_added = len(new_keys)

    # Write JSON artifact
    ts = now.strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(cfg.out_dir, f"wadiz_{project_no}_{ts}.json")
    payload = {
        "platform": "wadiz_qa",
        "project_no": project_no,
        "campaign_url": campaign_url,
        "qa_root_url": wadiz_qa_root_url(project_no),
        "campaign_title": campaign_title,
        "collected_at": collected_at,
        "pages": pages,
        "items_collected": len(collected),
        "rows_appended": appended,
        "dedup_added": dedup_added,
        "errors": errors,
        "items": collected,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    return {
        "project_no": project_no,
        "campaign_title": campaign_title,
        "out": out_path,
        "items_collected": len(collected),
        "rows_appended": appended,
        "dedup_added": dedup_added,
        "errors": errors,
    }


def _read_urls_file(path: str) -> List[str]:
    if not path or not os.path.exists(path):
        return []
    out: List[str] = []
    for ln in open(path, "r", encoding="utf-8"):
        s = ln.strip()
        if not s or s.startswith("#"):
            continue
        out.append(s)
    return out


def main(argv: Optional[Sequence[str]] = None) -> int:
    import argparse

    ap = argparse.ArgumentParser(description="Collect Wadiz campaign QA/community pages (public) and ingest into reviews_raw")
    ap.add_argument("--url", action="append", help="Wadiz QA page URL (repeatable). e.g. https://www.wadiz.kr/web/campaign/detail/qa/160013")
    ap.add_argument("--urls-file", default=DEFAULT_URLS_PATH, help=f"Path to newline-separated URLs (default: {DEFAULT_URLS_PATH})")

    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR)
    ap.add_argument("--dedup-path", default=None, help="Dedup state file (default: from sink config)")
    ap.add_argument("--browser-profile", default="openclaw")

    # IMPORTANT: By default, restrict ingestion to "yesterday" (KST) based on review_date.
    # This prevents re-ingesting large batches of old historical comments that trigger noisy alerts.
    # Override by passing --min-review-date "" (empty) is not supported by argparse; pass a very old date instead.
    default_min_review_date = os.environ.get("REVIEW_HUB_MIN_REVIEW_DATE")
    if not default_min_review_date:
        try:
            default_min_review_date = (kst_now().date() - dt.timedelta(days=1)).isoformat()
        except Exception:
            default_min_review_date = None
    ap.add_argument(
        "--min-review-date",
        default=default_min_review_date,
        help="Only ingest items whose review_date >= this YYYY-MM-DD (default: yesterday KST).",
    )

    ap.add_argument("--no-satisfaction", action="store_true", help="Skip satisfaction reviews")
    ap.add_argument("--no-comment", action="store_true", help="Skip cheer/opinion/experience comments")
    ap.add_argument("--include-signature", action="store_true", help="Include signature items (usually no text)")

    ap.add_argument("--max-scrolls", type=int, default=30)
    ap.add_argument("--stable-iters", type=int, default=3)
    ap.add_argument("--scroll-wait-ms", type=int, default=1200)
    ap.add_argument("--max-items-per-page", type=int, default=400)

    ap.add_argument("--dry-run", action="store_true", help="Collect and write JSON, but do not append to Sheets or update dedup state")
    ap.add_argument("--no-sheet-error-log", action="store_true", help="Disable writing errors to errors_reviews tab")

    args = ap.parse_args(list(argv) if argv is not None else None)

    sink = load_sink_config()
    dedup_path = args.dedup_path or os.path.join(WORKSPACE_ROOT, sink["dedup"]["localStatePath"])

    urls = []
    urls.extend([u for u in (args.url or []) if u and u.strip()])
    urls.extend(_read_urls_file(args.urls_file))

    # Dedup input URLs, preserve order
    seen_u: Set[str] = set()
    urls2: List[str] = []
    for u in urls:
        u = u.strip()
        if not u or u in seen_u:
            continue
        seen_u.add(u)
        urls2.append(u)

    if not urls2:
        ap.error("Provide at least one --url or a non-empty --urls-file")

    project_nos: List[str] = []
    for u in urls2:
        pn = _parse_wadiz_project_no(u)
        if pn:
            project_nos.append(pn)

    # Dedup project numbers
    project_nos = list(dict.fromkeys(project_nos))
    if not project_nos:
        raise SystemExit("No projectNo could be parsed from URLs")

    min_review_date = None
    try:
        s = str(args.min_review_date or "").strip()
        if s:
            min_review_date = dt.date.fromisoformat(s[:10])
    except Exception:
        min_review_date = None

    cfg = WadizCollectorConfig(
        out_dir=str(args.out_dir),
        dedup_path=dedup_path,
        browser_profile=str(args.browser_profile),
        min_review_date=min_review_date,
        include_satisfaction=not bool(args.no_satisfaction),
        include_comment=not bool(args.no_comment),
        include_signature=bool(args.include_signature),
        max_scrolls=int(args.max_scrolls),
        stable_scroll_iters=int(args.stable_iters),
        scroll_wait_ms=int(args.scroll_wait_ms),
        max_items_per_page=int(args.max_items_per_page),
        dry_run=bool(args.dry_run),
        log_errors_to_sheet=not bool(args.no_sheet_error_log),
    )

    # Shared dedup state for this run (strict)
    with file_lock(cfg.dedup_path + ".lock"):
        seen_keys = TextSet(cfg.dedup_path).load()

    client: Optional[GogSheetsClient]
    if cfg.dry_run:
        client = None
    else:
        client = GogSheetsClient(account=sink["account"], spreadsheet_id=sink["sheetId"])

    results: List[dict] = []
    with file_lock(DEFAULT_COLLECTOR_LOCK):
        for pn in project_nos:
            results.append(collect_for_project_no(pn, cfg=cfg, seen_keys=seen_keys, client=client))

    # Local summary log
    os.makedirs(os.path.join(WORKSPACE_ROOT, "logs/review-hub"), exist_ok=True)
    summary_path = os.path.join(WORKSPACE_ROOT, "logs/review-hub/collect-wadiz-qa.json")
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump({"when": kst_now().isoformat(), "projects": project_nos, "results": results}, f, ensure_ascii=False, indent=2)

    # Print machine-readable JSON
    total_appended = sum(int(r.get("rows_appended") or 0) for r in results)
    total_errors = sum(len(r.get("errors") or []) for r in results)
    print(json.dumps({"ok": total_errors == 0, "projects": len(project_nos), "rows_appended": total_appended, "errors": total_errors, "outputs": [r.get("out") for r in results]}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
