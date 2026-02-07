from __future__ import annotations

import hashlib
import json
import re
from typing import Dict, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from bs4 import BeautifulSoup

from review_hub.fetch import fetch_html


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def _extract_jsonld_reviews(soup: BeautifulSoup) -> List[dict]:
    """Best-effort extraction of Review objects embedded as JSON-LD."""
    reviews: List[dict] = []
    for script in soup.find_all("script", attrs={"type": re.compile(r"ld\+json", re.I)}):
        txt = script.string
        if not txt:
            continue
        txt = txt.strip()
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except Exception:
            continue

        # data can be dict or list
        stack = [data]
        while stack:
            cur = stack.pop()
            if isinstance(cur, dict):
                t = cur.get("@type")
                if t == "Review":
                    reviews.append(cur)
                else:
                    for v in cur.values():
                        if isinstance(v, (dict, list)):
                            stack.append(v)
            elif isinstance(cur, list):
                for v in cur:
                    if isinstance(v, (dict, list)):
                        stack.append(v)

    return reviews


def parse_imweb_product_page(html: str, page_url: str) -> Tuple[str, List[Dict[str, object]]]:
    """Parse an Imweb product page (when reviews are embedded as JSON-LD).

    Note: many Imweb stores load reviews dynamically; in that case this returns 0 reviews.
    """
    soup = BeautifulSoup(html, "lxml")

    title = None
    og = soup.find("meta", attrs={"property": "og:title"})
    if og and og.get("content"):
        title = og.get("content").strip()
    if not title and soup.title and soup.title.text:
        title = soup.title.text.strip()
    if not title:
        title = page_url

    reviews_ld = _extract_jsonld_reviews(soup)

    out: List[Dict[str, object]] = []
    for r in reviews_ld:
        body = (r.get("reviewBody") or "").strip()
        if not body:
            continue

        author = r.get("author")
        author_name = None
        if isinstance(author, dict):
            author_name = author.get("name")
        if not author_name:
            author_name = ""

        rating = None
        rr = r.get("reviewRating")
        if isinstance(rr, dict):
            rating = rr.get("ratingValue")
        try:
            rating = float(rating) if rating is not None else None
        except Exception:
            rating = None

        review_date = r.get("datePublished") or ""

        body_hash = _sha256(body)
        out.append(
            {
                "product_name": title,
                "review_id": r.get("@id") or "",
                "review_date": review_date,
                "rating": rating,
                "author": author_name,
                "title": "",
                "body": body,
                "body_hash": body_hash,
                "source_url": page_url,
            }
        )

    return title, out


def parse_imweb_interlock_review_page(html: str, page_url: str) -> Tuple[str, List[Dict[str, object]]]:
    """Parse an Imweb 'interlock=shop_review' board view page.

    These pages often contain the review text in the static HTML.
    """
    soup = BeautifulSoup(html, "lxml")

    # Product name + review body are commonly in .board_txt_area as lines.
    txt_area = soup.select_one(".board_txt_area")
    lines: List[str] = []
    if txt_area:
        lines = [ln.strip() for ln in txt_area.get_text("\n", strip=True).splitlines() if ln.strip()]

    product_name = lines[0] if lines else ""
    body = "\n".join(lines[1:]).strip() if len(lines) >= 2 else ""

    title_el = soup.select_one(".view_tit")
    title = title_el.get_text(" ", strip=True) if title_el else ""

    # author often: "ji23**** 전체 구매평 5시간전" -> take first token
    author_el = soup.select_one(".author")
    author_raw = author_el.get_text(" ", strip=True) if author_el else ""
    author = (author_raw.split() or [""])[0]

    date_el = soup.select_one(".date")
    review_date = date_el.get_text(" ", strip=True) if date_el else ""

    # rating: count active stars
    star_wrap = soup.select_one(".interlock_star_point")
    rating = None
    if star_wrap:
        active = star_wrap.select(".bt-star.active")
        if active:
            rating = float(len(active))

    # attempt to use query idx as id
    q = parse_qs(urlparse(page_url).query)
    review_id = (q.get("idx") or [""])[0]

    # Fall back if body missing
    if not body:
        body = title

    body_hash = _sha256(body or "")

    out = [
        {
            "product_name": product_name,
            "review_id": review_id,
            "review_date": review_date,
            "rating": rating,
            "author": author,
            "title": title,
            "body": body,
            "body_hash": body_hash,
            "source_url": page_url,
        }
    ] if (body or title) else []

    return product_name or page_url, out


def collect_imweb_reviews(product_url: str) -> Tuple[Optional[str], List[Dict[str, object]], Optional[str]]:
    fr = fetch_html(product_url)
    if fr.text is None or fr.status_code is None or fr.status_code >= 400:
        return None, [], fr.error or f"http_{fr.status_code}"

    try:
        q = parse_qs(urlparse(fr.url).query)
        is_interlock = (q.get("interlock") or [""])[0] == "shop_review"
        if is_interlock:
            product_name, reviews = parse_imweb_interlock_review_page(fr.text, fr.url)
        else:
            product_name, reviews = parse_imweb_product_page(fr.text, fr.url)
        return product_name, reviews, None
    except Exception as e:
        return None, [], f"parse_error: {e}"
