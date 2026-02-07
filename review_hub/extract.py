from __future__ import annotations

import re
from typing import Iterable, List, Set, Tuple
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup


def _norm_url(u: str) -> str:
    # drop fragments
    return u.split("#", 1)[0]


def extract_links(base_url: str, html: str) -> Set[str]:
    soup = BeautifulSoup(html, "lxml")
    out: Set[str] = set()
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        href = href.strip()
        if href.startswith("javascript:") or href.startswith("mailto:"):
            continue
        absu = urljoin(base_url, href)
        absu = _norm_url(absu)
        out.add(absu)
    return out


def classify_product_links(platform: str, links: Iterable[str]) -> List[str]:
    # Heuristic patterns (best-effort). We will refine per platform as we learn.
    patterns: List[Tuple[str, re.Pattern]] = []

    if platform in {"imweb"}:
        patterns += [
            ("imweb_shop_view", re.compile(r"/shop_view\b", re.I)),
            ("imweb_idx", re.compile(r"[?&]idx=\d+", re.I)),
        ]
    if platform in {"smartstore", "naver_brand"}:
        patterns += [
            ("smartstore_products", re.compile(r"smartstore\.naver\.com/.+/products/\d+", re.I)),
            ("brand_products", re.compile(r"brand\.naver\.com/.+/products/\d+", re.I)),
        ]
    if platform in {"ohou"}:
        patterns += [
            ("ohou_productions", re.compile(r"store\.ohou\.se/.+/productions/\d+", re.I)),
            ("ohou_productions2", re.compile(r"ohou\.se/.*/productions/\d+", re.I)),
        ]
    if platform.startswith("coupang"):
        patterns += [
            ("coupang_vp", re.compile(r"coupang\.com/vp/products/\d+", re.I)),
            ("coupang_product", re.compile(r"productId=\d+", re.I)),
        ]
    if platform.startswith("wadiz"):
        # already a specific page; no product discovery
        return []

    scored = []
    for u in links:
        for _, p in patterns:
            if p.search(u):
                scored.append(u)
                break

    # Dedup while preserving order
    seen = set()
    out = []
    for u in scored:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out
