from __future__ import annotations

import random
import time
from dataclasses import dataclass
from typing import Optional

import requests


@dataclass
class FetchResult:
    url: str
    status_code: Optional[int]
    text: Optional[str]
    error: Optional[str] = None


def fetch_html(url: str, *, timeout: int = 25, sleep_range=(0.2, 0.8)) -> FetchResult:
    # polite jitter to reduce burstiness
    time.sleep(random.uniform(*sleep_range))

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en;q=0.8",
    }

    try:
        r = requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
        ct = r.headers.get("content-type", "")
        if "text/html" not in ct and "application/xhtml+xml" not in ct:
            # still keep text for debugging
            return FetchResult(url=r.url, status_code=r.status_code, text=r.text, error=f"non-html content-type: {ct}")
        return FetchResult(url=r.url, status_code=r.status_code, text=r.text)
    except Exception as e:
        return FetchResult(url=url, status_code=None, text=None, error=str(e))
