#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import subprocess
from datetime import datetime
from typing import Any

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def load_sink() -> dict[str, Any]:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def gog_get_values(*, account: str, sheet_id: str, a1_range: str) -> list[list[str]]:
    out = subprocess.check_output(
        [
            "gog",
            "sheets",
            "get",
            sheet_id,
            a1_range,
            "--json",
            "--no-input",
            "--account",
            account,
        ],
        cwd=WORKSPACE_ROOT,
        text=True,
    )
    data = json.loads(out)
    return data.get("values") or []


def to_int(x: Any) -> int | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def to_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except Exception:
        return None


def main() -> None:
    sink = load_sink()
    account = sink["account"]
    sheet_id = sink["sheetId"]

    # Pull looker_reviews in pages to avoid the old hard cap (R5000).
    # Sheets will return only populated values within the requested range.
    # Prefer large pages to reduce Google API read quota usage.
    # NOTE: Very large ranges can exceed Google API response limits (especially with 18 columns).
    # Use a conservative default chunk to avoid silent truncation.
    chunk = int(os.environ.get("REVIEW_HUB_EXPORT_CHUNK") or "5000")
    chunk = max(500, min(chunk, 10000))
    max_rows = int(os.environ.get("REVIEW_HUB_EXPORT_MAX_ROWS") or "200000")

    headers: list[str] | None = None
    out_rows: list[dict[str, Any]] = []

    # We page the sheet data by row ranges.
    # IMPORTANT: Row 1 is headers. For the first page, request (chunk + 1) rows
    # so we still get `chunk` data rows.
    start_row = 1
    while start_row <= max_rows:
        end_row = min(max_rows, start_row + chunk - 1)

        # If this is the first page (includes headers), expand by 1.
        if start_row == 1:
            end_row = min(max_rows, end_row + 1)

        a1 = f"looker_reviews!A{start_row}:R{end_row}"
        values = gog_get_values(account=account, sheet_id=sheet_id, a1_range=a1)

        if not values:
            break

        # First page includes the header row.
        if start_row == 1:
            headers = [str(h or "") for h in (values[0] or [])]
            data_rows = values[1:]
        else:
            if headers is None:
                raise SystemExit("Internal error: headers missing")
            data_rows = values

        # If the page returns no data rows, we're done.
        if not data_rows:
            break

        for r in data_rows:
            # Skip completely empty rows (can appear from formulas).
            if not any(str(x or "").strip() for x in r):
                continue

            obj: dict[str, Any] = {}
            for i, h in enumerate(headers):
                obj[h] = (r[i] if i < len(r) else "")

            # light typing
            obj["rating_num"] = to_float(obj.get("rating_num"))
            obj["body_len"] = to_int(obj.get("body_len"))

            out_rows.append(obj)

        # If we got fewer than a full page, we likely reached the end.
        got = len(values)
        expected = (chunk + 1) if start_row == 1 else chunk
        if got < expected:
            break

        start_row += chunk

    if headers is None:
        raise SystemExit("No data returned from looker_reviews")

    generated_at = datetime.now().astimezone().isoformat()
    payload = {
        "generated_at": generated_at,
        "sheet_id": sheet_id,
        "count": len(out_rows),
        "rows": out_rows,
    }

    out_dir = os.path.join(WORKSPACE_ROOT, "data")
    os.makedirs(out_dir, exist_ok=True)
    out_path = os.path.join(out_dir, "reviews.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)

    print(out_path)


if __name__ == "__main__":
    main()
