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

    # Pull a sufficiently large range; Sheets will return only the populated values.
    values = gog_get_values(account=account, sheet_id=sheet_id, a1_range="looker_reviews!A1:R5000")
    if not values:
        raise SystemExit("No data returned from looker_reviews")

    headers = values[0]
    rows = values[1:]

    out_rows: list[dict[str, Any]] = []
    for r in rows:
        obj: dict[str, Any] = {}
        for i, h in enumerate(headers):
            obj[h] = (r[i] if i < len(r) else "")

        # light typing
        obj["rating_num"] = to_float(obj.get("rating_num"))
        obj["body_len"] = to_int(obj.get("body_len"))

        # Keep strings as-is; report UI will parse/format.
        out_rows.append(obj)

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
