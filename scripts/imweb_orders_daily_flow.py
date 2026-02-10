#!/usr/bin/env python3
"""Imweb orders XLSX -> Clean -> Pivot -> Google Sheets.

Usage examples:
  python3 scripts/imweb_orders_daily_flow.py --input ~/Downloads/기본_양식_20260210163212.xlsx
  python3 scripts/imweb_orders_daily_flow.py --from-downloads --since-minutes 30

This script:
- Loads the XLSX (Imweb export '기본 양식')
- Cleans rows:
  - drop rows where 취소사유 not empty
  - drop rows where 반품사유 not empty
  - drop rows where 주문상태 == 결제대기 (best-effort automation)
- Creates pivot:
  Rows: 주문일 -> 상품명 -> 옵션명
  Values: 구매수량 SUM, 품목실결제가 SUM
  Adds:
    - 실판매수량 (옵션명 contains '1+1' => 구매수량*2)
    - 객단가 = 품목실결제가 / 실판매수량
- Appends cleaned rows to Google Sheet tab A
- Clears & rewrites pivot tab B

Requires:
- gog CLI authenticated (Sheets scope)
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from openpyxl import load_workbook

try:
    from zoneinfo import ZoneInfo
except Exception:  # pragma: no cover
    ZoneInfo = None


DEFAULT_SHEET_ID = "1eSNPcGuOEdj4Lj-2tCQcdcFR-C1Z7sIS2ULbhezGtTo"
DEFAULT_TAB_RAW = "올리_아임웹_주문원본"
DEFAULT_TAB_PIVOT = "올리_아임웹_피벗집계"


@dataclass
class RunStats:
    input_path: str
    rows_total: int
    rows_cancel_dropped: int
    rows_return_dropped: int
    rows_pending_dropped: int
    rows_after_clean: int
    pivot_rows: int


def sh(cmd: list[str], *, text=True) -> str:
    p = subprocess.run(cmd, text=text, capture_output=True)
    if p.returncode != 0:
        raise RuntimeError(f"command failed: {' '.join(cmd)}\nstdout: {p.stdout}\nstderr: {p.stderr}")
    return p.stdout


def pick_latest_xlsx(downloads_dir: Path, since_minutes: int, prefix: str = "기본_양식_") -> Path:
    since_ts = datetime.now().timestamp() - since_minutes * 60
    cand = []
    for p in downloads_dir.glob(f"{prefix}*.xlsx"):
        try:
            st = p.stat()
        except FileNotFoundError:
            continue
        if st.st_mtime >= since_ts:
            cand.append((st.st_mtime, p))
    if not cand:
        raise FileNotFoundError(
            f"No recent XLSX found in {downloads_dir} within last {since_minutes} minutes matching {prefix}*.xlsx"
        )
    cand.sort(reverse=True)
    return cand[0][1]


def normalize_str(x) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    return "" if s.lower() in {"nan", "none"} else s


def to_number(x) -> float:
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = normalize_str(x)
    if not s:
        return 0.0
    # remove commas
    s = s.replace(",", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def load_xlsx_rows(path: Path) -> tuple[list[str], list[dict]]:
    wb = load_workbook(path, data_only=True, read_only=True)
    ws = wb.worksheets[0]

    rows_iter = ws.iter_rows(values_only=True)
    try:
        header_row = next(rows_iter)
    except StopIteration:
        raise ValueError("Empty xlsx")

    headers = [normalize_str(h) for h in header_row]

    data: list[dict] = []
    for row in rows_iter:
        if row is None:
            continue
        if all(v is None or normalize_str(v) == "" for v in row):
            continue
        d = {}
        for i, h in enumerate(headers):
            if not h:
                continue
            if i < len(row):
                d[h] = row[i]
        data.append(d)

    return headers, data


def clean_orders(rows: list[dict]) -> tuple[list[dict], dict]:
    total = len(rows)

    def is_nonempty(r: dict, col: str) -> bool:
        return normalize_str(r.get(col)) != ""

    cancel = 0
    ret = 0
    pending = 0
    out: list[dict] = []

    for r in rows:
        drop = False
        if is_nonempty(r, "취소사유"):
            cancel += 1
            drop = True
        if is_nonempty(r, "반품사유"):
            ret += 1
            drop = True
        if normalize_str(r.get("주문상태")) == "결제대기":
            pending += 1
            drop = True
        if not drop:
            out.append(r)

    stats = {
        "rows_total": total,
        "rows_cancel_dropped": cancel,
        "rows_return_dropped": ret,
        "rows_pending_dropped": pending,
        "rows_after_clean": len(out),
    }
    return out, stats


def build_pivot(rows: list[dict]) -> list[dict]:
    required = ["주문일", "상품명", "옵션명", "구매수량", "품목실결제가"]
    for c in required:
        if not any(c in r for r in rows):
            raise KeyError(f"Missing required column for pivot: {c}")

    from collections import defaultdict

    agg = defaultdict(lambda: {"구매수량_SUM": 0.0, "품목실결제가_SUM": 0.0})

    for r in rows:
        key = (normalize_str(r.get("주문일")), normalize_str(r.get("상품명")), normalize_str(r.get("옵션명")))
        agg[key]["구매수량_SUM"] += to_number(r.get("구매수량"))
        agg[key]["품목실결제가_SUM"] += to_number(r.get("품목실결제가"))

    out = []
    for (od, prod, opt), v in agg.items():
        qty = float(v["구매수량_SUM"])
        pay = float(v["품목실결제가_SUM"])
        sale_qty = qty * (2.0 if "1+1" in opt else 1.0)
        aov = (pay / sale_qty) if sale_qty else 0.0
        out.append(
            {
                "주문일": od,
                "상품명": prod,
                "옵션명": opt,
                "구매수량_SUM": qty,
                "품목실결제가_SUM": pay,
                "실판매수량": sale_qty,
                "객단가": aov,
            }
        )

    # deterministic sort
    out.sort(key=lambda r: (r["주문일"], r["상품명"], r["옵션명"]))
    return out


def sheets_get_header(sheet_id: str, tab: str, account: str | None) -> list[str] | None:
    args = ["gog", "sheets", "get", sheet_id, f"{tab}!A1:ZZ1", "--json", "--no-input"]
    if account:
        args += ["--account", account]
    raw = sh(args)
    j = json.loads(raw)
    values = j.get("values") or []
    if not values:
        return None
    return [normalize_str(x) for x in values[0] if normalize_str(x) != ""]


def sheets_append(sheet_id: str, tab: str, rows: list[list], account: str | None):
    # Append to a wide range; Google ignores extra columns beyond provided.
    args = [
        "gog",
        "sheets",
        "append",
        sheet_id,
        f"{tab}!A:ZZ",
        "--values-json",
        json.dumps(rows, ensure_ascii=False),
        "--insert",
        "INSERT_ROWS",
        "--no-input",
    ]
    if account:
        args += ["--account", account]
    sh(args)


def sheets_clear(sheet_id: str, tab: str, account: str | None):
    args = ["gog", "sheets", "clear", sheet_id, f"{tab}!A:ZZ", "--no-input"]
    if account:
        args += ["--account", account]
    sh(args)


def sheets_update(sheet_id: str, tab: str, start_cell: str, rows: list[list], account: str | None):
    args = [
        "gog",
        "sheets",
        "update",
        sheet_id,
        f"{tab}!{start_cell}",
        "--values-json",
        json.dumps(rows, ensure_ascii=False),
        "--input",
        "USER_ENTERED",
        "--no-input",
    ]
    if account:
        args += ["--account", account]
    sh(args)


def chunked(seq: list[list], n: int):
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def rows_to_matrix(rows: list[dict], cols: list[str]) -> list[list]:
    out: list[list] = []
    for r in rows:
        out.append([r.get(c, "") for c in cols])
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", help="Path to imweb xlsx export")
    ap.add_argument("--from-downloads", action="store_true", help="Pick latest 기본_양식_*.xlsx from ~/Downloads")
    ap.add_argument("--downloads-dir", default=str(Path.home() / "Downloads"))
    ap.add_argument("--since-minutes", type=int, default=60)

    ap.add_argument("--sheet-id", default=DEFAULT_SHEET_ID)
    ap.add_argument("--tab-raw", default=DEFAULT_TAB_RAW)
    ap.add_argument("--tab-pivot", default=DEFAULT_TAB_PIVOT)
    ap.add_argument("--account", default=os.environ.get("GOG_ACCOUNT"))

    ap.add_argument("--pivot-mode", choices=["replace"], default="replace")
    ap.add_argument("--append-chunk", type=int, default=300)

    args = ap.parse_args()

    if not args.input and not args.from_downloads:
        ap.error("Either --input or --from-downloads must be provided")

    if args.from_downloads:
        input_path = pick_latest_xlsx(Path(args.downloads_dir), args.since_minutes)
    else:
        input_path = Path(args.input).expanduser()

    headers, rows_raw = load_xlsx_rows(input_path)
    rows_clean, s = clean_orders(rows_raw)
    pivot_rows = build_pivot(rows_clean)

    # Append cleaned rows to RAW tab
    sheet_header = sheets_get_header(args.sheet_id, args.tab_raw, args.account)
    if sheet_header:
        cols = [c for c in sheet_header if c in headers]
        if not cols:
            cols = [c for c in headers if c]
    else:
        cols = [c for c in headers if c]
        sheets_update(args.sheet_id, args.tab_raw, "A1", [cols], args.account)

    matrix = rows_to_matrix(rows_clean, cols)
    for chunk in chunked(matrix, args.append_chunk):
        sheets_append(args.sheet_id, args.tab_raw, chunk, args.account)

    # Replace pivot tab
    if args.pivot_mode == "replace":
        sheets_clear(args.sheet_id, args.tab_pivot, args.account)
        pv_cols = [
            "주문일",
            "상품명",
            "옵션명",
            "구매수량_SUM",
            "품목실결제가_SUM",
            "실판매수량",
            "객단가",
        ]
        out_rows = [pv_cols] + rows_to_matrix(pivot_rows, pv_cols)
        row_cursor = 1
        for chunk in chunked(out_rows, 500):
            sheets_update(args.sheet_id, args.tab_pivot, f"A{row_cursor}", chunk, args.account)
            row_cursor += len(chunk)

    stats = RunStats(
        input_path=str(input_path),
        rows_total=s["rows_total"],
        rows_cancel_dropped=s["rows_cancel_dropped"],
        rows_return_dropped=s["rows_return_dropped"],
        rows_pending_dropped=s["rows_pending_dropped"],
        rows_after_clean=s["rows_after_clean"],
        pivot_rows=int(len(pivot_rows)),
    )

    log_dir = Path("logs/imweb-orders")
    log_dir.mkdir(parents=True, exist_ok=True)
    kst = ZoneInfo("Asia/Seoul") if ZoneInfo else timezone(timedelta(hours=9))
    ts_kst = datetime.now(tz=kst).strftime("%Y-%m-%d")
    log_path = log_dir / f"imweb-orders-{ts_kst}.json"
    with open(log_path, "w", encoding="utf-8") as f:
        json.dump(stats.__dict__, f, ensure_ascii=False, indent=2)

    print(
        json.dumps(
            {
                "status": "ok",
                "input": stats.input_path,
                "rows_total": stats.rows_total,
                "rows_after_clean": stats.rows_after_clean,
                "dropped": {
                    "cancel": stats.rows_cancel_dropped,
                    "return": stats.rows_return_dropped,
                    "pending": stats.rows_pending_dropped,
                },
                "pivot_rows": stats.pivot_rows,
                "sheet": args.sheet_id,
                "tab_raw": args.tab_raw,
                "tab_pivot": args.tab_pivot,
                "log": str(log_path),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"status": "error", "error": str(e)}, ensure_ascii=False))
        raise
