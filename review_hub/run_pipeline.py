from __future__ import annotations

import json
import os
import subprocess
from typing import Any, Dict

from review_hub.sheets_admin import ensure_tabs_exist

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def load_sink() -> Dict[str, Any]:
    import json as _json

    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return _json.load(f)


def run_cmd_json(cmd: list[str]) -> Dict[str, Any]:
    out = subprocess.check_output(cmd, cwd=WORKSPACE_ROOT).decode("utf-8")
    return json.loads(out)


def main(max_products: int = 50) -> None:
    sink = load_sink()
    account = sink["account"]
    sheet_id = sink["sheetId"]

    # Simple lock to prevent overlapping runs when scheduled frequently.
    lock_path = os.path.join(WORKSPACE_ROOT, "state/review-hub/pipeline.lock")
    os.makedirs(os.path.dirname(lock_path), exist_ok=True)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode("utf-8"))
        os.close(fd)
    except FileExistsError:
        # Another run is in progress; exit cleanly.
        print(json.dumps({"ok": True, "skipped": "locked"}, ensure_ascii=False))
        return

    # Ensure error tabs exist (user-requested).
    creds_path = os.path.expanduser("~/Library/Application Support/gogcli/credentials.json")
    ensure_tabs_exist(
        spreadsheet_id=sheet_id,
        account_email=account,
        titles=["errors_discovery", "errors_reviews"],
        credentials_path=creds_path,
    )

    try:
        # 1) Discovery
        discovery = run_cmd_json(["python3", "-m", "review_hub.run_daily"])

        # 2) Reviews (imweb only for now)
        reviews = run_cmd_json(["python3", "-m", "review_hub.collect_reviews", "--max-products", str(max_products)])

        summary = {
            "discovery": discovery,
            "reviews": reviews,
        }

        # local artifact (for monitoring)
        os.makedirs(os.path.join(WORKSPACE_ROOT, "logs/review-hub"), exist_ok=True)
        kst_stamp = subprocess.check_output(["date", "+%Y%m%d_%H%M%S"]).decode("utf-8").strip()
        out_path = os.path.join(WORKSPACE_ROOT, f"logs/review-hub/pipeline_{kst_stamp}.json")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        print(json.dumps({"ok": True, "log": out_path, **summary}, ensure_ascii=False))
    finally:
        try:
            os.remove(lock_path)
        except Exception:
            pass


if __name__ == "__main__":
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--max-products", type=int, default=50)
    args = ap.parse_args()
    main(max_products=args.max_products)
