from __future__ import annotations

import json
import os

from review_hub.sheets_admin import ensure_tabs_exist
from review_hub.sheets_client import GogSheetsClient

WORKSPACE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def load_sink_config() -> dict:
    path = os.path.join(WORKSPACE_ROOT, "config/review-hub/google-sheets.sink.json")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main() -> None:
    sink = load_sink_config()
    account = sink["account"]
    sheet_id = sink["sheetId"]

    # Ensure tab exists
    creds_path = os.path.join(os.path.expanduser("~"), "Library", "Application Support", "gogcli", "credentials.json")
    ensure_tabs_exist(
        spreadsheet_id=sheet_id,
        account_email=account,
        titles=["looker_reviews"],
        credentials_path=creds_path,
    )

    client = GogSheetsClient(account=account, spreadsheet_id=sheet_id)

    # One-cell ARRAYFORMULA that mirrors raw rows and adds a few normalized fields for Looker Studio.
    # Columns A:O are identical to raw (시트1). Additional columns:
    # P: review_date_norm (best-effort), Q: rating_num, R: body_len
    formula = (
        "=ARRAYFORMULA({"
        "{\"collected_date\",\"collected_at\",\"brand\",\"platform\",\"product_name\",\"product_url\",\"review_id\",\"review_date\",\"rating\",\"author\",\"title\",\"body\",\"body_hash\",\"dedup_key\",\"source_url\",\"review_date_norm\",\"rating_num\",\"body_len\"};"
        "IF(시트1!A3:A=\"\",,"
        "{"
        "시트1!A3:O,"
        "IFERROR(DATEVALUE(REGEXREPLACE(시트1!H3:H,\"\\.\",\"-\")),시트1!A3:A),"
        "IFERROR(VALUE(시트1!I3:I),),"
        "LEN(시트1!L3:L)"
        "}"
        ")"
        "})"
    )

    client.update("looker_reviews!A1", [[formula]])


if __name__ == "__main__":
    main()
